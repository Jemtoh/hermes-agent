"""Transport-only cron execution; output/delivery remain with the firing scheduler."""
import asyncio
import hashlib
import json
import uuid


class CronExecutionUnknown(RuntimeError):
    """The owner may have accepted this fire; never book it as failed or re-fire."""


def journal_path(job_id, request_id):
    from hermes_constants import get_hermes_home
    key = hashlib.sha256(json.dumps([job_id, request_id]).encode()).hexdigest()
    return get_hermes_home() / 'cron' / 'admissions' / (key + '.json')


def _pause_identity(paused):
    """The exact pause this recovery uncertainty caused, so only that pause is ever lifted."""
    return {'paused_at': paused.get('paused_at'), 'reason': paused.get('paused_reason')} if paused else True


def replayable_completions(job_id, completions):
    """The recorded completion ids whose admission journal still exists: only journal recovery
    can replay a completion into ``mark_job_run``, so an id without one is a dead tombstone."""
    return [eid for eid in completions or () if journal_path(job_id, eid).exists()]


def pause_for_unknown(job_id, execution_id, reason):
    """The firer pauses the job for this fire's unverified admission and records that exact pause
    on the fire's journal, so the receipt that later proves the run terminal can lift it."""
    from cron.jobs import pause_job
    from utils import atomic_json_write
    paused = pause_job(job_id, reason=reason)
    journal = journal_path(job_id, execution_id)
    if paused and journal.exists():
        record = json.loads(journal.read_text(encoding='utf-8-sig'))
        record['pause_recorded'] = _pause_identity(paused)
        atomic_json_write(journal, record, mode=0o600, fsync_dir=True)


def _lift_pause(job_id, pause):
    """A verified terminal receipt resolved the uncertainty that paused the schedule: resume it iff
    the job is still held by exactly that pause (an operator's own pause/resume is never touched)."""
    from cron.jobs import get_job, resume_job
    job = get_job(job_id) if isinstance(pause, dict) else None
    if (job is not None and job.get('state') == 'paused'
            and (job.get('paused_at'), job.get('paused_reason')) == (pause['paused_at'], pause['reason'])):
        try:
            resume_job(job_id)
        except ValueError:  # a one-shot whose instant has passed stays paused for the operator
            pass


def _lift_unstarted_pause(job_id, pause):
    """Clear EXACTLY the uncertainty pause an unstarted verdict resolved — decided and written in
    one locked read-modify-write, so a newer operator pause can never land between the two.

    Schedule-preserving by construction: no ``resume_job``, which recomputes ``next_run_at`` and
    would drop the queued run-now the refusal preserved (and refuses a past one-shot outright).

    Returns (the fence is what makes them distinguishable):

    * ``None``/``True`` — nothing owed, or the lift ran: the pause was not ours, or the job is
      gone, or it was cleared. All of these settle the uncertainty, so the receipt may go.
    * ``False`` — the fence was never acquired, so nothing was read and nothing written. That is
      RETRYABLE, not a no-op: the caller must keep its receipt and replay the lift.
    """
    if not isinstance(pause, dict):
        return None
    from cron.jobs import _activate_job_record, _under_fire_fence, _with_job, save_jobs

    def apply(jobs, _i, job):
        if (job.get('state') != 'paused'
                or (job.get('paused_at'), job.get('paused_reason')) != (pause['paused_at'], pause['reason'])):
            return False  # an operator's own later pause (or resume) is not ours to touch
        _activate_job_record(job)
        save_jobs(jobs)
        return True

    fenced = []

    def locked():
        fenced.append(True)  # provenance: the fence ran, so a False below is a decision, not a denial
        return _with_job(job_id, apply, False)

    _under_fire_fence(job_id, locked)
    if not fenced:
        return False
    return True


def run_canonical_job(job, *, extra_prompt=None, cancel_event=None, execution_id=None):
    from gateway.session_cron import owner_for_home, operation
    from hermes_cli.gateway_client import connect_gateway
    from hermes_constants import get_hermes_home
    from utils import atomic_json_write

    request_id = execution_id or job.get('execution_id')
    if not request_id:
        request_id = job['execution_id'] = uuid.uuid4().hex
    params = {'job_id': job['id'], 'request_id': request_id,
              'extra_prompt': extra_prompt}
    root = get_hermes_home() / 'cron' / 'admissions'
    journal = journal_path(params['job_id'], params['request_id'])
    record = {'params': params, 'receipt': None}
    from cron.consumer_run_lock import _consumer_name

    consumer = _consumer_name(job)
    if consumer:
        # The durable exclusion ACROSS owner loss: this fire's own OS hold is owned by our caller
        # right now, and this journal outlives us until the attempt is booked terminal.
        record['consumer'] = {'name': consumer, 'request_id': params['request_id']}
    if journal.exists():
        record = json.loads(journal.read_text(encoding='utf-8-sig'))
        if record['params'] != params:
            raise CronExecutionUnknown('cron admission identity conflict')
        if consumer:
            record['consumer'] = {'name': consumer, 'request_id': params['request_id']}
    attempted = journal.exists()
    owner = owner_for_home(get_hermes_home())

    def save():
        root.mkdir(parents=True, exist_ok=True)
        atomic_json_write(journal, record, mode=0o600, fsync_dir=True)

    async def observe(call):
        nonlocal attempted
        attempted = True
        save()
        try:
            receipt = await call('submit', params)
        except Exception as exc:
            from hermes_cli.gateway_client import GatewayRPCError
            # A returned owner exception is definite; a transport disconnect is not. Even an
            # owner error after commit must retain the admitted identity, so verify its absence.
            if owner is not None or isinstance(exc, GatewayRPCError):
                state = await call('recover', params)
                if state['status'] == 'missing':
                    error = f'{type(exc).__name__}: {exc}'
                    result = [False, f'# Cron Job: {job["id"]} (FAILED)\n\n{error}\n', '', error]
                    record['refusal'] = result
                    record['job'] = dict(job)
                    save()
                    return tuple(result)
                if state['status'] not in {'queued', 'started', 'terminal'}:
                    raise
                # Admitted before the answer was lost: observe it through the recovered receipt.
                receipt = {'session_id': state['session_id'], 'admission_id': state['admission_id']}
            else:
                raise
        record['receipt'] = receipt
        save()
        delay, progress = .1, None
        while True:
            if cancel_event is not None and cancel_event.is_set():
                await call('cancel', receipt)
            state = await call('status', receipt)
            if state.get('progress_at') not in (None, progress):
                # The owner's agent is still working: refresh OUR ledger row (the owner's stamp is
                # fenced to the row's owner), once per owner stamp, so the stale-claim sweep measures
                # the run's silence. A wedged owner stops stamping and the sweep still fires.
                from cron.executions import touch_execution_progress
                progress = state['progress_at']
                await asyncio.to_thread(touch_execution_progress, request_id)
            if state['status'] == 'terminal':
                job.update(state.get('job_flags') or {})
                result = tuple(state['result'])
                if job.get('_consumer_refusal'):
                    # The verdict is durable BEFORE this firer can die: recovery must settle the
                    # fire as unstarted, never save output or book a failure for it.
                    record['refusal'] = list(result)
                    record['unstarted'] = True
                    record['job'] = dict(state.get('job') or job)
                    save()
                return result

            if state['status'] == 'unknown':
                raise CronExecutionUnknown('unknown_execution: cron admission was not replayed')
            await asyncio.sleep(delay)
            delay = min(delay * 2, 2.0)

    async def remote():
        async with connect_gateway() as client:
            return await observe(lambda op, data: client.rpc('cron.' + op, **data))

    try:
        if owner is not None:
            authority, loop = owner
            try:
                current = asyncio.get_running_loop()
            except RuntimeError:
                current = None
            if current is loop:
                raise RuntimeError('cron synchronous execution must run off the owner event loop')
            return asyncio.run_coroutine_threadsafe(
                observe(lambda op, data: operation(authority, op, data)), loop).result()
        return asyncio.run(remote())
    except Exception as exc:
        if attempted:
            raise CronExecutionUnknown(f'Cron execution unverified; reconcile {journal}: {exc}') from exc
        error = f'{type(exc).__name__}: {exc}'
        return False, f'# Cron Job: {job["id"]} (FAILED)\n\n{error}\n', '', error

def _settle_recovered_unstarted(job, params, journal, pause=None) -> bool:
    """Book a DEAD firer's unstarted refusal once: ledger row, this fire's claims, then the receipt.

    The row is not this process's, so only the guarded recovered-settlement may write it; a live
    owner (or any refusal of that write) leaves the journal in place, which keeps the consumer's
    durable fence. Only after the row is settled are the fire's own claims retired — never a
    newer claim, because ``release_unstarted_fire`` matches the recorded owner — and only then is
    the recovery-owned pause lifted and the receipt dropped. Every step before that is replayable.
    False, with the journal retained, is returned when the pause lift's fence was unavailable.
    """
    import logging

    from cron.consumer_run_lock import release_unstarted_fire
    from cron.executions import _TERMINAL_STATES, get_execution, settle_recovered_unstarted

    job_id = params['job_id']
    execution_id = params['request_id']
    row = get_execution(execution_id)
    if row is not None and row['job_id'] != job_id:
        # A terminal row belonging to ANOTHER job never grants cleanup or deletion.
        logging.getLogger(__name__).warning(
            'Cron unstarted recovery: execution %s belongs to job %s, not %s',
            execution_id, row['job_id'], job_id)
        return False
    # ``unknown`` is RECOVERABLE, not settled: a given-up or dead-owner row is the one this
    # guarded recovered transition exists to resolve into the precise unstarted verdict.
    if not (row is not None and row['status'] in _TERMINAL_STATES and row['status'] != 'unknown'):
        if not settle_recovered_unstarted(execution_id, job_id, str(job['_consumer_refusal'])):
            return False
    claim = job.get('fire_claim')
    owner = str(claim.get('by') or '') if isinstance(claim, dict) else ''
    try:
        release_unstarted_fire(job_id, owner, expected_run_claim=job.get('run_claim'))
    except Exception:
        logging.getLogger(__name__).warning(
            'Cron unstarted recovery: claim cleanup failed for %s', job_id, exc_info=True)
        return False
    # The uncertainty that paused the schedule was this fire's own; the verdict just resolved it.
    # Lifting before the receipt goes keeps the job (and the queued intent) in one atomically
    # replayable state, and the locked identity guard still leaves an operator's pause alone.
    if _lift_unstarted_pause(job_id, pause) is False:
        # The fence never ran, so the automatic pause is still in place and this receipt is the only
        # record of whose it is. Dropping it would strand the pause with nothing left to lift it.
        # The row and this fire's claims are already settled, so the replay below is the lift alone.
        logging.getLogger(__name__).warning(
            'Cron unstarted recovery: pause lift fence unavailable for %s; receipt retained', job_id)
        return False
    journal.unlink(missing_ok=True)
    return True


def reconcile_pending(*, allow_connect=True):
    """Observe prepared fires; never submit missing or interrupted work.

    With allow_connect=False, a headless tick leaves durable receipts for the next
    live owner instead of ensuring or spawning a gateway.
    """
    from gateway.session_cron import owner_for_home, operation
    from hermes_constants import get_hermes_home
    from hermes_cli.gateway_client import connect_gateway
    from cron.executions import FIRER_FINISHED_STATES, execution_owner_live, get_execution
    from cron.jobs import get_job, pause_job
    from cron.scheduler import (_RunDelivery, _FireOwnership, _apply_agent_failure_marker,
                                _save_compose_deliver)
    from cron.scheduler_bookkeeping import fail_empty_response, finish_completed_run
    from utils import atomic_json_write
    import logging

    root = get_hermes_home() / 'cron' / 'admissions'
    for journal in sorted(root.glob('*.json')):
        try:
            record = json.loads(journal.read_text(encoding='utf-8-sig'))
            params = record['params']
            if journal != journal_path(params['job_id'], params['request_id']):
                raise ValueError('cron journal identity conflict')
            if execution_owner_live(params['request_id'], params['job_id']):
                # Its firer (this process included) still owns the row and its tail: re-saving the
                # output or re-marking the job on every tick would duplicate its side effects.
                continue
            row = get_execution(params['request_id'])
            if (row is not None and row['job_id'] == params['job_id']
                    and row['status'] in FIRER_FINISHED_STATES
                    and not record.get('unstarted')):
                # The firer booked this attempt's terminal outcome itself (ownership lost, shutdown
                # interrupt, a crashed tail): the receipt is not a result to save or deliver again.
                # An UNSTARTED verdict is exempt: its ledger row may be booked while the exact
                # claim cleanup that preserves the queued intent still has to be replayed.
                journal.unlink(missing_ok=True)
                continue
            if 'refusal' in record:
                state = {'status': 'terminal', 'result': record['refusal'], 'job': record['job']}
            else:
                owner = owner_for_home(get_hermes_home())
                if owner is None and not allow_connect:
                    continue
                async def observe():
                    if owner is not None:
                        return await operation(owner[0], 'recover', params)
                    async with connect_gateway() as client:
                        return await client.rpc('cron.recover', **params)
                if owner is not None:
                    state = asyncio.run_coroutine_threadsafe(observe(), owner[1]).result(timeout=20)
                else:
                    state = asyncio.run(observe())
            if state['status'] == 'missing':
                # The owner holds no admission for this fire: a definite non-admission, booked as
                # the same failed run the in-submit refusal books. Only ``unknown`` pauses.
                current = get_job(params['job_id'])
                if current is None:
                    journal.unlink(missing_ok=True)
                    continue
                error = 'Cron admission missing: the owner never admitted this fire'
                state = {'status': 'terminal', 'job': current,
                         'result': [False, f'# Cron Job: {params["job_id"]} (FAILED)\n\n{error}\n', '', error]}
            if state['status'] != 'terminal':
                if state['status'] == 'unknown' and not record.get('pause_recorded'):
                    reason = 'Canonical cron unknown; no automatic re-execution'
                    record['pause_recorded'] = _pause_identity(pause_job(params['job_id'], reason=reason))
                    atomic_json_write(journal, record, mode=0o600, fsync_dir=True)
                continue
            success, output, answer, error = state['result']
            job = dict(state['job'], **(state.get('job_flags') or {}))
            if record.get('unstarted') and not job.get('_consumer_refusal'):
                job['_consumer_refusal'] = str(error or '')
            if job.get('_consumer_refusal'):
                # A typed unstarted verdict: the fire computed nothing. Its journal is made
                # COMPLETE before any settlement write, so a settlement that dies half-way (or a
                # claim cleanup that fails once) is replayed by the next pass from disk instead of
                # being read as a firer-finished attempt. Then settle it through the DEAD firer's
                # ledger row and owner-fenced claims — no output, no notice, no incident, and the
                # finite dispatch / queued run-now the refusal preserved stay intact.
                record['refusal'] = [success, output, answer, error]
                record['unstarted'] = True
                record['job'] = dict(job)
                atomic_json_write(journal, record, mode=0o600, fsync_dir=True)
                _settle_recovered_unstarted(job, params, journal, record.get('pause_recorded'))
                continue
            job['execution_id'] = params['request_id']
            # The same result verdicts as the ordinary tail: an agent-declared [CRON_FAILURE] and a
            # blank answer are failures, whichever process books them.
            success, error, declared = _apply_agent_failure_marker(job, success, error, answer)
            delivery = _RunDelivery(job, success, error, agent_declared=declared)
            # The canonical receipt supplies the result; a dead scheduler's fire claim does not
            # authorize replay and must not prevent finishing its durable bookkeeping.
            fence = _FireOwnership(dict(job, fire_claim=None))
            # Exclusion must survive the owner's computation: its hold is kept until this row is
            # terminal, and this save JOINS that same fire (same execution id) before writing.
            from cron.consumer_run_lock import consumer_run_guard

            with consumer_run_guard(job, params['request_id'], settle=False) as may_save:
                if not may_save:
                    continue  # a live producer owns the consumer; the next tick retries
                _save_compose_deliver(delivery, fence, answer, output, adapters=None, loop=None,
                                     verbose=False, execution_token=None)
                fail_empty_response(delivery, answer)
                completed = finish_completed_run(delivery, None, params['request_id'], recovered=True)
            if completed:
                _lift_pause(params['job_id'], record.get('pause_recorded'))
        except Exception:
            logging.getLogger(__name__).warning('Cron receipt recovery deferred: %s', journal, exc_info=True)
