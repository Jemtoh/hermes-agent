"""Receipt recovery: a ``missing`` admission is a definite non-admission (booked failed, never a
pause), and a verified terminal receipt lifts the pause its own ``CronExecutionUnknown`` caused."""
from contextlib import asynccontextmanager
import json

from cron import executions, jobs, scheduler, scheduler_authority
from hermes_cli import gateway_client


def _peer(monkeypatch, answer):
    class Peer:
        async def rpc(self, method, **params):
            return answer(method)

    @asynccontextmanager
    async def connected():
        yield Peer()
    monkeypatch.setattr(gateway_client, 'connect_gateway', connected)


def _foreign_journal(job, fire):
    with executions._transaction() as conn:
        conn.execute('UPDATE executions SET process_id=?,pid=? WHERE id=?', ('old-owner', 999999, fire))
    journal = scheduler_authority.journal_path(job['id'], fire)
    journal.parent.mkdir(parents=True, exist_ok=True)
    journal.write_text(json.dumps({'params': {'job_id': job['id'], 'request_id': fire, 'extra_prompt': None},
                                   'receipt': None}))
    return journal


def test_missing_admission_is_booked_failed_not_paused(tmp_path, monkeypatch):
    """The firer died after writing its journal but before the owner admitted the fire."""
    _peer(monkeypatch, lambda method: {'status': 'missing', 'result': None})
    with jobs.use_cron_store(tmp_path / 'cron'):
        job = jobs.create_job(prompt='report', schedule='every 1h', deliver='local')
        fire = executions.create_execution(job['id'], source='test')['id']
        executions.mark_execution_running(fire)
        journal = _foreign_journal(job, fire)
        scheduler_authority.reconcile_pending()
        saved = jobs.get_job(job['id'])
        assert saved['state'] != 'paused' and saved['enabled'] is True
        assert saved['last_status'] == 'error'
        assert executions.get_execution(fire)['status'] == 'failed'
        assert not journal.exists()


def _unknown_fire(monkeypatch, job, fire, claim=None):
    """The firer journals the fire, then loses its observer: CronExecutionUnknown pauses the job."""
    journal = scheduler_authority.journal_path(job['id'], fire)

    def run_job(job_, **kwargs):
        journal.parent.mkdir(parents=True, exist_ok=True)
        journal.write_text(json.dumps({'params': {'job_id': job['id'], 'request_id': fire,
                                                  'extra_prompt': None}, 'receipt': None}))
        raise scheduler_authority.CronExecutionUnknown('Cron execution unverified: observer dropped')
    monkeypatch.setattr(scheduler, 'run_job', run_job)
    started = claim if claim is not None else jobs.get_job(job['id'])
    assert scheduler._run_one_job_body(dict(started, execution_id=fire)) is False
    assert jobs.get_job(job['id'])['state'] == 'paused'
    return journal


def _disconnected_fire(monkeypatch, job, fire, claim):
    """A claimed fire whose firer loses its observer AFTER admission: the same pause/give-up path,
    with a durable receipt for a still-open consumer claim."""
    return _unknown_fire(monkeypatch, job, fire, claim)


def test_recovered_refusal_lifts_only_the_pause_it_caused(tmp_path, monkeypatch):
    """A disconnect pauses the schedule; the later refusal resolves that uncertainty and the
    ORIGINAL queued request is runnable again with its prompt and slots untouched. A pause the
    operator took in between is never the receipt's to lift."""
    with jobs.use_cron_store(tmp_path / 'cron'):
        job = jobs.create_job(prompt='report', schedule='every 1h', deliver='local')
        scheduled = jobs.get_job(job['id'])['next_run_at']
        jobs.trigger_job(job['id'], extra_prompt='original request')
        claim = jobs.claim_job_for_fire(job['id'], manual=True, return_job=True)
        fire = executions.create_execution(job['id'], source='test')['id']
        journal = _disconnected_fire(monkeypatch, job, fire, claim)
        assert jobs.get_job(job['id'])['state'] == 'paused', 'the disconnect pauses the schedule'

        refusal = {'status': 'terminal', 'job': jobs.get_job(job['id']),
                   'result': [False, 'document', '', 'held; not started'],
                   'job_flags': {'_consumer_refusal': 'held; not started'}}
        _peer(monkeypatch, lambda method: refusal)
        scheduler_authority.reconcile_pending()

        saved = jobs.get_job(job['id'])
        assert (saved['state'], saved['enabled']) == ('scheduled', True)
        assert (saved['paused_at'], saved['paused_reason']) == (None, None)
        assert saved['manual_run_prompt'] == 'original request', 'the prompt is preserved'
        assert saved['next_run_at'] == saved['manual_run_at'], 'the run-now is due again'
        assert saved['manual_run_restore_at'] == scheduled, 'the slot was not recomputed'
        assert saved.get('fire_claim') is None, 'its own claim is retired'
        assert [j['id'] for j in jobs.get_due_jobs()] == [job['id']]
        settled = executions.get_execution(fire)
        assert settled['status'] == 'failed' and 'held; not started' in settled['error']
        assert not journal.exists()

        # An operator pause AFTER the automatic one is their intent, not the receipt's.
        jobs.trigger_job(job['id'], extra_prompt='second request')
        second_claim = jobs.claim_job_for_fire(job['id'], manual=True, return_job=True)
        second = executions.create_execution(job['id'], source='test')['id']
        second_journal = _disconnected_fire(monkeypatch, job, second, second_claim)
        jobs.pause_job(job['id'], reason='operator hold')
        operator_pause = jobs.get_job(job['id'])
        refusal = {'status': 'terminal', 'job': operator_pause,
                   'result': [False, 'document', '', 'held; not started'],
                   'job_flags': {'_consumer_refusal': 'held; not started'}}
        _peer(monkeypatch, lambda method: refusal)
        scheduler_authority.reconcile_pending()
        held = jobs.get_job(job['id'])
        assert (held['state'], held['paused_reason']) == ('paused', 'operator hold')
        assert held['enabled'] is False
        assert held.get('fire_claim') is None, 'settlement still retires its own claim'
        assert executions.get_execution(second)['status'] == 'failed'
        assert not second_journal.exists(), 'an operator pause is settled: the receipt is dropped'

        # And the operator's pause is all that is left: nothing replayable, nothing lifted.
        scheduler_authority.reconcile_pending()
        assert jobs.get_job(job['id'])['paused_reason'] == 'operator hold'


def test_unstarted_pause_lift_never_decides_from_an_out_of_lock_read(tmp_path, monkeypatch):
    """The identity check and the clearing write must be ONE locked read-modify-write.

    A newer operator pause lands in the gap between an out-of-lock read and the write it feeds,
    and that write then overwrites the operator's pause with the stale decision. Injecting exactly
    that pause on any ``get_job`` the lift makes is therefore a direct probe: the lift must make
    none (it decides inside the store's own mutation lock), and the automatic pause it DOES own is
    still cleared.
    """
    with jobs.use_cron_store(tmp_path / 'cron'):
        job = jobs.create_job(prompt='report', schedule='every 1h', deliver='local')
        jobs.trigger_job(job['id'], extra_prompt='original request')
        claim = jobs.claim_job_for_fire(job['id'], manual=True, return_job=True)
        fire = executions.create_execution(job['id'], source='test')['id']
        journal = _disconnected_fire(monkeypatch, job, fire, claim)

        real_get_job = jobs.get_job
        held = real_get_job(job['id'])
        racy = {'reads': 0}

        def get_job_that_races_the_window(job_id):
            record = real_get_job(job_id)
            if racy['reads'] == 0:
                racy['reads'] += 1
                jobs.pause_job(job_id, reason='operator hold')  # lands in the read-write gap
            return record

        refusal = {'status': 'terminal', 'job': held,
                   'result': [False, 'document', '', 'held; not started'],
                   'job_flags': {'_consumer_refusal': 'held; not started'}}
        monkeypatch.setattr(jobs, 'get_job', get_job_that_races_the_window)
        _peer(monkeypatch, lambda method: refusal)
        scheduler_authority.reconcile_pending()

        assert racy['reads'] == 0, 'the unstarted pause lift read the job outside the store lock'
        saved = real_get_job(job['id'])
        assert (saved['state'], saved['paused_at'], saved['paused_reason']) == ('scheduled', None, None)
        assert saved['next_run_at'] == saved['manual_run_at']
        assert not journal.exists()


def test_denied_pause_lift_fence_keeps_the_receipt_and_replays(tmp_path, monkeypatch):
    """A fence that could not be acquired lifted NOTHING, so the receipt must survive it.

    The ledger row and this fire's claims are already settled by then, which leaves the retained
    journal as the only record of WHOSE pause is still holding the schedule. Dropping it there
    disables the job for good; the next pass must clear exactly that pause, with the queued request
    and its slot intact.
    """
    with jobs.use_cron_store(tmp_path / 'cron'):
        job = jobs.create_job(prompt='report', schedule='every 1h', deliver='local')
        scheduled = jobs.get_job(job['id'])['next_run_at']
        jobs.trigger_job(job['id'], extra_prompt='original request')
        claim = jobs.claim_job_for_fire(job['id'], manual=True, return_job=True)
        # The instant the run-now was queued: the claim moved ``next_run_at`` back to the
        # scheduled slot, and an unstarted release must restore this stamp into it.
        assert isinstance(claim, dict), 'the run-now claim was refused'
        queued_at = claim['manual_run_at']
        fire = executions.create_execution(job['id'], source='test')['id']
        journal = _disconnected_fire(monkeypatch, job, fire, claim)
        paused = jobs.get_job(job['id'])
        assert paused['state'] == 'paused', 'the disconnect pauses the schedule'

        refusal = {'status': 'terminal', 'job': paused,
                   'result': [False, 'document', '', 'held; not started'],
                   'job_flags': {'_consumer_refusal': 'held; not started'}}
        _peer(monkeypatch, lambda method: refusal)

        real_fence = jobs._under_fire_fence
        denied = []

        def fence_that_denies_the_lift_once(job_id, fn):
            # The claim release runs under this fence first, so the lift is the call that arrives
            # with this fire's claim already retired. Denied, not raising: a lock timeout.
            if not denied and jobs.get_job(job_id).get('fire_claim') is None:
                denied.append(job_id)
                return False
            return real_fence(job_id, fn)

        monkeypatch.setattr(jobs, '_under_fire_fence', fence_that_denies_the_lift_once)
        scheduler_authority.reconcile_pending()

        assert denied == [job['id']], 'the test never denied the pause lift'
        assert journal.exists(), 'the receipt is the only record of the pause still owed'
        assert json.loads(journal.read_text())['pause_recorded'], 'the receipt still names the pause'
        held = jobs.get_job(job['id'])
        assert (held['paused_at'], held['paused_reason']) == (paused['paused_at'], paused['paused_reason'])
        assert held['next_run_at'] == queued_at, 'the run-now is queued again, not parked behind the slot'
        assert held['manual_run_prompt'] == 'original request'
        assert held['manual_run_restore_at'] == scheduled, 'the slot the trigger overwrote is kept'
        assert executions.get_execution(fire)['status'] == 'failed', 'the row settled before the lift'

        monkeypatch.setattr(jobs, '_under_fire_fence', real_fence)
        scheduler_authority.reconcile_pending()

        saved = jobs.get_job(job['id'])
        assert (saved['state'], saved['enabled']) == ('scheduled', True)
        assert (saved['paused_at'], saved['paused_reason']) == (None, None)
        assert saved['manual_run_prompt'] == 'original request', 'the prompt is preserved'
        assert saved['next_run_at'] == saved['manual_run_at'], 'the run-now is due again'
        assert saved['manual_run_restore_at'] == scheduled, 'the slot was not recomputed'
        assert not journal.exists(), 'the replayed lift releases the receipt'
        assert [j['id'] for j in jobs.get_due_jobs()] == [job['id']]


def test_missing_job_pause_lift_is_a_settled_noop(tmp_path):
    """A job deleted before the refusal lands settled the uncertainty it caused: nothing is owed,
    so the lift reports success and the caller drops the receipt. Only an unavailable fence may
    report ``False`` — that verdict read nothing, and is retried.
    """
    pause = {'paused_at': '2026-01-01T00:00:00+00:00', 'reason': 'Canonical cron unknown'}
    with jobs.use_cron_store(tmp_path / 'cron'):
        job_id = jobs.create_job(prompt='report', schedule='every 1h', deliver='local')['id']
        jobs.remove_job(job_id)
        assert scheduler_authority._lift_unstarted_pause(job_id, pause) is True
        assert jobs.load_jobs() == [], 'a no-op lift writes no job record'


def test_terminal_receipt_lifts_the_unknown_pause_it_caused(tmp_path, monkeypatch):
    """Once the owner's receipt proves the run terminal, the recurring schedule resumes by itself;
    an operator's own later pause is never lifted by a receipt."""
    with jobs.use_cron_store(tmp_path / 'cron'):
        job = jobs.create_job(prompt='report', schedule='every 1h', deliver='local')
        fire = executions.create_execution(job['id'], source='test')['id']
        journal = _unknown_fire(monkeypatch, job, fire)
        _peer(monkeypatch, lambda method: {'status': 'terminal', 'job': job,
                                           'result': [True, 'document', 'answer', None]})
        scheduler_authority.reconcile_pending()
        saved = jobs.get_job(job['id'])
        assert (saved['state'], saved['enabled'], saved['paused_reason']) == ('scheduled', True, None)
        assert executions.get_execution(fire)['status'] == 'completed'
        assert not journal.exists()

        second = executions.create_execution(job['id'], source='test')['id']
        _unknown_fire(monkeypatch, job, second)
        jobs.resume_job(job['id'])
        jobs.pause_job(job['id'], reason='operator hold')
        scheduler_authority.reconcile_pending()
        assert jobs.get_job(job['id'])['paused_reason'] == 'operator hold'
