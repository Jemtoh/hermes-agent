"""Consumer-lock ownership across processes, spans and abandoned work.

The lock is observed where the OS puts it: a REAL ``flock`` on a second descriptor reports a
lock another process holds, so every assertion here would fail if the module granted entry on
file text alone. A forked child stands in for a separate-process firer; the parent's own hold
stands in for the canonical owner. Fakes only: temp home, invented ids, no model/network.
"""
from __future__ import annotations

import concurrent.futures
import contextlib
import hashlib
import os
import signal
import subprocess
import sys
import textwrap
import threading
import time
from pathlib import Path

import pytest

import cron.scheduler as s

CONSUMER = "probe-consumer"
_REPO_ROOT = Path(__file__).resolve().parents[2]


@pytest.fixture
def temp_home(tmp_path, monkeypatch):
    monkeypatch.setenv("HERMES_HOME", str(tmp_path))
    yield tmp_path


def _probe(execution_id=None):
    """Try the consumer from a THREAD OF ITS OWN (the caller may legitimately own it)."""
    from cron.consumer_run_lock import ConsumerRunLockError, consumer_run_lock

    result = {}

    def _run():
        try:
            with consumer_run_lock(CONSUMER, execution_id=execution_id) as acquired:
                result["acquired"] = acquired
        except ConsumerRunLockError:  # a refused backend/journal is a refusal, not entry
            result["acquired"] = False

    thread = threading.Thread(target=_run, daemon=True)
    thread.start()
    thread.join(15)
    assert not thread.is_alive()
    return result["acquired"]


def _lock_path(home: Path, consumer: str = CONSUMER) -> Path:
    digest = hashlib.sha256(consumer.encode("utf-8")).hexdigest()
    return home / "cron" / f".consumer-run-{digest}.lock"


def _os_lock_free(path: Path) -> bool:
    """True when nothing holds the advisory lock (probe on a fresh descriptor)."""
    import fcntl

    fd = os.open(path, os.O_RDWR | os.O_CREAT, 0o600)
    try:
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            return True
        except OSError:
            return False
    finally:
        os.close(fd)


# --------------------------------------------------------------- separate-process partners
def test_firer_death_does_not_free_the_owners_hold(temp_home, tmp_path):
    """Two processes hold their OWN flock for one execution; killing the firer frees nothing.

    The owner joined while the firer was alive (shared lock + id re-read under it). After SIGKILL
    the owner's descriptor still excludes every other execution and every no-id producer."""
    from cron.consumer_run_lock import consumer_run_lock

    script = tmp_path / "firer.py"
    script.write_text(textwrap.dedent(f"""
        import sys
        from cron.consumer_run_lock import consumer_run_lock
        with consumer_run_lock({CONSUMER!r}, execution_id="exec-shared") as ok:
            print("READY" if ok else "REFUSED", flush=True)
            sys.stdin.read()
    """))
    env = {**os.environ, "HERMES_HOME": str(temp_home), "PYTHONPATH": str(_REPO_ROOT)}
    child = subprocess.Popen([sys.executable, str(script)], cwd=str(_REPO_ROOT), env=env,
                             stdin=subprocess.PIPE, stdout=subprocess.PIPE, text=True)
    try:
        assert child.stdout.readline().strip() == "READY"
        assert _os_lock_free(_lock_path(temp_home)) is False

        with consumer_run_lock(CONSUMER, execution_id="exec-shared") as owner:
            assert owner is True, "the owner must join its own fire's existing hold"
            child.send_signal(signal.SIGKILL)
            child.wait(10)
            # The firer's OS lock is gone with it, but the OWNER's own descriptor remains.
            assert _probe("exec-other") is False, "a different execution must be refused"
            assert _probe(None) is False, "a no-id producer never shares by identity"
        assert _probe("exec-other") is True, "the consumer is free once the owner's hold ends"
    finally:
        if child.poll() is None:
            child.kill()
            child.wait(10)
        if child.stdin:
            child.stdin.close()
        if child.stdout:
            child.stdout.close()


# ---------------------------------------------------------------- abandoned background work
def test_hold_survives_an_abandoned_conversation_future(temp_home):
    """A cancel/timed-out conversation outlives the guard: the consumer stays held until it exits."""
    from cron.consumer_run_lock import consumer_run_lock, keep_until

    path = _lock_path(temp_home)
    future: concurrent.futures.Future = concurrent.futures.Future()
    with consumer_run_lock(CONSUMER, execution_id="exec-live") as acquired:
        assert acquired is True
        assert keep_until(CONSUMER, "exec-live", future) is True
    assert _os_lock_free(path) is False, "the still-running conversation must keep the hold"
    with consumer_run_lock(CONSUMER, execution_id="exec-new") as contender:
        assert contender is False
    future.set_result(None)
    assert _os_lock_free(path) is True, "the future's own completion releases the consumer"
    with consumer_run_lock(CONSUMER, execution_id="exec-new") as after:
        assert after is True


# ---------------------------------------------------------------------- save span / refusal
def _fake_pipeline(monkeypatch, *, save_gate=None):
    seen = {"agent": [], "sent": [], "marks": [], "prompts": []}
    save_entered = threading.Event()

    def fake_run_job(job, **kw):
        seen["agent"].append(job["id"])
        seen["prompts"].append(kw.get("extra_prompt"))
        return (True, "out", "final", None)

    def fake_save(job_id, out):
        save_entered.set()
        if save_gate is not None:
            save_gate.wait(30)
        return f"/tmp/{job_id}.md"

    monkeypatch.setattr(s, "_launch_external_cron_worker", lambda job: False)
    monkeypatch.setattr(s, "run_job", fake_run_job)
    monkeypatch.setattr(s, "save_job_output", fake_save)
    monkeypatch.setattr(s, "_deliver_result", lambda *a, **kw: seen["sent"].append(True))
    monkeypatch.setattr(s, "mark_job_run", lambda *a, **kw: seen["marks"].append(True))
    seen["save_entered"] = save_entered
    return seen


def _claim(job_id: str, *, extra_prompt: str | None = None):
    from cron.executions import create_execution
    from cron.jobs import claim_job_for_fire, trigger_job

    trigger_job(job_id, extra_prompt=extra_prompt)
    claimed = claim_job_for_fire(job_id, manual=True, return_job=True)
    assert isinstance(claimed, dict)
    claimed = dict(claimed, execution_id=create_execution(job_id, source="probe")["id"])
    return claimed


def test_save_span_still_excludes_a_second_producer(temp_home, monkeypatch):
    """The lock spans the run's SAVED OUTPUT, not just the computation."""
    from cron.consumer_run_lock import consumer_run_lock
    from cron.jobs import create_job

    save_gate = threading.Event()
    seen = _fake_pipeline(monkeypatch, save_gate=save_gate)
    job = create_job(prompt="work", schedule="every 5m", name="save-span", run_lock=CONSUMER)
    claimed = _claim(job["id"])
    runner = threading.Thread(target=s.run_one_job, args=(claimed,), daemon=True)
    runner.start()
    try:
        # Synchronize on the ACTUAL save call, not on lock acquisition: the lock is taken long
        # before saving, so waiting for it would pass even if saving were released early.
        assert seen["save_entered"].wait(10), "the run never reached the save"
        assert _os_lock_free(_lock_path(temp_home)) is False, "saving the report must still exclude a second producer"
        with consumer_run_lock(CONSUMER, execution_id="exec-competitor") as contender:
            assert contender is False, "saving the report must still exclude a second producer"
    finally:
        save_gate.set()
        runner.join(20)
    assert not runner.is_alive()
    assert _os_lock_free(_lock_path(temp_home)) is True


@pytest.mark.platforms("posix")
def test_due_finite_one_shot_refusal_spends_no_dispatch(temp_home, monkeypatch):
    """A contended due fire starts nothing and is NOT charged to the finite dispatch budget."""
    from cron.executions import get_execution
    from cron.incidents import list_incidents
    from cron.jobs import create_job, get_job

    seen = _fake_pipeline(monkeypatch)
    job = create_job(prompt="work", schedule="in 30m", name="finite", run_lock=CONSUMER)
    claimed = _claim(job["id"])

    holder = threading.Event()
    release = threading.Event()

    def _hold():
        from cron.consumer_run_lock import consumer_run_lock

        with consumer_run_lock(CONSUMER, execution_id="exec-holder") as acquired:
            assert acquired is True
            holder.set()
            release.wait(30)

    thread = threading.Thread(target=_hold, daemon=True)
    thread.start()
    try:
        assert holder.wait(10)
        assert s.run_one_job(claimed) is True
    finally:
        release.set()
        thread.join(10)

    stored = get_job(job["id"])
    assert seen["agent"] == [] and seen["sent"] == [] and seen["marks"] == []
    assert list_incidents() == []
    assert stored["repeat"]["completed"] == 0, "a refusal must not spend the finite dispatch"
    row = get_execution(claimed["execution_id"])
    assert row["status"] == "failed" and "not started" in row["error"] and CONSUMER in row["error"]

    # Released: the next attempt (a FRESH execution) now executes exactly once.
    assert s.run_one_job(_claim(job["id"])) is True
    assert seen["agent"] == [job["id"]]


@pytest.mark.parametrize("no_agent", [False, True])
def test_refused_manual_run_stays_runnable_before_the_scheduled_slot(
        temp_home, monkeypatch, no_agent):
    """A refused run-now keeps its prompt AND its place in the queue, ahead of the scheduled slot."""
    from cron.extra_fire import RESTORE_KEY, queued
    from cron.jobs import create_job, get_due_jobs, get_job

    _fake_pipeline(monkeypatch)
    job = create_job(prompt="work", schedule="in 30m", name=f"manual-{no_agent}",
                     run_lock=CONSUMER, no_agent=no_agent, script="fake.py")
    scheduled = get_job(job["id"])["next_run_at"]
    claimed = _claim(job["id"], extra_prompt="run it now")

    release = threading.Event()
    holder = threading.Event()

    def _hold():
        from cron.consumer_run_lock import consumer_run_lock

        with consumer_run_lock(CONSUMER, execution_id="exec-holder") as acquired:
            assert acquired is True
            holder.set()
            release.wait(30)

    thread = threading.Thread(target=_hold, daemon=True)
    thread.start()
    try:
        assert holder.wait(10)
        assert s.run_one_job(claimed) is True
    finally:
        release.set()
        thread.join(10)

    stored = get_job(job["id"])
    assert stored["manual_run_prompt"] == "run it now", "the operator's prompt survives the refusal"
    assert queued(stored), "the run-now must be pending again, not parked behind the slot"
    assert stored[RESTORE_KEY] == scheduled, "the scheduled occurrence is preserved"
    assert stored["next_run_at"] == stored["manual_run_at"]
    assert stored["repeat"]["completed"] == 0
    assert [j["id"] for j in get_due_jobs()] == [job["id"]], "it is due again before the slot"


def test_newer_trigger_outranks_the_refused_one(temp_home, monkeypatch):
    """A trigger that lands after the refusal is the request that stays runnable."""
    from cron.jobs import create_job, get_job, trigger_job

    _fake_pipeline(monkeypatch)
    job = create_job(prompt="work", schedule="in 30m", name="newer", run_lock=CONSUMER)
    scheduled = get_job(job["id"])["next_run_at"]
    claimed = _claim(job["id"], extra_prompt="older request")

    release = threading.Event()
    holder = threading.Event()

    def _hold():
        from cron.consumer_run_lock import consumer_run_lock

        with consumer_run_lock(CONSUMER, execution_id="exec-holder") as acquired:
            assert acquired is True
            holder.set()
            release.wait(30)

    thread = threading.Thread(target=_hold, daemon=True)
    thread.start()
    try:
        assert holder.wait(10)
        trigger_job(job["id"], extra_prompt="newer request")
        assert s.run_one_job(claimed) is True
    finally:
        release.set()
        thread.join(10)

    stored = get_job(job["id"])
    assert stored["manual_run_prompt"] == "newer request"
    assert stored["next_run_at"] == stored["manual_run_at"]
    assert stored["manual_run_restore_at"] == scheduled


# ------------------------------------------------------------- canonical admission integration
def test_owner_dispatch_refusal_is_settled_by_the_firer(temp_home, monkeypatch):
    """scheduler → authority verdict: the owner refuses, the FIRER settles it as unstarted.

    No repeat/dispatch spend, no normal-failure bookkeeping, no delivery, and a precise reason —
    the verdict rides back as ``_consumer_refusal`` exactly like the gateway's admission result.
    """
    from cron.executions import get_execution
    from cron.jobs import create_job, get_job
    from gateway.session_cron import _run_identified

    seen = _fake_pipeline(monkeypatch)

    def owner_run_job(job, **kwargs):
        # The real owner seam, refusing before any computation; the body must never run.
        return _run_identified(
            lambda *a, **k: pytest.fail("a refused fire must not execute"),
            job, kwargs["execution_id"])

    monkeypatch.setattr(s, "run_job", owner_run_job)
    job = create_job(prompt="work", schedule="in 30m", name="owner-refusal", run_lock=CONSUMER)
    claimed = _claim(job["id"])

    release, holder = threading.Event(), threading.Event()

    def _hold():
        from cron.consumer_run_lock import consumer_run_lock

        with consumer_run_lock(CONSUMER, execution_id="exec-busy") as acquired:
            assert acquired is True
            holder.set()
            release.wait(30)

    thread = threading.Thread(target=_hold, daemon=True)
    thread.start()
    try:
        assert holder.wait(10)
        # The firer holds nothing yet: its own guard is what refuses here, so exercise the OWNER
        # verdict directly by exposing the consumer to the owner only.
        with monkeypatch.context() as patched:
            # Only the FIRER's guard is skipped here: the OWNER verdict is what this test drives.
            patched.setattr(s, "consumer_run_guard",
                            lambda job, execution_id, **kw: contextlib.nullcontext(True))
            assert s.run_one_job(claimed) is True
    finally:
        release.set()
        thread.join(10)

    stored = get_job(job["id"])
    assert seen["marks"] == [] and seen["sent"] == []
    assert stored["repeat"]["completed"] == 0
    row = get_execution(claimed["execution_id"])
    assert row["status"] == "failed" and "not started" in row["error"]


def test_post_admission_observer_disconnect_keeps_the_consumer_held(temp_home, monkeypatch):
    """The REAL transport path: ``submit`` is admitted, the observer's ``status`` disconnects.

    ``run_canonical_job`` raises ``CronExecutionUnknown`` and returns, but the owner keeps its own
    shared descriptor, so a third producer stays refused until the owner exits.
    """
    import asyncio

    import gateway.session_cron as sc
    from cron.consumer_run_lock import consumer_run_guard
    from cron.scheduler_authority import journal_path
    from cron.scheduler_authority import CronExecutionUnknown, run_canonical_job

    job = {"id": "canonical-disconnect", "name": "observer", "run_lock": CONSUMER}
    execution_id = "exec-observer"
    owner_ready, owner_release = threading.Event(), threading.Event()

    def fake_owner():
        with consumer_run_guard(dict(job, execution_id=execution_id), execution_id,
                                settle=False) as may_run:
            assert may_run is True
            owner_ready.set()
            owner_release.wait(30)

    owner = threading.Thread(target=fake_owner, daemon=True)
    loop = asyncio.new_event_loop()
    loop_thread = threading.Thread(target=loop.run_forever, daemon=True)
    loop_thread.start()

    async def fake_operation(authority, op, data, actor=None):
        if op == "submit":
            owner.start()
            assert owner_ready.wait(10), "the owner never admitted the fire"
            return {"session_id": "sess", "admission_id": "adm"}
        raise ConnectionError("observer disconnected after admission")

    monkeypatch.setattr(sc, "owner_for_home", lambda home: (object(), loop))
    monkeypatch.setattr(sc, "operation", fake_operation)
    try:
        with pytest.raises(CronExecutionUnknown):
            run_canonical_job(job, execution_id=execution_id)
        assert _probe("exec-third") is False, "the owner still holds the consumer"
        assert _probe(None) is False
    finally:
        owner_release.set()
        owner.join(20)
        loop.call_soon_threadsafe(loop.stop)
        loop_thread.join(10)
        loop.close()
    assert not owner.is_alive()
    # The owner is gone, but THIS fire's durable admission journal is still unresolved: the
    # consumer stays closed until terminal bookkeeping removes it (never by age or pid).
    assert journal_path(job["id"], execution_id).exists()
    assert _probe("exec-third") is False
    journal_path(job["id"], execution_id).unlink()
    assert _probe("exec-third") is True


def test_cancelled_conversation_keeps_the_consumer_while_it_lives(temp_home, monkeypatch):
    """The ACTUAL run_job path: an interrupted run defers teardown to a live conversation future.

    ``run_job`` records that future on the job; the owner's hold is kept until it completes, so a
    same-consumer producer cannot start while the cancelled worker is still running.
    """
    import concurrent.futures as cf

    from cron.consumer_run_lock import consumer_hold_survives_run, consumer_run_guard

    conversation: cf.Future = cf.Future()

    class _Scope:
        def __init__(self, *a, **k):
            self.workdir = None
            self.task_id = None

        def enter(self):
            pass

        def exit(self):
            pass

    class _Setup:
        blocked = None
        model = "fake-model"
        fallback_notice = None

    class _Cfg:
        cfg: dict = {}
        model = "fake-model"

    monkeypatch.setattr(s, "_prepare_job_prompt", lambda *a, **k: (None, "prompt"))
    monkeypatch.setattr(s, "_CronRunScope", _Scope)
    monkeypatch.setattr(s, "_reload_dotenv_and_publish_delivery_target", lambda *a: None)
    monkeypatch.setattr(s, "_load_cron_job_config", lambda *a: _Cfg())
    monkeypatch.setattr(s, "_resolve_cron_agent_setup", lambda *a: _Setup())
    monkeypatch.setattr(s, "_open_cron_session_db", lambda *a: None)
    monkeypatch.setattr(s, "_construct_cron_agent", lambda *a, **k: object())
    monkeypatch.setattr(s, "_finalize_cron_session", lambda *a, **k: None)

    def interrupted(agent, prompt, job, job_id, job_name, task_id, cancel, worker_state=None):
        worker_state["future"] = conversation
        raise RuntimeError("Cron job 'inactive' inactivity timeout after 600s")

    monkeypatch.setattr(s, "_run_agent_with_watchdog", interrupted)

    job = {"id": "inactive", "name": "inactive", "run_lock": CONSUMER,
           "execution_id": "exec-inactive", "no_agent": True}
    with consumer_run_guard(job, "exec-inactive") as acquired:
        assert acquired is True
        success, _out, _resp, error = s.run_job(job, execution_id="exec-inactive")
        assert success is False and "inactivity" in (error or "")
        assert job.get("_deferred_worker") is conversation, "the live future must be recorded"
        assert consumer_hold_survives_run(CONSUMER, "exec-inactive",
                                          job["_deferred_worker"]) is True
    assert _os_lock_free(_lock_path(temp_home)) is False, "the conversation is still running"
    assert _probe("exec-after-cancel") is False
    conversation.set_result(None)
    assert _os_lock_free(_lock_path(temp_home)) is True


def test_joined_owner_thread_rejects_a_nested_run_after_the_firer_has_exited(temp_home):
    """A JOINED owner thread is a participant: its own nested run is recursion, not contention.

    The firer authored the fire in its thread and is gone by the time the owner continues; the
    owner joined the SAME execution from its own thread and still holds its own descriptor. A
    nested ``run_one_job`` from that thread must be refused BEFORE it creates an execution or
    retires the outer fire's claims — treating it as ordinary contention would settle the outer
    run as unstarted while its own lock is still live.
    """
    from cron.consumer_run_lock import (
        ConsumerRunLockRecursionError, consumer_run_lock)

    fire = "exec-joined"
    firer_ready, firer_exit, owner_ready, owner_done = (
        threading.Event(), threading.Event(), threading.Event(), threading.Event())
    outcome = {}

    def _firer():
        with consumer_run_lock(CONSUMER, execution_id=fire) as acquired:
            assert acquired is True
            firer_ready.set()
            firer_exit.wait(20)

    def _owner():
        try:
            with consumer_run_lock(CONSUMER, execution_id=fire) as acquired:
                assert acquired is True, "the owner must join its own fire's hold"
                owner_ready.set()
                owner_done.wait(20)
                # The initial firer has exited; THIS thread is the remaining participant.
                with pytest.raises(ConsumerRunLockRecursionError):
                    s.run_one_job({"id": "nested", "run_lock": CONSUMER,
                                   "execution_id": fire})
                outcome["refused"] = True
        except BaseException as exc:  # surfaced below, never swallowed in a thread
            outcome.setdefault("error", exc)
            owner_ready.set()

    firer = threading.Thread(target=_firer, daemon=True)
    owner = threading.Thread(target=_owner, daemon=True)
    firer.start()
    assert firer_ready.wait(10)
    owner.start()
    try:
        assert owner_ready.wait(10)
        firer_exit.set()
        firer.join(10)
        assert not firer.is_alive()
        assert owner.is_alive(), "the owner's descriptor is what keeps the consumer held"
        assert _probe("exec-after-firer") is False
    finally:
        owner_done.set()
        owner.join(20)
        firer_exit.set()
        firer.join(1)
    assert "error" not in outcome, outcome.get("error")
    assert outcome.get("refused") is True
    assert _os_lock_free(_lock_path(temp_home)) is True


def test_finite_one_shot_refused_by_the_owner_keeps_its_dispatch(temp_home, monkeypatch):
    """scheduler → authority → OWNER: a busy consumer refuses a finite one-shot UNSTARTED.

    The owner seam is the real ``_run_identified``; only the firer's own guard is bypassed so the
    owner's verdict is what refuses. The finite dispatch budget must be untouched, nothing sent or
    marked, the row closed as unstarted, and the release must let the one-shot execute exactly once.
    """
    from cron.executions import get_execution
    from cron.jobs import create_job, get_job
    from gateway.session_cron import _run_identified

    seen = _fake_pipeline(monkeypatch)
    ran: list = []

    def owner_run_job(job, **kwargs):
        return _run_identified(lambda *a, **k: ran.append(job["id"]) or (True, "o", "f", None),
                               job, kwargs["execution_id"])

    monkeypatch.setattr(s, "run_job", owner_run_job)
    job = create_job(prompt="work", schedule="in 30m", name="finite-owner",
                     run_lock=CONSUMER, repeat=1)
    claimed = _claim(job["id"])

    release, holder = threading.Event(), threading.Event()

    def _hold():
        from cron.consumer_run_lock import consumer_run_lock

        with consumer_run_lock(CONSUMER, execution_id="exec-busy-finite") as acquired:
            assert acquired is True
            holder.set()
            release.wait(30)

    thread = threading.Thread(target=_hold, daemon=True)
    thread.start()
    try:
        assert holder.wait(10)
        with monkeypatch.context() as patched:
            patched.setattr(s, "consumer_run_guard",
                            lambda job_, execution_id, **kw: contextlib.nullcontext(True))
            assert s.run_one_job(claimed) is True
    finally:
        release.set()
        thread.join(10)

    stored = get_job(job["id"])
    assert stored is not None
    assert ran == [], "a refused fire must never reach the computation"
    assert seen["sent"] == [] and seen["marks"] == []
    assert stored["repeat"]["completed"] == 0, "a refusal must not spend the finite dispatch"
    row = get_execution(claimed["execution_id"])
    assert row is not None
    assert row["status"] == "failed" and "not started" in row["error"]
    assert CONSUMER in row["error"], "the ledger carries the precise consumer refusal"

    retry = _claim(job["id"])
    with monkeypatch.context() as patched:
        # The gateway runs the owner on its OWN thread; driving it inline here would re-enter the
        # firer's own guard on this thread, which the module correctly refuses as recursion.
        patched.setattr(s, "consumer_run_guard",
                        lambda job_, execution_id, **kw: contextlib.nullcontext(True))
        assert s.run_one_job(retry) is True, get_execution(retry["execution_id"])
    assert ran == [job["id"]], "the released one-shot executes exactly once"


@pytest.mark.parametrize("no_agent", [False, True])
def test_refused_manual_run_executes_once_after_retry(temp_home, monkeypatch, no_agent):
    """The ORIGINAL queued run-now is what the next due fire runs — once, and no re-trigger.

    The retry goes through the store's due path (``get_due_jobs`` + the tick's own
    ``claim_job_for_fire``), never ``trigger_job``: a re-trigger would replace the operator's
    preserved prompt with a NEW request and prove nothing about the refused one.
    """
    from cron.jobs import claim_job_for_fire, create_job, get_due_jobs, get_job

    real_mark_job_run = s.mark_job_run
    _fake_pipeline(monkeypatch)
    job = create_job(prompt="work", schedule="in 30m", name=f"retry-{no_agent}",
                     run_lock=CONSUMER, no_agent=no_agent, script="fake.py")
    scheduled = get_job(job["id"])["next_run_at"]
    claimed = _claim(job["id"], extra_prompt="the original prompt")

    release, holder = threading.Event(), threading.Event()

    def _hold():
        from cron.consumer_run_lock import consumer_run_lock

        with consumer_run_lock(CONSUMER, execution_id="exec-busy") as acquired:
            assert acquired is True
            holder.set()
            release.wait(30)

    thread = threading.Thread(target=_hold, daemon=True)
    thread.start()
    try:
        assert holder.wait(10)
        assert s.run_one_job(claimed) is True
    finally:
        release.set()
        thread.join(10)

    stored = get_job(job["id"])
    assert stored["manual_run_prompt"] == "the original prompt"
    assert [j["id"] for j in get_due_jobs()] == [job["id"]], "the preserved request is still due"

    # The tick's own claim of the due record: no trigger_job, no new prompt.
    retry = claim_job_for_fire(job["id"], return_job=True)
    assert isinstance(retry, dict) and retry["manual_run_prompt"] == "the original prompt"
    seen = _fake_pipeline(monkeypatch)
    # The store write is REAL here: only the run/agent/delivery are faked, because consuming the
    # queued request is exactly what mark_job_run does.
    monkeypatch.setattr(s, "mark_job_run", real_mark_job_run)
    assert s.run_one_job(retry) is True
    assert seen["agent"] == [job["id"]], "the preserved request executes exactly ONCE"
    assert seen["prompts"] == ["the original prompt"], "the ORIGINAL prompt is what ran"
    after = get_job(job["id"])
    assert "manual_run_prompt" not in after and "manual_run_at" not in after, "the request is consumed"
    assert after["next_run_at"] == scheduled, "the scheduled occurrence is preserved"
    assert after["repeat"]["completed"] == 0, "the run-now never spent the scheduled budget"
    assert [j["id"] for j in get_due_jobs()] == [], "a later scan must not fire it again"


# ------------------------------------------------------------ owner verdict / dead-firer recovery
def test_refusal_carries_the_real_backend_cause(temp_home, monkeypatch):
    """An unsupported/I-O refusal keeps its OWN cause, not the synthesized contention text."""
    from cron import consumer_run_lock
    from gateway.session_cron import _run_identified

    monkeypatch.setattr(consumer_run_lock, "_posix_flock_available", lambda: False)
    job = {"id": "cause", "name": "cause", "run_lock": CONSUMER}
    result = _run_identified(lambda *a, **k: pytest.fail("a refused fire must not compute"),
                             job, "exec-cause")
    assert result[0] is False and "not started" in result[3]
    assert "unsupported" in job["_consumer_refusal"], job["_consumer_refusal"]
    assert "held by another producer" not in job["_consumer_refusal"]
    assert job["_consumer_refusal"] in result[3]


_OWNER_REFUSAL_SCRIPT = textwrap.dedent("""
    import json, sys
    from gateway.session_cron import _run_identified
    payload = json.loads(sys.argv[1])
    def _never(*a, **k):
        raise AssertionError("a refused fire must never compute")
    result = _run_identified(_never, payload["job"], payload["execution_id"])
    print(json.dumps({"result": list(result), "reason": payload["job"].get("_consumer_refusal")}))
""")


@pytest.mark.platforms("posix")
def test_due_finite_refusal_stays_unstarted_through_dead_firer_recovery(temp_home, monkeypatch):
    """A GENUINELY due finite occurrence is refused by an owner in ANOTHER process, the firer dies
    before settling, and receipt recovery books it unstarted: no output, notice, incident or
    dispatch spend, and the occurrence stays due."""
    import asyncio
    import json

    import gateway.session_cron as sc
    from cron.executions import create_execution, get_execution
    from cron.incidents import list_incidents
    from cron.jobs import (claim_job_for_fire, create_job, get_due_jobs, get_job,
                           update_job)
    from cron.scheduler_authority import journal_path, reconcile_pending
    from hermes_time import now as _now
    from utils import atomic_json_write

    seen = _fake_pipeline(monkeypatch)
    job = create_job(prompt="work", schedule="in 45m", name="due-finite-recovery",
                     run_lock=CONSUMER, repeat=1)
    # A genuinely DUE occurrence: the store's own due scan finds it (no trigger_job extra fire).
    update_job(job["id"], {"next_run_at": (_now() - __import__("datetime").timedelta(seconds=45))
                           .isoformat()})
    assert [j["id"] for j in get_due_jobs()] == [job["id"]], "the occurrence must be genuinely due"
    claimed = claim_job_for_fire(job["id"], return_job=True)  # the ticker's own claim
    assert isinstance(claimed, dict) and claimed.get("fire_claim")
    assert get_job(job["id"])["repeat"]["completed"] == 0

    release, holder = threading.Event(), threading.Event()

    def _hold():
        from cron.consumer_run_lock import consumer_run_lock

        with consumer_run_lock(CONSUMER, execution_id="exec-competitor") as acquired:
            assert acquired is True
            holder.set()
            release.wait(30)

    competitor = threading.Thread(target=_hold, daemon=True)
    competitor.start()
    assert holder.wait(10)
    try:
        # The OWNER lives in its own process; the firer never reaches it in this test.
        script = temp_home / "owner_process.py"
        script.write_text(_OWNER_REFUSAL_SCRIPT)
        env = {**os.environ, "HERMES_HOME": str(temp_home), "PYTHONPATH": str(_REPO_ROOT)}
        seed = textwrap.dedent(f"""
            import json
            from cron.executions import create_execution
            from cron.jobs import get_job
            row = create_execution({job["id"]!r}, source="scheduled")
            print(json.dumps({{"job": get_job({job["id"]!r}), "execution_id": row["id"]}}))
        """)
        seeded = subprocess.run([sys.executable, "-c", seed], cwd=str(_REPO_ROOT), env=env,
                                capture_output=True, text=True, timeout=60, check=False)
        assert seeded.returncode == 0, seeded.stderr
        payload = json.loads(seeded.stdout.strip().splitlines()[-1])
        owner = subprocess.run([sys.executable, str(script), json.dumps(payload)],
                               cwd=str(_REPO_ROOT), env=env, capture_output=True, text=True,
                               timeout=60, check=False)
        assert owner.returncode == 0, owner.stderr
        verdict = json.loads(owner.stdout.strip().splitlines()[-1])
    finally:
        release.set()
        competitor.join(10)
    assert not competitor.is_alive()
    assert verdict["result"][0] is False and "not started" in verdict["result"][3]
    assert CONSUMER in verdict["reason"] and "held by another producer" in verdict["reason"]
    execution_id = payload["execution_id"]

    # The firer's durable admission state survives it; the owner's terminal verdict is the receipt.
    params = {"job_id": job["id"], "request_id": execution_id, "extra_prompt": None}
    atomic_json_write(journal_path(job["id"], execution_id),
                      {"params": params, "receipt": {"session_id": "s", "admission_id": "a"},
                       "consumer": {"name": CONSUMER, "request_id": execution_id}},
                      mode=0o600, fsync_dir=True)

    async def fake_operation(authority, name, data, actor=None):
        assert name == "recover"
        return {"status": "terminal", "result": verdict["result"],
                "job": payload["job"], "job_flags": {"_consumer_refusal": verdict["reason"]}}

    monkeypatch.setattr(sc, "owner_for_home", lambda home: (object(), loop))
    monkeypatch.setattr(sc, "operation", fake_operation)
    loop = asyncio.new_event_loop()
    loop_thread = threading.Thread(target=loop.run_forever, daemon=True)
    loop_thread.start()
    try:
        reconcile_pending()
    finally:
        loop.call_soon_threadsafe(loop.stop)
        loop_thread.join(10)
        loop.close()

    row = get_execution(execution_id)
    assert row["status"] == "failed" and "not started" in row["error"]
    assert seen["agent"] == [] and seen["sent"] == [] and seen["marks"] == []
    assert list_incidents() == [], "an unstarted refusal raises no incident"
    stored = get_job(job["id"])
    assert stored["repeat"]["completed"] == 0, "a refused finite occurrence spends no dispatch"
    assert stored.get("fire_claim") is None, "the dead firer's claim is fenced away"
    assert [j["id"] for j in get_due_jobs()] == [job["id"]], "the occurrence is still due"
    assert not journal_path(job["id"], execution_id).exists()


@pytest.mark.parametrize("mode", ["cancel", "inactive"])
def test_owner_hold_survives_a_real_interrupted_conversation(temp_home, monkeypatch, mode):
    """REAL cancellation and REAL inactivity detection, through the owner's ``_run_identified``.

    The watchdog is not replaced and ``consumer_hold_survives_run`` is not called by hand: the
    blocked fake conversation goes through the production ``_run_agent_with_watchdog``, and the
    owner's own hookup is what keeps the consumer held while that worker is still alive.
    """
    import concurrent.futures as cf

    from cron.cancellation import CancelRequest, _CombinedCancelEvent
    import gateway.session_cron as sc
    from gateway.session_cron import _run_identified

    started, release = threading.Event(), threading.Event()
    conversation: cf.Future = cf.Future()

    class _Blocked:
        def run_conversation(self, prompt, task_id=None):
            started.set()
            release.wait(30)
            conversation.set_result(None)
            return {"final_response": "late"}

        def get_activity_summary(self):
            return {"seconds_since_activity": 10 ** 6 if mode == "inactive" else 0}

        def interrupt(self, message=None):
            self.interrupted = message

        def close(self):
            pass

    class _Scope:
        def __init__(self, *a, **k):
            self.workdir = None
            self.task_id = None

        def enter(self):
            pass

        def exit(self):
            pass

    class _Setup:
        blocked = None
        model = "fake-model"
        fallback_notice = None

    class _Cfg:
        cfg: dict = {}
        model = "fake-model"

    monkeypatch.setattr(s, "_prepare_job_prompt", lambda *a, **k: (None, "prompt"))
    monkeypatch.setattr(s, "_CronRunScope", _Scope)
    monkeypatch.setattr(s, "_reload_dotenv_and_publish_delivery_target", lambda *a: None)
    monkeypatch.setattr(s, "_load_cron_job_config", lambda *a: _Cfg())
    monkeypatch.setattr(s, "_resolve_cron_agent_setup", lambda *a: _Setup())
    monkeypatch.setattr(s, "_open_cron_session_db", lambda *a: None)
    monkeypatch.setattr(s, "_construct_cron_agent", lambda *a, **k: _Blocked())
    monkeypatch.setattr(s, "_finalize_cron_session", lambda *a, **k: None)

    execution_id = "exec-interrupted"
    from cron.jobs import create_job

    stored = create_job(prompt="work", schedule="every 5m", name="interrupted", run_lock=CONSUMER)
    raw = threading.Event()
    cancel = _CombinedCancelEvent(CancelRequest(execution_id), raw)
    job = {"id": stored["id"], "name": "interrupted", "prompt": "work",
           "run_lock": CONSUMER, "execution_id": execution_id}
    outcome = {}

    def _owner():
        # Production's canonical owner runs inside ``execute()``, which binds the SCHEDULER's
        # execution identity (gateway.session_cron._execution) before the run; the ContextVar is
        # thread-local to the context, so the owner thread must bind it exactly as ``execute`` does.
        token = sc.bind_execution(None, "sess", job["id"], execution_id)
        try:
            outcome["result"] = _run_identified(s.run_job, job, execution_id, cancel_event=cancel)
        except BaseException as exc:  # surfaced below
            outcome["error"] = exc
        finally:
            sc._execution.reset(token)

    owner = threading.Thread(target=_owner, daemon=True)
    owner.start()
    try:
        assert started.wait(15), f"the fake conversation never started: {outcome}"
        if mode == "cancel":
            raw.set()
        owner.join(30)
        assert not owner.is_alive(), "the watchdog never interrupted the blocked conversation"
        assert "error" not in outcome, outcome.get("error")
        assert outcome["result"][0] is False
        assert job.get("_deferred_worker") is not None, "the live worker must be recorded"
        assert _probe("exec-after") is False, "the conversation is still alive; keep excluding"
    finally:
        release.set()
        owner.join(20)
    deadline = time.time() + 10
    while time.time() < deadline and not _os_lock_free(_lock_path(temp_home)):
        time.sleep(0.02)
    assert _os_lock_free(_lock_path(temp_home)) is True


def test_admission_journal_excludes_until_the_report_is_saved(temp_home, monkeypatch):
    """Firer gone, owner finished: the DURABLE journal carries exclusion across that gap.

    No process holds the OS lock, yet another execution of the consumer is refused because this
    fire's admission journal is unresolved. Recovery then saves the report; a competitor stays
    refused for the whole save (synchronized on the actual save entry, not on lock acquisition)
    and is admitted only after the terminal bookkeeping removes the journal.
    """
    import asyncio
    import json

    import gateway.session_cron as sc
    from cron.jobs import claim_job_for_fire, create_job
    from cron.scheduler_authority import journal_path, reconcile_pending
    from utils import atomic_json_write

    save_entered, save_gate = threading.Event(), threading.Event()
    saved: list = []

    def fake_save(job_id, out):
        save_entered.set()
        save_gate.wait(30)
        saved.append(job_id)

    monkeypatch.setattr(s, "save_job_output", fake_save)
    monkeypatch.setattr(s, "_deliver_result", lambda *a, **k: None)
    job = create_job(prompt="work", schedule="every 5m", name="journal-fence", run_lock=CONSUMER)
    claim_job_for_fire(job["id"], return_job=True)

    seed = textwrap.dedent(f"""
        import json
        from cron.executions import create_execution
        from cron.jobs import get_job
        row = create_execution({job["id"]!r}, source="scheduled")
        print(json.dumps({{"job": get_job({job["id"]!r}), "execution_id": row["id"]}}))
    """)
    env = {**os.environ, "HERMES_HOME": str(temp_home), "PYTHONPATH": str(_REPO_ROOT)}
    seeded = subprocess.run([sys.executable, "-c", seed], cwd=str(_REPO_ROOT), env=env,
                            capture_output=True, text=True, timeout=60, check=False)
    assert seeded.returncode == 0, seeded.stderr
    payload = json.loads(seeded.stdout.strip().splitlines()[-1])
    execution_id = payload["execution_id"]
    params = {"job_id": job["id"], "request_id": execution_id, "extra_prompt": None}
    journal = journal_path(job["id"], execution_id)
    atomic_json_write(journal, {"params": params, "receipt": {"session_id": "s", "admission_id": "a"},
                                "consumer": {"name": CONSUMER, "request_id": execution_id}},
                      mode=0o600, fsync_dir=True)

    assert _probe("exec-competitor") is False, "the unresolved journal must exclude even unlocked"

    async def fake_operation(authority, name, data, actor=None):
        assert name == "recover"
        return {"status": "terminal", "result": [True, "out", "final", None], "job": payload["job"]}

    loop = asyncio.new_event_loop()
    loop_thread = threading.Thread(target=loop.run_forever, daemon=True)
    loop_thread.start()
    monkeypatch.setattr(sc, "owner_for_home", lambda home: (object(), loop))
    monkeypatch.setattr(sc, "operation", fake_operation)
    recovery = threading.Thread(target=reconcile_pending, daemon=True)
    try:
        recovery.start()
        assert save_entered.wait(20), "recovery never entered the save"
        assert _probe("exec-competitor") is False, "exclusion must hold through the save"
    finally:
        save_gate.set()
        recovery.join(30)
        loop.call_soon_threadsafe(loop.stop)
        loop_thread.join(10)
        loop.close()
    assert not recovery.is_alive()
    assert saved == [job["id"]]
    assert not journal.exists(), "terminal bookkeeping removes the durable fence"
    assert _probe("exec-competitor") is True, "admitted once the attempt is booked"


# ------------------------------------------------- journal identity compatibility / fail-closed
def _write_journal(home, job_id, request_id, record):
    from cron.scheduler_authority import journal_path
    from utils import atomic_json_write

    path = journal_path(job_id, request_id)
    atomic_json_write(path, record, mode=0o600, fsync_dir=True)
    return path


@pytest.mark.parametrize("legacy", [True, False])
@pytest.mark.parametrize("case,blocked", [
    ("same-consumer", True),
    ("different-consumer", False),
    ("no-consumer", False),
    ("current-job", True),
    ("malformed-name", True),
    ("wrong-request-id", True),
    ("not-an-object", True),
])
def test_admission_journal_identity_never_silently_admits(temp_home, legacy, case, blocked):
    """Legacy, malformed and mismatched journals: only a VALID unrelated identity may pass."""
    from cron.jobs import create_job

    job = create_job(prompt="work", schedule="every 5m", name=f"ident-{case}", run_lock=CONSUMER)
    request_id = "exec-journal"
    record = {"params": {"job_id": job["id"], "request_id": request_id, "extra_prompt": None},
              "receipt": {"session_id": "s", "admission_id": "a"}}
    if case == "not-an-object":
        record = ["not", "a", "journal"]
    elif case == "malformed-name":
        record["consumer"] = {"name": 5, "request_id": request_id}
    elif case == "wrong-request-id":
        record["consumer"] = {"name": CONSUMER, "request_id": "some-other-execution"}
    elif not legacy and case != "no-consumer":
        record["consumer"] = {"name": "another-consumer" if case == "different-consumer" else CONSUMER,
                              "request_id": request_id}
    elif case == "current-job":
        pass  # no frozen job either: the CURRENT stored job must resolve the consumer
    else:
        # An older journal: only its frozen job (or the current stored job) names the consumer.
        frozen = dict(job)
        if case == "no-consumer":
            frozen.pop("run_lock", None)
        elif case == "different-consumer":
            frozen["run_lock"] = "another-consumer"
        record["job"] = frozen
    _write_journal(temp_home, job["id"], request_id, record)
    try:
        assert _probe("exec-other") is (not blocked)
    finally:
        from cron.scheduler_authority import journal_path

        journal_path(job["id"], request_id).unlink(missing_ok=True)


# ------------------------------------------------- typed-unstarted recovery: replay + identity
def _run_reconcile(monkeypatch, terminal_state):
    import asyncio

    import gateway.session_cron as sc
    from cron.scheduler_authority import reconcile_pending

    async def fake_operation(authority, name, data, actor=None):
        assert name == "recover"
        return terminal_state

    loop = asyncio.new_event_loop()
    loop_thread = threading.Thread(target=loop.run_forever, daemon=True)
    loop_thread.start()
    monkeypatch.setattr(sc, "owner_for_home", lambda home: (object(), loop))
    monkeypatch.setattr(sc, "operation", fake_operation)
    try:
        reconcile_pending()
    finally:
        loop.call_soon_threadsafe(loop.stop)
        loop_thread.join(10)
        loop.close()


@pytest.mark.platforms("posix")
def test_unstarted_recovery_replays_claim_cleanup_after_partial_failure(temp_home, monkeypatch):
    """Ledger settled + cleanup failed: the NEXT reconciliation finishes it, journal retained."""
    import json

    from cron import consumer_run_lock
    from cron.executions import get_execution
    from cron.incidents import list_incidents
    from cron.jobs import create_job, get_due_jobs, get_job, trigger_job
    from cron.scheduler_authority import journal_path

    seen = _fake_pipeline(monkeypatch)
    created = create_job(prompt="work", schedule="in 30m", name="replay", run_lock=CONSUMER)
    scheduled = get_job(created["id"])["next_run_at"]
    job = trigger_job(created["id"], extra_prompt="original request")
    prompt = get_job(job["id"])["manual_run_prompt"]
    _claim(job["id"], extra_prompt=prompt)
    # The dead firer's ledger row (created by another process).
    seed = textwrap.dedent(f"""
        import json
        from cron.executions import create_execution
        row = create_execution({job["id"]!r}, source="scheduled")
        print(json.dumps({{"id": row["id"]}}))
    """)
    env = {**os.environ, "HERMES_HOME": str(temp_home), "PYTHONPATH": str(_REPO_ROOT)}
    seeded = subprocess.run([sys.executable, "-c", seed], cwd=str(_REPO_ROOT), env=env,
                            capture_output=True, text=True, timeout=60, check=False)
    assert seeded.returncode == 0, seeded.stderr
    execution_id = json.loads(seeded.stdout.strip().splitlines()[-1])["id"]
    params = {"job_id": job["id"], "request_id": execution_id, "extra_prompt": None}
    # An ORDINARY journal as the firer left it: no unstarted marker, no verdict, no frozen job.
    # The refusal arrives through recover for the first time, which is the path that used to
    # settle the row and then lose the cleanup replay.
    _write_journal(temp_home, job["id"], execution_id,
                   {"params": params, "receipt": {"session_id": "s", "admission_id": "a"},
                    "consumer": {"name": CONSUMER, "request_id": execution_id}})
    state = {"status": "terminal", "result": [False, "out", "", "held; not started"],
             "job": get_job(job["id"]), "job_flags": {"_consumer_refusal": "held; not started"}}

    real_release = consumer_run_lock.release_unstarted_fire
    calls = {"n": 0}

    def flaky_release(*a, **kw):
        calls["n"] += 1
        if calls["n"] == 1:
            raise OSError("claim store unavailable")
        return real_release(*a, **kw)

    monkeypatch.setattr(consumer_run_lock, "release_unstarted_fire", flaky_release)
    _run_reconcile(monkeypatch, state)
    assert get_execution(execution_id)["status"] == "failed", "the dead row is booked unstarted"
    assert journal_path(job["id"], execution_id).exists(), "a failed cleanup must keep the receipt"
    assert get_job(job["id"])["fire_claim"] is not None, "the claim was not retired"
    # The verdict, the marker and the frozen job are durable BEFORE the settlement write, so the
    # next pass replays cleanup instead of taking the firer-finished early deletion.
    persisted = json.loads(journal_path(job["id"], execution_id).read_text(encoding="utf-8-sig"))
    assert persisted["unstarted"] is True
    assert persisted["refusal"] == state["result"]
    assert persisted["job"]["fire_claim"] == get_job(job["id"])["fire_claim"]

    _run_reconcile(monkeypatch, state)
    assert not journal_path(job["id"], execution_id).exists()
    stored = get_job(job["id"])
    assert stored.get("fire_claim") is None
    assert stored["manual_run_prompt"] == prompt, "the operator's queued request survives"
    assert [j["id"] for j in get_due_jobs()] == [job["id"]], "and is still runnable"
    assert stored.get("manual_run_restore_at") == scheduled
    assert stored["repeat"]["completed"] == 0
    assert seen["agent"] == [] and seen["sent"] == [] and seen["marks"] == []
    assert list_incidents() == []


@pytest.mark.platforms("posix")
def test_unstarted_recovery_leaves_a_newer_claim_alone(temp_home, monkeypatch):
    """The replay retires only the claim it recorded: a NEWER fire's claim survives the settlement.

    Its owner mismatch is a legitimate no-op of ``release_unstarted_fire``, not a reason to keep
    the journal: the row is settled and the queued run-now is the newer fire's business.
    """
    from cron import executions as execution_store
    from cron.executions import create_execution, get_execution
    from cron.jobs import claim_job_for_fire, create_job, get_job, trigger_job
    from cron.scheduler_authority import journal_path
    from cron.consumer_run_lock import release_unstarted_fire

    seen = _fake_pipeline(monkeypatch)
    created = create_job(prompt="work", schedule="in 30m", name="newer-claim", run_lock=CONSUMER)
    job = trigger_job(created["id"], extra_prompt="original request")
    prompt = get_job(job["id"])["manual_run_prompt"]
    _claim(job["id"], extra_prompt=prompt)
    execution_id = create_execution(job["id"], source="probe")["id"]
    # The row belongs to the DEAD firer that wrote the journal, not to this reconciling process.
    with execution_store._transaction() as conn:
        conn.execute("UPDATE executions SET process_id=?, pid=? WHERE id=?",
                     ("dead-firer", 999999, execution_id))
    params = {"job_id": job["id"], "request_id": execution_id, "extra_prompt": None}
    frozen = get_job(job["id"])  # the claim this fire held when the owner refused it
    _write_journal(temp_home, job["id"], execution_id,
                   {"params": params, "receipt": {"session_id": "s", "admission_id": "a"},
                    "consumer": {"name": CONSUMER, "request_id": execution_id}})

    # Our fire's claim is retired elsewhere and a NEWER fire claims the same job.
    assert release_unstarted_fire(job["id"], frozen["fire_claim"]["by"]) is True
    newer = claim_job_for_fire(job["id"], manual=True, return_job=True)
    assert isinstance(newer, dict) and newer["fire_claim"]["by"] != frozen["fire_claim"]["by"]

    state = {"status": "terminal", "result": [False, "out", "", "held; not started"],
             "job": frozen, "job_flags": {"_consumer_refusal": "held; not started"}}
    _run_reconcile(monkeypatch, state)

    assert get_execution(execution_id)["status"] == "failed"
    assert not journal_path(job["id"], execution_id).exists()
    stored = get_job(job["id"])
    assert stored["fire_claim"]["by"] == newer["fire_claim"]["by"], "the newer claim survives"
    assert stored["manual_run_prompt"] == prompt
    assert stored["next_run_at"] == newer["next_run_at"], "the newer fire's own slot is untouched"
    assert seen["agent"] == [] and seen["sent"] == [] and seen["marks"] == []


@pytest.mark.platforms("posix")
def test_live_firer_cleanup_failure_stays_recoverable_unstarted(temp_home, monkeypatch):
    """A typed refusal whose claim cleanup FAILS is not a crash: the live firer relinquishes the
    attempt and the next reconciliation replays the cleanup, with the journal it wrote as receipt.

    Nothing is sent, marked, turned into an incident, or charged to the manual request / finite
    dispatch for a fire that computed nothing.
    """
    from cron import consumer_run_lock
    from cron.executions import get_execution
    from cron.incidents import list_incidents
    from cron.jobs import create_job, get_due_jobs, get_job
    from cron.scheduler_authority import journal_path, reconcile_pending

    seen = _fake_pipeline(monkeypatch)
    job = create_job(prompt="work", schedule="in 30m", name="live-cleanup", run_lock=CONSUMER,
                     repeat=1)
    scheduled = get_job(job["id"])["next_run_at"]
    claimed = _claim(job["id"], extra_prompt="original request")
    fire = claimed["execution_id"]
    params = {"job_id": job["id"], "request_id": fire, "extra_prompt": "original request"}

    def refused_run_job(job_, **kwargs):
        # The canonical firer's own durable state after the owner's typed refusal.
        _write_journal(temp_home, job["id"], fire,
                       {"params": params, "receipt": {"session_id": "s", "admission_id": "a"},
                        "consumer": {"name": CONSUMER, "request_id": fire},
                        "unstarted": True, "refusal": [False, "out", "", "held; not started"],
                        "job": dict(get_job(job["id"]))})
        job_["_consumer_refusal"] = "held; not started"
        return False, "out", "", "held; not started"

    monkeypatch.setattr(s, "run_job", refused_run_job)

    real_release = consumer_run_lock.release_unstarted_fire
    calls = {"n": 0}

    def flaky_release(*a, **kw):
        calls["n"] += 1
        if calls["n"] == 1:
            raise OSError("claim store unavailable")
        return real_release(*a, **kw)

    monkeypatch.setattr(consumer_run_lock, "release_unstarted_fire", flaky_release)

    assert s.run_one_job(claimed) is True
    assert seen["sent"] == [] and seen["marks"] == [], "no crash notice, no failure bookkeeping"
    assert list_incidents() == []
    stored = get_job(job["id"])
    assert stored["repeat"]["completed"] == 0, "a fire that computed nothing spends no dispatch"
    assert stored["manual_run_prompt"] == "original request", "the request is not consumed"
    assert stored["fire_claim"] is not None, "a failed cleanup keeps the claim for replay"
    assert journal_path(job["id"], fire).exists(), "the receipt survives the failure"
    row = get_execution(fire)
    assert row["status"] == "unknown", "the attempt is relinquished to recovery, not booked failed"
    assert "cleanup failed" in row["error"]

    reconcile_pending()  # the replay: settle the row, retire THIS fire's claim, drop the receipt
    assert not journal_path(job["id"], fire).exists()
    row = get_execution(fire)
    assert row["status"] == "failed" and "held; not started" in row["error"]
    stored = get_job(job["id"])
    assert stored["fire_claim"] is None
    assert stored["manual_run_prompt"] == "original request"
    assert [j["id"] for j in get_due_jobs()] == [job["id"]], "the request is runnable again"
    assert stored.get("manual_run_restore_at") == scheduled
    assert seen["sent"] == [] and seen["marks"] == [] and list_incidents() == []


@pytest.mark.platforms("posix")
def test_unstarted_recovery_refuses_a_row_of_another_job(temp_home, monkeypatch):
    """A terminal row belonging to a DIFFERENT job grants no settlement and no cleanup."""
    import json

    from cron.executions import get_execution
    from cron.jobs import get_job
    from cron.scheduler_authority import journal_path

    from cron.jobs import claim_job_for_fire, create_job

    mine = create_job(prompt="work", schedule="in 30m", name="mine", run_lock=CONSUMER)
    claim_job_for_fire(mine["id"], return_job=True)
    other = create_job(prompt="work", schedule="in 30m", name="other-job")
    seed = textwrap.dedent(f"""
        import json
        from cron.executions import create_execution
        row = create_execution({other["id"]!r}, source="scheduled")
        print(json.dumps({{"id": row["id"]}}))
    """)
    env = {**os.environ, "HERMES_HOME": str(temp_home), "PYTHONPATH": str(_REPO_ROOT)}
    seeded = subprocess.run([sys.executable, "-c", seed], cwd=str(_REPO_ROOT), env=env,
                            capture_output=True, text=True, timeout=60, check=False)
    assert seeded.returncode == 0, seeded.stderr
    execution_id = json.loads(seeded.stdout.strip().splitlines()[-1])["id"]
    params = {"job_id": mine["id"], "request_id": execution_id, "extra_prompt": None}
    _write_journal(temp_home, mine["id"], execution_id,
                   {"params": params, "receipt": {"session_id": "s", "admission_id": "a"},
                    "consumer": {"name": CONSUMER, "request_id": execution_id},
                    "unstarted": True, "job": get_job(mine["id"])})
    _run_reconcile(monkeypatch, {"status": "terminal", "result": [False, "out", "", "not started"],
                                 "job": get_job(mine["id"]),
                                 "job_flags": {"_consumer_refusal": "not started"}})
    assert get_execution(execution_id)["status"] == "claimed", "another job's row is untouched"
    assert journal_path(mine["id"], execution_id).exists()
    assert get_job(mine["id"])["fire_claim"] is not None, "a mismatched receipt clears no claim"
