"""Behaviour tests for the optional per-job consumer run lock (B1 prerequisite).

``website/docs/developer-guide/consumer-run-lock-plan.md`` is the contract: one non-reentrant,
non-blocking mutex per (profile home, consumer name), acquired in ``run_one_job`` after the
external-worker handoff and before ``claim_dispatch``/preflight/agent work. Nothing here touches
a model, a network adapter, a live job or a real ``~/.hermes``: every home is a ``tmp_path`` and
every transport/agent seam is a fake.

The lock is observed the way the OS sees it: ``flock`` treats each open file description
independently, so a probe on a SECOND fd reports a lock this process already holds. That is what
``_os_lock_free`` measures — the real advisory lock, not the module's own bookkeeping.
"""
from __future__ import annotations

import contextlib
import errno
import hashlib
import os
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
    """Isolated HERMES_HOME: the cron store, both ledgers and the lock file land in the temp dir."""
    monkeypatch.setenv("HERMES_HOME", str(tmp_path))
    yield tmp_path


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------

def _os_lock_free(path: Path) -> bool:
    """True when nothing holds the advisory lock on *path* (probe on a fresh fd)."""
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


def _lock_path(home: Path, consumer: str = CONSUMER) -> Path:
    digest = hashlib.sha256(consumer.encode("utf-8")).hexdigest()
    return home / "cron" / f".consumer-run-{digest}.lock"


def _claim_one_shot(job_id: str):
    """Give one-shot *job_id* the runtime state the due scan and a manual fire leave behind:
    a ``run_claim``, the manual stamp, and this fire's ``fire_claim`` owner."""
    from cron.jobs import claim_job_for_fire, get_due_jobs, get_job, trigger_job

    trigger_job(job_id)
    due = [job for job in get_due_jobs() if job["id"] == job_id]
    assert due, "a manually triggered one-shot is due and carries the run claim"
    assert get_job(job_id)["run_claim"], "the due scan stamped the one-shot run claim"
    claimed = claim_job_for_fire(job_id, manual=True, return_job=True)
    assert isinstance(claimed, dict), "this fire won the claim"
    return claimed


def _with_execution(job: dict) -> dict:
    from cron.executions import create_execution

    job = dict(job)
    job["execution_id"] = create_execution(job["id"], source="probe")["id"]
    return job


def _fresh_fire(name: str, schedule: str = "every 5m") -> dict:
    """A job of this test's own, claimed for one fire, with its own execution row."""
    from cron.jobs import claim_job_for_fire, create_job

    job = create_job(prompt="work", schedule=schedule, name=name, run_lock=CONSUMER)
    claimed = claim_job_for_fire(job["id"], return_job=True)
    assert isinstance(claimed, dict), "this fire won the claim"
    return _with_execution(claimed)


@contextlib.contextmanager
def _another_thread_holds(consumer: str = CONSUMER):
    """Hold the consumer's lock on a sibling thread — genuine cross-thread contention, which is
    NOT the same-thread recursion case."""
    from cron.consumer_run_lock import consumer_run_lock

    holding, release = threading.Event(), threading.Event()

    def _holder():
        with consumer_run_lock(consumer) as acquired:
            assert acquired is True
            holding.set()
            release.wait(30)

    thread = threading.Thread(target=_holder, daemon=True)
    thread.start()
    try:
        assert holding.wait(10), "the sibling thread never took the lock"
        yield
    finally:
        release.set()
        thread.join(10)


def _silence_run_side_effects(monkeypatch):
    """Record notices/agent work so a test can assert they did NOT happen."""
    seen = {"agent": [], "sent": [], "marks": []}
    monkeypatch.setattr(s, "_launch_external_cron_worker", lambda job: False)
    monkeypatch.setattr(
        s, "run_job",
        lambda job, **kw: seen["agent"].append(job["id"]) or (True, "out", "final", None))
    monkeypatch.setattr(s, "save_job_output", lambda job_id, out: f"/tmp/{job_id}.md")
    monkeypatch.setattr(
        s, "_deliver_result",
        lambda job, content, **kw: seen["sent"].append(job["id"]) or None)
    monkeypatch.setattr(
        s, "mark_job_run", lambda *a, **kw: seen["marks"].append((a, kw)) or True)
    return seen


# ---------------------------------------------------------------------------
# the lock itself: path, recursion, threads, homes, processes
# ---------------------------------------------------------------------------

@pytest.mark.platforms("posix")
def test_lock_path_is_a_full_sha256_name_under_the_profile_cron_dir(temp_home):
    """The lock path is private, per profile, and hashes the consumer name with the full SHA-256
    (never the fire fence's UUID5 spelling)."""
    from cron.consumer_run_lock import consumer_lock_path

    path = consumer_lock_path(CONSUMER)
    assert path.parent == temp_home / "cron"
    assert hashlib.sha256(CONSUMER.encode("utf-8")).hexdigest() in path.name


def test_absent_or_blank_consumer_opens_no_lock_file(temp_home):
    """Absence and blank are the unchanged current behaviour: the context is a true no-op that
    never creates a lock file."""
    from cron.consumer_run_lock import consumer_run_lock

    for blank in ("", "   ", None):
        with consumer_run_lock(blank) as acquired:
            assert acquired is True
    assert not (temp_home / "cron").exists() or not list(
        (temp_home / "cron").glob(".consumer-run-*"))


def test_same_thread_recursion_raises_a_distinct_error(temp_home):
    """Re-entering one consumer on the same thread is a bug, not contention: it raises a distinct
    error instead of reporting the outer owner's lock as busy."""
    from cron.consumer_run_lock import (
        ConsumerRunLockRecursionError, consumer_run_lock,
    )

    with consumer_run_lock(CONSUMER) as acquired:
        assert acquired is True
        with pytest.raises(ConsumerRunLockRecursionError):
            with consumer_run_lock(CONSUMER):
                pass


def test_second_thread_of_one_consumer_contends(temp_home):
    """Threads serving one consumer collide: the second is told to skip."""
    from cron.consumer_run_lock import consumer_run_lock

    holding = threading.Event()
    release = threading.Event()

    def _holder():
        with consumer_run_lock(CONSUMER) as acquired:
            assert acquired is True
            holding.set()
            release.wait(10)

    thread = threading.Thread(target=_holder, daemon=True)
    thread.start()
    try:
        assert holding.wait(10)
        with consumer_run_lock(CONSUMER) as acquired:
            assert acquired is False
    finally:
        release.set()
        thread.join(10)
    with consumer_run_lock(CONSUMER) as acquired:
        assert acquired is True


def test_other_consumers_and_other_homes_are_independent(temp_home, tmp_path, monkeypatch):
    """One profile, two consumers: independent. One consumer name, two homes: independent."""
    from cron.consumer_run_lock import consumer_run_lock

    with consumer_run_lock("consumer-a") as first:
        assert first is True
        with consumer_run_lock("consumer-b") as second:
            assert second is True

    other_home = tmp_path / "other-profile"
    other_home.mkdir()
    with consumer_run_lock(CONSUMER) as here:
        assert here is True
        monkeypatch.setenv("HERMES_HOME", str(other_home))
        with consumer_run_lock(CONSUMER) as there:
            assert there is True


_CHILD_SOURCE = textwrap.dedent(
    """
    import sys, time
    from cron.consumer_run_lock import consumer_run_lock

    with consumer_run_lock(sys.argv[1]) as acquired:
        assert acquired is True, "child failed to acquire"
        print("held", flush=True)
        time.sleep(300)
    """
)


@pytest.mark.platforms("posix")
def test_native_subprocess_contention_and_death_release(temp_home):
    """Native POSIX: a second PROCESS contends, and killing the holder releases the OS lock
    (no TTL, no cleanup step in this process)."""
    from cron.consumer_run_lock import consumer_run_lock

    env = {**os.environ, "HERMES_HOME": str(temp_home), "PYTHONPATH": str(_REPO_ROOT)}
    child = subprocess.Popen(
        [sys.executable, "-c", _CHILD_SOURCE, CONSUMER],
        env=env, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
    )
    try:
        line = child.stdout.readline().strip()
        assert line == "held", child.stderr.read()
        with consumer_run_lock(CONSUMER) as acquired:
            assert acquired is False
    finally:
        child.kill()
        child.wait(timeout=30)

    deadline = time.monotonic() + 10
    acquired = False
    while not acquired and time.monotonic() < deadline:
        with consumer_run_lock(CONSUMER) as acquired:
            if not acquired:
                time.sleep(0.1)
    assert acquired is True, "process death must release the advisory lock"


# ---------------------------------------------------------------------------
# the seam: what the lock spans
# ---------------------------------------------------------------------------

@pytest.mark.platforms("posix")
def test_lock_spans_preflight_agent_save_and_delivery(temp_home, monkeypatch):
    """The lock is held across the pre-agent gate, the (stand-in) agent call, the saved output and
    delivery — and released once the run returns."""
    from cron.jobs import claim_job_for_fire, create_job
    from cron.consumer_run_lock import consumer_run_lock

    path = _lock_path(temp_home)
    (temp_home / "scripts").mkdir()
    script = temp_home / "scripts" / "preflight.py"
    script.write_text(textwrap.dedent(f'''
        import fcntl
        with open({str(path)!r}, "r+") as lock:
            try:
                fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError:
                print("producer lock held inside preflight")
            else:
                raise RuntimeError("producer lock was free inside preflight")
    '''))
    job = create_job(prompt="work", schedule="every 5m", name="span", run_lock=CONSUMER,
                     script=str(script))
    claimed = claim_job_for_fire(job["id"], return_job=True)
    assert isinstance(claimed, dict)
    claimed = _with_execution(claimed)
    observations = []
    real_prepare = s._prepare_job_prompt

    def _held(where):
        assert _os_lock_free(path) is False, f"consumer lock was free during {where}"
        observations.append(where)

    def fake_run_job(job_, *, defer_agent_teardown=None, **kw):
        # The REAL pre-agent gate runs inside the locked span. run_job itself is a stand-in: this
        # test must not build a model-backed agent, so the gate's own verdict is not used.
        early, prompt = real_prepare(job_, job_["id"], job_.get("name"), None, None)
        assert early is None and "producer lock held inside preflight" in prompt
        _held("preflight")
        _held("agent")
        return (False, "probe output", "probe final", "probe failure")

    monkeypatch.setattr(s, "_launch_external_cron_worker", lambda job_: False)
    monkeypatch.setattr(s, "run_job", fake_run_job)
    monkeypatch.setattr(s, "save_job_output", lambda job_id, out: _held("save") or "/tmp/x.md")
    monkeypatch.setattr(s, "_deliver_result", lambda *a, **kw: _held("deliver"))
    monkeypatch.setattr(s, "mark_job_run", lambda *a, **kw: _held("mark") or True)
    monkeypatch.setattr(s, "finish_execution", lambda *a, **kw: None)

    assert s.run_one_job(claimed) is True
    assert {"preflight", "agent", "save", "deliver"} <= set(observations)
    assert _os_lock_free(path) is True, "the lock must be released when the run returns"
    with consumer_run_lock(CONSUMER) as acquired:
        assert acquired is True


@pytest.mark.platforms("posix")
def test_lock_is_released_on_early_return_exception_and_hard_interrupt(temp_home, monkeypatch):
    """Every exit from the run releases the lock: the dispatch-limit early return, a raised
    exception, and a hard interrupt."""
    from cron.jobs import create_job

    path = _lock_path(temp_home)
    monkeypatch.setattr(s, "_launch_external_cron_worker", lambda job_: False)
    monkeypatch.setattr(s, "mark_job_run", lambda *a, **kw: True)
    monkeypatch.setattr(s, "finish_execution", lambda *a, **kw: None)
    monkeypatch.setattr(s, "_deliver_result", lambda *a, **kw: None)
    create_job(prompt="work", schedule="every 5m", name="exits")

    # (a) early return: the finite one-shot dispatch budget is already spent.
    monkeypatch.setattr(s, "claim_dispatch", lambda job_id: False)
    early = _fresh_fire("exits-early")
    assert s.run_one_job(early) is True
    assert _os_lock_free(path) is True, "early return must release the lock"

    # (b) exception out of the run.
    monkeypatch.setattr(s, "claim_dispatch", lambda job_id: True)
    monkeypatch.setattr(
        s, "run_job",
        lambda *a, **kw: (_ for _ in ()).throw(RuntimeError("probe boom")))
    raised = _fresh_fire("exits-raise")
    assert s.run_one_job(raised) is False
    assert _os_lock_free(path) is True, "a raised run must release the lock"

    # (c) hard interrupt.
    monkeypatch.setattr(
        s, "run_job", lambda *a, **kw: (_ for _ in ()).throw(KeyboardInterrupt()))
    interrupted = _fresh_fire("exits-interrupt")
    with pytest.raises(KeyboardInterrupt):
        s.run_one_job(interrupted)
    assert _os_lock_free(path) is True, "an interrupt must release the lock"


# ---------------------------------------------------------------------------
# contention: nothing starts, nothing leaks
# ---------------------------------------------------------------------------

def test_contention_spends_no_repeat_starts_nothing_and_leaves_no_claims(temp_home, monkeypatch):
    """A contended run must not reach claim_dispatch, the agent, delivery, the repeat budget or
    the incident ledger — and must retire only its own unstarted claims."""
    from cron.executions import get_execution
    from cron.incidents import list_incidents
    from cron.jobs import create_job, get_job

    seen = _silence_run_side_effects(monkeypatch)
    job = create_job(prompt="work", schedule="in 30m", name="contended", run_lock=CONSUMER)
    claimed = _with_execution(_claim_one_shot(job["id"]))
    execution_id = claimed["execution_id"]
    before = get_job(job["id"])["next_run_at"]

    with _another_thread_holds():
        assert s.run_one_job(claimed) is True  # processed: skipped, not an error

    stored = get_job(job["id"])
    assert seen["agent"] == [] and seen["sent"] == [] and seen["marks"] == []
    assert list_incidents() == []
    # this owner's claims are retired
    assert stored["fire_claim"] is None
    assert stored["run_claim"] is None
    assert "manual_run_at" not in stored and "manual_run_prompt" not in stored
    # everything else is untouched
    assert stored["repeat"]["completed"] == 0, "a contended one-shot must not spend a dispatch"
    assert stored["last_status"] is None and stored["last_run_at"] is None
    assert stored["failure_streak"] == 0 and stored["last_error"] is None
    assert stored["next_run_at"] == before
    assert stored.get("preflight_alerted") is None
    row = get_execution(execution_id)
    assert row["status"] == "failed"
    assert "not started" in row["error"] and CONSUMER in row["error"]


def test_contention_preserves_a_newer_owner_and_a_missing_owner_never_clears(temp_home):
    """Fencing: a stale snapshot must not erase the claims a NEWER owner holds, and an absent
    owner (a directly handed-in job dict) authorises nothing."""
    from cron.jobs import (
        _jobs_lock, create_job, get_job, load_jobs, release_unstarted_fire, save_jobs,
    )
    from hermes_time import now as _now

    job = create_job(prompt="work", schedule="in 30m", name="fenced", run_lock=CONSUMER)
    claimed = _claim_one_shot(job["id"])
    stale_owner = claimed["fire_claim"]["by"]

    # A newer producer takes the fire claim over (a reclaimed lease after a crash).
    with _jobs_lock():
        jobs = load_jobs()
        for record in jobs:
            if record["id"] == job["id"]:
                record["fire_claim"] = {"at": _now().isoformat(), "by": "newer-owner-token"}
        save_jobs(jobs)

    assert release_unstarted_fire(job["id"], stale_owner) is False
    stored = get_job(job["id"])
    assert stored["fire_claim"] == {"at": stored["fire_claim"]["at"], "by": "newer-owner-token"}
    assert stored.get("manual_run_at") and stored.get("run_claim")

    # No owner at all (direct handed dict): never clear another producer's claim.
    assert release_unstarted_fire(job["id"], "") is False
    assert release_unstarted_fire(job["id"], None) is False
    assert get_job(job["id"])["fire_claim"]["by"] == "newer-owner-token"


def test_recursive_run_raises_before_touching_claims_or_the_ledger(temp_home, monkeypatch):
    """Same-thread recursion inside a live run is refused loudly: no claim is retired, no ledger
    row is written, and the outer owner's claim survives."""
    from cron.executions import get_execution
    from cron.jobs import create_job, get_job
    from cron.consumer_run_lock import ConsumerRunLockRecursionError, consumer_run_lock

    job = create_job(prompt="work", schedule="in 30m", name="recursive", run_lock=CONSUMER)
    claimed = _with_execution(_claim_one_shot(job["id"]))
    owner = claimed["fire_claim"]["by"]
    seen = _silence_run_side_effects(monkeypatch)

    with consumer_run_lock(CONSUMER) as holder:
        assert holder is True
        with pytest.raises(ConsumerRunLockRecursionError):
            s.run_one_job(claimed)

    stored = get_job(job["id"])
    assert stored["fire_claim"]["by"] == owner, "the outer owner keeps its claim"
    assert stored.get("run_claim") and stored.get("manual_run_at")
    assert seen["agent"] == [] and seen["sent"] == []
    assert get_execution(claimed["execution_id"])["status"] == "claimed"


def test_backend_refusal_is_recorded_as_a_failed_run_not_contention(temp_home, monkeypatch):
    """A refused lock (backend/I-O) is a hard, precise failure: the run does not start, it is
    recorded as failed, and it is never reported as clean contention."""
    from cron.executions import get_execution
    from cron.incidents import list_incidents
    from cron.jobs import claim_job_for_fire, create_job, get_job
    import cron.consumer_run_lock as crl

    job = create_job(prompt="work", schedule="every 5m", name="refused", run_lock=CONSUMER)
    claimed = claim_job_for_fire(job["id"], return_job=True)
    assert isinstance(claimed, dict)
    claimed = _with_execution(claimed)
    seen = _silence_run_side_effects(monkeypatch)

    import contextlib as _contextlib

    @_contextlib.contextmanager
    def _refusing(consumer):
        raise crl.ConsumerRunLockError(f"consumer run lock unavailable for {consumer!r}: boom")
        yield  # pragma: no cover

    monkeypatch.setattr(crl, "consumer_run_lock", _refusing)

    assert s.run_one_job(claimed) is True
    assert seen["agent"] == [] and seen["sent"] == []
    assert list_incidents() == []
    row = get_execution(claimed["execution_id"])
    assert row["status"] == "failed"
    assert "boom" in row["error"] and "not started" in row["error"]
    assert get_job(job["id"])["fire_claim"] is None


@pytest.mark.platforms("posix")
def test_io_failure_refuses_instead_of_reporting_contention(temp_home):
    """An unwritable lock directory is an I/O refusal: it raises instead of masquerading as a
    clean 'someone else is running' skip."""
    from cron.consumer_run_lock import ConsumerRunLockError, consumer_run_lock

    (temp_home / "cron").write_text("not a directory", encoding="utf-8")
    with pytest.raises(ConsumerRunLockError) as refusal:
        with consumer_run_lock(CONSUMER):
            pass  # pragma: no cover - the acquire refuses
    assert CONSUMER in str(refusal.value) and isinstance(refusal.value, OSError)


def test_unsupported_posix_backend_refuses_rather_than_blocking(temp_home, monkeypatch):
    """Where the POSIX flock helper cannot be used, the lock fails closed with an explicit
    unsupported refusal — it never falls through to the blocking Windows helper."""
    import cron.consumer_run_lock as crl

    monkeypatch.setattr(crl, "_posix_flock_available", lambda: False)
    with pytest.raises(crl.ConsumerRunLockUnsupportedError) as refusal:
        with crl.consumer_run_lock(CONSUMER):
            pass  # pragma: no cover - the acquire refuses
    assert "posix" in str(refusal.value).lower()


@pytest.mark.platforms("windows")
def test_windows_native_arm_fails_closed_until_a_nonblocking_backend_exists(temp_home):
    """NATIVE Windows: opt-in jobs fail closed (no nonblocking backend is verified there) while
    unmodified jobs keep running unchanged."""
    from cron.consumer_run_lock import ConsumerRunLockUnsupportedError, consumer_run_lock

    with pytest.raises(ConsumerRunLockUnsupportedError):
        with consumer_run_lock(CONSUMER):
            pass  # pragma: no cover - the acquire refuses on this host
    with consumer_run_lock("") as acquired:
        assert acquired is True


# ---------------------------------------------------------------------------
# producers cross the seam
# ---------------------------------------------------------------------------

@pytest.mark.platforms("posix")
def test_manual_and_provider_producers_both_cross_the_lock_seam(
    temp_home, monkeypatch, make_cron_provider,
):
    """Both producers route through the shared seam, so both are serialized by the consumer lock
    with the model, delivery and worker launch disabled."""
    from cron.jobs import create_job, get_job
    from tools.cronjob_tools import _execute_job_now

    path = _lock_path(temp_home)
    monkeypatch.setattr(s, "_launch_external_cron_worker", lambda job_: False)
    monkeypatch.setattr(s, "save_job_output", lambda job_id, out: f"/tmp/{job_id}.md")
    monkeypatch.setattr(s, "_deliver_result", lambda *a, **kw: None)
    monkeypatch.setattr(s, "mark_job_run", lambda *a, **kw: True)
    monkeypatch.setattr(s, "finish_execution", lambda *a, **kw: None)

    observations = []

    def fake_run_job(job_, **kw):
        observations.append(_os_lock_free(path))
        return (True, "out", "final", None)

    monkeypatch.setattr(s, "run_job", fake_run_job)

    provider_job = create_job(prompt="work", schedule="every 5m", name="provider", run_lock=CONSUMER)
    make_cron_provider().fire_due(provider_job["id"], manual=True)
    assert observations == [False], "the provider's run must hold the consumer lock"
    assert _os_lock_free(path) is True

    manual_job = create_job(prompt="work", schedule="every 5m", name="manual", run_lock=CONSUMER)
    _execute_job_now(get_job(manual_job["id"]))
    assert observations == [False, False], "the manual run must hold the consumer lock"
    assert _os_lock_free(path) is True


# ---------------------------------------------------------------------------
# the stored field: inert when unused, preserved when set
# ---------------------------------------------------------------------------

def test_optional_field_absent_blank_unset_round_trips_and_survives_merge(temp_home):
    """Optional-text semantics: only a non-blank string is stored; load/get/list read it back;
    blank and non-strings leave a legacy record byte-identical; update can set and clear it."""
    from cron.job_definition import merge_job_definition
    from cron.jobs import create_job, get_job, list_jobs, update_job

    plain = create_job(prompt="p", schedule="every 5m", name="plain")
    assert "run_lock" not in plain and "run_lock" not in get_job(plain["id"])

    for quiet in ("", "   ", 7, ["x"], None):
        record = create_job(prompt="p", schedule="every 5m", name=f"quiet-{quiet!r}",
                            run_lock=quiet)
        assert "run_lock" not in record

    locked = create_job(prompt="p", schedule="every 5m", name="locked", run_lock="  alpha  ")
    assert locked["run_lock"] == "alpha"
    assert get_job(locked["id"])["run_lock"] == "alpha"
    assert next(j for j in list_jobs() if j["id"] == locked["id"])["run_lock"] == "alpha"

    assert update_job(locked["id"], {"run_lock": "beta"})["run_lock"] == "beta"
    assert get_job(locked["id"])["run_lock"] == "beta"
    assert update_job(locked["id"], {"run_lock": "   "})["run_lock"] is None

    merged = merge_job_definition(
        {"id": "imported", "enabled": False, "state": "paused",
         "schedule": {"kind": "interval", "minutes": 5}},
        {"run_lock": "shipped-consumer"})
    assert merged["run_lock"] == "shipped-consumer"


def test_optional_field_changes_no_runtime_or_toolset_resolution(temp_home, monkeypatch):
    """The field is inert: provider, model, base URL, fallback chain and toolsets resolve
    identically with and without it."""
    from cron.scheduler import (
        _job_fallback_chain, _load_cron_job_config, _resolve_cron_enabled_toolsets,
        _resolve_job_runtime,
    )

    recorded = []

    def fake_resolve(**kwargs):
        recorded.append(dict(kwargs))
        return {"provider": "probe", "model": kwargs.get("target_model")}

    monkeypatch.setattr("hermes_cli.runtime_provider.resolve_runtime_provider", fake_resolve)

    base = {"id": "runtime", "name": "runtime", "prompt": "p",
            "model": "probe-model", "provider": None, "base_url": None}
    with_lock = {**base, "run_lock": CONSUMER}
    jc = _load_cron_job_config(base, "runtime", "runtime")

    assert _resolve_job_runtime(base, "runtime", jc) == _resolve_job_runtime(
        with_lock, "runtime", jc)
    assert recorded[0] == recorded[1]

    cfg: dict = {}
    assert _resolve_cron_enabled_toolsets(base, cfg) == _resolve_cron_enabled_toolsets(
        with_lock, cfg)
    assert _job_fallback_chain(base, cfg) == _job_fallback_chain(with_lock, cfg)


@pytest.mark.platforms("posix")
@pytest.mark.parametrize("failure", [errno.EIO, errno.ENOLCK, errno.EBADF])
def test_flock_backend_errors_are_not_contention(temp_home, monkeypatch, failure):
    from cron.jobs import fcntl
    from cron.consumer_run_lock import ConsumerRunLockError, consumer_run_lock

    def fail(*args):
        raise OSError(failure, "probe backend failure")

    monkeypatch.setattr(fcntl, "flock", fail)
    with pytest.raises(ConsumerRunLockError, match="probe backend failure"):
        with consumer_run_lock(CONSUMER):
            pass


@pytest.mark.platforms("posix")
def test_thread_contention_never_calls_the_os_backend(temp_home, monkeypatch):
    from cron.jobs import fcntl
    from cron.consumer_run_lock import consumer_run_lock

    with _another_thread_holds():
        def unexpected(*args):
            pytest.fail("local contention must not evaluate the OS backend")
        with monkeypatch.context() as patch:
            patch.setattr(fcntl, "flock", unexpected)
            with consumer_run_lock(CONSUMER) as acquired:
                assert acquired is False


@pytest.mark.platforms("posix")
def test_lock_file_is_private(temp_home):
    import stat
    from cron.consumer_run_lock import consumer_run_lock

    with consumer_run_lock(CONSUMER):
        assert stat.S_IMODE(_lock_path(temp_home).stat().st_mode) == 0o600


@pytest.mark.platforms("posix")
def test_direct_recursion_creates_no_execution_or_handoff(temp_home, monkeypatch):
    from cron.executions import list_executions
    from cron.jobs import create_job
    from cron.consumer_run_lock import ConsumerRunLockRecursionError, consumer_run_lock

    job = create_job(prompt="work", schedule="every 5m", run_lock=CONSUMER)
    handoffs = []
    monkeypatch.setattr(s, "_launch_external_cron_worker", lambda job: handoffs.append(job) or False)
    before = list_executions(job_id=job["id"])
    with consumer_run_lock(CONSUMER):
        with pytest.raises(ConsumerRunLockRecursionError):
            s.run_one_job(job)
    assert list_executions(job_id=job["id"]) == before
    assert "execution_id" not in job and handoffs == []


def test_unstarted_cleanup_preserves_a_later_manual_trigger(temp_home, monkeypatch):
    from cron.jobs import create_job, get_job, trigger_job

    job = create_job(prompt="work", schedule="in 30m", run_lock=CONSUMER)
    claimed = _with_execution(_claim_one_shot(job["id"]))
    _silence_run_side_effects(monkeypatch)
    later = trigger_job(job["id"], extra_prompt="newer operator request")
    with _another_thread_holds():
        assert s.run_one_job(claimed) is True
    stored = get_job(job["id"])
    assert stored["fire_claim"] is None
    assert stored["manual_run_at"] == later["manual_run_at"]
    assert stored["manual_run_prompt"] == "newer operator request"
    assert stored["next_run_at"] == later["next_run_at"]


@pytest.mark.platforms("posix")
def test_parent_handoff_never_acquires_consumer_lock(temp_home, monkeypatch):
    from cron.jobs import create_job

    job = create_job(prompt="work", schedule="every 5m", run_lock=CONSUMER)
    def unexpected(*args):
        pytest.fail("parent must not acquire the consumer lock before handing off")
    monkeypatch.setattr(s, "consumer_run_guard", unexpected)
    monkeypatch.setattr(s, "_launch_external_cron_worker", lambda job: True)
    monkeypatch.setattr("cron.scheduler_worker_failure.record_unknown_worker_outcome", lambda *a, **kw: None)
    assert s.run_one_job(job) is True


def test_contention_keeps_the_one_shot_outage_marker(temp_home, monkeypatch):
    from cron.jobs import _jobs_lock, create_job, get_job, load_jobs, save_jobs

    job = create_job(prompt="work", schedule="in 30m", run_lock=CONSUMER)
    claimed = _with_execution(_claim_one_shot(job["id"]))
    claimed["run_claim"]["outage"] = True
    with _jobs_lock():
        jobs = load_jobs()
        record = next(j for j in jobs if j["id"] == job["id"])
        record["run_claim"] = claimed["run_claim"].copy()
        save_jobs(jobs)
    _silence_run_side_effects(monkeypatch)
    with _another_thread_holds():
        assert s.run_one_job(claimed) is True
    assert get_job(job["id"])["run_claim"] == {"outage": True}
