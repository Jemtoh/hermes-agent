"""One non-reentrant, non-blocking mutex per (profile home, consumer name).

An optional stored-job field ``run_lock`` names a *consumer*; every producer of that consumer —
the ticker, an external provider fire, a manual/tool run, a detached worker — must not overlap
another producer of the SAME consumer. The mutex is a process-local ``threading.Lock`` (in-process
threads) plus an OS advisory ``flock`` on a private file under the owning profile's cron directory
(other processes, including detached workers). The OS releases the advisory lock when the holder
dies, so there is no TTL and nothing to clean up; the lock file is deliberately never unlinked.

Absent or blank means NO lock: nothing is opened and the context yields True immediately, so an
unmodified job behaves exactly as before.

The shared acquisition helper masks all OS errors as contention. This feature uses
POSIX LOCK_NB directly to distinguish backend refusal. Where ``fcntl`` is unavailable the lock fails closed with an explicit unsupported refusal:
the caller must record a refused computation, never run unlocked.

Contract and rationale: website/docs/developer-guide/consumer-run-lock-plan.md
"""
from __future__ import annotations

import contextlib
import errno
import json
import os
import hashlib
import logging
import threading
import time
from pathlib import Path
from typing import Any, Iterator, Optional

logger = logging.getLogger(__name__)

# The local guard, keyed by lock path (which already carries the profile home + the consumer hash).
# A guard is never removed: another thread may still hold a reference to it, and a fresh Lock for
# the same key would hand two threads "the same" mutex.
_guards: dict[str, threading.Lock] = {}
# Every THREAD currently participating in a lock path: the firer, plus any owner/monitor thread
# that joined the same fire. Recursion is judged per thread, so a nested run from a JOINED thread
# must be refused exactly like one from the firer.
_guard_owners: dict[str, set] = {}
_guards_mutex = threading.Lock()

# The OPEN hold for a lock path. One fd per path carries the OS lock; ``depth`` lets the same fire
# join it from another thread (the canonical owner thread) instead of colliding with its own firer,
# and lets an abandoned conversation keep it alive after the caller's span ends.
_holds: dict[str, dict] = {}

_NOT_STARTED = "the computation was not started."


class ConsumerRunLockError(OSError):
    """The consumer lock could not be evaluated at all (backend or I/O refusal). Fail closed."""


class ConsumerRunLockUnsupportedError(ConsumerRunLockError):
    """No usable non-blocking lock backend on this platform (e.g. native Windows). Fail closed."""


class ConsumerRunLockRecursionError(RuntimeError):
    """This thread already owns the consumer's mutex: a nested run must not proceed.

    Distinct from contention on purpose — the outer owner is THIS run, so treating it as ordinary
    contention would retire the outer fire's claims.
    """


def _posix_flock_available() -> bool:
    """Whether the shared flock helper has its non-blocking branch on this host."""
    from cron.jobs import fcntl

    return fcntl is not None


def consumer_lock_path(consumer: str) -> Path:
    """Private lock file for *consumer*, under the ACTIVE profile's cron directory.

    Resolved at call time like every other cron path; the name is hashed with the FULL SHA-256
    (never the fire fence's truncated UUID5) so the filename leaks nothing about the consumer.
    """
    from cron.jobs import _current_cron_store

    digest = hashlib.sha256(consumer.encode("utf-8")).hexdigest()
    return _current_cron_store().cron_dir.resolve() / f".consumer-run-{digest}.lock"


def _holder_text(lock_fd) -> str:
    """The execution id stamped in the lock file, read through a HELD descriptor."""
    try:
        lock_fd.seek(0)
        return str(lock_fd.read() or b"", "utf-8", "replace").strip()
    except (OSError, ValueError):
        return ""


def _write_holder(lock_fd, execution_id: Optional[str]) -> None:
    """Record/discharge the holder identity on the lock file (same fd that owns the OS lock)."""
    try:
        lock_fd.seek(0)
        lock_fd.truncate()
        if execution_id:
            lock_fd.write(str(execution_id).encode("utf-8"))
        lock_fd.flush()
    except OSError:  # identity is an aid, never the lock itself
        logger.debug("Consumer run lock: holder stamp failed", exc_info=True)


def _register_thread(key: str, ident: int) -> None:
    with _guards_mutex:
        _guard_owners.setdefault(key, set()).add(ident)


def _unregister_thread(key: str) -> None:
    with _guards_mutex:
        threads = _guard_owners.get(key)
        if threads is not None:
            threads.discard(threading.get_ident())
            if not threads:
                _guard_owners.pop(key, None)


def _drop_hold(key: str) -> None:
    """Release one reference; the OS lock goes when the last reference does."""
    from cron.jobs import _release_flock

    with _guards_mutex:
        hold = _holds.get(key)
        if hold is None:
            return
        hold["depth"] -= 1
        if hold["depth"] > 0:
            return
        _holds.pop(key, None)
    try:
        # The identity text is NOT cleared: the id is only ever trusted under a held shared lock,
        # and a fresh execution overwrites it while holding the exclusive lock.
        _release_flock(hold["fd"])
    except OSError:
        logger.debug("Consumer run lock: release failed for %s", key, exc_info=True)


def keep_until(consumer: str, execution_id: Optional[str], future) -> bool:
    """Keep this process's hold until *future* finishes, so an abandoned conversation stays exclusive.

    A cancel or an inactivity interrupt can return from the run while the agent's conversation is
    still executing; ``run_job`` hands that future to its deferred-teardown callback. Releasing the
    consumer at that point would let a second producer overlap live work, so the last reference is
    dropped when the future completes instead. False when this process holds nothing to keep.
    """
    name = consumer.strip() if isinstance(consumer, str) else ""
    if not name or future is None or not execution_id:
        return False
    key = str(consumer_lock_path(name))
    with _guards_mutex:
        hold = _holds.get(key)
        if hold is None or hold["execution_id"] != str(execution_id):
            return False
        hold["depth"] += 1
    future.add_done_callback(lambda _f: _drop_hold(key))
    return True


_JOIN_ATTEMPTS = 200
_JOIN_PAUSE = 0.001


def _lock_fd(lock_fd, name: str, fire: str):
    """Take this process's REAL OS lock for one fire; ``None`` means clean contention.

    ``fire`` (an execution id) makes the hold shareable by the SAME fire in other processes: a
    fresh execution wins ``LOCK_EX`` non-blocking, stamps its id, DOWNGRADES to ``LOCK_SH`` and
    revalidates the id it can still read, so the id is only ever trusted while this process holds
    a lock. A second participant never grants itself anything from unlocked file text: it takes
    its own ``LOCK_SH`` and re-reads the id while holding it. A different execution is refused.

    Every participant keeps its own descriptor, so one process dying cannot free another's
    exclusion — the firer and the owner (and the save/future spans) each hold their own.
    No-id locks are exclusive only and are never shared by identity.
    """
    from cron.jobs import fcntl

    if not fire:
        try:
            fcntl.flock(lock_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError as exc:
            if exc.errno not in {errno.EAGAIN, errno.EACCES}:
                raise ConsumerRunLockError(
                    f"Consumer run lock {name!r} could not be taken: {exc}") from exc
            return None
        return lock_fd

    try:
        fcntl.flock(lock_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except OSError as exc:
        if exc.errno not in {errno.EAGAIN, errno.EACCES}:
            raise ConsumerRunLockError(
                f"Consumer run lock {name!r} could not be taken: {exc}") from exc
        return _join_fd(lock_fd, name, fire)
    _write_holder(lock_fd, fire)
    try:
        # Nonblocking downgrade: nobody can hold SH while we hold EX, so this cannot wait on a
        # live peer — if it ever refuses, the descriptor is dropped instead of blocking the caller.
        fcntl.flock(lock_fd, fcntl.LOCK_SH | fcntl.LOCK_NB)
    except OSError:
        return None
    if _holder_text(lock_fd) != fire:            # revalidate under OUR OWN shared lock
        return None
    return lock_fd


def _join_fd(lock_fd, name: str, fire: str):
    """Join an EXISTING hold of *fire*: shared lock first, identity read while holding it."""
    from cron.jobs import fcntl

    for _ in range(_JOIN_ATTEMPTS):
        try:
            fcntl.flock(lock_fd, fcntl.LOCK_SH | fcntl.LOCK_NB)
        except OSError as exc:
            if exc.errno in {errno.EAGAIN, errno.EACCES}:
                # An exclusive holder is mid-claim or mid-downgrade; the identity is not readable.
                time.sleep(_JOIN_PAUSE)
                continue
            raise ConsumerRunLockError(
                f"Consumer run lock {name!r} could not be joined: {exc}") from exc
        holder = _holder_text(lock_fd)
        if holder and holder == fire:
            return lock_fd
        fcntl.flock(lock_fd, fcntl.LOCK_UN)
        return None
    return None


def _journal_identity(record, path):
    """``(consumer, request_id)`` an admission journal names, or ``ValueError`` when unsafe.

    A journal may only authorize/refuse a run when its identity RESOLVES: the params identity and
    the filename must agree, and the consumer comes either from the frozen stamp or — for a
    journal written before that stamp existed — from the job it names (its frozen record, else the
    current stored job). An unrelated job with no ``run_lock`` resolves to ``''`` (no consumer) and
    blocks nobody; anything unknown is corrupt, and corrupt never authorizes another run.
    """
    from cron.scheduler_authority import journal_path

    if not isinstance(record, dict) or not isinstance(record.get("params"), dict):
        raise ValueError("malformed record")
    params = record["params"]
    job_id, request_id = params.get("job_id"), params.get("request_id")
    if not (isinstance(job_id, str) and job_id and isinstance(request_id, str) and request_id):
        raise ValueError("missing params identity")
    if path != journal_path(job_id, request_id):
        raise ValueError("journal identity conflict")
    consumer = record.get("consumer")
    if consumer is not None:
        if not isinstance(consumer, dict):
            raise ValueError("malformed consumer stamp")
        name = consumer.get("name")
        if not (isinstance(name, str) and name) or consumer.get("request_id") != request_id:
            raise ValueError("malformed consumer identity")
        return name, request_id
    job = record.get("job") if isinstance(record.get("job"), dict) else None
    if job is not None and job.get("id") != job_id:
        raise ValueError("frozen job identity conflict")
    if job is None:
        from cron.jobs import get_job

        job = get_job(job_id)
    if job is None:
        raise ValueError("unattributable journal")
    return _consumer_name(job), request_id


def _unresolved_admission(name: str, fire: str) -> Optional[str]:
    """A durable admission for a DIFFERENT execution of *name* is still unresolved.

    The OS lock is released when a firer dies, so the lock alone cannot carry exclusion across
    owner-loss until the report is saved. The firer's admission journal can: it is written before
    submit while that firer's OS hold is owned, and it is removed only by the completed/terminal
    bookkeeping that ends the attempt. Read under the consumer's OS lock, so no firer can add a
    journal concurrently; the SAME execution is always admitted (owner join, receipt recovery).
    Stale journals are never taken over by age or pid: existing reconciliation settles them.
    """
    from hermes_constants import get_hermes_home

    root = get_hermes_home() / "cron" / "admissions"
    try:
        paths = sorted(root.glob("*.json"))
    except OSError as exc:
        raise ConsumerRunLockError(
            f"Consumer run lock {name!r} could not read {root}: {exc}") from exc
    for path in paths:
        try:
            record = json.loads(path.read_text(encoding="utf-8-sig"))
        except (OSError, ValueError) as exc:
            # Cannot attribute it, so it COULD be this consumer's: fail closed, never run unlocked.
            return (f"Consumer run lock {name!r} has an unreadable admission journal "
                    f"{path.name!r} ({exc}); {_NOT_STARTED}")
        try:
            other_name, other = _journal_identity(record, path)
        except (OSError, ValueError) as exc:
            # Unknown/corrupt identity cannot authorize a run either: fail closed.
            return (f"Consumer run lock {name!r} has an unattributable admission journal "
                    f"{path.name!r} ({exc}); {_NOT_STARTED}")
        if other_name != name or other == fire:
            continue  # another consumer / no consumer, or this very execution (owner+recovery)
        return (f"Consumer run lock {name!r} has an unresolved admission journal for execution "
                f"{other!r}; {_NOT_STARTED}")
    return None


@contextlib.contextmanager
def consumer_run_lock(consumer: str, *, execution_id: Optional[str] = None) -> Iterator[bool]:
    """Non-blocking acquire of *consumer*'s mutex.

    Yields True when this caller owns it, False when another live holder does (clean contention).
    Raises ``ConsumerRunLockRecursionError`` when THIS thread already owns it — the caller must not
    treat that as contention — and ``ConsumerRunLockError`` (or its unsupported subclass) when the
    lock cannot be evaluated, where the caller must fail closed rather than run unlocked.

    ``execution_id`` identifies the FIRE. The same fire re-entering from another thread (the
    canonical owner executing what its firer admitted) joins the open hold instead of colliding
    with it, and a firer holding the lock in another process is recognised by its stamp on the
    lock file, so the owner neither refuses its own fire nor runs outside it.
    """
    from cron.jobs import ensure_dirs, fcntl

    name = consumer.strip() if isinstance(consumer, str) else ""
    if not name:
        yield True  # absent/blank: unchanged behaviour, no file opened
        return

    key = str(consumer_lock_path(name))
    fire = str(execution_id) if execution_id else ""
    with _guards_mutex:
        if threading.get_ident() in _guard_owners.get(key, ()):
            raise ConsumerRunLockRecursionError(
                f"Consumer run lock {name!r} is already held by this thread; a nested run of one "
                f"consumer cannot proceed.")
    if fire:
        joined = False
        with _guards_mutex:
            held = _holds.get(key)
            if held is not None and held["execution_id"] == fire:
                held["depth"] += 1
                joined = True
        if joined:
            _register_thread(key, threading.get_ident())
            try:
                yield True
            finally:
                _unregister_thread(key)
                _drop_hold(key)
            return

    recursive = False
    with _guards_mutex:
        guard = _guards.setdefault(key, threading.Lock())
        owns_guard = guard.acquire(blocking=False)
        if owns_guard:
            # Registered directly: _guards_mutex is already held here and is NOT reentrant, so
            # calling _register_thread (which re-acquires it) deadlocks this thread for ever.
            _guard_owners.setdefault(key, set()).add(threading.get_ident())
        else:
            recursive = threading.get_ident() in _guard_owners.get(key, ())
    if recursive:
        raise ConsumerRunLockRecursionError(
            f"Consumer run lock {name!r} is already held by this thread; a nested run of one "
            f"consumer cannot proceed.")

    if not owns_guard:
        yield False
        return

    try:
        if not _posix_flock_available():
            raise ConsumerRunLockUnsupportedError(
                f"Consumer run lock {name!r} is unsupported on this platform: no non-blocking "
                f"POSIX flock backend is available, so an opt-in consumer job fails closed.")
        try:
            ensure_dirs()
            fd = os.open(consumer_lock_path(name), os.O_RDWR | os.O_CREAT, 0o600)
            lock_fd = open(fd, "r+b")
        except OSError as exc:
            raise ConsumerRunLockError(
                f"Consumer run lock {name!r} could not be opened: {exc}") from exc
        try:
            acquired = _lock_fd(lock_fd, name, fire)
        except BaseException:
            lock_fd.close()
            raise
        if acquired is None:
            lock_fd.close()
            yield False
            return
        conflict = _unresolved_admission(name, fire)
        if conflict:
            lock_fd.close()
            raise ConsumerRunLockError(conflict)
        with _guards_mutex:
            _holds[key] = {"fd": acquired, "depth": 1, "execution_id": fire}
        try:
            yield True
        finally:
            _drop_hold(key)
    finally:
        if owns_guard:
            _unregister_thread(key)
            guard.release()


def _consumer_name(job: Any) -> str:
    """The job's ``run_lock`` consumer name; ``""`` for absent/blank/non-text (no lock)."""
    if not isinstance(job, dict):
        return ""
    value = job.get("run_lock")
    return value.strip() if isinstance(value, str) else ""


def _record_unstarted(job: dict, execution_id: Optional[str], reason: str) -> None:
    """Retire this owner's unstarted claims and close the run's execution row as failed.

    Best-effort, without notifications; the ledger helper handles expected SQLite/I/O errors.
    """
    from cron.executions import settle_unstarted_execution

    job_id = str(job.get("id") or "")
    claim = job.get("fire_claim")
    owner = str(claim.get("by") or "") if isinstance(claim, dict) else ""
    try:
        release_unstarted_fire(
            job_id, owner, expected_manual_run_at=job.get("manual_run_at"),
            expected_run_claim=job.get("run_claim"))
    except Exception:
        logger.warning(
            "Consumer run lock: could not retire unstarted claims for job %s", job_id,
            exc_info=True)
    if execution_id:
        settle_unstarted_execution(str(execution_id), job_id, reason)


@contextlib.contextmanager
def consumer_run_guard(job: dict, execution_id: Optional[str], *, settle: bool = True) -> Iterator[bool]:
    """Wrap one producer run with its consumer's mutex (the ``run_one_job`` seam).

    Yields True when the run may proceed — the lock is then held across the whole computation,
    preflight → agent → saved output — and False when it must be skipped: another producer holds
    the consumer's lock, or the lock refused (backend/I-O). A skip runs no script/model/send,
    spends no repeat budget, raises no notice/incident, and retires only this owner's unstarted
    claims (fire claim, manual stamp, one-shot run claim) while closing the execution row as failed.

    Recursion is NOT caught: a nested run must never retire its outer owner's claims, so
    ``ConsumerRunLockRecursionError`` propagates to the caller.

    ``settle=False``: the caller only takes the mutex and reports the verdict. The canonical owner
    uses it because the LEDGER ROW BELONGS TO THE FIRER — possibly in another process — so the
    firer performs the owner-fenced unstarted settlement from the typed verdict it receives.
    """
    name = _consumer_name(job)
    if not name:
        yield True  # no run_lock: unchanged behaviour, no lock, no bookkeeping
        return

    refused: Optional[str] = None
    with contextlib.ExitStack() as stack:
        try:
            acquired = stack.enter_context(
                consumer_run_lock(name, execution_id=execution_id))
        except ConsumerRunLockRecursionError:
            raise
        except ConsumerRunLockError as exc:
            acquired, refused = False, str(exc)
        else:
            if acquired:
                yield True
                return
        reason = (
            f"Consumer run lock {name!r} refused: {refused}; {_NOT_STARTED}" if refused else
            f"Consumer run lock {name!r} is held by another producer; {_NOT_STARTED}")
    # Outside the lock: the skip's own bookkeeping, then the verdict.
    job["_consumer_refusal_reason"] = reason
    if settle:
        _record_unstarted(job, execution_id, reason)
    yield False


def reject_recursive_consumer_run(job: dict) -> None:
    """Refuse same-thread reentry before a direct run creates a receipt or hands off."""
    name = _consumer_name(job)
    if not name:
        return
    with _guards_mutex:
        if threading.get_ident() in _guard_owners.get(str(consumer_lock_path(name)), ()):
            raise ConsumerRunLockRecursionError(
                f"Consumer run lock {name!r} is already held by this thread.")


def consumer_hold_survives_run(consumer: str, execution_id: Optional[str], future) -> bool:
    """Keep the consumer held while an interrupted conversation is still alive."""
    return keep_until(consumer, execution_id, future)


def refusal_reason(job: dict) -> str:
    """The precise reason a consumer refused this fire (matches ``consumer_run_guard``)."""
    refused = job.get("_consumer_refusal_reason")
    if refused:
        return str(refused)
    name = _consumer_name(job)
    return f"Consumer run lock {name!r} is held by another producer; {_NOT_STARTED}"


def release_unstarted_fire(
    job_id: str, expected_owner: Optional[str], *,
    expected_manual_run_at: Optional[str] = None, expected_run_claim: Optional[dict] = None,
) -> bool:
    """Clear only this fire's claims; preserve later operator triggers and outage markers.

    The operator's run-now request is never retired here: the computation did not start, so
    ``manual_run_at`` and its ``manual_run_prompt`` stay pending AND become runnable again — the
    claim already restored ``next_run_at`` to the future scheduled slot, which would otherwise
    park the request behind that slot. ``manual_run_restore_at`` keeps the overwritten slot (the
    shape ``trigger_job`` leaves) so the retry is an EXTRA fire and the scheduled occurrence and
    repeat budget survive it.
    """
    del expected_manual_run_at  # the stamp this refusal observed; a newer one is kept too
    from cron.extra_fire import RESTORE_KEY, _future_slot
    from cron.jobs import _under_fire_fence, _with_job, save_jobs
    from hermes_time import now as _now

    def apply(jobs, _i, job):
        claim = job.get("fire_claim")
        if not expected_owner or not isinstance(claim, dict) or claim.get("by") != expected_owner:
            return False
        extra_slot = claim.get("extra_slot")
        job["fire_claim"] = None
        manual_at = job.get("manual_run_at")
        slot = extra_slot or _future_slot(job.get("next_run_at"), _now())
        if manual_at and slot and job.get("next_run_at") != manual_at:
            job[RESTORE_KEY] = slot
            job["next_run_at"] = manual_at
        run_claim = job.get("run_claim")
        if run_claim is not None and run_claim == expected_run_claim:
            job["run_claim"] = (
                {"outage": True}
                if isinstance(run_claim, dict) and run_claim.get("outage") else None)
        save_jobs(jobs)
        return True

    return _under_fire_fence(job_id, lambda: _with_job(job_id, apply, False))
