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
import os
import hashlib
import logging
import threading
from pathlib import Path
from typing import Any, Iterator, Optional

logger = logging.getLogger(__name__)

# The local guard, keyed by lock path (which already carries the profile home + the consumer hash).
# A guard is never removed: another thread may still hold a reference to it, and a fresh Lock for
# the same key would hand two threads "the same" mutex.
_guards: dict[str, threading.Lock] = {}
_guard_owners: dict[str, int] = {}
_guards_mutex = threading.Lock()

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


@contextlib.contextmanager
def consumer_run_lock(consumer: str) -> Iterator[bool]:
    """Non-blocking acquire of *consumer*'s mutex.

    Yields True when this caller owns it, False when another live holder does (clean contention).
    Raises ``ConsumerRunLockRecursionError`` when THIS thread already owns it — the caller must not
    treat that as contention — and ``ConsumerRunLockError`` (or its unsupported subclass) when the
    lock cannot be evaluated, where the caller must fail closed rather than run unlocked.
    """
    from cron.jobs import _release_flock, ensure_dirs, fcntl

    name = consumer.strip() if isinstance(consumer, str) else ""
    if not name:
        yield True  # absent/blank: unchanged behaviour, no file opened
        return

    key = str(consumer_lock_path(name))
    recursive = False
    with _guards_mutex:
        guard = _guards.setdefault(key, threading.Lock())
        owns_guard = guard.acquire(blocking=False)
        if owns_guard:
            _guard_owners[key] = threading.get_ident()
        else:
            recursive = _guard_owners.get(key) == threading.get_ident()
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
            fcntl.flock(lock_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError as exc:
            lock_fd.close()
            if exc.errno not in {errno.EAGAIN, errno.EACCES}:
                raise ConsumerRunLockError(
                    f"Consumer run lock {name!r} could not be taken: {exc}") from exc
            yield False
            return
        try:
            yield True
        finally:
            _release_flock(lock_fd)
    finally:
        if owns_guard:
            with _guards_mutex:
                _guard_owners.pop(key, None)
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
def consumer_run_guard(job: dict, execution_id: Optional[str]) -> Iterator[bool]:
    """Wrap one producer run with its consumer's mutex (the ``run_one_job`` seam).

    Yields True when the run may proceed — the lock is then held across the whole computation,
    preflight → agent → saved output — and False when it must be skipped: another producer holds
    the consumer's lock, or the lock refused (backend/I-O). A skip runs no script/model/send,
    spends no repeat budget, raises no notice/incident, and retires only this owner's unstarted
    claims (fire claim, manual stamp, one-shot run claim) while closing the execution row as failed.

    Recursion is NOT caught: a nested run must never retire its outer owner's claims, so
    ``ConsumerRunLockRecursionError`` propagates to the caller.
    """
    name = _consumer_name(job)
    if not name:
        yield True  # no run_lock: unchanged behaviour, no lock, no bookkeeping
        return

    refused: Optional[str] = None
    with contextlib.ExitStack() as stack:
        try:
            acquired = stack.enter_context(consumer_run_lock(name))
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
    _record_unstarted(job, execution_id, reason)
    yield False


def reject_recursive_consumer_run(job: dict) -> None:
    """Refuse same-thread reentry before a direct run creates a receipt or hands off."""
    name = _consumer_name(job)
    if not name:
        return
    with _guards_mutex:
        if _guard_owners.get(str(consumer_lock_path(name))) == threading.get_ident():
            raise ConsumerRunLockRecursionError(
                f"Consumer run lock {name!r} is already held by this thread.")


def release_unstarted_fire(
    job_id: str, expected_owner: Optional[str], *,
    expected_manual_run_at: Optional[str] = None, expected_run_claim: Optional[dict] = None,
) -> bool:
    """Clear only this fire's claims; preserve later operator triggers and outage markers."""
    from cron.jobs import _under_fire_fence, _with_job, save_jobs

    def apply(jobs, _i, job):
        claim = job.get("fire_claim")
        if not expected_owner or not isinstance(claim, dict) or claim.get("by") != expected_owner:
            return False
        job["fire_claim"] = None
        if job.get("manual_run_at") == expected_manual_run_at:
            job.pop("manual_run_at", None)
            job.pop("manual_run_prompt", None)
        run_claim = job.get("run_claim")
        if run_claim is not None and run_claim == expected_run_claim:
            job["run_claim"] = (
                {"outage": True}
                if isinstance(run_claim, dict) and run_claim.get("outage") else None)
        save_jobs(jobs)
        return True

    return _under_fire_fence(job_id, lambda: _with_job(job_id, apply, False))
