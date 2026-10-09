"""Profile-local durable audit ledger for cron execution attempts.

The ledger records what is known about each attempt; it is not a retry queue. Interrupted attempts
become ``unknown`` only after their owner process is proved gone — a start-time reading that fails
to match the claim-time fingerprint is not proof of death. Terminal states are immutable.
"""

from __future__ import annotations

import logging
import json
import hashlib
import math
import os
import sqlite3
import threading
import time
import uuid
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterator, List, Optional

from hermes_constants import get_hermes_home
from hermes_time import now as _hermes_now
from cron.constants import CLAIM_TTL_INACTIVITY_HEADROOM
from hermes_cli.observability.shared_metrics_gateway import record_cron_finish

logger = logging.getLogger(__name__)

# Optional test override. Production resolves the path at transaction time so dashboard operations
# that temporarily enter another profile cannot leak that profile's records into the import-time
# home.
EXECUTIONS_FILE: Optional[Path] = None
MAX_TERMINAL_EXECUTIONS = 1000
HANDOFF_ADOPTION_GRACE_SECONDS = 30.0
# Floor for the live-owner stale-claim bound (#115692); see _live_owner_stale_after_seconds.
LIVE_OWNER_STALE_CLAIM_FLOOR_SECONDS = 7200.0
_TERMINAL_STATES = ("completed", "failed", "unknown")
_lock = threading.RLock()
_PROCESS_ID = uuid.uuid4().hex


# --- executions ledger --------------------------------------------------------------------------

def _connect() -> sqlite3.Connection:
    # Late imports: a scheduler daemon that outlives an on-disk upgrade already has the OLD
    # ``hermes_cli.sqlite_util`` / ``cron.jobs`` cached, so new names must be resolved at call time,
    # not at import time (the guarantee cron/ledger.py used to carry, see e24c8499).
    from cron.jobs import _ensure_cron_dir
    from hermes_cli.sqlite_util import open_db

    path = EXECUTIONS_FILE or (get_hermes_home().resolve() / "cron" / "executions.db")
    _ensure_cron_dir(path.parent)
    return open_db(path, db_label="cron/executions.db", synchronous_full=True, initialize=_initialize_schema)


def _initialize_schema(conn: sqlite3.Connection) -> None:
    from hermes_cli.sqlite_util import add_column_if_missing

    conn.execute(
        """CREATE TABLE IF NOT EXISTS executions (
             id TEXT PRIMARY KEY,
             job_id TEXT NOT NULL,
             source TEXT NOT NULL,
             process_id TEXT NOT NULL,
             pid INTEGER NOT NULL,
             process_started_at INTEGER,
             status TEXT NOT NULL CHECK(status IN
               ('claimed','running','completed','failed','unknown')),
             handoff_pending INTEGER NOT NULL DEFAULT 0,
             handoff_started_at REAL,
             claimed_at TEXT NOT NULL,
             started_at TEXT,
             finished_at TEXT,
             error TEXT
           )"""
    )
    add_column_if_missing(
        conn, "executions", "handoff_pending",
        "handoff_pending INTEGER NOT NULL DEFAULT 0",
    )
    add_column_if_missing(
        conn, "executions", "handoff_started_at", "handoff_started_at REAL"
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_executions_job_claimed "
        "ON executions(job_id, claimed_at DESC, id DESC)"
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_executions_status_claimed "
        "ON executions(status, claimed_at DESC, id DESC)"
    )
    add_column_if_missing(conn, "executions", "delivery_receipt", "delivery_receipt TEXT")
    try:
        conn.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_delivery_request_key "
                     "ON executions(json_extract(delivery_receipt, '$.request_key')) "
                     "WHERE delivery_receipt IS NOT NULL "
                     "AND json_extract(delivery_receipt, '$.state') != 'failed_certain'")
    except sqlite3.OperationalError:
        logger.warning("Artifact delivery JSON index unavailable; opt-in sends are fenced")
    add_column_if_missing(conn, "executions", "delivery_outcome", "delivery_outcome TEXT")
    add_column_if_missing(conn, "executions", "scheduled_instant", "scheduled_instant TEXT")
    add_column_if_missing(conn, "executions", "progress_at", "progress_at TEXT")
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_executions_occurrence "
        "ON executions(job_id, scheduled_instant) WHERE status='completed'"
    )


@contextmanager
def _transaction(*, immediate: bool = False) -> Iterator[sqlite3.Connection]:
    from hermes_cli.sqlite_util import transaction

    with _lock, transaction(_connect(), immediate=immediate) as conn:
        yield conn


def _fetch(conn: sqlite3.Connection, execution_id: str) -> Optional[dict[str, Any]]:
    row = conn.execute("SELECT * FROM executions WHERE id=?", (execution_id,)).fetchone()
    return dict(row) if row is not None else None


def _emit_execution_state(
    record: Optional[dict[str, Any]], *, delivery_outcome: Optional[str] = None
) -> None:
    """Project durable state to monitoring without affecting ledger behavior."""
    try:
        from agent.monitoring.cron_health import emit_execution_state

        emit_execution_state(record, delivery_outcome=delivery_outcome)
    except Exception:
        pass


def _process_start_time(pid: int) -> Optional[int]:
    try:
        from gateway.status import get_process_start_time
        return get_process_start_time(pid)
    except Exception:
        return None


def _owner_is_live(pid: int, started_at: Optional[int]) -> bool:
    try:
        from gateway.status import _pid_exists
        if not _pid_exists(pid):
            return False
    except Exception:
        return True  # fail safe: inability to prove death must not rewrite state
    if started_at is None:
        return pid == os.getpid()
    current = _process_start_time(pid)
    if current is None:
        return True  # cannot compare -> cannot prove death; a misread must not rewrite state
    # Drifted same-host readings (#117505) are not proof of death; a live misread is still
    # bounded by the stale-claim sweep below.
    from gateway.status import start_time_fingerprints_match
    return start_time_fingerprints_match(started_at, current)


def _live_owner_stale_after_seconds() -> Optional[float]:
    """Age past which a claimed/running row with a LIVE owner is treated as wedged.

    Derived from the existing knobs, never a bare wall-clock constant:
    ``max(3 × HERMES_CRON_TIMEOUT, cron script timeout, 7200)``. Returns ``None`` (never reclaim
    live owners — today's behaviour) when the inactivity timeout is 0/unlimited or not a finite
    positive number: with no bound to derive from, fail closed.
    """
    from cron.scheduler import _cron_inactivity_seconds
    from cron.scheduler_script import _get_script_timeout

    inactivity = float(_cron_inactivity_seconds())
    if not math.isfinite(inactivity) or inactivity <= 0:
        return None
    return max(
        inactivity * CLAIM_TTL_INACTIVITY_HEADROOM,
        float(_get_script_timeout()),
        LIVE_OWNER_STALE_CLAIM_FLOOR_SECONDS,
    )


def _claim_age_seconds(claimed_at: str) -> float:
    """Seconds since ``claimed_at`` (NOT NULL, always the aware ISO string from hermes_time.now)."""
    return (_hermes_now() - datetime.fromisoformat(claimed_at)).total_seconds()


def _stale_age_seconds(claimed_at: str, progress_at: Optional[str]) -> float:
    """Seconds since the owner last proved it was making progress.

    A wedged worker (#115692) stops stamping ``progress_at``; a long but healthy run keeps
    stamping it, so the derived stale bound measures silence, not run length. Rows written
    before the column existed have no stamp and fall back to claim age.
    """
    return _claim_age_seconds(progress_at or claimed_at)


def touch_execution_progress(execution_id: str) -> bool:
    """Stamp ``progress_at`` on a running attempt this process owns. Returns False when the row
    is no longer ours or no longer running (the caller treats that as informational only)."""
    now = _hermes_now().isoformat()
    with _transaction() as conn:
        cur = conn.execute(
            """UPDATE executions SET progress_at=?
               WHERE id=? AND status IN ('claimed','running') AND process_id=? AND pid=?""",
            (now, execution_id, _PROCESS_ID, os.getpid()),
        )
        return cur.rowcount == 1


def _prune_unlocked(conn: sqlite3.Connection) -> None:
    conn.execute(
        """DELETE FROM executions WHERE id IN (
             SELECT id FROM executions
             WHERE status IN ('completed','failed','unknown') AND delivery_receipt IS NULL
             ORDER BY julianday(finished_at) DESC, finished_at DESC,
                      julianday(claimed_at) DESC, claimed_at DESC, id DESC LIMIT -1 OFFSET ?
           )""",
        (max(0, int(MAX_TERMINAL_EXECUTIONS)),),
    )


def create_execution(
    job_id: str, *, source: str, scheduled_instant: Optional[str] = None,
) -> dict[str, Any]:
    """Persist a claimed attempt before executor/provider dispatch."""
    from cron.occurrences import scheduled_instant as canonical_instant

    now = _hermes_now().isoformat()
    execution_id = uuid.uuid4().hex
    pid = os.getpid()
    with _transaction() as conn:
        conn.execute(
            """INSERT INTO executions
               (id, job_id, source, process_id, pid, process_started_at,
                status, claimed_at, scheduled_instant)
               VALUES (?, ?, ?, ?, ?, ?, 'claimed', ?, ?)""",
            (execution_id, str(job_id), str(source), _PROCESS_ID, pid,
             _process_start_time(pid), now, canonical_instant(scheduled_instant)),
        )
        record = _fetch(conn, execution_id)
    _emit_execution_state(record)
    return record  # type: ignore[return-value]


def set_execution_occurrence(execution_id: str, instant: Optional[str]) -> None:
    """Bind the store-claimed snapshot before a provider hands it to a worker."""
    from cron.occurrences import scheduled_instant

    with _transaction() as conn:
        cur = conn.execute(
            "UPDATE executions SET scheduled_instant=? WHERE id=? AND status='claimed' "
            "AND handoff_pending=0 AND process_id=? AND pid=?",
            (scheduled_instant(instant), execution_id, _PROCESS_ID, os.getpid()),
        )
        if cur.rowcount != 1:
            raise RuntimeError("Cron occurrence could not be bound before dispatch")


def mark_execution_handoff_pending(execution_id: str) -> Optional[dict[str, Any]]:
    """Fence restart recovery while an external worker is adopting a claim."""
    with _transaction() as conn:
        cur = conn.execute(
            """UPDATE executions
               SET handoff_pending=1, handoff_started_at=?
               WHERE id=? AND status='claimed'
                 AND process_id=? AND pid=?""",
            (time.time(), execution_id, _PROCESS_ID, os.getpid()),
        )
        if cur.rowcount != 1:
            return None
        record = _fetch(conn, execution_id)
    _emit_execution_state(record)
    return record


def adopt_claimed_execution(execution_id: str) -> Optional[dict[str, Any]]:
    """Atomically transfer and start an attempt in its worker process.

    The dispatching gateway creates the row before spawning a restart-safe
    worker.  Adoption is the single ``claimed`` → ``running`` gate: only the
    winner may acknowledge ownership or run side effects.
    """
    pid = os.getpid()
    process_started_at = _process_start_time(pid)
    now = _hermes_now().isoformat()
    with _transaction() as conn:
        cur = conn.execute(
            """UPDATE executions
               SET process_id=?, pid=?, process_started_at=?,
                   status='running', started_at=?, handoff_pending=0,
                   handoff_started_at=NULL
               WHERE id=? AND status='claimed' AND handoff_pending=1""",
            (_PROCESS_ID, pid, process_started_at, now, execution_id),
        )
        if cur.rowcount != 1:
            return None
        record = _fetch(conn, execution_id)
    _emit_execution_state(record)
    return record


def mark_execution_running(execution_id: str) -> Optional[dict[str, Any]]:
    """Transition one claimed attempt to running exactly once."""
    now = _hermes_now().isoformat()
    with _transaction() as conn:
        cur = conn.execute(
            """UPDATE executions
               SET status='running', started_at=?, handoff_pending=0,
                   handoff_started_at=NULL
               WHERE id=? AND status='claimed' AND handoff_pending=0
                 AND process_id=? AND pid=?""",
            (now, execution_id, _PROCESS_ID, os.getpid()),
        )
        if cur.rowcount != 1:
            return None
        record = _fetch(conn, execution_id)
    _emit_execution_state(record)
    return record


def finish_execution(
    execution_id: str, *, success: bool, error: Optional[str] = None,
    delivery_outcome: Optional[str] = None,
) -> Optional[dict[str, Any]]:
    """Write a terminal result once; terminal attempts cannot be rewritten.

    The receipt is reread INSIDE this transaction: a late provider proof that already
    settled an opt-in request to ``verified`` outranks the outcome the caller computed
    from the send call it saw time out, so a stale timeout cannot clobber it.
    """
    now = _hermes_now().isoformat()
    status = "completed" if success else "failed"
    detail = None if success else (str(error) if error else "unknown failure")
    with _transaction(immediate=True) as conn:
        delivery_outcome = _receipt_delivery_outcome(_fetch(conn, execution_id), delivery_outcome)
        cur = conn.execute(
            """UPDATE executions
               SET status=?, finished_at=?, error=?, handoff_pending=0,
                   handoff_started_at=NULL, delivery_outcome=?
               WHERE id=? AND status IN ('claimed','running')
                 AND process_id=? AND pid=?""",
            (status, now, detail, delivery_outcome, execution_id, _PROCESS_ID, os.getpid()),
        )
        if cur.rowcount != 1:
            return None
        _prune_unlocked(conn)
        record = _fetch(conn, execution_id)
    _emit_execution_state(record, delivery_outcome=delivery_outcome)
    record_cron_finish(record, delivery_outcome)
    return record


_OWNER_GONE_REASON = (
    "Scheduler restarted after this execution's owner exited before a durable "
    "terminal state; whether side effects ran is unknown."
)
_OWNER_WEDGED_REASON = (
    "Owner process is still alive but the claim outlived the derived stale bound; "
    "treated as wedged (#115692). The process was not terminated; whether side effects "
    "ran is unknown."
)


def settle_unstarted_execution(execution_id: str, job_id: str, error: str) -> None:
    """Close the receipt of a run that never started: a ``claimed`` row never resolves. Best-effort
    so a ledger write cannot mask the failure the caller logs or re-raises."""
    try:
        finish_execution(execution_id, success=False, error=error)
    except (sqlite3.Error, OSError) as record_err:
        logger.error("Job '%s': failed to close execution receipt %s (%s): %s",
                     job_id, execution_id, error, record_err)


def recover_interrupted_executions() -> int:
    """Mark abandoned attempts unknown without scheduling retries: rows whose owner is provably
    dead, plus rows whose live owner holds a claim older than the derived stale bound (the
    process is not killed)."""
    now = _hermes_now().isoformat()
    changed = 0
    recovered: list[dict[str, Any]] = []
    # Derived on the first live-owned row only: the bound reads config, and the idle gateway
    # tick must stay config-free (tests/cron/test_idle_tick_config_skip.py).
    stale_after: Optional[float] = None
    stale_after_resolved = False
    with _transaction() as conn:
        rows = conn.execute(
            """SELECT id, status, process_id, pid, process_started_at,
                      handoff_pending, handoff_started_at, claimed_at, progress_at
               FROM executions
               WHERE status IN ('claimed','running')"""
        ).fetchall()
        for row in rows:
            if row["process_id"] == _PROCESS_ID:
                continue
            reason = _OWNER_GONE_REASON
            if _owner_is_live(int(row["pid"]), row["process_started_at"]):
                # A live owner is normally a legitimately running job. A worker permanently
                # deadlocked (e.g. futex_wait behind a route/proxy flip, #115692) also passes
                # this check, so a claim SILENT for longer than the derived bound is treated
                # as wedged and released — the external-worker wait loop polls this ledger
                # for a terminal status, so the job can fire again. Silence is measured from
                # the owner's last ``progress_at`` stamp (the run monitor refreshes it while
                # the agent is active), so a healthy multi-hour run is never reclaimed while
                # it is still working. The wedged worker PROCESS is NOT terminated here
                # (leaked until host restart); rows owned by this process (process_id ==
                # _PROCESS_ID, in-process runs) are skipped above and remain out of scope.
                if not stale_after_resolved:
                    stale_after = _live_owner_stale_after_seconds()
                    stale_after_resolved = True
                if (
                    stale_after is None
                    or _stale_age_seconds(row["claimed_at"], row["progress_at"]) <= stale_after
                ):
                    continue
                reason = _OWNER_WEDGED_REASON
            handoff_started_at = row["handoff_started_at"]
            if (
                row["handoff_pending"]
                and handoff_started_at is not None
                and time.time() - float(handoff_started_at)
                < HANDOFF_ADOPTION_GRACE_SECONDS
            ):
                continue
            cur = conn.execute(
                """UPDATE executions
                   SET status='unknown', finished_at=?, error=?,
                       handoff_pending=0, handoff_started_at=NULL
                   WHERE id=? AND status=? AND process_id=? AND pid=?
                     AND handoff_pending=?
                     AND handoff_started_at IS ?""",
                (now, reason, row["id"], row["status"], row["process_id"], row["pid"],
                 row["handoff_pending"], row["handoff_started_at"]),
            )
            changed += cur.rowcount
            if cur.rowcount:
                record = _fetch(conn, row["id"])
                if record is not None:
                    recovered.append(record)
        if changed:
            _prune_unlocked(conn)
    for record in recovered:
        _emit_execution_state(record)
    return changed


def terminalize_dead_owner(execution_id: str, *, reason: str) -> bool:
    """Record one attempt as ``unknown`` with a cause this process actually observed.

    ``recover_interrupted_executions`` sweeps every attempt whose owner is provably
    dead, and knows nothing but that absence — so all it can write is
    ``_OWNER_GONE_REASON``, which asserts a scheduler restart. A waiter that held the
    worker's ``Popen`` knows more: the owner was that external worker, and it exited
    with a known status. Without this, a manual run whose worker dies is filed as
    "Scheduler restarted ..." (a restart that never happened) and, because the sweep
    leaves the row terminal, the waiter reports success and never records the run —
    the job's ``fire_claim`` then blocks the next manual fire for the whole lease
    (#128509).

    The attempt stays ``unknown``, not ``failed``: whether side effects ran is still
    unknown. Only the CAUSE becomes truthful. Returns False — leaving the caller to
    fall back to the generic sweep — when the row is absent, already terminal, owned by
    this process, inside the handoff adoption grace, or owned by a live process: a
    worker that is still running must never be terminalized out from under itself.
    """
    now = _hermes_now().isoformat()
    with _transaction() as conn:
        row = conn.execute(
            """SELECT id, status, process_id, pid, process_started_at,
                      handoff_pending, handoff_started_at
               FROM executions WHERE id=?""",
            (execution_id,),
        ).fetchone()
        if row is None or row["status"] not in ("claimed", "running"):
            return False
        if row["process_id"] == _PROCESS_ID:
            return False
        if _owner_is_live(int(row["pid"]), row["process_started_at"]):
            return False
        handoff_started_at = row["handoff_started_at"]
        if (
            row["handoff_pending"]
            and handoff_started_at is not None
            and time.time() - float(handoff_started_at) < HANDOFF_ADOPTION_GRACE_SECONDS
        ):
            return False
        cur = conn.execute(
            """UPDATE executions
               SET status='unknown', finished_at=?, error=?,
                   handoff_pending=0, handoff_started_at=NULL
               WHERE id=? AND status=? AND process_id=? AND pid=?""",
            (now, reason, row["id"], row["status"], row["process_id"], row["pid"]),
        )
        if cur.rowcount != 1:
            return False
        record = _fetch(conn, execution_id)
        _prune_unlocked(conn)
    _emit_execution_state(record)
    return True


def list_executions(
    *, job_id: Optional[str] = None, limit: int = 50, before_claimed_at: Optional[str] = None,
) -> list[dict[str, Any]]:
    """Return indexed, newest-first execution history with cursor pagination."""
    clauses: list[str] = []
    params: list[Any] = []
    if job_id is not None:
        clauses.append("job_id=?")
        params.append(str(job_id))
    if before_claimed_at is not None:
        # Same (instant, text) key as the ORDER BY, so a page never skips or repeats a row.
        clauses.append("(julianday(claimed_at), claimed_at) < (julianday(?), ?)")
        params.extend([str(before_claimed_at)] * 2)
    where = " WHERE " + " AND ".join(clauses) if clauses else ""
    params.append(max(1, min(int(limit), 500)))
    # Stamps carry the local offset, which changes at DST and on a timezone change, so text order
    # is not time order. julianday() compares instants (ms); the text breaks same-ms ties.
    with _transaction() as conn:
        rows = conn.execute(
            "SELECT * FROM executions" + where
            + " ORDER BY julianday(claimed_at) DESC, claimed_at DESC, id DESC LIMIT ?",
            params,
        ).fetchall()
    return [dict(row) for row in rows]


def get_execution(execution_id: str) -> Optional[dict[str, Any]]:
    """Return one exact execution attempt, or ``None`` when it is absent."""
    with _transaction() as conn:
        row = conn.execute(
            "SELECT * FROM executions WHERE id=?",
            (str(execution_id),),
        ).fetchone()
    return dict(row) if row is not None else None


def latest_execution(job_id: str) -> Optional[dict[str, Any]]:
    rows = list_executions(job_id=job_id, limit=1)
    return rows[0] if rows else None


def live_inflight_execution(job_id: str) -> Optional[dict[str, Any]]:
    """The job's latest attempt while it is still claimed/running under a LIVE owner, else ``None``.

    This is scheduler OWNERSHIP, not recent activity: a run inside a long tool call writes no
    heartbeat yet stays owned, while a run whose process died (watchdog kill, crash) does not.
    Read-only — unlike ``recover_interrupted_executions`` it never rewrites a row.
    """
    record = latest_execution(job_id)
    if not record or record.get("status") not in ("claimed", "running"):
        return None
    if not _owner_is_live(int(record["pid"]), record.get("process_started_at")):
        return None
    return record


def latest_executions(job_ids: list[str]) -> dict[str, dict[str, Any]]:
    """Load latest execution for many jobs in one query."""
    clean = [str(job_id) for job_id in dict.fromkeys(job_ids) if job_id]
    if not clean:
        return {}
    placeholders = ",".join("?" for _ in clean)
    # One windowed sort: a per-row correlated ORDER BY julianday() cannot use the index and
    # grows quadratically with history (~90 ms at 1000 rows).
    with _transaction() as conn:
        rows = conn.execute(
            f"""SELECT e.* FROM executions e WHERE e.id IN (
                  SELECT id FROM (
                    SELECT id, ROW_NUMBER() OVER (
                             PARTITION BY job_id
                             ORDER BY julianday(claimed_at) DESC, claimed_at DESC, id DESC
                           ) AS rn
                    FROM executions WHERE job_id IN ({placeholders}))
                  WHERE rn=1)""",
            clean,
        ).fetchall()
    return {row["job_id"]: dict(row) for row in rows}


def _delivery_identity(request: dict) -> tuple[str, str]:
    from cron.artifact_delivery import _hex, _keys

    _keys(request, ('token', 'purpose', 'target', 'message_sha256', 'artifacts'))
    _hex(request['token'], 32)
    _hex(request['message_sha256'], 64)
    if request['purpose'] not in ('report', 'review'):
        raise ValueError('invalid artifact purpose')
    target = request['target']
    _keys(target, ('platform', 'chat_id', 'thread_id'))
    if target['platform'] != 'telegram' or not isinstance(target['chat_id'], str) or not target['chat_id']:
        raise ValueError('artifact delivery requires a Telegram target')
    if target['thread_id'] is not None and not isinstance(target['thread_id'], str):
        raise ValueError('invalid artifact thread')
    if not isinstance(request['artifacts'], list) or not request['artifacts']:
        raise ValueError('artifact identity requires evidence')
    kinds = set()
    for artifact in request['artifacts']:
        _keys(artifact, ('kind', 'sha256', 'transport', 'size'))
        kind = artifact['kind']
        allowed = ('report',) if request['purpose'] == 'report' else ('review', 'review_result')
        if not isinstance(kind, str) or kind not in allowed or kind in kinds:
            raise ValueError('invalid or duplicate artifact identity')
        kinds.add(kind)
        _hex(artifact['sha256'], 64)
        if artifact['transport'] not in ('text', 'document'):
            raise ValueError('invalid artifact transport')
        if type(artifact['size']) is not int or artifact['size'] < 0:
            raise ValueError('invalid artifact size')
    digest = hashlib.sha256(json.dumps(request, sort_keys=True, separators=(',', ':')).encode()).hexdigest()
    return request['purpose'] + ':' + request['token'], digest


def _receipt_records(conn, key):
    if not conn.execute("SELECT 1 FROM sqlite_master WHERE type='index' AND name='idx_delivery_request_key'").fetchone():
        raise ValueError('artifact delivery requires SQLite JSON index support')
    return conn.execute(
        "SELECT * FROM executions WHERE json_extract(delivery_receipt, '$.request_key')=? "
        "ORDER BY claimed_at DESC, id DESC", (key,)).fetchall()


def _decode_receipt(row):
    receipt = json.loads(row['delivery_receipt'])
    if not isinstance(receipt, dict) or not isinstance(receipt.get('request'), dict):
        raise ValueError('corrupt artifact receipt anchor')
    key, digest = _delivery_identity(receipt['request'])
    if (type(receipt.get('version')) is not int or receipt.get('version') != 1
            or receipt.get('execution_id') != row['id']
            or receipt.get('request_key') != key or receipt.get('request_sha256') != digest
            or receipt.get('state') not in ('unsent', 'sending', 'verified', 'failed_certain', 'unknown', 'suppressed')):
        raise ValueError('corrupt artifact receipt anchor')
    if receipt['state'] == 'verified':
        from cron.artifact_proof import evidence as validate_evidence

        validate_evidence(receipt.get('evidence'), receipt)
    return receipt


def _delivery_owner_is_dead(row) -> bool:
    from gateway.status import _pid_exists

    try:
        if not _pid_exists(row['pid']):
            return True
        if row['process_started_at'] is None:
            return False
        return not _owner_is_live(row['pid'], row['process_started_at'])
    except Exception:
        logger.warning('Artifact owner liveness is unverifiable; keeping fence', exc_info=True)
        return False


def prepare_delivery_request(execution_id: str, job_id: str, request: dict) -> dict:
    """Bind an owned execution to immutable ID/hash metadata before any dispatch."""
    key, digest = _delivery_identity(request)
    with _transaction(immediate=True) as conn:
        owner = _fetch(conn, execution_id)
        if (not owner or owner['job_id'] != job_id or owner['process_id'] != _PROCESS_ID
                or owner['pid'] != os.getpid() or owner['status'] not in ('claimed', 'running')):
            raise ValueError('artifact request execution is not owned and active')
        active = None
        for row in _receipt_records(conn, key):
            receipt = _decode_receipt(row)
            if receipt['request_sha256'] != digest:
                raise ValueError('artifact request identity conflict')
            if (receipt['state'] == 'unsent' and not receipt.get('attempt_nonce')
                    and not receipt.get('evidence')
                    and _delivery_owner_is_dead(row)):
                receipt.update(state='failed_certain', reason='owner exited before dispatch')
                conn.execute('UPDATE executions SET delivery_receipt=? WHERE id=? AND delivery_receipt=?',
                             (json.dumps(receipt), row['id'], row['delivery_receipt']))
            elif receipt['state'] != 'failed_certain':
                if active is not None:
                    raise ValueError('duplicate active artifact request')
                active = receipt
        if active is not None:
            return active
        if owner.get('delivery_receipt') is not None:
            raise ValueError('execution already has an artifact request')
        receipt = {'version': 1, 'execution_id': execution_id, 'request_key': key,
                   'request_sha256': digest, 'request': request, 'state': 'unsent'}
        changed = conn.execute(
            "UPDATE executions SET delivery_receipt=? WHERE id=? AND job_id=? "
            "AND process_id=? AND pid=? AND status IN ('claimed','running') AND delivery_receipt IS NULL",
            (json.dumps(receipt), execution_id, job_id, _PROCESS_ID, os.getpid())).rowcount
        if changed != 1:
            raise ValueError('artifact request ownership changed')
        return receipt


def claim_delivery_request(execution_id: str, request_sha256: str) -> Optional[dict]:
    """Exactly one sender wins; queued workers retain their original execution anchor."""
    with _transaction(immediate=True) as conn:
        row = _fetch(conn, execution_id)
        if not row or not row.get('delivery_receipt'):
            raise ValueError('artifact request anchor is missing')
        receipt = _decode_receipt(row)
        if receipt['request_sha256'] != request_sha256:
            raise ValueError('artifact request digest conflict')
        if receipt['state'] != 'unsent':
            return None
        receipt.update(state='sending', attempt_nonce=uuid.uuid4().hex, sender_pid=os.getpid(),
                       sender_started_at=_process_start_time(os.getpid()),
                       sender_profile_sha256=delivery_profile_sha256())
        changed = conn.execute('UPDATE executions SET delivery_receipt=? WHERE id=? AND delivery_receipt=?',
                               (json.dumps(receipt), execution_id, row['delivery_receipt'])).rowcount
        if changed != 1:
            return None
        return receipt


_DELIVERY_SETTLED_STATES = ('verified', 'unknown', 'failed_certain', 'suppressed')
# Receipt state -> delivery_outcome. In-flight states have no positive evidence yet.
_RECEIPT_OUTCOME = {
    'unsent': 'unknown', 'sending': 'unknown', 'unknown': 'unknown',
    'verified': 'delivered', 'suppressed': 'suppressed', 'failed_certain': 'failed',
}


def _receipt_delivery_outcome(row, delivery_outcome):
    """Receipt-derived disposition. A settled receipt is authoritative; an in-flight one
    only fills a caller that had nothing to record."""
    if not row or row.get('delivery_receipt') is None:
        return delivery_outcome
    try:
        receipt = _decode_receipt(row)
    except (ValueError, TypeError, KeyError):
        return 'unknown'
    if receipt['state'] in ('unsent', 'sending'):
        return 'unknown'
    return _RECEIPT_OUTCOME[receipt['state']]


def delivery_profile_sha256() -> str:
    """Stable identity of the profile whose execution store owns a receipt."""
    return hashlib.sha256(str(get_hermes_home().resolve()).encode()).hexdigest()


def settle_delivery_request(
    execution_id: str, request_sha256: str, *, attempt_nonce: str, state: str,
    evidence: Optional[dict] = None, reason: Optional[str] = None,
    profile_sha256: Optional[str] = None,
) -> Optional[dict]:
    """Settle ONE claimed attempt to a terminal receipt state, exactly once.

    CAS on the exact execution id, request digest and minted attempt nonce, under the
    owning profile. ``None`` means another writer already settled it (or the anchor
    moved) — never a licence to send again. A late provider proof uses the same call
    with the attempt nonce captured at claim time.
    """
    from cron.artifact_delivery import _hex

    if state not in _DELIVERY_SETTLED_STATES:
        raise ValueError('invalid artifact receipt state')
    if state in ('failed_certain', 'suppressed') and evidence is not None:
        raise ValueError('no-send disposition cannot retain acceptance evidence')
    if state == 'failed_certain' and reason != 'gateway loop unavailable before dispatch':
        raise ValueError('failed_certain requires proven pre-dispatch refusal')
    if not isinstance(attempt_nonce, str) or not attempt_nonce:
        raise ValueError('artifact settlement requires the claim attempt nonce')
    if reason is not None and (not isinstance(reason, str) or len(reason) > 300):
        raise ValueError('invalid artifact settlement reason')
    _hex(request_sha256, 64)
    with _transaction(immediate=True) as conn:
        row = _fetch(conn, str(execution_id))
        if not row or not row.get('delivery_receipt'):
            raise ValueError('artifact request anchor is missing')
        receipt = _decode_receipt(row)
        if receipt['request_sha256'] != request_sha256:
            raise ValueError('artifact request digest conflict')
        if receipt['state'] != 'sending' or receipt.get('attempt_nonce') != attempt_nonce:
            return None
        if (receipt.get('sender_pid') != os.getpid()
                or receipt.get('sender_profile_sha256') != delivery_profile_sha256()
                or profile_sha256 is not None and receipt.get('sender_profile_sha256') != profile_sha256):
            return None
        from cron.artifact_proof import evidence as validate_evidence

        if state == 'verified' or evidence is not None:
            validate_evidence(evidence, receipt)
        receipt.update(state=state, settled_at=_hermes_now().isoformat())
        if evidence is not None:
            receipt['evidence'] = evidence
        if reason is not None:
            receipt['reason'] = reason
        changed = conn.execute(
            'UPDATE executions SET delivery_receipt=?, delivery_outcome=? WHERE id=? AND delivery_receipt=?',
            (json.dumps(receipt), _RECEIPT_OUTCOME[state], row['id'], row['delivery_receipt'])).rowcount
        if changed != 1:
            return None
        return receipt


def reconcile_delivery_request(
    execution_id: str, request_sha256: str, *, attempt_nonce: Optional[str] = None,
    job_id: Optional[str] = None,
) -> Optional[dict]:
    """Reread the receipt for one exact attempt under the owning profile — never mutates.

    A dead process leaves ``sending``; that is the honest answer, not success. Identity
    conflict raises instead of returning another attempt's receipt.
    """
    from cron.artifact_delivery import _hex

    _hex(request_sha256, 64)
    with _transaction() as conn:
        row = _fetch(conn, str(execution_id))
    if not row or not row.get('delivery_receipt'):
        return None
    if job_id is not None and row['job_id'] != job_id:
        raise ValueError('artifact execution job conflict')
    receipt = _decode_receipt(row)
    if receipt['request_sha256'] != request_sha256:
        raise ValueError('artifact request digest conflict')
    if attempt_nonce is not None and receipt.get('attempt_nonce') != attempt_nonce:
        raise ValueError('artifact attempt nonce conflict')
    return receipt


_PRE_DISPATCH_REFUSAL_REASONS = frozenset({
    'artifact snapshot or path policy refused before dispatch',
    'artifact request identity conflict before dispatch',
    'artifact transport unavailable before dispatch',
})


def settle_undispatched_delivery_request(
    execution_id: str, request_sha256: str, *, reason: str,
) -> Optional[dict]:
    """Settle a request proven NEVER dispatched: CAS ``unsent`` -> ``failed_certain``.

    Distinct from :func:`settle_delivery_request`, which requires a claimed attempt nonce.
    The queue sender revalidates the prepared request BEFORE the send claim, so a refusal
    there is a proven no-send — but it still has to be durable, or its queue row could never
    honestly read ``failed``. Only a code-owned refusal reason is accepted; an arbitrary
    exception must never reach this state. ``None`` means another writer moved the anchor.
    """
    from cron.artifact_delivery import _hex

    if reason not in _PRE_DISPATCH_REFUSAL_REASONS:
        raise ValueError('failed_certain requires a proven pre-dispatch refusal')
    _hex(request_sha256, 64)
    with _transaction(immediate=True) as conn:
        row = _fetch(conn, str(execution_id))
        if not row or not row.get('delivery_receipt'):
            return None
        receipt = _decode_receipt(row)
        if receipt['request_sha256'] != request_sha256:
            raise ValueError('artifact request digest conflict')
        if receipt['state'] != 'unsent' or receipt.get('attempt_nonce') or receipt.get('evidence'):
            return None
        receipt.update(state='failed_certain', reason=reason, settled_at=_hermes_now().isoformat())
        changed = conn.execute(
            'UPDATE executions SET delivery_receipt=?, delivery_outcome=? WHERE id=? AND delivery_receipt=?',
            (json.dumps(receipt), _RECEIPT_OUTCOME['failed_certain'], row['id'], row['delivery_receipt'])).rowcount
        if changed != 1:
            return None
        return receipt


def execution_delivery_receipt(execution_id: str) -> Optional[dict]:
    """Decoded durable receipt for one exact execution ID, or ``None`` — never mutates.

    The queue needs this for an execution whose own payload has been redacted (its anchor is
    gone); identity is then re-derived from the store, never from the queue row.
    """
    with _transaction() as conn:
        row = _fetch(conn, str(execution_id))
    if not row or not row.get('delivery_receipt'):
        return None
    return _decode_receipt(row)


def get_artifact_delivery_receipt(token: str, purpose: str, *, expected_request_sha256=None) -> Optional[dict]:
    from cron.artifact_delivery import _hex

    _hex(token, 32)
    if purpose not in ('report', 'review'):
        raise ValueError('invalid artifact purpose')
    if expected_request_sha256 is not None:
        _hex(expected_request_sha256, 64)
    with _transaction() as conn:
        receipts = [_decode_receipt(row) for row in _receipt_records(conn, purpose + ':' + token)]
    if not receipts:
        return None
    if len({receipt['request_sha256'] for receipt in receipts}) != 1:
        raise ValueError('conflicting artifact receipt identities')
    active = [receipt for receipt in receipts if receipt['state'] != 'failed_certain']
    if len(active) > 1:
        raise ValueError('duplicate active artifact receipts')
    receipt = active[0] if active else receipts[0]
    if expected_request_sha256 is not None and receipt['request_sha256'] != expected_request_sha256:
        raise ValueError('artifact receipt request digest conflict')
    return receipt
