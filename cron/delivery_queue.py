"""Profile-local durable handoff for cron delivery through live gateway adapters.

A restart-safe cron worker executes outside the gateway cgroup.  It cannot own
relay/E2EE adapter objects, so it queues the final send here.  A gateway claims
each row at most once.  If that gateway dies after claiming, the outcome is
marked unknown and never retried: losing a delivery is safer than duplicating a
possibly-completed send.
"""

from __future__ import annotations

import json
import logging
import os
import sqlite3
import threading
import time
import uuid
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Callable, Iterator, Optional

from agent.redact import redact_sensitive_text
from cron.executions import _owner_is_live, _process_start_time
from hermes_constants import get_hermes_home
from hermes_time import now as _hermes_now

logger = logging.getLogger(__name__)

DELIVERY_DB: Optional[Path] = None
_PROCESS_ID = uuid.uuid4().hex
_lock = threading.RLock()
_ACTIVE_DELIVERIES: set[str] = set()
_TERMINAL = ("delivered", "failed", "unknown", "suppressed")
MAX_TERMINAL_DELIVERIES = 1000
DEFAULT_DELIVERY_WAIT_TIMEOUT_SECONDS = 300.0


def _prune_terminal_unlocked(conn: sqlite3.Connection) -> None:
    """Redact terminal payloads and retain only bounded outcome metadata."""
    conn.execute(
        """UPDATE deliveries SET job_json='{}', content=''
           WHERE status IN ('delivered','failed','unknown','suppressed')
             AND (job_json != '{}' OR content != '')"""
    )
    keep = max(0, int(MAX_TERMINAL_DELIVERIES))
    terminal_count = int(
        conn.execute(
            "SELECT COUNT(*) FROM deliveries "
            "WHERE status IN ('delivered','failed','unknown','suppressed')"
        ).fetchone()[0]
    )
    excess = terminal_count - keep
    if excess > 0:
        conn.execute(
            """INSERT OR IGNORE INTO delivery_tombstones
               (execution_id, terminal_status, finished_at)
               SELECT execution_id, status, finished_at FROM deliveries
               WHERE status IN ('delivered','failed','unknown','suppressed')
               ORDER BY julianday(finished_at), finished_at,
                        julianday(created_at), created_at, execution_id
               LIMIT ?""",
            (excess,),
        )
        conn.execute(
            """DELETE FROM deliveries WHERE execution_id IN (
                 SELECT execution_id FROM deliveries
                 WHERE status IN ('delivered','failed','unknown','suppressed')
                 ORDER BY julianday(finished_at), finished_at,
                          julianday(created_at), created_at, execution_id
                 LIMIT ?
               )""",
            (excess,),
        )


def queue_path(home: Optional[Path] = None) -> Path:
    """The queue file of ``home`` (the active home when None); a test override wins."""
    if DELIVERY_DB is not None:
        return DELIVERY_DB
    root = Path(home) if home is not None else get_hermes_home()
    return root.resolve() / "cron" / "deliveries.db"


def _path() -> Path:
    return queue_path()


def _initialize_schema(conn: sqlite3.Connection) -> None:
    from hermes_cli.sqlite_util import add_column_if_missing

    # SQLite cannot widen a CHECK in place. Preserve all old rows atomically,
    # including claimed sends, while admitting a distinct never-sent disposition.
    for table in ("deliveries", "delivery_tombstones"):
        row = conn.execute("SELECT sql FROM sqlite_master WHERE type='table' AND name=?", (table,)).fetchone()
        if row and "'suppressed'" not in row[0]:
            conn.execute("SAVEPOINT notification_disposition")
            try:
                conn.execute(f"ALTER TABLE {table} RENAME TO {table}_old")
                conn.execute(row[0].replace("'unknown'", "'unknown','suppressed'"))
                conn.execute(f"INSERT INTO {table} SELECT * FROM {table}_old")
                conn.execute(f"DROP TABLE {table}_old")
                conn.execute("RELEASE notification_disposition")
            except BaseException:
                conn.execute("ROLLBACK TO notification_disposition")
                conn.execute("RELEASE notification_disposition")
                raise

    conn.execute(
        """CREATE TABLE IF NOT EXISTS deliveries (
             execution_id TEXT PRIMARY KEY,
             job_json TEXT NOT NULL,
             content TEXT NOT NULL,
             for_failure INTEGER NOT NULL DEFAULT 0,
             status TEXT NOT NULL CHECK(status IN
               ('pending','delivering','delivered','failed','unknown','suppressed')),
             owner_process_id TEXT,
             owner_pid INTEGER,
             owner_started_at INTEGER,
             created_at TEXT NOT NULL,
             finished_at TEXT,
             error TEXT
           )"""
    )
    conn.execute(
        """CREATE TABLE IF NOT EXISTS delivery_tombstones (
             execution_id TEXT PRIMARY KEY,
             terminal_status TEXT NOT NULL CHECK(terminal_status IN
               ('delivered','failed','unknown','suppressed')),
             finished_at TEXT
           )"""
    )
    add_column_if_missing(
        conn, "deliveries", "for_failure",
        "for_failure INTEGER NOT NULL DEFAULT 0",
    )


def _connect() -> sqlite3.Connection:
    # Late imports: a scheduler daemon that outlives an on-disk upgrade already has the OLD
    # ``hermes_cli.sqlite_util`` / ``cron.jobs`` cached, so new names must be resolved at call time,
    # not at import time (the guarantee cron/ledger.py used to carry, see e24c8499).
    from hermes_cli.sqlite_util import open_db

    path = _path()
    conn = open_db(path, db_label="cron/deliveries.db", synchronous_full=True, initialize=_initialize_schema)
    try:
        path.chmod(0o600)
    except OSError:
        pass
    return conn


@contextmanager
def _transaction(*, immediate: bool = False) -> Iterator[sqlite3.Connection]:
    # Pruning is done explicitly by the paths that create terminal
    # rows (_finish / recover_abandoned / _terminalize_wait_timeout);
    # read-only polls must not pay for a full-table UPDATE + COUNT.
    from hermes_cli.sqlite_util import transaction

    with _lock, transaction(_connect(), immediate=immediate) as conn:
        yield conn


# ------------------------------------------------ opted-in artifact queue discipline
# An opted-in delivery row carries the worker's prepared anchor inside its existing job
# payload. The DURABLE RECEIPT in the execution store is the ONLY authority for its outcome:
# the queue mirrors it, and only ever writes a status that receipt already proves. No new
# column, store or ledger — identity stays in the payload the row already had.
_ARTIFACT_STATUS = {
    "verified": "delivered", "suppressed": "suppressed", "failed_certain": "failed",
    "unknown": "unknown", "sending": "unknown", "unsent": "unknown",
}
_ARTIFACT_UNKNOWN_ERROR = (
    "Artifact delivery outcome is unknown; no complete provider evidence was recorded and "
    "the send was not retried."
)
# Receipt states that already decide a queue row: only these may terminalize it.
_TERMINAL_SETTLED = ("delivered", "failed", "suppressed")
_ADOPTION_REFUSED = "artifact delivery adoption refused: request anchor or identity mismatch"
_LEGACY_REFUSED = (
    "artifact delivery adoption refused: this queue slot belongs to an admitted opted-in "
    "request and a legacy delivery cannot adopt it"
)


def _anchor_of(job_json) -> Optional[tuple]:
    """``(execution_id, request_sha256, job_id)`` from a job payload, or ``None`` when absent."""
    try:
        job = json.loads(job_json) if isinstance(job_json, str) else job_json
    except (TypeError, ValueError):
        return None
    anchor = job.get("_artifact_anchor") if isinstance(job, dict) else None
    if not isinstance(anchor, dict):
        return None
    execution_id = str(anchor.get("execution_id") or "")
    request_sha256 = str(anchor.get("request_sha256") or "")
    job_id = str(anchor.get("job_id") or "")
    if not execution_id or not request_sha256 or not job_id:
        return None
    return execution_id, request_sha256, job_id


def _receipt_for(anchor) -> Optional[dict]:
    """The exact durable receipt for a carried anchor, or ``None`` (absent/conflicting/corrupt)."""
    from cron.executions import reconcile_delivery_request

    try:
        return reconcile_delivery_request(anchor[0], anchor[1], job_id=anchor[2])
    except ValueError:
        return None


def _settled_from_receipt(receipt) -> Optional[tuple]:
    """``(status, error)`` once a durable receipt is terminal, else ``None`` (still in flight)."""
    state = (receipt or {}).get("state")
    if state == "verified":
        return "delivered", None
    if state == "suppressed":
        return "suppressed", None
    if state == "failed_certain":
        return "failed", "artifact delivery was refused before dispatch; no provider send occurred."
    return None


def _settled_outcome(anchor) -> Optional[tuple]:
    """``(status, error)`` once the receipt is terminal, else ``None`` (still in flight)."""
    return _settled_from_receipt(_receipt_for(anchor))


def _artifact_outcome(anchor, fallback_error) -> tuple:
    """Queue status/error for an ATTEMPTED opted-in row — derived only from the receipt."""
    status = _ARTIFACT_STATUS.get((_receipt_for(anchor) or {}).get("state"))
    if status == "delivered":
        return "delivered", None
    if status == "suppressed":
        return "suppressed", None
    if status == "failed":
        return "failed", fallback_error or "artifact delivery was refused before dispatch"
    return "unknown", fallback_error or _ARTIFACT_UNKNOWN_ERROR


def _queue_admitted(receipt) -> bool:
    """Whether this exact receipt records that its queue row was ADMITTED by this feature.

    A receipt that was merely PREPARED — or one written on an execution ID whose queue slot
    belongs to a legacy row or tombstone — carries no such marker, so it can never adopt that
    slot, however settled it later becomes.
    """
    return isinstance(receipt, dict) and receipt.get("queue_admitted") is True


def _admitted_receipt(execution_id) -> Optional[dict]:
    """The durable receipt for an execution whose queue slot this feature admitted, else ``None``."""
    from cron.executions import execution_delivery_receipt

    try:
        receipt = execution_delivery_receipt(str(execution_id))
    except (ValueError, TypeError, KeyError):
        return None
    return receipt if _queue_admitted(receipt) else None


def _admitted_settled(execution_id) -> Optional[tuple]:
    """Receipt-proven outcome for a REDACTED row: identity re-derived by exact execution ID.

    Only a queue slot this feature admitted is resolved this way, and a ``verified`` receipt must
    also carry the claiming attempt nonce and THIS profile's sender identity — so a legacy row or
    tombstone can never be adopted by a receipt a later run prepared on the same execution ID.
    """
    receipt = _admitted_receipt(execution_id)
    if receipt is None:
        return None
    if receipt.get("state") == "verified":
        from cron.executions import delivery_profile_sha256

        if (not receipt.get("attempt_nonce")
                or receipt.get("sender_profile_sha256") != delivery_profile_sha256()):
            return None
    return _settled_from_receipt(receipt)


def _row_settled(row) -> Optional[tuple]:
    """Receipt-proven settled outcome for a queue row, else ``None`` (legacy or in flight)."""
    if row is None:
        return None
    anchor = _anchor_of(row["job_json"])
    if anchor is not None:
        return _settled_outcome(anchor)
    return _admitted_settled(row["execution_id"])


def _apply_settled(execution_id: str, settled: tuple, *, statuses) -> bool:
    """Write a receipt-proven terminal status onto a queue row, WITHOUT any send."""
    status, error = settled
    placeholders = ",".join("?" for _ in statuses)
    with _transaction() as conn:
        changed = conn.execute(
            f"UPDATE deliveries SET status=?, finished_at=?, error=? "
            f"WHERE execution_id=? AND status IN ({placeholders})",
            (status, _hermes_now().isoformat(), error, str(execution_id), *statuses)).rowcount == 1
        if changed:
            _prune_terminal_unlocked(conn)
    return changed


def _decode_job(job_json) -> Optional[dict]:
    """A parsed job payload, whether it arrived as JSON text or already decoded."""
    try:
        job = json.loads(job_json) if isinstance(job_json, str) else job_json
    except (TypeError, ValueError):
        return None
    return job if isinstance(job, dict) else None


def _adoption(execution_id, job, content) -> tuple:
    """``(opted_in, receipt, refusal)`` — read-only validation against the durable receipt."""
    anchor = job.get("_artifact_anchor")
    if anchor is None:
        return False, None, None
    if not isinstance(anchor, dict):
        return True, None, _ADOPTION_REFUSED
    target_execution = str(anchor.get("execution_id") or "")
    request_sha256 = str(anchor.get("request_sha256") or "")
    job_id = str(anchor.get("job_id") or "")
    request = anchor.get("request")
    if (target_execution != str(execution_id) or not request_sha256 or not job_id
            or not isinstance(request, dict)):
        return True, None, _ADOPTION_REFUSED
    from cron.executions import _delivery_identity, reconcile_delivery_request

    try:
        _key, digest = _delivery_identity(request)
    except (ValueError, TypeError, KeyError):
        return True, None, _ADOPTION_REFUSED
    if digest != request_sha256:
        return True, None, _ADOPTION_REFUSED
    try:
        receipt = reconcile_delivery_request(target_execution, request_sha256, job_id=job_id)
    except ValueError:
        return True, None, _ADOPTION_REFUSED
    if receipt is None or not _payload_matches(job, content, receipt):
        return True, None, _ADOPTION_REFUSED
    return True, receipt, None


def _payload_matches(job_json, content, receipt) -> bool:
    """The carried payload must restate the receipt's canonical identity, INCLUDING its own IDs.

    A copied stale reference is not validation: a job whose own ``id``/``execution_id`` disagree
    with its anchor — or whose anchor, envelope, authorized target or queued content disagree with
    the durable receipt — is refused rather than adopted.
    """
    job = _decode_job(job_json)
    if job is None or not isinstance(receipt, dict):
        return False
    anchor = job.get("_artifact_anchor")
    stored = receipt.get("request")
    if not isinstance(anchor, dict) or not isinstance(anchor.get("request"), dict):
        return False
    if not isinstance(stored, dict):
        return False
    request = anchor["request"]
    if (str(anchor.get("execution_id") or "") != str(receipt.get("execution_id") or "")
            or str(anchor.get("request_sha256") or "") != str(receipt.get("request_sha256") or "")
            or request != stored):
        return False
    if str(job.get("execution_id") or "") != str(anchor.get("execution_id") or ""):
        return False
    if str(job.get("id") or "") != str(anchor.get("job_id") or ""):
        return False
    from cron.artifact_delivery import validate_envelope
    from cron.artifact_transport import _targets, _expected_target

    try:
        validate_envelope(job.get("_artifact_delivery"), verify_files=False)
        targets = _targets(job)
        if (len(targets) != 1 or str(targets[0].get("platform", "")).lower() != "telegram"
                or _expected_target(targets[0]["chat_id"], targets[0].get("thread_id")) != request["target"]):
            return False
    except (ValueError, TypeError, KeyError):
        return False
    return _envelope_matches(job.get("_artifact_delivery"), content, request)


def _envelope_matches(envelope, content, request) -> bool:
    """The carried envelope and queued content must restate the anchor's canonical identity."""
    if not isinstance(envelope, dict):
        return False
    if (envelope.get("token") != request["token"]
            or envelope.get("purpose") != request["purpose"]
            or envelope.get("message_sha256") != request["message_sha256"]):
        return False
    artifacts = envelope.get("artifacts")
    if not isinstance(artifacts, list):
        return False
    declared = [(item.get("kind"), item.get("sha256"), item.get("transport")) for item in artifacts]
    expected = [(item["kind"], item["sha256"], item["transport"]) for item in request["artifacts"]]
    if declared != expected:
        return False
    import hashlib

    return hashlib.sha256(str(content).encode("utf-8")).hexdigest() == request["message_sha256"]


def _existing_refusal(existing, job, receipt) -> Optional[str]:
    """``None`` when an existing row may be adopted, else the refusal reason."""
    if existing["status"] in _TERMINAL:
        if receipt is None or not _queue_admitted(receipt):
            return "terminal queue row has no admitted artifact provenance"
        settled = _settled_from_receipt(receipt)
        derived = settled[0] if settled is not None else None
        if derived is None:
            return "terminal queue row has no matching durable receipt"
        if existing["status"] in ("unknown", derived):
            return None
        return "terminal queue row contradicts the durable receipt"
    carried, incoming = _anchor_of(existing["job_json"]), _anchor_of(job)
    if carried is None or incoming is None or carried != incoming:
        return "queued payload anchor does not match the incoming request"
    if not _payload_matches(existing["job_json"], existing["content"], receipt):
        return "queued payload does not restate its durable receipt"
    return None


def _record_admission(execution_id, receipt) -> None:
    # Persist provenance before the queue transaction exposes work to another process.
    from cron.executions import mark_queue_admitted, reconcile_delivery_request

    mark_queue_admitted(execution_id, receipt["request_sha256"])
    admitted = reconcile_delivery_request(execution_id, receipt["request_sha256"])
    if not _queue_admitted(admitted):
        raise ValueError("artifact queue admission could not be persisted")


def _refusal(execution_id, reason) -> dict:
    """A refusal is an error, never a fabricated terminal success, and is never inserted."""
    return {"execution_id": str(execution_id), "status": "failed", "error": reason}


def enqueue(
    execution_id: str,
    job: dict,
    content: str,
    *,
    for_failure: bool = False,
) -> dict:
    """Persist one idempotent delivery request before the worker waits.

    An opted-in request is validated against the durable receipt under THIS home before any
    existing row or tombstone is adopted. A legacy job keeps its current behaviour exactly —
    except that it may never read back a queue slot this feature admitted, and a failure alert
    (``for_failure=True``) is always the legacy route even when it carries a stale anchor.
    """
    if for_failure:
        job = dict(job)
        job.pop("_artifact_anchor", None)
        job.pop("_artifact_delivery", None)
    opted, receipt, refusal = (
        (False, None, None) if for_failure else _adoption(execution_id, job, content))
    execution_id = str(execution_id)
    if opted and refusal is not None:
        # A changed message/envelope/target/size must never ride in on an existing row.
        return _refusal(execution_id, refusal)
    # Serialize admission against other queue writers, including retention.
    with _transaction(immediate=True) as conn:
        tombstone = conn.execute(
            "SELECT terminal_status, finished_at FROM delivery_tombstones "
            "WHERE execution_id=?",
            (execution_id,),
        ).fetchone()
        if tombstone is not None:
            if not opted:
                if _admitted_receipt(execution_id) is not None:
                    return _refusal(execution_id, _LEGACY_REFUSED)
                return {
                    "execution_id": execution_id,
                    "status": tombstone["terminal_status"],
                    "finished_at": tombstone["finished_at"],
                }
            settled = _settled_from_receipt(receipt)
            derived = settled[0] if settled is not None else None
            if derived is None or not _queue_admitted(receipt):
                return _refusal(execution_id, _ADOPTION_REFUSED)
            if tombstone["terminal_status"] not in ("unknown", derived):
                return _refusal(execution_id, "terminal tombstone contradicts the durable receipt")
            return {"execution_id": execution_id, "status": derived,
                    "finished_at": tombstone["finished_at"]}
        existing = conn.execute(
            "SELECT * FROM deliveries WHERE execution_id=?", (execution_id,)
        ).fetchone()
        if existing is not None:
            if not opted:
                if (_anchor_of(existing["job_json"]) is not None
                        or existing["status"] in _TERMINAL
                        and _admitted_receipt(execution_id) is not None):
                    return _refusal(execution_id, _LEGACY_REFUSED)
                return dict(existing)
            reason = _existing_refusal(existing, job, receipt)
            if reason is not None:
                return _refusal(execution_id, reason)
            if existing["status"] not in _TERMINAL:
                _record_admission(execution_id, receipt)
            settled = _settled_outcome(_anchor_of(job))
            if settled is not None:
                return {**dict(existing), "status": settled[0], "error": settled[1]}
            return dict(existing)
        if not opted and _admitted_receipt(execution_id) is not None:
            return _refusal(execution_id, _LEGACY_REFUSED)
        conn.execute(
            """INSERT OR IGNORE INTO deliveries
               (execution_id, job_json, content, for_failure, status, created_at)
               VALUES (?, ?, ?, ?, 'pending', ?)""",
            (
                execution_id,
                json.dumps(job, ensure_ascii=False, sort_keys=True),
                str(content),
                int(bool(for_failure)),
                _hermes_now().isoformat(),
            ),
        )
        row = conn.execute(
            "SELECT * FROM deliveries WHERE execution_id=?", (execution_id,)
        ).fetchone()
        if opted:
            _record_admission(execution_id, receipt)
    return dict(row)


def _status_rows(execution_id: str) -> tuple:
    """``(row, tombstone)`` for one execution in a single transaction."""
    execution_id = str(execution_id)
    with _transaction() as conn:
        row = conn.execute(
            "SELECT * FROM deliveries WHERE execution_id=?", (execution_id,)
        ).fetchone()
        tombstone = None
        if row is None:
            tombstone = conn.execute(
                "SELECT execution_id, terminal_status, finished_at "
                "FROM delivery_tombstones WHERE execution_id=?",
                (execution_id,),
            ).fetchone()
    return row, tombstone


def _tombstone_settled(execution_id: str) -> Optional[str]:
    """Upgrade an ``unknown`` tombstone from the durable receipt — never from a job flag.

    A redacted/pruned tombstone carries no identity of its own, so the receipt is re-derived
    from the execution store by exact execution ID — and only for a tombstone whose queue row
    this feature admitted, so a legacy tombstone is never adopted on a shared execution ID.
    """
    settled = _admitted_settled(execution_id)
    if settled is None or settled[0] not in _TERMINAL_SETTLED:
        return None
    with _transaction() as conn:
        conn.execute(
            "UPDATE delivery_tombstones SET terminal_status=? "
            "WHERE execution_id=? AND terminal_status='unknown'", (settled[0], str(execution_id)))
    return settled[0]


def get_status(execution_id: str) -> Optional[dict]:
    execution_id = str(execution_id)
    row, tombstone = _status_rows(execution_id)
    if row is not None:
        settled = _row_settled(row)
        if settled is not None:
            _apply_settled(
                execution_id, settled, statuses=("pending", "delivering", "unknown"))
            row, tombstone = _status_rows(execution_id)
    if row is not None:
        return dict(row)
    if tombstone is None:
        return None
    status = tombstone["terminal_status"]
    if status == "unknown":
        status = _tombstone_settled(execution_id) or status
    return {
        "execution_id": tombstone["execution_id"],
        "status": status,
        "finished_at": tombstone["finished_at"],
        "error": None,
    }


def claim_next() -> Optional[dict]:
    """Atomically claim one pending send before touching the transport."""
    pid = os.getpid()
    started = _process_start_time(pid)
    with _transaction() as conn:
        # created_at carries a DST-varying offset: order by instant, not text.
        row = conn.execute(
            "SELECT execution_id FROM deliveries WHERE status='pending' "
            "ORDER BY julianday(created_at), created_at, execution_id LIMIT 1"
        ).fetchone()
        if row is None:
            return None
        cur = conn.execute(
            """UPDATE deliveries SET status='delivering', owner_process_id=?,
               owner_pid=?, owner_started_at=?
               WHERE execution_id=? AND status='pending'""",
            (_PROCESS_ID, pid, started, row["execution_id"]),
        )
        if cur.rowcount != 1:
            return None
        claimed = conn.execute(
            "SELECT * FROM deliveries WHERE execution_id=?", (row["execution_id"],)
        ).fetchone()
        _ACTIVE_DELIVERIES.add(row["execution_id"])
    result = dict(claimed)
    result["job"] = json.loads(result.pop("job_json"))
    return result


def _finish(
    execution_id: str, *, error: Optional[str], suppressed: bool = False, unverified: bool = False
) -> bool:
    status = "failed" if error else "suppressed" if suppressed else "delivered"
    if unverified and status == "delivered":
        status = "unknown"
        error = "Gateway send was unverified; outcome is unknown and was not retried."
    safe_error = (
        redact_sensitive_text(str(error), force=True, redact_url_credentials=True)
        if error
        else None
    )
    with _transaction() as conn:
        row = conn.execute(
            "SELECT job_json FROM deliveries WHERE execution_id=?", (execution_id,)
        ).fetchone()
        anchor = _anchor_of(row["job_json"]) if row is not None else None
        if anchor is not None:
            # An opted-in row's disposition comes from the durable receipt, full stop. The
            # caller's return value, suppression flag and generic unverified marker prove
            # nothing about a send that only complete provider evidence can attest.
            status, safe_error = _artifact_outcome(anchor, safe_error)
        cur = conn.execute(
            """UPDATE deliveries SET status=?, finished_at=?, error=?
               WHERE execution_id=? AND status='delivering'
                 AND owner_process_id=? AND owner_pid=?""",
            (
                status,
                _hermes_now().isoformat(),
                safe_error,
                execution_id,
                _PROCESS_ID,
                os.getpid(),
            ),
        )
        _prune_terminal_unlocked(conn)
    return cur.rowcount == 1


def _reconcile_artifact_rows() -> int:
    """Terminalize rows the durable receipt already decided — the receipt-first crash gap.

    A sender writes its receipt BEFORE its queue row, so a crash between the two leaves a
    pending/delivering row whose outcome is already known; a queue timeout fences ``unknown``
    first, and a receipt that lands after that write failure is still recoverable here. Either
    way the row is resolved from its own anchor or by exact execution ID, and sends nothing.
    """
    with _transaction() as conn:
        rows = conn.execute(
            "SELECT execution_id, status, job_json FROM deliveries "
            "WHERE status IN ('pending','delivering','unknown')"
        ).fetchall()
    changed = 0
    for row in rows:
        settled = _row_settled(row)
        if settled is not None and _apply_settled(
                row["execution_id"], settled, statuses=(row["status"],)):
            changed += 1
    return changed


def reconcile_late_delivery(
    execution_id: str, request_sha256: str, *, attempt_nonce: Optional[str] = None
) -> bool:
    """Narrow late transition: a durable VERIFIED receipt settles its queue row/tombstone.

    Receipt-FIRST: this only reads the receipt the late callback already wrote durably.
    A wrong nonce, a different owning profile, a failed/suppressed receipt, a receipt whose
    queue row was never admitted (a legacy row on the same execution ID) or a missing queue
    row all refuse — a direct-send callback legitimately finds no queue row at all.
    """
    from cron.executions import delivery_profile_sha256, reconcile_delivery_request

    try:
        receipt = reconcile_delivery_request(
            str(execution_id), str(request_sha256), attempt_nonce=attempt_nonce)
    except ValueError:
        return False
    if receipt is None or receipt.get("state") != "verified":
        return False
    if not _queue_admitted(receipt):
        return False
    if receipt.get("sender_profile_sha256") != delivery_profile_sha256():
        return False
    execution_id = str(execution_id)
    changed = False
    with _transaction() as conn:
        row = conn.execute(
            "SELECT status, owner_process_id FROM deliveries WHERE execution_id=?",
            (execution_id,),
        ).fetchone()
        if row is not None and row["status"] in ("delivering", "unknown"):
            if row["status"] == "delivering" and row["owner_process_id"] != _PROCESS_ID:
                return False
            changed = conn.execute(
                "UPDATE deliveries SET status='delivered', finished_at=?, error=NULL "
                "WHERE execution_id=? AND status=?",
                (_hermes_now().isoformat(), execution_id, row["status"]),
            ).rowcount == 1
            if changed:
                _prune_terminal_unlocked(conn)
        if not changed:
            changed = conn.execute(
                "UPDATE delivery_tombstones SET terminal_status='delivered' "
                "WHERE execution_id=? AND terminal_status='unknown'", (execution_id,)
            ).rowcount == 1
    return changed


def recover_abandoned() -> int:
    """Fence dead delivery owners as unknown; never replay uncertain sends."""
    changed = 0
    with _transaction() as conn:
        rows = conn.execute(
            "SELECT execution_id, owner_process_id, owner_pid, owner_started_at, job_json "
            "FROM deliveries WHERE status='delivering'"
        ).fetchall()
        for row in rows:
            same_process = row["owner_process_id"] == _PROCESS_ID
            if same_process:
                with _lock:
                    if row["execution_id"] in _ACTIVE_DELIVERIES:
                        continue
            elif _owner_is_live(int(row["owner_pid"]), row["owner_started_at"]):
                continue
            settled = _row_settled(row)
            if settled is not None:
                # The owner died AFTER its receipt landed: the durable outcome is known.
                cur = conn.execute(
                    """UPDATE deliveries SET status=?, finished_at=?, error=?
                       WHERE execution_id=? AND status='delivering'""",
                    (settled[0], _hermes_now().isoformat(), settled[1], row["execution_id"]),
                )
                changed += cur.rowcount
                continue
            error = (
                "Gateway finished delivery but could not persist its outcome; "
                "send was not retried."
                if same_process
                else "Gateway exited during delivery; send outcome is unknown and was not retried."
            )
            cur = conn.execute(
                """UPDATE deliveries SET status='unknown', finished_at=?, error=?
                   WHERE execution_id=? AND status='delivering'""",
                (
                    _hermes_now().isoformat(),
                    error,
                    row["execution_id"],
                ),
            )
            changed += cur.rowcount
        _prune_terminal_unlocked(conn)
    return changed


def drain(
    send: Callable[[dict, str, bool], Optional[str]], *, limit: int = 20
) -> int:
    """Deliver pending rows through *send*, terminalizing every claimed row."""
    recover_abandoned()
    _reconcile_artifact_rows()
    processed = 0
    for _ in range(max(0, limit)):
        row = claim_next()
        if row is None:
            break
        with _lock:
            _ACTIVE_DELIVERIES.add(row["execution_id"])
        try:
            try:
                error = send(
                    row["job"], row["content"], bool(row["for_failure"])
                )
            except BaseException as exc:
                error = f"{type(exc).__name__}: {exc}"
            _finish(row["execution_id"], error=error,
                    suppressed=bool(row["job"].get("_notification_all_targets_suppressed")),
                    unverified=bool(row["job"].get("last_delivery_unverified")))
        finally:
            with _lock:
                _ACTIVE_DELIVERIES.discard(row["execution_id"])
        processed += 1
    return processed


def _terminalize_wait_timeout(execution_id: str) -> str:
    """Fence a delivery whose worker can no longer wait for confirmation.

    A row still ``pending`` was provably never attempted, so it is left queued
    for whichever gateway comes up next (a restart that includes an update can
    easily exceed the worker's wait budget).  That is a deferral, not a
    failure: report success so the job is not recorded ``delivery_failed`` for
    a message the drain will still send.  Only a row caught mid-send is
    uncertain and gets fenced ``unknown``. An opted-in row is reread against its durable receipt
    FIRST: a receipt that already settled beats the stale timeout, and a receipt-proven
    terminal status is applied without any send.
    """
    execution_id = str(execution_id)
    now = _hermes_now().isoformat()
    uncertain_error = (
        "timed out while gateway delivery was in progress; outcome is unknown and "
        "was not retried"
    )
    with _transaction() as conn:
        row = conn.execute(
            "SELECT execution_id, status, job_json FROM deliveries WHERE execution_id=?",
            (execution_id,),
        ).fetchone()
        settled = _row_settled(row) if row is not None else None
        if row is not None and row["status"] == "pending" and settled is None:
            logger.warning(
                "Cron delivery %s: no live gateway within the wait budget; "
                "left queued for the next gateway",
                execution_id,
            )
            return ""
        if settled is not None and row is not None:
            cur = conn.execute(
                """UPDATE deliveries SET status=?, finished_at=?, error=?
                   WHERE execution_id=? AND status=?""",
                (settled[0], now, settled[1], execution_id, row["status"]),
            )
            if cur.rowcount:
                _prune_terminal_unlocked(conn)
                return "" if settled[0] in {"delivered", "suppressed"} else str(
                    settled[1] or f"delivery {settled[0]}")
        conn.execute(
            """UPDATE deliveries SET status='unknown', finished_at=?, error=?
               WHERE execution_id=? AND status='delivering'""",
            (now, uncertain_error, execution_id),
        )
        row = conn.execute(
            "SELECT status, error FROM deliveries WHERE execution_id=?",
            (execution_id,),
        ).fetchone()
        _prune_terminal_unlocked(conn)
    if row is None:
        return "timed out waiting for live gateway delivery"
    if row["status"] in {"delivered", "suppressed"}:
        return ""
    return str(row["error"] or f"delivery {row['status']}")


def enqueue_and_wait(
    execution_id: str,
    job: dict,
    content: str,
    *,
    for_failure: bool = False,
    timeout: Optional[float] = None,
) -> Optional[str]:
    """Queue delivery and wait for a gateway's terminal at-most-once outcome."""
    queued = enqueue(execution_id, job, content, for_failure=for_failure)
    if queued["status"] in _TERMINAL:
        return None if queued["status"] in {"delivered", "suppressed"} else str(
            queued.get("error") or f"delivery {queued['status']}"
        )
    wait_timeout = (
        DEFAULT_DELIVERY_WAIT_TIMEOUT_SECONDS if timeout is None else max(0.0, timeout)
    )
    deadline = time.monotonic() + wait_timeout
    while time.monotonic() < deadline:
        row = get_status(execution_id)
        if row and row["status"] in _TERMINAL:
            return None if row["status"] in {"delivered", "suppressed"} else str(
                row.get("error") or f"delivery {row['status']}"
            )
        time.sleep(1.0)
    return _terminalize_wait_timeout(execution_id) or None
