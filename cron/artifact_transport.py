"""Opt-in artifact transport: exact-snapshot dispatch, provider proof and settlement.

Reached only for a trusted ``no_agent`` script job that opted in with
``script_output_format: delivery-v1`` (see ``cron/artifact_delivery.py``). The durable
receipt in ``cron/executions.py`` is the only authority: a request is claimed BEFORE the
first byte leaves, settled once from complete provider evidence, and NEVER replayed after
an unknown or partial outcome.

Bound scope: direct live-adapter delivery only. Queued/external-worker artifact delivery
is deliberately fail-closed here and lands in a separate task.
"""

from __future__ import annotations

import hashlib
import logging
import os
import threading
from typing import Optional

logger = logging.getLogger("cron.scheduler")

# One bounded wait for the whole dispatch. A send that outlasts it is LEFT RUNNING; the
# late callback owns the outcome from then on.
_DISPATCH_TIMEOUT_SECS = 60.0
_LATE_CALLBACKS: set = set()
_LATE_CALLBACKS_LOCK = threading.Lock()


def _sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _expected_target(chat_id, thread_id) -> dict:
    """Must match ``plugins/platforms/telegram/adapter._delivery_target_proof`` exactly."""
    return {"platform": "telegram", "chat_id": str(chat_id),
            "thread_id": str(thread_id) if thread_id is not None else None}


def build_request_identity(envelope: dict, target: dict) -> dict:
    """Canonical immutable request identity: token/purpose/target/digests/artifact metadata.

    No message text, no local path, no document bytes — the receipt is durable identity,
    not evidence. Sizes come from the real file so a policy refusal surfaces here, before
    the request is ever claimed.
    """
    from cron.artifact_delivery import _hex
    from gateway.platforms.base import validate_media_delivery_path

    _hex(envelope["token"], 32)
    _hex(envelope["message_sha256"], 64)
    artifacts = []
    for artifact in envelope["artifacts"]:
        _hex(artifact["sha256"], 64)
        resolved = validate_media_delivery_path(artifact["path"])
        if resolved is None:
            raise ValueError("artifact path refused by media policy")
        artifacts.append({
            "kind": artifact["kind"], "sha256": artifact["sha256"],
            "transport": artifact["transport"], "size": os.path.getsize(resolved),
        })
    return {
        "token": envelope["token"], "purpose": envelope["purpose"],
        "target": _expected_target(target["chat_id"], target.get("thread_id")),
        "message_sha256": envelope["message_sha256"], "artifacts": artifacts,
    }


def snapshot_request(envelope: dict) -> list:
    """Revalidate the media policy and read each artifact's bytes EXACTLY ONCE.

    Retains the immutable byte snapshot for native upload; a file that changed since the
    envelope was parsed fails here, before any claim or send. No path is stored in the
    receipt or in any evidence.
    """
    from gateway.platforms.base import validate_media_delivery_path

    snapshots = []
    for artifact in envelope["artifacts"]:
        resolved = validate_media_delivery_path(artifact["path"])
        if resolved is None:
            raise ValueError("artifact path refused by media policy")
        with open(resolved, "rb") as handle:
            data = handle.read()
        if _sha256(data) != artifact["sha256"]:
            raise ValueError("artifact digest changed before dispatch")
        snapshots.append({
            "kind": artifact["kind"], "transport": artifact["transport"],
            "sha256": artifact["sha256"], "bytes": data,
            "path": os.path.basename(resolved), "name": os.path.basename(resolved),
        })
    return snapshots


def _receipt_from(result) -> dict:
    if result is None:
        return {}
    raw = result.get("raw_response") if isinstance(result, dict) else getattr(result, "raw_response", None)
    if not isinstance(raw, dict):
        return {}
    proof = raw.get("delivery_receipt")
    return proof if isinstance(proof, dict) else {}


def _notification_problem(result, text: str, target: dict) -> str:
    """Structural completeness of a notification receipt. A single first ID proves nothing."""
    proof = _receipt_from(result)
    if not proof:
        return "no notification receipt"
    if proof.get("complete") is not True or proof.get("role") != "notification":
        return "notification receipt is incomplete"
    if proof.get("incoming_sha256") != _sha256(text.encode("utf-8")):
        return "notification digest mismatch"
    if proof.get("target") != target:
        return "notification target mismatch"
    chunks = proof.get("chunks")
    if not isinstance(chunks, list) or not chunks or proof.get("count") != len(chunks):
        return "notification chunk count mismatch"
    seen = set()
    for index, entry in enumerate(chunks):
        if not isinstance(entry, dict) or entry.get("index") != index:
            return "notification chunk index gap"
        message_id = entry.get("message_id")
        if not isinstance(message_id, str) or not message_id or message_id in seen:
            return "notification chunk message id is missing or duplicated"
        seen.add(message_id)
        if not isinstance(entry.get("sha256"), str):
            return "notification chunk hash is missing"
    return ""


def _document_problem(result, snapshot: dict, target: dict) -> str:
    """A fallback link/text, a missing ID or a changed file can never grant verified."""
    proof = _receipt_from(result)
    if not proof:
        return "no native document receipt"
    if proof.get("role") != "document" or proof.get("method") != "send_document":
        return "document was not sent natively"
    if proof.get("sha256") != snapshot["sha256"]:
        return "document digest mismatch"
    message_id = proof.get("message_id")
    if not isinstance(message_id, str) or not message_id:
        return "document message id is missing"
    if proof.get("target") != target:
        return "document target mismatch"
    return ""


def _evidence(execution_id, request_sha256, attempt_nonce, target, notification, artifacts) -> dict:
    return {
        "execution_id": execution_id,
        "request_sha256": request_sha256,
        "attempt_nonce": attempt_nonce,
        "target": target,
        "notification": notification,
        "artifacts": artifacts,
    }


def _collect(message, target, notification_result, artifact_results) -> tuple:
    """``(ok, evidence, reason)`` — complete provider proof for every required artifact."""
    notification_problem = _notification_problem(notification_result, message, target)
    if notification_problem:
        return False, None, notification_problem
    artifacts = []
    for snapshot, result in artifact_results:
        problem = _document_problem(result, snapshot, target)
        if problem:
            return False, None, problem
        proof = _receipt_from(result)
        artifacts.append({"kind": snapshot["kind"], "sha256": snapshot["sha256"],
                          "message_id": proof["message_id"], "method": proof["method"],
                          "size": len(snapshot["bytes"])})
    proof = _receipt_from(notification_result)
    notification = {"sha256": proof["incoming_sha256"], "count": proof["count"],
                    "chunks": [dict(entry) for entry in proof["chunks"]]}
    return True, {"notification": notification, "artifacts": artifacts}, ""


async def _dispatch(transport, config, chat_id, thread_id, message, snapshots, metadata):
    """Send the full notification and every document, then return what the provider acked."""
    from gateway.config import Platform
    from gateway.delivery import DeliveryRouter, DeliveryTarget

    router = DeliveryRouter(config, {Platform.TELEGRAM: transport.adapter})
    send_metadata = dict(metadata or {})
    if thread_id is not None:
        send_metadata["thread_id"] = str(thread_id)
    target = DeliveryTarget(platform=Platform.TELEGRAM, chat_id=str(chat_id),
                            thread_id=str(thread_id) if thread_id is not None else None,
                            is_explicit=True)

    notification_result = None
    try:
        notification_result = await router._deliver_to_platform(
            target, message, send_metadata, transport=transport)
    except Exception as exc:
        logger.warning("Artifact notification send failed (%s)", type(exc).__name__, exc_info=True)

    artifact_results = []
    for snapshot in snapshots:
        try:
            result = await transport.adapter.send_document(
                chat_id=str(chat_id), file_path=snapshot["path"], file_name=snapshot["name"],
                snapshot=snapshot["bytes"], metadata=send_metadata or None)
        except Exception as exc:
            logger.warning("Artifact %s send failed (%s)", snapshot["kind"], type(exc).__name__,
                           exc_info=True)
            result = None
        artifact_results.append((snapshot, result))
    return notification_result, artifact_results


def _settle(execution_id, request_sha256, attempt_nonce, profile_sha256, message, target,
            notification_result, artifact_results) -> tuple:
    """Settle the exact attempt once: verified only on complete evidence, else unknown."""
    from cron.executions import settle_delivery_request

    ok, evidence, reason = _collect(message, target, notification_result, artifact_results)
    if ok:
        evidence = _evidence(execution_id, request_sha256, attempt_nonce, target,
                             evidence["notification"], evidence["artifacts"])
    receipt = settle_delivery_request(
        execution_id, request_sha256, attempt_nonce=attempt_nonce,
        state="verified" if ok else "unknown", evidence=evidence, reason=None if ok else reason,
        profile_sha256=profile_sha256)
    if receipt is None:
        logger.warning("Artifact attempt %s was already settled; late evidence not applied",
                       attempt_nonce)
    return ok, reason


def _install_late_callback(future, *, execution_id, request_sha256, attempt_nonce,
                           profile_sha256, message, target) -> bool:
    """Install EXACTLY ONE late-completion callback for this claimed attempt.

    The receipt settle is itself a CAS on the captured attempt nonce, so a duplicate callback
    could not double-record; the guard here keeps the callback list from growing per retry.
    """
    key = (execution_id, attempt_nonce)
    with _LATE_CALLBACKS_LOCK:
        if key in _LATE_CALLBACKS:
            return False
        _LATE_CALLBACKS.add(key)

    def _late(done):
        try:
            notification_result, artifact_results = done.result()
            _settle(execution_id, request_sha256, attempt_nonce, profile_sha256, message,
                    target, notification_result, artifact_results)
        except Exception as exc:
            logger.warning("Artifact late completion could not be recorded (%s)",
                           type(exc).__name__, exc_info=True)
        finally:
            with _LATE_CALLBACKS_LOCK:
                _LATE_CALLBACKS.discard(key)

    future.add_done_callback(_late)
    return True


def deliver_artifact(job: dict, *, adapters=None, loop=None):
    """Deliver an opted-in artifact request through the LIVE native adapter, or refuse.

    Returns ``None`` on success, else an explicit error string (never a silent success).
    """
    if not job.get("_artifact_delivery"):
        return None
    try:
        return _deliver(job, job["_artifact_delivery"], adapters=adapters, loop=loop)
    except Exception as exc:
        logger.error("Job '%s': artifact delivery refused: %s", job.get("id", "?"), exc,
                     exc_info=True)
        return f"artifact delivery refused: {exc}"


def _deliver(job: dict, envelope: dict, *, adapters, loop) -> Optional[str]:
    from cron.executions import (
        claim_delivery_request, prepare_delivery_request, settle_delivery_request,
    )
    from gateway.config import Platform
    from gateway.delivery import resolve_delivery_transport

    job_id = str(job.get("id") or "")
    execution_id = str(job.get("execution_id") or "")
    if not execution_id:
        return "artifact delivery requires an owned execution attempt; not sent"
    if adapters is None or loop is None or not getattr(loop, "is_running", lambda: False)():
        # Queue integration is a separate task: fail closed rather than replay unproven.
        return ("artifact delivery requires a live gateway adapter; queued artifact "
                "delivery is not activated; not sent")
    targets = _targets(job)
    if len(targets) != 1 or str(targets[0].get("platform", "")).lower() != "telegram":
        return "artifact delivery requires exactly one authorized Telegram target; not sent"

    from gateway.config import load_gateway_config

    config = load_gateway_config()
    target = targets[0]
    transport = resolve_delivery_transport(Platform.TELEGRAM, config, adapters)
    if transport is None or transport.is_relay:
        return ("artifact delivery requires a live native Telegram transport "
                "(relay cannot carry a byte snapshot); not sent")

    snapshots = snapshot_request(envelope)
    identity = build_request_identity(envelope, target)
    expected_target = identity["target"]
    receipt = prepare_delivery_request(execution_id, job_id, identity)
    claimed = claim_delivery_request(execution_id, receipt["request_sha256"])
    if claimed is None:
        return ("artifact request already claimed or settled; not resent "
                f"(state={receipt.get('state')})")
    attempt_nonce = claimed["attempt_nonce"]
    profile_sha256 = claimed.get("sender_profile_sha256")

    from agent.async_utils import safe_schedule_threadsafe

    coro = _dispatch(transport, config, target["chat_id"], target.get("thread_id"),
                     envelope["message"], snapshots, _metadata(job))
    future = safe_schedule_threadsafe(coro, loop)
    if future is None:
        settle_delivery_request(execution_id, receipt["request_sha256"],
                                attempt_nonce=attempt_nonce, state="failed_certain",
                                reason="gateway loop unavailable before dispatch",
                                profile_sha256=profile_sha256)
        return "artifact delivery could not reach the gateway loop; not sent"
    try:
        notification_result, artifact_results = future.result(timeout=_DISPATCH_TIMEOUT_SECS)
    except TimeoutError:
        # Keep the future ALIVE: the send may still land. The callback records the outcome.
        _install_late_callback(
            future, execution_id=execution_id, request_sha256=receipt["request_sha256"],
            attempt_nonce=attempt_nonce, profile_sha256=profile_sha256,
            message=envelope["message"], target=expected_target)
        return ("artifact delivery to telegram is unverified (timed out in flight); "
                "completion recorded when it lands; do not resend")
    ok, reason = _settle(execution_id, receipt["request_sha256"], attempt_nonce, profile_sha256,
                         envelope["message"], expected_target, notification_result, artifact_results)
    return None if ok else f"artifact delivery unverified: {reason}"


def _targets(job: dict) -> list:
    from cron.scheduler_delivery import _resolve_delivery_targets

    return _resolve_delivery_targets(job)


def _metadata(job: dict) -> dict:
    notify = True
    try:
        from cron.scheduler_delivery import _cron_delivery_notify_enabled

        notify = _cron_delivery_notify_enabled(_load_config())
    except Exception:
        logger.debug("Artifact delivery notify flag unavailable; defaulting to notify", exc_info=True)
    return {"job_id": job.get("id"), "notify": notify}


def _load_config():
    from cron import scheduler as _sched

    return _sched.load_config()
