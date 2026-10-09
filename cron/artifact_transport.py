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
import json
from contextvars import copy_context
import logging
import os
import threading
from typing import Optional

logger = logging.getLogger("cron.scheduler")

# One bounded wait for the whole dispatch. A send that outlasts it is LEFT RUNNING; the
# late callback owns the outcome from then on.
_DISPATCH_TIMEOUT_SECS = 60.0
_LATE_CALLBACKS: dict = {}
_LATE_CALLBACKS_LOCK = threading.Lock()


def _sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _expected_target(chat_id, thread_id) -> dict:
    """Must match ``plugins/platforms/telegram/adapter_delivery.delivery_target`` exactly."""
    return {"platform": "telegram", "chat_id": str(chat_id),
            "thread_id": str(thread_id) if thread_id is not None else None}


def build_request_identity(envelope: dict, target: dict, snapshots=None) -> dict:
    """Canonical immutable request identity: token/purpose/target/digests/artifact metadata.

    No message text, no local path, no document bytes — the receipt is durable identity,
    not evidence. Sizes come from retained byte snapshots, before any dispatch claim.
    """
    from cron.artifact_delivery import validate_envelope

    envelope = validate_envelope(json.loads(json.dumps(envelope)), verify_files=False)
    snapshots = snapshot_request(envelope) if snapshots is None else snapshots
    if len(snapshots) != len(envelope["artifacts"]):
        raise ValueError("artifact snapshot inventory mismatch")
    for item, expected in zip(snapshots, envelope["artifacts"]):
        if (any(item[field] != expected[field] for field in ("kind", "transport", "sha256"))
                or not isinstance(item["bytes"], bytes) or _sha256(item["bytes"]) != expected["sha256"]):
            raise ValueError("artifact snapshot identity mismatch")
    artifacts = [{"kind": item["kind"], "sha256": item["sha256"],
                  "transport": item["transport"], "size": len(item["bytes"])}
                 for item in snapshots]

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
        if artifact["transport"] == "text" and data != envelope["message"].encode("utf-8"):
            raise ValueError("text artifact must match exact notification")
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
    from cron.artifact_proof import notification

    if not _succeeded(result):
        return "notification send was not successful"
    try:
        proof = _receipt_from(result)
        notification(proof, _sha256(text.encode("utf-8")), target)
        if proof["incoming_size"] != len(text.encode("utf-8")):
            return "notification size mismatch"
        if _result_id(result) != proof["chunks"][0]["message_id"]:
            return "notification provider result id mismatch"
    except (ValueError, TypeError, KeyError) as exc:
        return str(exc)
    return ""


def _result_id(result):
    return result.get("message_id") if isinstance(result, dict) else getattr(result, "message_id", None)


def _succeeded(result):
    return (result.get("success") if isinstance(result, dict)
            else getattr(result, "success", None)) is True


def _document_problem(result, snapshot: dict, target: dict) -> str:
    from cron.artifact_proof import document

    if not _succeeded(result):
        return "document send was not successful"
    try:
        document(_receipt_from(result), {"sha256": snapshot["sha256"],
                                       "size": len(snapshot["bytes"])}, target)
        if _result_id(result) != _receipt_from(result)["message_id"]:
            return "document provider result id mismatch"
    except (ValueError, TypeError, KeyError) as exc:
        return str(exc)
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


def _collect(message, target, notification_result, artifact_results, *, expected_artifacts) -> tuple:
    """``(ok, evidence, reason)`` — complete provider proof for every required artifact."""
    actual = [{"kind": snapshot["kind"], "transport": snapshot["transport"],
               "sha256": snapshot["sha256"], "size": len(snapshot["bytes"])}
              for snapshot, _ in artifact_results]
    if actual != expected_artifacts:
        return False, None, "artifact receipt inventory mismatch"
    notification_problem = _notification_problem(notification_result, message, target)
    if notification_problem:
        return False, None, notification_problem
    artifacts = []
    proof = _receipt_from(notification_result)
    for snapshot, result in artifact_results:
        if snapshot["transport"] == "text":
            if snapshot["sha256"] != _sha256(message.encode("utf-8")) or snapshot["bytes"] != message.encode("utf-8"):
                return False, None, "text artifact differs from notification"
            artifact_proof = proof
        else:
            problem = _document_problem(result, snapshot, target)
            if problem:
                return False, None, problem
            artifact_proof = _receipt_from(result)
        artifacts.append({"kind": snapshot["kind"], "transport": snapshot["transport"],
                          "proof": artifact_proof})
    return True, {"notification": proof, "artifacts": artifacts}, ""


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
    if isinstance(notification_result, dict) and notification_result.get("delivered") is False:
        return notification_result, artifact_results
    for snapshot in snapshots:
        if snapshot["transport"] == "text":
            artifact_results.append((snapshot, notification_result))
            continue
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
    from cron.executions import reconcile_delivery_request, settle_delivery_request
    from cron.artifact_proof import evidence as validate_evidence

    suppressed = (isinstance(notification_result, dict)
                  and notification_result.get("delivered") is False
                  and notification_result.get("filtered") == "silence_narration")
    anchor = reconcile_delivery_request(execution_id, request_sha256, attempt_nonce=attempt_nonce)
    if anchor is None:
        return False, "artifact request anchor missing; evidence not applied"
    ok, evidence, reason = _collect(message, target, notification_result, artifact_results,
                                    expected_artifacts=anchor["request"]["artifacts"])
    if suppressed:
        ok, evidence, reason = False, None, "delivery suppressed; no provider send"
    if ok:
        evidence = _evidence(execution_id, request_sha256, attempt_nonce, target,
                             evidence["notification"], evidence["artifacts"])
    if ok:
        try:
            validate_evidence(evidence, anchor)
        except (ValueError, TypeError, KeyError) as exc:
            ok, evidence, reason = False, None, str(exc)
    receipt = settle_delivery_request(
        execution_id, request_sha256, attempt_nonce=attempt_nonce,
        state="suppressed" if suppressed else "verified" if ok else "unknown", evidence=evidence, reason=None if ok else reason,
        profile_sha256=profile_sha256)
    if receipt is None:
        ok, reason = False, "attempt already settled; evidence not applied"
        logger.warning("Artifact attempt %s was already settled; late evidence not applied",
                       attempt_nonce)
    return ok, reason


def _install_late_callback(future, *, execution_id, request_sha256, attempt_nonce,
                           profile_sha256, message, target) -> bool:
    """Install EXACTLY ONE late-completion callback for this claimed attempt.

    The receipt settle is itself a CAS on the captured attempt nonce, so a duplicate callback
    could not double-record; the guard here keeps the callback list from growing per retry.
    """
    key = (profile_sha256, execution_id, attempt_nonce)
    with _LATE_CALLBACKS_LOCK:
        if key in _LATE_CALLBACKS:
            return False
        _LATE_CALLBACKS[key] = future

    context = copy_context()

    def _late_in_scope(done):
        try:
            notification_result, artifact_results = done.result()
            _settle(execution_id, request_sha256, attempt_nonce, profile_sha256, message,
                    target, notification_result, artifact_results)
        except Exception as exc:
            from cron.executions import settle_delivery_request

            settle_delivery_request(execution_id, request_sha256, attempt_nonce=attempt_nonce,
                                    state="unknown", reason="late provider completion failed",
                                    profile_sha256=profile_sha256)
            logger.warning("Artifact late completion could not be recorded (%s)",
                           type(exc).__name__, exc_info=True)
        finally:
            with _LATE_CALLBACKS_LOCK:
                _LATE_CALLBACKS.pop(key, None)

    def _late(done):
        try:
            context.run(_late_in_scope, done)
        except Exception:
            logger.warning("Artifact late settlement failed; outcome remains unknown", exc_info=True)

    future.add_done_callback(_late)
    return True


def deliver_artifact(job: dict, *, adapters=None, loop=None):
    """Deliver an opted-in artifact request through the LIVE native adapter, or refuse.

    Returns ``None`` on success, else an explicit error string (never a silent success).
    """
    if not job.get("_artifact_delivery"):
        return "artifact delivery envelope missing; not sent"
    try:
        return _deliver(job, job["_artifact_delivery"], adapters=adapters, loop=loop)
    except Exception as exc:
        logger.error("Job '%s': artifact delivery refused: %s", job.get("id", "?"), exc,
                     exc_info=True)
        return f"artifact delivery refused: {exc}"


def _deliver(job: dict, envelope: dict, *, adapters, loop) -> Optional[str]:
    from cron.executions import (
        claim_delivery_request, settle_delivery_request,
    )
    from gateway.config import Platform
    from gateway.delivery import resolve_delivery_transport

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

    from cron.scheduler_provider import _profile_cron_scope
    from hermes_constants import get_hermes_home

    owning_home = get_hermes_home()
    envelope, target, snapshots, receipt = prepare_artifact_request(job)
    expected_target = receipt["request"]["target"]
    if receipt["execution_id"] != execution_id:
        return ("artifact request already claimed or settled; not resent "
                f"(state={receipt['state']}, execution={receipt['execution_id']})")
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
        with _profile_cron_scope(owning_home):
            _install_late_callback(
                future, execution_id=execution_id, request_sha256=receipt["request_sha256"],
            attempt_nonce=attempt_nonce, profile_sha256=profile_sha256,
            message=envelope["message"], target=expected_target)
        return ("artifact delivery to telegram is unverified (timed out in flight); "
                "completion recorded when it lands; do not resend")
    except Exception:
        logger.warning("Artifact dispatch failed in flight; no resend", exc_info=True)
        settle_delivery_request(execution_id, receipt["request_sha256"], attempt_nonce=attempt_nonce,
                                state="unknown", reason="provider dispatch failed in flight",
                                profile_sha256=profile_sha256)
        return "artifact delivery unverified: provider dispatch failed; do not resend"
    ok, reason = _settle(execution_id, receipt["request_sha256"], attempt_nonce, profile_sha256,
                         envelope["message"], expected_target, notification_result, artifact_results)
    if reason == "delivery suppressed; no provider send":
        return "artifact delivery suppressed; not sent"
    return None if ok else f"artifact delivery unverified: {reason}"


def prepare_artifact_request(job: dict) -> tuple:
    """Prepare under the active execution owner; return ephemeral sender inputs and anchor.

    Returns ``(envelope, target, snapshots, receipt)``. Only the receipt's identity is
    JSON-safe queue metadata: never serialize snapshots. A prior execution anchor is a
    refusal to enqueue/send, including when its state is already verified.
    """
    from cron.artifact_delivery import validate_envelope
    from cron.executions import prepare_delivery_request

    targets = _targets(job)
    if len(targets) != 1 or str(targets[0].get("platform", "")).lower() != "telegram":
        raise ValueError("artifact delivery requires exactly one authorized Telegram target")
    envelope = validate_envelope(json.loads(json.dumps(job["_artifact_delivery"])), verify_files=False)
    snapshots = snapshot_request(envelope)
    target = targets[0]
    identity = build_request_identity(envelope, target, snapshots)
    receipt = prepare_delivery_request(str(job["execution_id"]), str(job["id"]), identity)
    return envelope, target, snapshots, receipt


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
