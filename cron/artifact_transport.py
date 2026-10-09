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
            # Receipt FIRST: the queue row/tombstone is reconciled only from the durable
            # receipt this attempt just wrote — never from the future's value here.
            try:
                from cron.delivery_queue import reconcile_late_delivery

                reconcile_late_delivery(execution_id, request_sha256, attempt_nonce=attempt_nonce)
            except Exception:
                logger.warning("Artifact late queue reconciliation failed; receipt remains "
                               "authoritative", exc_info=True)
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

    A job carrying the prepared queue anchor is a gateway adoption and NEVER prepares a
    replacement receipt under this process identity; a job without one is the direct sender.
    Returns ``None`` on success, else an explicit error string (never a silent success).
    """
    if not job.get("_artifact_delivery"):
        return "artifact delivery envelope missing; not sent"
    try:
        if job.get("_artifact_anchor") is not None:
            return _deliver_queued(job, adapters=adapters, loop=loop)
        return _deliver(job, job["_artifact_delivery"], adapters=adapters, loop=loop)
    except Exception as exc:
        logger.error("Job '%s': artifact delivery refused: %s", job.get("id", "?"), exc,
                     exc_info=True)
        return f"artifact delivery refused: {exc}"


_PRE_DISPATCH_REFUSAL_REASON = "artifact snapshot or path policy refused before dispatch"
_IDENTITY_REFUSAL_REASON = "artifact request identity conflict before dispatch"
_TRANSPORT_REFUSAL_REASON = "artifact transport unavailable before dispatch"


class _TransportRefusal(Exception):
    """A refusal raised BEFORE any byte leaves; carries its failed_certain reason code."""

    def __init__(self, message, reason_code=None):
        super().__init__(message)
        self.reason_code = reason_code


def _target_transport(job, adapters):
    """The single authorized native Telegram transport for this job, or ``_TransportRefusal``."""
    from gateway.config import Platform
    from gateway.delivery import resolve_delivery_transport

    targets = _targets(job)
    if len(targets) != 1 or str(targets[0].get("platform", "")).lower() != "telegram":
        raise _TransportRefusal(
            "artifact delivery requires exactly one authorized Telegram target; not sent")
    from gateway.config import load_gateway_config

    config = load_gateway_config()
    transport = resolve_delivery_transport(Platform.TELEGRAM, config, adapters)
    if transport is None or transport.is_relay:
        raise _TransportRefusal(
            "artifact delivery requires a live native Telegram transport "
            "(relay cannot carry a byte snapshot); not sent",
            reason_code=_TRANSPORT_REFUSAL_REASON)
    return targets[0], transport, config


def _claim_and_dispatch(job, envelope, target, snapshots, execution_id, request_sha256,
                        attempt_nonce, profile_sha256, expected_target, transport, config, loop):
    """Shared tail: dispatch once on the live loop, then settle or fence on timeout."""
    from cron.executions import settle_delivery_request
    from cron.scheduler_provider import _profile_cron_scope
    from hermes_constants import get_hermes_home

    owning_home = get_hermes_home()
    from agent.async_utils import safe_schedule_threadsafe

    coro = _dispatch(transport, config, target["chat_id"], target.get("thread_id"),
                     envelope["message"], snapshots, _metadata(job))
    future = safe_schedule_threadsafe(coro, loop)
    if future is None:
        settle_delivery_request(execution_id, request_sha256, attempt_nonce=attempt_nonce,
                                state="failed_certain",
                                reason="gateway loop unavailable before dispatch",
                                profile_sha256=profile_sha256)
        return "artifact delivery could not reach the gateway loop; not sent"
    try:
        notification_result, artifact_results = future.result(timeout=_DISPATCH_TIMEOUT_SECS)
    except TimeoutError:
        # Keep the future ALIVE: the send may still land. The callback records the outcome.
        with _profile_cron_scope(owning_home):
            _install_late_callback(
                future, execution_id=execution_id, request_sha256=request_sha256,
                attempt_nonce=attempt_nonce, profile_sha256=profile_sha256,
                message=envelope["message"], target=expected_target)
        return ("artifact delivery to telegram is unverified (timed out in flight); "
                "completion recorded when it lands; do not resend")
    except Exception:
        logger.warning("Artifact dispatch failed in flight; no resend", exc_info=True)
        settle_delivery_request(execution_id, request_sha256, attempt_nonce=attempt_nonce,
                                state="unknown", reason="provider dispatch failed in flight",
                                profile_sha256=profile_sha256)
        return "artifact delivery unverified: provider dispatch failed; do not resend"
    ok, reason = _settle(execution_id, request_sha256, attempt_nonce, profile_sha256,
                         envelope["message"], expected_target, notification_result, artifact_results)
    if reason == "delivery suppressed; no provider send":
        return "artifact delivery suppressed; not sent"
    return None if ok else f"artifact delivery unverified: {reason}"


def enqueue_artifact_request(job: dict, content: str) -> Optional[str]:
    """Worker side of the queue handoff: prepare under the ACTIVE owned execution, then enqueue.

    Preparation runs while this process still owns its ``running`` execution row; the gateway
    cannot do it (its PID owns nothing). Only canonical identity/path metadata is serialized —
    the captured byte snapshots stay in this process and are never written to ``job_json``.
    """
    from cron.delivery_queue import enqueue_and_wait, get_status

    execution_id = str(job.get("execution_id") or "")
    try:
        _envelope, _target, _snapshots, receipt = prepare_artifact_request(job)
    except Exception as exc:
        logger.error("Job '%s': artifact preparation refused: %s", job.get("id", "?"), exc,
                     exc_info=True)
        return f"artifact delivery refused: {exc}"
    if receipt["execution_id"] != execution_id:
        return ("artifact request already claimed or settled; not enqueued "
                f"(state={receipt['state']}, execution={receipt['execution_id']})")
    job["_artifact_anchor"] = {
        "execution_id": receipt["execution_id"], "job_id": str(job.get("id") or ""),
        "request_sha256": receipt["request_sha256"], "request": receipt["request"],
    }
    error = enqueue_and_wait(execution_id, job, content)
    status = get_status(execution_id)
    if status and status["status"] == "suppressed":
        job["_notification_all_targets_suppressed"] = True
    return error


def _deliver_queued(job: dict, *, adapters, loop) -> Optional[str]:
    """Gateway side of the queue handoff: revalidate the STORED anchor, capture, claim, send.

    The gateway revalidates the carried envelope and re-reads every artifact path policy into
    immutable byte snapshots BEFORE the ``unsent -> sending`` claim. It never prepares a
    replacement receipt: the worker's durable anchor is the only authority.
    """
    from cron.artifact_delivery import validate_envelope
    from cron.executions import (
        _delivery_identity, claim_delivery_request, reconcile_delivery_request,
        settle_undispatched_delivery_request,
    )

    anchor = job.get("_artifact_anchor") or {}
    execution_id = str(anchor.get("execution_id") or "")
    request_sha256 = str(anchor.get("request_sha256") or "")
    job_id = str(anchor.get("job_id") or "")
    if not execution_id or not request_sha256 or not job_id:
        return "queued artifact delivery requires the prepared request anchor; not sent"
    if str(job.get("execution_id") or "") != execution_id or str(job.get("id") or "") != job_id:
        return "queued artifact delivery anchor does not match this job; not sent"
    if adapters is None or loop is None or not getattr(loop, "is_running", lambda: False)():
        return ("artifact delivery requires a live gateway adapter; "
                "queued artifact delivery is not activated; not sent")

    def _refused(reason, *, reason_code=None):
        if reason_code is not None:
            try:
                settle_undispatched_delivery_request(execution_id, request_sha256, reason=reason_code)
            except Exception:
                logger.warning("Artifact pre-dispatch refusal could not settle; no send was made",
                               exc_info=True)
        return reason

    try:
        target, transport, config = _target_transport(job, adapters)
    except _TransportRefusal as exc:
        return _refused(str(exc), reason_code=exc.reason_code)

    try:
        envelope = validate_envelope(json.loads(json.dumps(job["_artifact_delivery"])),
                                     verify_files=False)
        snapshots = snapshot_request(envelope)
    except (ValueError, TypeError, KeyError, OSError) as exc:
        return _refused(f"artifact delivery refused before dispatch: {exc}",
                        reason_code=_PRE_DISPATCH_REFUSAL_REASON)
    try:
        rebuilt = build_request_identity(envelope, target, snapshots)
        _key, digest = _delivery_identity(rebuilt)
    except (ValueError, TypeError, KeyError) as exc:
        return _refused(f"artifact delivery refused before dispatch: {exc}",
                        reason_code=_PRE_DISPATCH_REFUSAL_REASON)
    if digest != request_sha256 or anchor.get("request") != rebuilt:
        return _refused("artifact delivery identity conflict before dispatch",
                        reason_code=_IDENTITY_REFUSAL_REASON)
    try:
        stored = reconcile_delivery_request(execution_id, request_sha256, job_id=job_id)
    except ValueError as exc:
        return f"artifact delivery anchor conflict: {exc}"
    if stored is None:
        return "artifact request anchor is missing; not sent"
    if stored.get("request") != rebuilt:
        return _refused("artifact delivery request conflict before dispatch",
                        reason_code=_IDENTITY_REFUSAL_REASON)
    if stored.get("state") != "unsent":
        return f"artifact request already {stored.get('state')}; not resent"
    claimed = claim_delivery_request(execution_id, request_sha256)
    if claimed is None:
        return f"artifact request already claimed or settled; not resent (state={stored.get('state')})"
    return _claim_and_dispatch(
        job, envelope, target, snapshots, execution_id, request_sha256,
        claimed["attempt_nonce"], claimed.get("sender_profile_sha256"),
        stored["request"]["target"], transport, config, loop)


def _deliver(job: dict, envelope: dict, *, adapters, loop) -> Optional[str]:
    from cron.executions import claim_delivery_request

    execution_id = str(job.get("execution_id") or "")
    if not execution_id:
        return "artifact delivery requires an owned execution attempt; not sent"
    if adapters is None or loop is None or not getattr(loop, "is_running", lambda: False)():
        # Queue integration is a separate task: fail closed rather than replay unproven.
        return ("artifact delivery requires a live gateway adapter; queued artifact "
                "delivery is not activated; not sent")
    try:
        target, transport, config = _target_transport(job, adapters)
    except _TransportRefusal as exc:
        return str(exc)
    envelope, target, snapshots, receipt = prepare_artifact_request(job)
    expected_target = receipt["request"]["target"]
    if receipt["execution_id"] != execution_id:
        return ("artifact request already claimed or settled; not resent "
                f"(state={receipt['state']}, execution={receipt['execution_id']})")
    claimed = claim_delivery_request(execution_id, receipt["request_sha256"])
    if claimed is None:
        return ("artifact request already claimed or settled; not resent "
                f"(state={receipt.get('state')})")
    return _claim_and_dispatch(
        job, envelope, target, snapshots, execution_id, receipt["request_sha256"],
        claimed["attempt_nonce"], claimed.get("sender_profile_sha256"),
        expected_target, transport, config, loop)


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
