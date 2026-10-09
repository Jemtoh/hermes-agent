"""Opted-in artifact delivery through the DURABLE QUEUE: adoption, drain, late reconciliation.

Fakes only: a temp execution store, a temp delivery queue, a temp artifact file, an
in-process fake adapter and a real asyncio loop. No live cache, vault, Notion, broker,
provider, network or message is touched. Covers the five required case groups of
``website/docs/developer-guide/cron-artifact-delivery-plan.md`` + the appended queue plan.
"""

import asyncio
import concurrent.futures
import hashlib
import json
import sqlite3
import threading
import time
from unittest.mock import MagicMock

import pytest

from cron import artifact_transport as t
from cron import delivery_queue as q
from cron import executions as e
from gateway.config import Platform, PlatformConfig

REPORT = b"full report body\n"
NOTICE = "Report ready.\nMEDIA:/untrusted/prose.txt\n"


def _sha(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _target(chat_id="123") -> dict:
    return {"platform": "telegram", "chat_id": str(chat_id), "thread_id": None}


@pytest.fixture(autouse=True)
def _ledger(tmp_path, monkeypatch):
    monkeypatch.setattr(e, "EXECUTIONS_FILE", tmp_path / "executions.db")
    return e


@pytest.fixture(autouse=True)
def _queue(tmp_path, monkeypatch):
    monkeypatch.setattr(q, "DELIVERY_DB", tmp_path / "deliveries.db")
    return q


@pytest.fixture
def live_loop():
    loop = asyncio.new_event_loop()
    thread = threading.Thread(target=loop.run_forever, daemon=True)
    thread.start()
    try:
        yield loop
    finally:
        loop.call_soon_threadsafe(loop.stop)
        thread.join(timeout=5)
        loop.close()


@pytest.fixture
def artifact(tmp_path):
    path = tmp_path / "full.txt"
    path.write_bytes(REPORT)
    return path


@pytest.fixture
def envelope(artifact):
    return {"version": 1, "token": "a" * 32, "purpose": "report", "message": NOTICE,
            "message_sha256": _sha(NOTICE.encode()),
            "artifacts": [{"kind": "report", "path": str(artifact), "sha256": _sha(REPORT),
                           "transport": "document"}]}


def _config():
    config = MagicMock()
    config.platforms = {Platform.TELEGRAM: PlatformConfig(enabled=True)}
    config.get_home_channel = lambda platform: None
    return config


class _FakeAdapter:
    """Emits the Telegram proof shape and records exactly what it was asked to upload."""

    splits_long_messages = False

    def __init__(self, *, chunks=1, document_proof=True, delay=0.0):
        self.sent = []
        self.documents = []
        self.chunks = chunks
        self.document_proof = document_proof
        self.delay = delay

    async def send(self, chat_id, content, metadata=None):
        if self.delay:
            await asyncio.sleep(self.delay)
        self.sent.append(content)
        size = max(1, len(content) // self.chunks)
        pieces = [content[i * size:(i + 1) * size] for i in range(self.chunks)]
        return {"success": True, "message_id": "100", "raw_response": {"delivery_receipt": {
            "role": "notification", "provider": "telegram", "method": "send_message",
            "complete": True, "count": len(pieces), "incoming_size": len(content.encode()),
            "incoming_sha256": _sha(content.encode()), "target": _target(chat_id),
            "chunks": [{"index": i, "sha256": _sha(piece.encode()), "message_id": str(100 + i)}
                       for i, piece in enumerate(pieces)]}}}

    async def send_document(self, chat_id, file_path=None, file_name=None, snapshot=None,
                            metadata=None, **kwargs):
        self.documents.append(snapshot)
        if not self.document_proof:
            return {"success": True, "message_id": None}
        return {"success": True, "message_id": "55", "raw_response": {"delivery_receipt": {
            "role": "document", "provider": "telegram", "method": "send_document",
            "sha256": _sha(snapshot), "size": len(snapshot), "message_id": "55",
            "target": _target(chat_id)}}}


def _worker_job(envelope, execution, *, job_id=None):
    """The job dict a restart-safe worker would hand to the queue (JSON-safe)."""
    return {"id": job_id or execution["job_id"], "name": "Fake report", "deliver": "origin",
            "execution_id": execution["id"],
            "origin": {"platform": "telegram", "chat_id": "123"},
            "_artifact_delivery": envelope}


def _prepared(envelope, execution, *, job_id=None):
    """Prepare on the WORKER side (prepares durable receipt, mints the JSON-safe anchor)."""
    job = _worker_job(envelope, execution, job_id=job_id)
    _env, _target_dict, _snapshots, receipt = t.prepare_artifact_request(job)
    job["_artifact_anchor"] = {"execution_id": receipt["execution_id"],
                               "job_id": str(job["id"]),
                               "request_sha256": receipt["request_sha256"],
                               "request": receipt["request"]}
    return job


def _send(adapter, loop):
    """The gateway drain callback: a real (fake-adapter) queued artifact send."""
    return lambda job, content, for_failure: t.deliver_artifact(
        job, adapters={Platform.TELEGRAM: adapter}, loop=loop)


def _receipt(execution_id):
    return e.execution_delivery_receipt(execution_id)


def _state(execution_id):
    return (_receipt(execution_id) or {}).get("state")


def _evidence(claimed):
    """The exact provider proof shape ``cron.artifact_proof`` accepts (no bodies/paths)."""
    target = claimed["request"]["target"]
    digest = claimed["request"]["artifacts"][0]["sha256"]
    return {"execution_id": claimed["execution_id"],
            "request_sha256": claimed["request_sha256"],
            "attempt_nonce": claimed["attempt_nonce"], "target": target,
            "notification": {"role": "notification", "provider": "telegram",
                             "method": "send_message", "complete": True,
                             "incoming_size": len(NOTICE.encode()),
                             "incoming_sha256": claimed["request"]["message_sha256"],
                             "count": 1, "target": target,
                             "chunks": [{"index": 0, "message_id": "7",
                                         "sha256": _sha(NOTICE.encode())}]},
            "artifacts": [{"kind": "report", "transport": "document", "proof": {
                "role": "document", "provider": "telegram", "target": target,
                "sha256": digest, "message_id": "8", "method": "send_document",
                "size": claimed["request"]["artifacts"][0]["size"]}}]}


# ------------------------------------------------------------------ 1. adoption refusals
def test_pending_row_refuses_a_changed_request_identity(envelope, artifact, monkeypatch):
    execution = e.create_execution("fake-report", source="builtin")
    job = _prepared(envelope, execution)
    assert q.enqueue(execution["id"], job, NOTICE)["status"] == "pending"

    # (a) the existing pending row's OWN carried anchor is compared against the incoming one
    forged = {**job, "_artifact_anchor": {**job["_artifact_anchor"], "request_sha256": "0" * 64}}
    refused = q.enqueue(execution["id"], forged, NOTICE)
    assert refused["status"] == "failed" and refused["error"]
    assert q.get_status(execution["id"])["status"] == "pending"
    assert _state(execution["id"]) == "unsent"

    # (b) with no row at all, the incoming anchor's own digest must match its canonical request
    fresh = e.create_execution("fake-report", source="builtin")
    other = _prepared({**envelope, "token": "e" * 32}, fresh)
    forged_job = {**other, "_artifact_anchor": {**other["_artifact_anchor"],
                                                "request_sha256": "0" * 64}}
    assert q.enqueue(fresh["id"], forged_job, NOTICE)["status"] == "failed"
    assert q.get_status(fresh["id"]) is None      # a refusal is never inserted


def test_verified_terminal_row_returns_success_without_another_send(
        envelope, monkeypatch, live_loop):
    monkeypatch.setattr("gateway.config.load_gateway_config", _config)
    execution = e.create_execution("fake-report", source="builtin")
    job = _prepared(envelope, execution)
    q.enqueue(execution["id"], job, NOTICE)
    adapter = _FakeAdapter()

    assert q.drain(_send(adapter, live_loop)) == 1
    row = q.get_status(execution["id"])
    assert row["status"] == "delivered" and row["job_json"] == "{}" and row["content"] == ""
    assert _state(execution["id"]) == "verified"

    replay = q.enqueue(execution["id"], job, NOTICE)
    assert replay["status"] == "delivered"
    assert len(adapter.documents) == len(adapter.sent) == 1

    forged = {**job, "_artifact_delivery": {**envelope, "token": "b" * 32}}
    assert q.enqueue(execution["id"], forged, NOTICE)["status"] == "failed"


def test_pruned_tombstone_matching_receipt_is_success_and_mismatch_refuses(
        envelope, monkeypatch, live_loop):
    monkeypatch.setattr("gateway.config.load_gateway_config", _config)
    monkeypatch.setattr(q, "MAX_TERMINAL_DELIVERIES", 0)
    execution = e.create_execution("fake-report", source="builtin")
    job = _prepared(envelope, execution)
    q.enqueue(execution["id"], job, NOTICE)
    adapter = _FakeAdapter()
    assert q.drain(_send(adapter, live_loop)) == 1

    assert q.get_status(execution["id"])["status"] == "delivered"  # only the tombstone survives
    replay = q.enqueue(execution["id"], job, NOTICE)
    assert replay["status"] == "delivered"
    assert len(adapter.documents) == 1

    forged = {**job, "_artifact_delivery": {**envelope, "purpose": "report", "token": "c" * 32}}
    refusal = q.enqueue(execution["id"], forged, NOTICE)
    assert refusal["status"] == "failed" and "refused" in refusal["error"]


def test_opted_in_adoption_of_a_legacy_row_refuses(envelope, artifact):
    execution = e.create_execution("fake-report", source="builtin")
    job = _prepared(envelope, execution)
    q.enqueue(execution["id"], {"id": job["id"]}, "legacy content")

    refused = q.enqueue(execution["id"], job, NOTICE)

    assert refused["status"] == "failed" and "does not match" in refused["error"]
    assert q.get_status(execution["id"])["status"] == "pending"


def test_forged_envelope_against_the_anchor_refuses(envelope, artifact):
    execution = e.create_execution("fake-report", source="builtin")
    job = _prepared(envelope, execution)
    job["_artifact_delivery"] = {**envelope, "token": "d" * 32}

    refused = q.enqueue(execution["id"], job, NOTICE)

    assert refused["status"] == "failed" and "refused" in refused["error"]


# ------------------------------------------------------- 2. worker / gateway integration
def test_worker_prepares_durably_before_enqueue_and_serializes_no_bytes(
        envelope, artifact, monkeypatch):
    execution = e.create_execution("fake-report", source="builtin")
    job = _worker_job(envelope, execution)
    seen = {}

    def _spy(execution_id, queued_job, content, *, for_failure=False, timeout=None):
        seen["receipt_state"] = _state(execution_id)
        seen["payload"] = json.dumps(queued_job)
        seen["anchor"] = queued_job.get("_artifact_anchor")
        return None

    monkeypatch.setattr(q, "enqueue_and_wait", _spy)
    monkeypatch.setattr("gateway.config.load_gateway_config", _config)

    assert t.enqueue_artifact_request(job, NOTICE) is None
    assert seen["receipt_state"] == "unsent"          # prepared BEFORE the queue insert
    assert seen["anchor"]["execution_id"] == execution["id"]
    assert seen["anchor"]["job_id"] == job["id"]
    assert "full report body" not in seen["payload"]  # never the snapshot bytes
    assert str(artifact) in seen["payload"]           # paths only, revalidated at the sender


def test_gateway_queued_send_uses_the_worker_anchor_and_original_ids(
        envelope, monkeypatch, live_loop):
    monkeypatch.setattr("gateway.config.load_gateway_config", _config)
    execution = e.create_execution("fake-report", source="builtin")
    job = _prepared(envelope, execution)
    q.enqueue(execution["id"], job, NOTICE)
    adapter = _FakeAdapter(chunks=2)

    assert q.drain(_send(adapter, live_loop)) == 1

    receipt = _receipt(execution["id"])
    assert receipt["state"] == "verified"
    assert receipt["execution_id"] == execution["id"]
    assert receipt["request"]["target"] == _target()
    assert receipt["evidence"]["notification"]["count"] == 2
    assert q.get_status(execution["id"])["status"] == "delivered"
    assert adapter.documents == [REPORT]


def test_changed_file_is_refused_before_the_receipt_claim_and_provider_call(
        envelope, artifact, monkeypatch, live_loop):
    monkeypatch.setattr("gateway.config.load_gateway_config", _config)
    execution = e.create_execution("fake-report", source="builtin")
    job = _prepared(envelope, execution)
    q.enqueue(execution["id"], job, NOTICE)
    adapter = _FakeAdapter()
    artifact.write_bytes(b"swapped after the worker prepared")

    assert q.drain(_send(adapter, live_loop)) == 1

    assert adapter.sent == [] and adapter.documents == []
    assert _state(execution["id"]) == "failed_certain"
    assert q.get_status(execution["id"])["status"] == "failed"


def test_a_nested_execution_id_cannot_adopt_the_anchor(envelope, monkeypatch, live_loop):
    monkeypatch.setattr("gateway.config.load_gateway_config", _config)
    execution = e.create_execution("fake-report", source="builtin")
    job = _prepared(envelope, execution)
    q.enqueue(execution["id"], job, NOTICE)
    adapter = _FakeAdapter()
    nested = {**job, "execution_id": "another-execution"}

    error = t.deliver_artifact(nested, adapters={Platform.TELEGRAM: adapter}, loop=live_loop)

    assert "does not match this job" in error
    assert adapter.sent == [] and adapter.documents == []
    assert _state(execution["id"]) == "unsent"


def test_deliver_result_routes_an_external_worker_to_prepare_then_enqueue(envelope, monkeypatch):
    """Worker -> prepare -> enqueue; the gateway path never takes this branch."""
    from cron import scheduler_delivery

    monkeypatch.setenv("_HERMES_CRON_EXTERNAL_WORKER", "exec-worker")
    seen = {}

    def _fake_enqueue(job, content):
        seen["job"], seen["content"] = job, content
        return "queued"

    monkeypatch.setattr("cron.artifact_transport.enqueue_artifact_request", _fake_enqueue)
    result = scheduler_delivery._deliver_result(
        {"id": "fake-report", "execution_id": "exec-worker", "_artifact_delivery": envelope},
        NOTICE)

    assert result == "queued"
    assert seen["job"]["execution_id"] == "exec-worker" and seen["content"] == NOTICE

    monkeypatch.setattr("cron.artifact_transport.enqueue_artifact_request",
                        lambda *a, **k: pytest.fail("gateway took the worker enqueue branch"))
    monkeypatch.setattr("cron.artifact_transport.deliver_artifact", lambda *a, **k: "adopted")
    assert scheduler_delivery._deliver_result(
        {"id": "fake-report", "execution_id": "exec-worker", "_artifact_delivery": envelope},
        NOTICE, adapters={"telegram": object()}, loop=object()) == "adopted"


# ------------------------------------------------------------------------ 3. race fences
def test_dead_owner_prepare_reclaims_only_after_failed_certain(envelope, monkeypatch, live_loop):
    """Preparation -> queue crash gap: the dead unsent anchor is closed, not stranded."""
    first = e.create_execution("fake-report", source="builtin")
    job = _prepared(envelope, first)
    q.enqueue(first["id"], job, NOTICE)
    monkeypatch.setattr("gateway.status._pid_exists", lambda pid: False)

    second = e.create_execution("fake-report", source="builtin")
    retry = _prepared(envelope, second)

    assert retry["_artifact_anchor"]["execution_id"] == second["id"]
    old = json.loads(e.get_execution(first["id"])["delivery_receipt"])
    assert old["state"] == "failed_certain" and old["reason"] == "owner exited before dispatch"
    # The superseded queue row can never dispatch: drain reconciles it from the receipt.
    monkeypatch.setattr("gateway.config.load_gateway_config", _config)
    adapter = _FakeAdapter()
    assert q.drain(_send(adapter, live_loop)) == 0
    assert adapter.sent == [] and adapter.documents == []
    assert q.get_status(first["id"])["status"] == "failed"


def test_failed_certain_pending_row_never_dispatches(envelope, monkeypatch, live_loop):
    monkeypatch.setattr("gateway.config.load_gateway_config", _config)
    execution = e.create_execution("fake-report", source="builtin")
    job = _prepared(envelope, execution)
    q.enqueue(execution["id"], job, NOTICE)
    e.settle_undispatched_delivery_request(
        execution["id"], job["_artifact_anchor"]["request_sha256"],
        reason="artifact snapshot or path policy refused before dispatch")
    adapter = _FakeAdapter()

    # Even a direct gateway adoption of the row may not send.
    direct = t.deliver_artifact(job, adapters={Platform.TELEGRAM: adapter}, loop=live_loop)
    assert "already failed_certain; not resent" in direct
    assert adapter.sent == [] and adapter.documents == []
    assert q.drain(_send(adapter, live_loop)) == 0
    assert adapter.sent == [] and adapter.documents == []
    assert q.get_status(execution["id"])["status"] == "failed"


def test_live_owner_keeps_the_fence_and_pid_reuse_does_not_reclaim(envelope, monkeypatch):
    first = e.create_execution("fake-report", source="builtin")
    job = _prepared(envelope, first)
    assert job["_artifact_anchor"]["execution_id"] == first["id"]
    monkeypatch.setattr("gateway.status._pid_exists", lambda pid: True)
    monkeypatch.setattr(e, "_owner_is_live", lambda *a: True)

    second = e.create_execution("fake-report", source="builtin")
    again = _prepared(envelope, second)

    assert again["_artifact_anchor"]["execution_id"] == first["id"]
    assert _state(first["id"]) == "unsent"


def test_two_connection_claim_contention_sends_once():
    q.enqueue("exec-shared", {"id": "job"}, "brief")
    ready = threading.Barrier(2)

    def _claim():
        ready.wait(timeout=5)
        return q.claim_next()

    with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
        claimed = list(pool.map(lambda _: _claim(), range(2)))

    assert sum(1 for row in claimed if row is not None) == 1


# ------------------------------------------------------------------ 4. delayed completion
def test_timeout_then_late_verified_reconciles_the_queue_row(envelope, monkeypatch, live_loop):
    monkeypatch.setattr(t, "_DISPATCH_TIMEOUT_SECS", 0.05)
    monkeypatch.setattr("gateway.config.load_gateway_config", _config)
    execution = e.create_execution("fake-report", source="builtin")
    job = _prepared(envelope, execution)
    q.enqueue(execution["id"], job, NOTICE)
    adapter = _FakeAdapter(delay=0.5)

    assert q.drain(_send(adapter, live_loop)) == 1
    assert q.get_status(execution["id"])["status"] == "unknown"
    assert q.get_status(execution["id"])["job_json"] == "{}"    # payload redacted

    deadline = time.time() + 10
    while time.time() < deadline and _state(execution["id"]) != "verified":
        time.sleep(0.05)
    assert _state(execution["id"]) == "verified"
    assert q.get_status(execution["id"])["status"] == "delivered"
    assert len(adapter.documents) == len(adapter.sent) == 1     # never replayed


def test_late_verified_recovers_a_tombstone_only(envelope, monkeypatch, live_loop):
    monkeypatch.setattr(t, "_DISPATCH_TIMEOUT_SECS", 0.05)
    monkeypatch.setattr("gateway.config.load_gateway_config", _config)
    monkeypatch.setattr(q, "MAX_TERMINAL_DELIVERIES", 0)
    execution = e.create_execution("fake-report", source="builtin")
    job = _prepared(envelope, execution)
    q.enqueue(execution["id"], job, NOTICE)
    adapter = _FakeAdapter(delay=0.5)

    assert q.drain(_send(adapter, live_loop)) == 1
    assert q.get_status(execution["id"])["status"] == "unknown"

    deadline = time.time() + 10
    while time.time() < deadline and q.get_status(execution["id"])["status"] != "delivered":
        time.sleep(0.05)
    assert q.get_status(execution["id"])["status"] == "delivered"
    assert len(adapter.documents) == 1


def test_wrong_nonce_or_absent_verified_receipt_refuses_reconciliation(envelope):
    execution = e.create_execution("fake-report", source="builtin")
    job = _prepared(envelope, execution)
    q.enqueue(execution["id"], job, NOTICE)
    q.claim_next()
    sha = job["_artifact_anchor"]["request_sha256"]

    assert q.reconcile_late_delivery(execution["id"], sha, attempt_nonce="wrong-nonce") is False
    assert q.reconcile_late_delivery(execution["id"], "0" * 64) is False
    assert q.get_status(execution["id"])["status"] == "delivering"   # nothing adopted


def test_late_settlement_failure_stays_unknown(envelope, monkeypatch, live_loop):
    monkeypatch.setattr(t, "_DISPATCH_TIMEOUT_SECS", 0.05)
    monkeypatch.setattr("gateway.config.load_gateway_config", _config)
    execution = e.create_execution("fake-report", source="builtin")
    job = _prepared(envelope, execution)
    q.enqueue(execution["id"], job, NOTICE)
    adapter = _FakeAdapter(delay=0.5)
    monkeypatch.setattr(t, "_settle",
                        lambda *a, **k: (_ for _ in ()).throw(RuntimeError("storage down")))

    assert q.drain(_send(adapter, live_loop)) == 1
    deadline = time.time() + 10
    while time.time() < deadline and _state(execution["id"]) != "unknown":
        time.sleep(0.05)

    assert _state(execution["id"]) == "unknown"
    assert q.get_status(execution["id"])["status"] == "unknown"
    assert len(adapter.documents) == 1


# -------------------------------------------------------------------- 5. crash gaps
def test_receipt_first_crash_is_reconciled_on_drain_without_send(envelope, monkeypatch, live_loop):
    monkeypatch.setattr("gateway.config.load_gateway_config", _config)
    execution = e.create_execution("fake-report", source="builtin")
    job = _prepared(envelope, execution)
    q.enqueue(execution["id"], job, NOTICE)
    assert q.claim_next() is not None                      # sender owned the queue row
    # The sender wrote its durable receipt and died BEFORE its queue write.
    claim = e.claim_delivery_request(execution["id"], job["_artifact_anchor"]["request_sha256"])
    e.settle_delivery_request(execution["id"], job["_artifact_anchor"]["request_sha256"],
                              attempt_nonce=claim["attempt_nonce"], state="verified",
                              evidence=_evidence(claim))
    adapter = _FakeAdapter()

    assert q.drain(_send(adapter, live_loop)) == 0         # nothing to send: already settled
    assert adapter.sent == [] and adapter.documents == []
    assert q.get_status(execution["id"])["status"] == "delivered"


def test_worker_finish_race_keeps_the_verified_outcome(envelope):
    execution = e.create_execution("fake-report", source="builtin")
    job = _prepared(envelope, execution)
    claim = e.claim_delivery_request(execution["id"], job["_artifact_anchor"]["request_sha256"])

    e.settle_delivery_request(execution["id"], job["_artifact_anchor"]["request_sha256"],
                              attempt_nonce=claim["attempt_nonce"], state="verified",
                              evidence=_evidence(claim))
    record = e.finish_execution(execution["id"], success=False, error="stale timeout",
                                delivery_outcome="unknown")

    assert record["delivery_outcome"] == "delivered"
    assert record["status"] == "failed" and record["error"] == "stale timeout"


def test_failed_certain_maps_to_failed_and_no_evidence_stays_unknown(envelope):
    execution = e.create_execution("fake-report", source="builtin")
    job = _prepared(envelope, execution)
    q.enqueue(execution["id"], job, NOTICE)
    q.claim_next()

    # A generic caller success with no provider evidence can never become delivered.
    assert q._finish(execution["id"], error=None) is True
    assert q.get_status(execution["id"])["status"] == "unknown"
    assert _state(execution["id"]) == "unsent"

    second = e.create_execution("fake-report", source="builtin")
    job2 = _prepared({**envelope, "token": "f" * 32}, second)
    q.enqueue(second["id"], job2, NOTICE)
    q.claim_next()
    e.settle_undispatched_delivery_request(
        second["id"], job2["_artifact_anchor"]["request_sha256"],
        reason="artifact transport unavailable before dispatch")
    assert q._finish(second["id"], error=None) is True
    assert q.get_status(second["id"])["status"] == "failed"


# ------------------------------------------------- 6. acceptance-regression extensions
def test_existing_corrupt_payload_is_refused_not_adopted(envelope):
    """A stored row whose OWN payload disagrees with the receipt is never adopted.

    The forged row is internally consistent (envelope digest matches its content), so only the
    comparison against the durable receipt — not against the incoming job — can catch it.
    """
    execution = e.create_execution("fake-report", source="builtin")
    job = _prepared(envelope, execution)
    assert q.enqueue(execution["id"], job, NOTICE)["status"] == "pending"

    swapped = b"swapped after the row was written"
    forged = {**job, "_artifact_delivery": {**envelope, "message": swapped.decode(),
                                            "message_sha256": _sha(swapped)}}
    with sqlite3.connect(str(q.DELIVERY_DB)) as conn:
        conn.execute("UPDATE deliveries SET job_json=?, content=? WHERE execution_id=?",
                     (json.dumps(forged, ensure_ascii=False, sort_keys=True),
                      swapped.decode(), execution["id"]))

    refused = q.enqueue(execution["id"], job, NOTICE)
    assert refused["status"] == "failed" and refused["error"]
    assert q.get_status(execution["id"])["status"] == "pending"
    assert _state(execution["id"]) == "unsent"


def test_failure_alert_carrying_an_anchor_cannot_adopt_the_artifact_row(envelope):
    """``for_failure=True`` is always the legacy route, anchor or not, and never adopts the slot."""
    execution = e.create_execution("fake-report", source="builtin")
    job = _prepared(envelope, execution)
    assert q.enqueue(execution["id"], job, NOTICE)["status"] == "pending"

    carried = q.enqueue(execution["id"], job, "failure alert text", for_failure=True)
    assert carried["status"] == "failed" and "legacy" in carried["error"]
    bare = q.enqueue(execution["id"], {"id": job["id"]}, "failure alert text", for_failure=True)
    assert bare["status"] == "failed" and "legacy" in bare["error"]
    assert q.get_status(execution["id"])["status"] == "pending"
    assert _state(execution["id"]) == "unsent"


def test_legacy_delivery_cannot_adopt_an_admitted_tombstone(
        envelope, monkeypatch, live_loop):
    monkeypatch.setattr("gateway.config.load_gateway_config", _config)
    monkeypatch.setattr(q, "MAX_TERMINAL_DELIVERIES", 0)
    execution = e.create_execution("fake-report", source="builtin")
    job = _prepared(envelope, execution)
    q.enqueue(execution["id"], job, NOTICE)
    adapter = _FakeAdapter()
    assert q.drain(_send(adapter, live_loop)) == 1
    assert q.get_status(execution["id"])["status"] == "delivered"   # tombstone only

    refused = q.enqueue(execution["id"], {"id": job["id"]}, "failure alert text",
                        for_failure=True)
    assert refused["status"] == "failed" and "legacy" in refused["error"]
    assert q.get_status(execution["id"])["status"] == "delivered"


def test_prepared_receipt_cannot_adopt_an_unadmitted_legacy_terminal_row(envelope):
    """A receipt prepared on a legacy execution ID never adopts that row's terminal slot."""
    execution = e.create_execution("fake-report", source="builtin")
    q.enqueue(execution["id"], {"id": "invented"}, "invented legacy content")
    q.claim_next()
    q._finish(execution["id"], error=None, unverified=True)
    assert q.get_status(execution["id"])["status"] == "unknown"

    job = _prepared(envelope, execution)
    refused = q.enqueue(execution["id"], job, NOTICE)

    assert refused["status"] == "failed" and "provenance" in refused["error"]
    assert q.get_status(execution["id"])["status"] == "unknown"
    assert q.get_status(execution["id"])["job_json"] == "{}"


def test_contended_claim_of_an_admitted_row_sends_once(envelope):
    execution = e.create_execution("fake-report", source="builtin")
    job = _prepared(envelope, execution)
    q.enqueue(execution["id"], job, NOTICE)
    ready = threading.Barrier(2)

    def _claim():
        ready.wait(timeout=5)
        return q.claim_next()

    with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
        claimed = list(pool.map(lambda _: _claim(), range(2)))

    assert sum(1 for row in claimed if row is not None) == 1


def test_old_failed_certain_pending_row_never_dispatches_after_a_new_attempt(
        envelope, monkeypatch, live_loop):
    monkeypatch.setattr("gateway.config.load_gateway_config", _config)
    first = e.create_execution("fake-report", source="builtin")
    old = _prepared(envelope, first)
    q.enqueue(first["id"], old, NOTICE)
    e.settle_undispatched_delivery_request(
        first["id"], old["_artifact_anchor"]["request_sha256"],
        reason="artifact snapshot or path policy refused before dispatch")

    second = e.create_execution("fake-report", source="builtin")
    new = _prepared(envelope, second)
    assert new["_artifact_anchor"]["execution_id"] == second["id"]

    adapter = _FakeAdapter()
    assert q.drain(_send(adapter, live_loop)) == 0
    assert adapter.sent == [] and adapter.documents == []
    assert q.get_status(first["id"])["status"] == "failed"


def test_failed_certain_is_only_a_proven_no_send(envelope):
    """A no-send disposition needs a code-owned refusal, and can never retain evidence."""
    first = e.create_execution("fake-report", source="builtin")
    job = _prepared(envelope, first)
    sha = job["_artifact_anchor"]["request_sha256"]

    with pytest.raises(ValueError):
        e.settle_undispatched_delivery_request(first["id"], sha, reason="something went wrong")
    assert _state(first["id"]) == "unsent"

    claim = e.claim_delivery_request(first["id"], sha)
    with pytest.raises(ValueError):
        e.settle_delivery_request(first["id"], sha, attempt_nonce=claim["attempt_nonce"],
                                  state="failed_certain",
                                  reason="gateway loop unavailable before dispatch",
                                  evidence=_evidence(claim))
    assert _state(first["id"]) == "sending"      # neither attempt settled anything

    second = e.create_execution("fake-report", source="builtin")
    retry = _prepared({**envelope, "token": "b" * 32}, second)
    settled = e.settle_undispatched_delivery_request(
        second["id"], retry["_artifact_anchor"]["request_sha256"],
        reason="artifact transport unavailable before dispatch")
    assert settled["state"] == "failed_certain" and "evidence" not in settled


# ------------------------------------------- 7. root acceptance probes (9 October 2026)
# The four canonical root probes, kept with their own assertions intact. Their canonical
# out-of-tree copy lives outside this repo; they are owned regressions here so the queue
# acceptance contract cannot regress without a red test in this file.
def test_changed_incoming_payload_cannot_adopt_existing_pending(envelope):
    ex = e.create_execution("invented", source="test")
    job = _prepared(envelope, ex)
    q.enqueue(ex["id"], job, NOTICE)
    bad = json.loads(json.dumps(job))
    bad["_artifact_delivery"]["message"] = "different saved content"
    result = q.enqueue(ex["id"], bad, "different saved content")
    assert result["status"] == "failed", result["status"]


def test_receipt_first_crash_gap_redacted_unknown_row_reconciles_on_read(envelope):
    ex = e.create_execution("invented", source="test")
    job = _prepared(envelope, ex)
    q.enqueue(ex["id"], job, NOTICE)
    q.claim_next()
    q._terminalize_wait_timeout(ex["id"])
    claim = e.claim_delivery_request(ex["id"], job["_artifact_anchor"]["request_sha256"])
    e.settle_delivery_request(ex["id"], claim["request_sha256"],
                              attempt_nonce=claim["attempt_nonce"], state="verified",
                              profile_sha256=claim["sender_profile_sha256"],
                              evidence=_evidence(claim))
    result = q.get_status(ex["id"])
    assert result["status"] == "delivered", result["status"]
    assert result.get("job_json", "{}") == "{}"


def test_optin_cannot_adopt_legacy_unknown_tombstone(envelope, monkeypatch):
    ex = e.create_execution("invented", source="test")
    monkeypatch.setattr(q, "MAX_TERMINAL_DELIVERIES", 0)
    q.enqueue(ex["id"], {"id": "invented"}, "invented legacy content")
    q.claim_next()
    q._finish(ex["id"], error=None, unverified=True)
    job = _prepared(envelope, ex)
    result = q.enqueue(ex["id"], job, NOTICE)
    assert result["status"] == "failed", result["status"]


def test_reused_sender_pid_with_changed_start_cannot_settle_verified(envelope, monkeypatch):
    ex = e.create_execution("invented", source="test")
    job = _prepared(envelope, ex)
    claim = e.claim_delivery_request(ex["id"], job["_artifact_anchor"]["request_sha256"])
    monkeypatch.setattr(e, "_process_start_time", lambda pid: "invented changed process incarnation")
    result = e.settle_delivery_request(ex["id"], claim["request_sha256"],
                                       attempt_nonce=claim["attempt_nonce"], state="verified",
                                       profile_sha256=claim["sender_profile_sha256"],
                                       evidence=_evidence(claim))
    assert result is None


def test_pending_retry_repairs_interrupted_admission(envelope):
    execution = e.create_execution("invented", source="test")
    job = _prepared(envelope, execution)
    # Reproduce the former crash window: a valid row committed without its marker.
    with q._transaction() as conn:
        conn.execute("INSERT INTO deliveries (execution_id,job_json,content,status,created_at) VALUES (?,?,?,'pending',?)",
                     (execution["id"], json.dumps(job), NOTICE, "2026-10-09T00:00:00+00:00"))
    assert q.enqueue(execution["id"], job, NOTICE)["status"] == "pending"
    assert e.execution_delivery_receipt(execution["id"])["queue_admitted"] is True


def test_failed_admission_provenance_exposes_no_queue_work(envelope, monkeypatch):
    execution = e.create_execution("invented", source="test")
    job = _prepared(envelope, execution)
    monkeypatch.setattr(e, "mark_queue_admitted", lambda *args, **kwargs: False)
    with pytest.raises(ValueError, match="admission could not be persisted"):
        q.enqueue(execution["id"], job, NOTICE)
    assert q.get_status(execution["id"]) is None


@pytest.mark.parametrize("changed", ["message", "target"])
def test_terminal_adoption_rejects_changed_actual_request(envelope, changed):
    execution = e.create_execution("invented", source="test")
    job = _prepared(envelope, execution)
    q.enqueue(execution["id"], job, NOTICE)
    q.claim_next()
    claim = e.claim_delivery_request(execution["id"], job["_artifact_anchor"]["request_sha256"])
    e.settle_delivery_request(execution["id"], claim["request_sha256"],
                             attempt_nonce=claim["attempt_nonce"], state="verified", evidence=_evidence(claim))
    q._finish(execution["id"], error=None)
    incoming = json.loads(json.dumps(job))
    if changed == "message":
        incoming["_artifact_delivery"]["message"] = "changed with stale digest"
    else:
        incoming["origin"]["chat_id"] = "999"
    assert q.enqueue(execution["id"], incoming, NOTICE)["status"] == "failed"


def test_failure_lane_copies_job_without_artifact_authority(envelope):
    execution = e.create_execution("invented", source="test")
    job = _prepared(envelope, execution)
    q.enqueue(execution["id"], job, "failure notice", for_failure=True)
    sent = []
    def send(queued, content, for_failure):
        assert for_failure and "_artifact_anchor" not in queued and "_artifact_delivery" not in queued
        sent.append(content)
    q.drain(send)
    assert sent == ["failure notice"]
    assert q.get_status(execution["id"])["status"] == "delivered"
    assert "_artifact_anchor" in job


def test_legacy_insertion_cannot_take_reserved_artifact_slot(envelope):
    execution = e.create_execution("invented", source="test")
    job = _prepared(envelope, execution)
    # Receipt admission survives a rolled-back queue transaction.
    assert e.mark_queue_admitted(execution["id"], job["_artifact_anchor"]["request_sha256"])
    assert q.enqueue(execution["id"], job, "failure notice", for_failure=True)["status"] == "failed"
    assert q.get_status(execution["id"]) is None


def test_sender_incarnation_requires_exact_start(envelope, monkeypatch):
    execution = e.create_execution("invented", source="test")
    job = _prepared(envelope, execution)
    claim = e.claim_delivery_request(execution["id"], job["_artifact_anchor"]["request_sha256"])
    monkeypatch.setattr(e, "_process_start_time", lambda pid: int(claim["sender_started_at"]) + 1)
    assert e.settle_delivery_request(execution["id"], claim["request_sha256"],
                                    attempt_nonce=claim["attempt_nonce"], state="verified",
                                    evidence=_evidence(claim)) is None
