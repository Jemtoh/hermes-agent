"""Opt-in artifact transport: exact snapshot, provider proof, settlement and late completion.

Fakes only: a temp execution store, a temp artifact file and an in-process fake adapter.
No live cache, vault, broker, Notion or transport is touched.
"""

import asyncio
import hashlib
import json
import threading
import time
from unittest.mock import MagicMock

import pytest

from cron import artifact_transport as t
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
            "role": "notification", "provider": "telegram", "complete": True,
            "count": len(pieces), "incoming_sha256": _sha(content.encode()),
            "target": _target(chat_id),
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


def _job(envelope, execution_id):
    return {"id": "fake-report", "name": "Fake report", "deliver": "origin",
            "execution_id": execution_id,
            "origin": {"platform": "telegram", "chat_id": "123"},
            "_artifact_delivery": envelope}


def _receipt(state_only=True):
    receipt = e.get_artifact_delivery_receipt("a" * 32, "report")
    return receipt["state"] if state_only else receipt


def test_request_identity_carries_no_body_or_path(envelope, tmp_path):
    identity = t.build_request_identity(envelope, _target())
    blob = json.dumps(identity)
    assert NOTICE not in blob and str(tmp_path) not in blob
    assert identity["message_sha256"] == _sha(NOTICE.encode())
    assert identity["artifacts"] == [{"kind": "report", "sha256": _sha(REPORT),
                                      "transport": "document", "size": len(REPORT)}]


def test_complete_provider_evidence_settles_verified(monkeypatch, live_loop, envelope):
    monkeypatch.setattr("gateway.config.load_gateway_config", _config)
    execution = e.create_execution("fake-report", source="builtin")
    adapter = _FakeAdapter(chunks=2)

    assert t.deliver_artifact(_job(envelope, execution["id"]),
                              adapters={Platform.TELEGRAM: adapter}, loop=live_loop) is None

    assert adapter.sent == [NOTICE]         # exact literal notification, MEDIA left as text
    assert adapter.documents == [REPORT]    # the snapshot bytes, never a reopened path
    receipt = _receipt(state_only=False)
    assert receipt["state"] == "verified"
    assert receipt["evidence"]["notification"]["count"] == 2
    assert receipt["evidence"]["artifacts"] == [
        {"kind": "report", "sha256": _sha(REPORT), "message_id": "55",
         "method": "send_document", "size": len(REPORT)}]
    stored = json.dumps(receipt)
    assert str(envelope["artifacts"][0]["path"]) not in stored
    assert "untrusted/prose.txt" not in stored


def test_text_fallback_for_an_artifact_never_grants_verified(monkeypatch, live_loop, envelope):
    monkeypatch.setattr("gateway.config.load_gateway_config", _config)
    execution = e.create_execution("fake-report", source="builtin")
    adapter = _FakeAdapter(document_proof=False)

    result = t.deliver_artifact(_job(envelope, execution["id"]),
                                adapters={Platform.TELEGRAM: adapter}, loop=live_loop)

    assert result is not None and "unverified" in result
    assert adapter.documents == [REPORT]
    assert _receipt() == "unknown"


def test_queued_path_stays_fail_closed_without_claiming(envelope):
    execution = e.create_execution("fake-report", source="builtin")
    result = t.deliver_artifact(_job(envelope, execution["id"]), adapters=None, loop=None)
    assert result is not None and "live gateway adapter" in result
    assert _receipt(state_only=False) is None


def test_changed_file_refuses_before_any_send(monkeypatch, live_loop, envelope, artifact):
    monkeypatch.setattr("gateway.config.load_gateway_config", _config)
    execution = e.create_execution("fake-report", source="builtin")
    adapter = _FakeAdapter()
    artifact.write_bytes(b"swapped after validation")

    result = t.deliver_artifact(_job(envelope, execution["id"]),
                                adapters={Platform.TELEGRAM: adapter}, loop=live_loop)

    assert result is not None and "refused" in result
    assert adapter.sent == [] and adapter.documents == []
    assert _receipt(state_only=False) is None


def test_unknown_outcome_is_never_replayed(monkeypatch, live_loop, envelope):
    monkeypatch.setattr("gateway.config.load_gateway_config", _config)
    execution = e.create_execution("fake-report", source="builtin")
    adapter = _FakeAdapter(document_proof=False)
    job = _job(envelope, execution["id"])

    assert "unverified" in t.deliver_artifact(
        job, adapters={Platform.TELEGRAM: adapter}, loop=live_loop)
    replay = t.deliver_artifact(job, adapters={Platform.TELEGRAM: adapter}, loop=live_loop)

    assert "not resent" in replay
    assert len(adapter.documents) == 1 and len(adapter.sent) == 1
    assert _receipt() == "unknown"


def test_inflight_timeout_keeps_the_future_alive_and_settles_late(
        monkeypatch, live_loop, envelope):
    monkeypatch.setattr(t, "_DISPATCH_TIMEOUT_SECS", 0.05)
    monkeypatch.setattr("gateway.config.load_gateway_config", _config)
    execution = e.create_execution("fake-report", source="builtin")
    adapter = _FakeAdapter(delay=0.5)

    result = t.deliver_artifact(_job(envelope, execution["id"]),
                                adapters={Platform.TELEGRAM: adapter}, loop=live_loop)

    assert result is not None and "unverified" in result
    assert _receipt() == "sending"          # in flight is NOT success
    deadline = time.time() + 10
    while time.time() < deadline and _receipt() != "verified":
        time.sleep(0.05)
    assert _receipt() == "verified"
    # The late proof outranks the outcome the timeout path computed.
    assert e.finish_execution(
        execution["id"], success=True, delivery_outcome="unknown")["delivery_outcome"] == "delivered"


def test_deliver_result_routes_opt_in_and_leaves_the_failure_lane_alone(monkeypatch, envelope):
    from cron import scheduler_delivery

    seen = {}

    def _fake_transport(job, *, adapters=None, loop=None):
        seen["job"] = job
        return "routed"

    monkeypatch.setattr("cron.artifact_transport.deliver_artifact", _fake_transport)
    assert scheduler_delivery._deliver_result(
        {"id": "fake-report", "_artifact_delivery": envelope}, "body") == "routed"
    assert seen["job"]["_artifact_delivery"] is envelope

    monkeypatch.setattr("cron.artifact_transport.deliver_artifact",
                        lambda *a, **k: pytest.fail("failure lane reached the opt-in transport"))
    assert scheduler_delivery._deliver_result(
        {"id": "fake-report", "deliver": "local", "_artifact_delivery": envelope}, "body",
        for_failure=True) is None
