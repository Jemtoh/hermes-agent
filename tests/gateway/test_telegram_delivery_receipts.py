"""Telegram's native send owners return provider acceptance evidence for opt-in cron delivery.

A single first message ID proves nothing: the receipt names the incoming digest, every
submitted chunk's index/hash/message ID, the actual target, and — for a document — the
uploaded snapshot's digest. A text fallback carries no such receipt.
"""

import hashlib
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest

from gateway.config import PlatformConfig
from gateway.platforms.base import SendResult
from plugins.platforms.telegram.adapter import TelegramAdapter


def _sha(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _target(chat_id="12345", thread_id=None) -> dict:
    return {"platform": "telegram", "chat_id": str(chat_id),
            "thread_id": str(thread_id) if thread_id is not None else None}


@pytest.fixture
def adapter() -> TelegramAdapter:
    a = TelegramAdapter(PlatformConfig(enabled=True, token="fake-token"))
    a._bot = MagicMock()
    a._retrigger_typing = AsyncMock()
    return a


@pytest.mark.asyncio
async def test_send_chunks_receipt_names_every_chunk_in_order(adapter):
    ids = iter(["11", "12", "13"])

    async def _fake_send(chat_id, chunk, index, reply_to, metadata, thread_id, used, error_types):
        return SimpleNamespace(message_id=next(ids)), False

    adapter._send_chunk_with_retries = _fake_send
    chunks = ["first", "second", "third"]
    result = await adapter._send_chunks("12345", chunks, [], None, {}, (), incoming="raw incoming")

    proof = result.raw_response["delivery_receipt"]
    assert proof["role"] == "notification" and proof["complete"] is True
    assert proof["count"] == 3 == len(proof["chunks"])
    assert [entry["index"] for entry in proof["chunks"]] == [0, 1, 2]
    assert [entry["message_id"] for entry in proof["chunks"]] == ["11", "12", "13"]
    assert [entry["sha256"] for entry in proof["chunks"]] == [
        _sha(b"first"), _sha(b"second"), _sha(b"third")]
    assert proof["incoming_sha256"] == _sha(b"raw incoming")
    assert proof["target"] == _target("12345")


@pytest.mark.asyncio
async def test_send_chunks_without_incoming_keeps_the_legacy_shape(adapter):
    async def _fake_send(chat_id, chunk, index, reply_to, metadata, thread_id, used, error_types):
        return SimpleNamespace(message_id="21"), False

    adapter._send_chunk_with_retries = _fake_send
    result = await adapter._send_chunks("12345", ["only"], [], None, {}, ())
    assert "delivery_receipt" not in result.raw_response
    assert result.raw_response["message_ids"] == ["21"]


@pytest.mark.asyncio
async def test_rich_send_receipt_hashes_the_exact_payload_once(adapter):
    adapter._bot.do_api_request = AsyncMock(return_value={"message_id": 51})
    result = await adapter._try_send_rich("12345", "rich body", None, {})
    assert result.success is True
    proof = result.raw_response["delivery_receipt"]
    assert proof["count"] == 1 and proof["complete"] is True
    assert proof["incoming_sha256"] == _sha(b"rich body")
    assert proof["chunks"][0]["index"] == 0
    assert proof["chunks"][0]["message_id"] == "51"
    assert proof["chunks"][0]["sha256"] == proof["payload_sha256"]


@pytest.mark.asyncio
async def test_native_document_send_proves_the_exact_snapshot(adapter, tmp_path):
    path = tmp_path / "full.txt"
    path.write_bytes(b"on disk, never uploaded")
    snapshot = b"snapshot bytes"
    uploaded = {}

    async def _capture(**kwargs):
        uploaded["bytes"] = kwargs["document"].read()
        return SimpleNamespace(message_id=99)

    adapter._bot.send_document = AsyncMock(side_effect=_capture)

    result = await adapter.send_document(
        chat_id="12345", file_path=str(path), snapshot=snapshot, metadata={"thread_id": "7"})

    assert result.success is True
    proof = result.raw_response["delivery_receipt"]
    assert proof["role"] == "document"
    assert proof["method"] == "send_document"
    assert proof["sha256"] == _sha(snapshot)
    assert proof["message_id"] == "99"
    assert proof["target"] == _target("12345", "7")
    assert uploaded["bytes"] == snapshot


@pytest.mark.asyncio
async def test_document_text_fallback_never_carries_native_proof(adapter, tmp_path):
    path = tmp_path / "full.txt"
    path.write_bytes(b"body")
    adapter._bot.send_document = AsyncMock(side_effect=RuntimeError("Telegram API error"))
    adapter.send = AsyncMock(return_value=SendResult(success=True, message_id="fallback"))

    result = await adapter.send_document(chat_id="12345", file_path=str(path), snapshot=b"body")

    assert result.success is True and result.message_id == "fallback"
    assert result.raw_response is None


@pytest.mark.asyncio
async def test_document_without_a_snapshot_keeps_the_legacy_shape(adapter, tmp_path):
    path = tmp_path / "full.txt"
    path.write_bytes(b"body")
    adapter._bot.send_document = AsyncMock(return_value=SimpleNamespace(message_id=7))
    result = await adapter.send_document(chat_id="12345", file_path=str(path))
    assert result.success is True and result.raw_response is None
