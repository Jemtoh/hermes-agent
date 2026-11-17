"""Provider acceptance receipts for Telegram's native send owners.

A cron delivery that must be *proven* cannot rely on ``success=True`` or a single first
message ID. These builders turn what a send owner actually submitted — and what Telegram
actually acknowledged — into the exact shape ``cron/artifact_transport.py`` validates:
the incoming text digest, every submitted chunk's index/hash, the real target, and the
full digest of an uploaded snapshot.
"""

from __future__ import annotations

import hashlib
import io
import json
import os
from typing import Any, Optional


def sha256_hex(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def open_upload_source(path: str, snapshot: Optional[bytes]):
    """Blocking binary handle for one native upload, or ``None`` when ``path`` is gone.

    Sync on purpose: the async send owner must do no blocking path/open work itself
    (ASYNC230/ASYNC240). ``snapshot`` wins — the upload always carries those exact bytes.
    """
    if snapshot is not None:
        return io.BytesIO(snapshot)
    if not os.path.exists(path):
        return None
    return open(path, "rb")


def delivery_target(chat_id: Any, thread_id: Any) -> dict:
    """Canonical target identity the artifact transport compares against."""
    return {"platform": "telegram", "chat_id": str(chat_id),
            "thread_id": str(thread_id) if thread_id is not None else None}


def chunked_notification_proof(incoming: str, chunks: list, chat_id: Any, thread_id: Any) -> dict:
    """One entry per submitted MarkdownV2 chunk, in order, with each landed message ID."""
    return {
        "role": "notification", "provider": "telegram", "method": "send_message", "complete": True,
        "count": len(chunks), "incoming_sha256": sha256_hex(incoming.encode("utf-8")),
        "incoming_size": len(incoming.encode("utf-8")),
        "target": delivery_target(chat_id, thread_id), "chunks": chunks,
    }


def rich_notification_proof(incoming: str, payload: dict, chat_id: Any, thread_id: Any,
                            message_id: Any) -> Optional[dict]:
    """One raw ``sendRichMessage`` call = one chunk; the proof hashes that exact payload.

    ``None`` when Telegram returned no message ID, so a caller never has to assume the
    legacy MarkdownV2 chunk path ran.
    """
    if not valid_message_id(message_id):
        return None
    payload_digest = sha256_hex(json.dumps(
        payload, sort_keys=True, separators=(",", ":"), default=str).encode("utf-8"))
    return {
        "role": "notification", "provider": "telegram", "method": "sendRichMessage", "complete": True, "count": 1,
        "incoming_sha256": sha256_hex(incoming.encode("utf-8")),
        "incoming_size": len(incoming.encode("utf-8")),
        "payload_sha256": payload_digest, "target": delivery_target(chat_id, thread_id),
        "chunks": [{"index": 0, "sha256": payload_digest, "message_id": str(message_id)}],
    }


def document_proof(snapshot: bytes, chat_id: Any, thread_id: Any, message_id: Any,
                   media_key: str = "document") -> Optional[dict]:
    """Proof that THESE bytes were uploaded natively and Telegram returned this message ID."""
    if not valid_message_id(message_id):
        return None
    return {
        "role": "document", "provider": "telegram", "method": f"send_{media_key}",
        "sha256": sha256_hex(snapshot), "size": len(snapshot), "message_id": str(message_id),
        "target": delivery_target(chat_id, thread_id),
    }


def valid_message_id(value):
    return type(value) in (str, int) and len(str(value)) <= 20 and str(value).isascii() and str(value).isdigit() and int(value) > 0
