"""Hand a payload the live lane rejected as reconnect-only to the delivery ledger.

Moved out of ``cron/scheduler_delivery.py`` (which is over its file-size target) as a
topical sibling; the caller reaches it through the module alias, the same late-bound
pattern the other ``scheduler_delivery_*`` siblings use.
"""

from __future__ import annotations

import logging
from typing import Any

logger = logging.getLogger("cron.scheduler")


def queue_for_live_reconnect(t: Any, content: str, media_files: list, delivery_errors: list) -> None:
    """Hand a payload the live lane rejected as reconnect-only (``send_path_degraded``) and the
    standalone lane then failed to send to the delivery ledger, as a failed reconnect-only row
    owned by the adapter that rejected it: the post-reconnect sweep redelivers it (#125363). Only
    reached after standalone failed, so nothing was sent and a replay cannot duplicate. The ledger
    carries text only; dropped attachments are reported."""
    try:
        from gateway.delivery_ledger import (
            compute_obligation_id, is_reconnect_only, ledger_enabled, mark_failed, record_obligation)
        if not is_reconnect_only(t.live_error) or not ledger_enabled():
            return
        session_key = f"cron:{t.platform_name}:{t.chat_id}" + (f":{t.thread_id}" if t.thread_id else "")
        obligation_id = compute_obligation_id(session_key, f"job:{t.job.get('id', '?')}", content)
        record_obligation(
            obligation_id=obligation_id, session_key=session_key, platform=t.platform_name,
            chat_id=str(t.chat_id), thread_id=t.thread_id, content=content,
            adapter_profile=getattr(getattr(t.transport, "adapter", None), "_owner_profile", None))
        mark_failed(obligation_id, str(t.live_error))
    except Exception:
        logger.warning("Job '%s': could not queue %s for post-reconnect redelivery",
                       t.job.get("id"), t.where, exc_info=True)
        return
    note = f"queued text for {t.where} for redelivery once the live adapter reconnects"
    if media_files:
        note += f" ({len(media_files)} attachment(s) not queued)"
    logger.warning("Job '%s': %s", t.job.get("id"), note)
    delivery_errors.append(note)
