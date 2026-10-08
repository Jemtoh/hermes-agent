"""Prerequisite capability probes for the cron run/delivery lifecycle extension.

These are the fake-seam measurements the extension spec cites:
``website/docs/developer-guide/fundraising-runtime-extension-spec.md`` (Phase B/C/D
prerequisites) and its plan ``fundraising-runtime-extension-plan.md``.

They run against a temporary ``HERMES_HOME`` and call the REAL seams with fake
transport functions. No model, no network, no gateway adapter is started, and no
live job is fired.

Each assertion annotated ``BASELINE GAP`` pins the PRE-extension contract that
blocks a phase. Those probes are evidence, not a permanent regression suite: the
extension's own acceptance tests replace them once the phase is commissioned
(the plan says which). Keep them honest; do not "fix" one by weakening it.
"""
from __future__ import annotations

import sqlite3

import pytest


@pytest.fixture
def temp_home(tmp_path, monkeypatch):
    """Isolated HERMES_HOME; the cron store and both ledgers land in the temp dir."""
    monkeypatch.setenv("HERMES_HOME", str(tmp_path))
    yield tmp_path


# --------------------------------------------------------------------------
# Phase B — producer lifecycle / fire claim
# --------------------------------------------------------------------------

def test_fire_claim_fence_identity_is_the_job_id_not_the_consumer(temp_home):
    """Two producers serving one consumer do NOT collide: the fire fence is keyed by
    job id + owner, so a second job (or a second manual run of another job) passes
    the same fence. A consumer-global lock does not exist here."""
    from cron.jobs import claim_job_for_fire, create_job, fire_claim_fence

    job_a = create_job(prompt="a", schedule="every 5m", name="producer-a")
    job_b = create_job(prompt="b", schedule="every 5m", name="producer-b")

    claimed_a = claim_job_for_fire(job_a["id"], manual=True, return_job=True)
    claimed_b = claim_job_for_fire(job_b["id"], manual=True, return_job=True)
    owner_a = claimed_a["fire_claim"]["by"]
    owner_b = claimed_b["fire_claim"]["by"]
    assert owner_a and owner_b and owner_a != owner_b

    with fire_claim_fence(job_a["id"], expected_owner=owner_a) as owns_a:
        assert owns_a is True
        with fire_claim_fence(job_b["id"], expected_owner=owner_b) as owns_b:
            assert owns_b is True
    # A different job's owner is not this job's owner.
    with fire_claim_fence(job_a["id"], expected_owner=owner_b) as cross:
        assert cross is False


def test_running_registration_dedupes_one_job_id_only(temp_home):
    """``try_register_running_job`` is the ticker/manual in-flight dedupe and is per
    job id: the SAME job cannot run twice, a sibling job is admitted immediately.
    Two producers for one consumer therefore overlap freely."""
    from cron.scheduler import (
        release_running_job, try_register_running_job,
    )

    owner = object()
    assert try_register_running_job("producer-a", owner=owner) is True
    assert try_register_running_job("producer-a", owner=owner) is False
    assert try_register_running_job("producer-b", owner=owner) is True
    release_running_job("producer-a", owner=owner)
    release_running_job("producer-b", owner=owner)
    # Released: the same id is registrable again.
    assert try_register_running_job("producer-a", owner=owner) is True
    release_running_job("producer-a", owner=owner)


def test_pre_agent_stage_returns_before_the_agent_is_built(temp_home):
    """``_prepare_job_prompt`` is the pre-agent gate stage and can short-circuit.
    Anything holding a lock only around the agent build therefore does NOT span
    preflight -> agent -> saved output: the early return leaves it uncovered."""
    from cron.scheduler import _prepare_job_prompt

    early, prompt = _prepare_job_prompt(
        {"id": "empty-payload", "name": "empty", "prompt": None},
        "empty-payload", "empty", None, None,
    )
    assert early is not None
    assert prompt is None


# --------------------------------------------------------------------------
# Phase C — execution / delivery ledger correlation
# --------------------------------------------------------------------------

def test_execution_ledger_stores_an_outcome_without_artifact_binding(temp_home):
    """The executions ledger records a delivery disposition that the scheduler
    writes; the row carries no sweep token, report digest or ordered-chunk
    evidence, so ``delivered`` cannot be proved against saved bytes."""
    from cron.executions import create_execution, finish_execution, get_execution

    execution = create_execution("job-1", source="direct")
    finish_execution(execution["id"], success=True, delivery_outcome="delivered")
    row = get_execution(execution["id"])
    assert row["status"] == "completed"
    assert row["delivery_outcome"] == "delivered"

    # BASELINE GAP: the delivery columns carry no artifact correlation. Replace this
    # probe with the extension's own receipt-schema tests when Phase C is commissioned.
    db = temp_home / "cron" / "executions.db"
    with sqlite3.connect(db) as conn:
        columns = {r[1] for r in conn.execute("PRAGMA table_info(executions)")}
    assert not columns & {
        "report_sha256", "sweep_token", "chunk_ids", "notification_sha256",
    }


def test_delivery_queue_keeps_queued_failed_and_unknown_distinct(temp_home):
    """Reuse the existing durable queue as the C extension surface. A queued send is
    ``pending``; an abandoned mid-send is ``unknown`` (never replayed); a drain whose
    transport raises is ``failed``. None of them is ``delivered``."""
    from cron.delivery_queue import (
        _terminalize_wait_timeout, claim_next, drain, enqueue, get_status,
    )

    job = {"id": "job-1", "name": "probe"}
    enqueue("exec-1", job, "body")
    assert get_status("exec-1")["status"] == "pending"
    # Idempotent by execution id: a second enqueue reuses the row.
    assert enqueue("exec-1", job, "body")["status"] == "pending"

    def _refusing_send(_job, _content, _for_failure):
        raise RuntimeError("transport refused")

    assert drain(_refusing_send) == 1
    failed = get_status("exec-1")
    assert failed["status"] == "failed"
    assert "transport refused" in failed["error"]

    # A row still pending when the waiter gives up stays QUEUED (deferral, not failure).
    enqueue("exec-2", job, "body")
    assert _terminalize_wait_timeout("exec-2") == ""
    assert get_status("exec-2")["status"] == "pending"

    # A row caught mid-send is fenced unknown and is never retried.
    claimed = claim_next()
    assert claimed is not None and claimed["execution_id"] == "exec-2"
    assert "unknown" in _terminalize_wait_timeout("exec-2")
    unknown = get_status("exec-2")
    assert unknown["status"] == "unknown"
    assert "not retried" in unknown["error"]


def test_adapter_success_without_evidence_is_accepted_but_flagged():
    """Delivery honesty today rests on the adapter's returned fields, not on the
    artifact: a bare ``success=True`` is accepted and only flagged unverified, while
    the silence-filter dict and a failed result are refused."""
    from cron.scheduler_delivery import _confirm_adapter_delivery
    from gateway.platforms.base import SendResult

    unverified: list = []
    assert _confirm_adapter_delivery(SendResult(success=True), "job-1", unverified) is True
    assert unverified  # logged as UNVERIFIED, not proved

    assert _confirm_adapter_delivery(
        SendResult(success=True, message_id="42"), "job-1", []) is True
    assert _confirm_adapter_delivery(SendResult(success=False, error="boom"), "job-1", []) is False
    assert _confirm_adapter_delivery({"success": True, "delivered": False}, "job-1", []) is False
    assert _confirm_adapter_delivery(None, "job-1", []) is False


def test_queued_outcome_is_not_the_delivered_outcome():
    """The scheduler's outcome vocabulary already distinguishes queued / suppressed /
    failed from delivered — the C extension must reuse it, not invent a second one."""
    from cron.scheduler import _classify_delivery_outcome

    def classify(**over):
        kwargs = dict(
            delivery_error=None, should_deliver=True, unresolved_origin=False,
            normalized_deliver="home", incident_acked=False, success=True,
            delivery_queued=None, notification_suppressed=False)
        kwargs.update(over)
        return _classify_delivery_outcome(**kwargs)

    assert classify(delivery_queued=True) == "queued"
    assert classify() == "delivered"
    assert classify(delivery_error="timeout") == "failed"
    assert classify(notification_suppressed=True) == "suppressed"


# --------------------------------------------------------------------------
# Phase D — transport shape and limits
# --------------------------------------------------------------------------

def test_split_transport_is_not_byte_identical_to_the_saved_artifact():
    """Chunking appends ``(n/N)`` indicators, so transported bytes differ from the
    saved artifact. A digest of the saved file can only be correlated with the
    delivered message AFTER reconstructing the chunked form."""
    from gateway.platforms.base import BasePlatformAdapter

    original = ("word " * 900).strip()
    chunks = BasePlatformAdapter.truncate_message(original, 4096)
    assert len(chunks) > 1
    assert all(len(chunk) <= 4096 for chunk in chunks)
    assert any("(1/" in chunk for chunk in chunks)
    assert "".join(chunks) != original


def test_telegram_declares_protocol_caps_and_caption_is_truncated():
    """The D inventory, read from the adapter: 4096-char messages, 32,768-char rich
    messages, 1024-char attachment captions, 20 MiB document limit on the public Bot
    API (2 GiB when a local ``base_url`` is configured). Attachment ORDERED ids exist
    but no payload digest does."""
    from gateway.config import PlatformConfig
    from plugins.platforms.telegram.adapter import TelegramAdapter

    assert TelegramAdapter.MAX_MESSAGE_LENGTH == 4096
    assert TelegramAdapter.RICH_MESSAGE_MAX_CHARS == 32768
    assert len(TelegramAdapter._caption_1024("x" * 5000)) == 1024

    public = TelegramAdapter(PlatformConfig(enabled=True, token="123456:fixture", extra={}))
    local = TelegramAdapter(PlatformConfig(
        enabled=True, token="123456:fixture", extra={"base_url": "http://127.0.0.1:8081"}))
    assert public._bot is None and local._bot is None  # constructed, never connected
    assert public._max_doc_bytes == 20 * 1024 * 1024
    assert local._max_doc_bytes == 2 * 1024 * 1024 * 1024
