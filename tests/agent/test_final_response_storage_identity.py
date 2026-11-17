"""A turn's answer is ONE byte string: stored row, returned result, terminal stream.

The portfolio pre-write consumer authenticates a review by hashing the native session
row and comparing it with the terminal ``result`` text. Producer-side normalization
(credential masking, inline think-block stripping) used to reach the row only, so a
response containing an inlined credential was stored masked and delivered raw — two
representations of one response, and nothing attributable at the end of them.

These are behavior tests over the real turn loop, a real temporary SessionDB and the
native ``StreamJsonEmitter``. No provider is contacted; the scripted client is the only
fake.
"""

import json

import pytest

from agent import redact
from hermes_cli.stream_json import StreamJsonEmitter
import tests.agent.test_empty_response_recovery_persistence as _native
from tests.agent.test_empty_response_recovery_persistence import _response


@pytest.fixture(name="native_loop")
def _native_loop_fixture(tmp_path, monkeypatch):
    """The exported real-loop fixture, driven through its own generator.

    Registering it locally keeps the fixture name distinct from any imported
    symbol (no shadowed name) while using the SAME real AIAgent, temporary
    SessionDB and scripted provider as the module it comes from.
    """
    generator_function = getattr(getattr(_native, "real_loop", None), "__wrapped__", None)
    if generator_function is None:
        raise AssertionError("the exported real-loop fixture has no generator")
    generator = generator_function(tmp_path, monkeypatch)
    loop = next(generator)
    try:
        yield loop
    finally:
        next(generator, None)

# Shaped like a real provider key so the production redactor matches it; invented.
_FAKE_SECRET = "sk-invented-not-a-live-key-0123456789"
# Fake POLICY prose in the shape the retained evidence records. A2/P4 choose the
# producer-normalized representation, so the raw input loses its section marker and
# BOTH stored and delivered text carry the mask. The raw form stays the input.
_AUTHORIZATION_RAW = "AUTHORIZATION: CONTRACT \u00a71 remains binding."
_AUTHORIZATION_MASKED = "AUTHORIZATION: CONTRACT *** remains binding."


def _assistant_rows(db, sid):
    return [m for m in db.get_messages(sid) if m["role"] == "assistant"]


def _terminal_text(result, sid, capsys):
    capsys.readouterr()
    StreamJsonEmitter(model="test/model", session_id=sid).emit_result(result, session_id=sid)
    events = [json.loads(line) for line in capsys.readouterr().out.splitlines()]
    terminals = [event for event in events if event["type"] == "result"]
    assert len(terminals) == 1
    return terminals[0]["text"]


@pytest.mark.parametrize("raw", [
    "Review complete; retain the existing rule.",
    "Review complete.\n" + _AUTHORIZATION_RAW,
    "Review complete.\nAuthorization: Bearer " + _FAKE_SECRET + "\nretain the existing rule.",
])
def test_the_stored_answer_is_the_delivered_answer(native_loop, monkeypatch, capsys, raw):
    monkeypatch.setattr(redact, "_REDACT_ENABLED", True)
    native_loop.agent.skip_background_review = True

    result = native_loop.run([_response(raw)], "Review invented policy. Do not use tools.")

    assert result["completed"] is True
    stored = _assistant_rows(native_loop.db, native_loop.sid)[-1]["content"]
    terminal = _terminal_text(result, native_loop.sid, capsys)
    assert stored == result["final_response"]
    assert terminal == stored


def test_ordinary_text_is_stored_and_delivered_unchanged(native_loop, monkeypatch, capsys):
    monkeypatch.setattr(redact, "_REDACT_ENABLED", True)
    native_loop.agent.skip_background_review = True
    raw = "Review complete; retain the existing rule."

    result = native_loop.run([_response(raw)], "Review invented policy. Do not use tools.")

    assert result["final_response"] == raw
    assert _assistant_rows(native_loop.db, native_loop.sid)[-1]["content"] == raw
    assert _terminal_text(result, native_loop.sid, capsys) == raw


def test_authorization_prose_is_delivered_in_the_approved_masked_form(native_loop, monkeypatch, capsys):
    """Raw `AUTHORIZATION: CONTRACT §1` in, `***` out — in the row AND the answer.

    A2/P4 chose the producer-normalized representation explicitly; the defect was never
    that masking happens, it was that the row was masked while the delivered answer kept
    the raw bytes.
    """
    monkeypatch.setattr(redact, "_REDACT_ENABLED", True)
    native_loop.agent.skip_background_review = True
    raw = "Review complete.\n" + _AUTHORIZATION_RAW

    result = native_loop.run([_response(raw)], "Review invented policy. Do not use tools.")

    stored = _assistant_rows(native_loop.db, native_loop.sid)[-1]["content"]
    delivered = _terminal_text(result, native_loop.sid, capsys)
    # The approved normalized representation, in the row AND the answer.
    assert _AUTHORIZATION_MASKED in delivered
    assert "\u00a71" not in delivered
    assert stored == result["final_response"] == delivered


def test_a_fake_credential_never_reaches_the_stored_or_delivered_answer(
        native_loop, monkeypatch, capsys):
    monkeypatch.setattr(redact, "_REDACT_ENABLED", True)
    native_loop.agent.skip_background_review = True
    raw = "Review complete.\nkey=" + _FAKE_SECRET + "\nretain the existing rule."

    result = native_loop.run([_response(raw)], "Review invented policy. Do not use tools.")

    delivered = _terminal_text(result, native_loop.sid, capsys)
    stored = _assistant_rows(native_loop.db, native_loop.sid)[-1]["content"]
    assert _FAKE_SECRET not in stored
    assert _FAKE_SECRET not in delivered
    assert "retain the existing rule." in delivered
    assert stored == delivered


def test_a_plugin_rewrite_is_stored_as_the_bytes_the_user_receives(
        native_loop, monkeypatch, capsys):
    monkeypatch.setattr(redact, "_REDACT_ENABLED", True)
    native_loop.agent.skip_background_review = True
    rewrite = "Reviewed.\nkey=" + _FAKE_SECRET + "\nretain the existing rule."

    def transform(name, _logger, **payload):
        return [rewrite] if name == "transform_llm_output" else []

    monkeypatch.setattr("agent.turn_finalizer._invoke_hook_safely", transform)

    result = native_loop.run([_response("draft reply")], "Review invented policy.")

    stored = _assistant_rows(native_loop.db, native_loop.sid)[-1]["content"]
    delivered = _terminal_text(result, native_loop.sid, capsys)
    assert _FAKE_SECRET not in stored
    assert stored == delivered == result["final_response"]
    assert "Reviewed." in delivered


@pytest.mark.parametrize("raw", ["The existing rule is retained.",
                                  "Answer.\nkey=" + _FAKE_SECRET,
                                  _AUTHORIZATION_RAW])
def test_promoted_reasoning_stays_out_of_the_visible_row(native_loop, monkeypatch, capsys, raw):
    monkeypatch.setattr(redact, "_REDACT_ENABLED", True)
    native_loop.agent.skip_background_review = True
    monkeypatch.setattr("agent.turn_final_response.answer_in_reasoning_capability",
                        lambda _agent: True)
    reply = _response("")
    reply.choices[0].message.reasoning_content = raw

    result = native_loop.run([reply], "Review invented policy.")

    row = _assistant_rows(native_loop.db, native_loop.sid)[-1]
    assert row["content"] == ""
    assert row["api_content"] == result["final_response"]
    assert _FAKE_SECRET not in row["api_content"]
    assert _terminal_text(result, native_loop.sid, capsys) == row["api_content"]


def test_a_joined_continuation_keeps_the_fragment_rows_and_stores_only_its_piece(
        native_loop, monkeypatch):
    """A stitched reply must not re-store the earlier fragments in the last row.

    The joined text is the DELIVERED answer; the durable transcript keeps the
    baseline layout — each fragment in its own row — or the next turn replays the
    answer twice.
    """
    monkeypatch.setattr(redact, "_REDACT_ENABLED", True)
    native_loop.agent.skip_background_review = True
    first, second = "Review part one.", "Review part two."

    result = native_loop.run(
        [_response(first, finish_reason="length"), _response(second)],
        "Review invented policy. Do not use tools.")

    rows = [row["content"] for row in _assistant_rows(native_loop.db, native_loop.sid)]
    assert rows == [first, second], "a fragment row was duplicated or rewritten"
    assert first in result["final_response"]
    assert second in result["final_response"]


def test_recovery_only_text_is_normalized_before_it_becomes_the_row(tmp_path, monkeypatch):
    """A stream-recovered answer has no producer-side pass; the finalizer's seam is it.

    Without normalization there, the user sees the streamed text AND the durable row
    keeps the credential — the two agree only because neither was ever masked.
    """
    from hermes_state import SessionDB
    from tests.agent.test_turn_finalizer_final_response_persistence import FakeAgent
    from agent.turn_finalizer import finalize_turn

    monkeypatch.setattr(redact, "_REDACT_ENABLED", True)
    monkeypatch.setattr("hermes_cli.plugins.invoke_hook", lambda *_a, **_kw: [])

    db = SessionDB(db_path=tmp_path / "state.db")
    db.create_session("sess-test", source="cli")
    agent = FakeAgent()
    vars(agent)["_current_streamed_assistant_text"] = (
            "Recovered answer.\nkey=" + _FAKE_SECRET + "\nretain the existing rule.")

    def persist(messages, conversation_history):
        db.replace_messages(agent.session_id, messages)
        agent.persisted_messages = db.get_messages_as_conversation(agent.session_id)

    agent._persist_session = persist
    messages = [{"role": "user", "content": "summarize"},
                {"role": "assistant", "content": "", "_db_persisted": True}]

    result = finalize_turn(
        agent, final_response="", api_call_count=1, interrupted=False, failed=False,
        messages=messages, conversation_history=[], effective_task_id="task",
        turn_id="turn", user_message="summarize", original_user_message="summarize",
        _should_review_memory=False, _turn_exit_reason="text_response(final)",
    )

    delivered = result["final_response"]
    assert _FAKE_SECRET not in delivered
    assert "retain the existing rule." in delivered
    assert [m for m in (agent.persisted_messages or [])
            if m.get("role") == "assistant"][-1]["content"] == delivered
    reopened = SessionDB(db_path=tmp_path / "state.db")
    try:
        assert reopened.get_messages_as_conversation("sess-test")[-1]["content"] == delivered
    finally:
        reopened.close()
        db.close()
