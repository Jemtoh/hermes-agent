"""Real temporary execution-store fences for immutable artifact attempts."""
import json

import pytest

from cron import executions as e


@pytest.fixture
def ledger(tmp_path, monkeypatch):
    monkeypatch.setattr(e, 'EXECUTIONS_FILE', tmp_path / 'executions.db')
    return e


@pytest.fixture
def identity():
    return {'token': 'a' * 32, 'purpose': 'report',
            'target': {'platform': 'telegram', 'chat_id': '123', 'thread_id': None},
            'message_sha256': 'b' * 64,
            'artifacts': [{'kind': 'report', 'sha256': 'c' * 64,
                           'transport': 'document', 'size': 20}]}


def prepare(ledger, execution, identity):
    assert callable(getattr(ledger, 'prepare_delivery_request', None)), 'durable request API missing'
    return ledger.prepare_delivery_request(execution['id'], execution['job_id'], identity)


def test_prepare_reuses_exact_identity_and_refuses_drift(ledger, identity):
    first = ledger.create_execution('fake-report', source='builtin')
    receipt = prepare(ledger, first, identity)
    assert receipt['state'] == 'unsent'
    second = ledger.create_execution('fake-report', source='builtin')
    assert prepare(ledger, second, identity)['execution_id'] == first['id']
    changed = {**identity, 'message_sha256': 'd' * 64}
    with pytest.raises(ValueError):
        prepare(ledger, second, changed)
    saved = ledger.get_execution(first['id'])['delivery_receipt']
    assert json.loads(saved)['request'] == identity
    assert 'path' not in saved and 'message"' not in saved


def test_prepare_cannot_claim_foreign_or_terminal_execution(ledger, identity, monkeypatch):
    execution = ledger.create_execution('fake-report', source='builtin')
    monkeypatch.setattr(ledger, '_PROCESS_ID', 'another-process')
    with pytest.raises(ValueError):
        prepare(ledger, execution, identity)
    monkeypatch.setattr(ledger, '_PROCESS_ID', execution['process_id'])
    ledger.finish_execution(execution['id'], success=True)
    with pytest.raises(ValueError):
        prepare(ledger, execution, identity)


def test_send_claim_is_single_and_digest_bound(ledger, identity):
    execution = ledger.create_execution('fake-report', source='builtin')
    receipt = prepare(ledger, execution, identity)
    with pytest.raises(ValueError):
        ledger.claim_delivery_request(execution['id'], '0' * 64)
    claimed = ledger.claim_delivery_request(execution['id'], receipt['request_sha256'])
    assert claimed['state'] == 'sending' and claimed['attempt_nonce']
    assert ledger.claim_delivery_request(execution['id'], receipt['request_sha256']) is None
    assert ledger.get_artifact_delivery_receipt(identity['token'], 'report')['state'] == 'sending'
    with pytest.raises(ValueError):
        ledger.get_artifact_delivery_receipt(identity['token'], 'report', expected_request_sha256='0' * 64)


def test_dead_unsent_owner_can_retry_but_live_and_sending_are_fenced(ledger, identity, monkeypatch):
    first = ledger.create_execution('fake-report', source='builtin')
    receipt = prepare(ledger, first, identity)
    second = ledger.create_execution('fake-report', source='builtin')
    monkeypatch.setattr(ledger, '_owner_is_live', lambda *a: True)
    assert prepare(ledger, second, identity)['execution_id'] == first['id']
    monkeypatch.setattr(ledger, '_owner_is_live', lambda *a: False)
    retry = prepare(ledger, second, identity)
    assert retry['execution_id'] == second['id']
    old = json.loads(ledger.get_execution(first['id'])['delivery_receipt'])
    assert old['state'] == 'failed_certain'
    assert ledger.claim_delivery_request(first['id'], receipt['request_sha256']) is None
    claimed = ledger.claim_delivery_request(second['id'], retry['request_sha256'])
    third = ledger.create_execution('fake-report', source='builtin')
    assert prepare(ledger, third, identity)['execution_id'] == second['id']
    assert claimed['state'] == 'sending'


def test_receipt_tombstone_survives_legacy_execution_pruning(ledger, identity, monkeypatch):
    monkeypatch.setattr(ledger, 'MAX_TERMINAL_EXECUTIONS', 0)
    execution = ledger.create_execution('fake-report', source='builtin')
    prepare(ledger, execution, identity)
    ledger.finish_execution(execution['id'], success=True)
    assert ledger.get_execution(execution['id']) is not None
    ordinary = ledger.create_execution('ordinary', source='builtin')
    ledger.finish_execution(ordinary['id'], success=True)
    assert ledger.get_execution(ordinary['id']) is None


def test_live_owner_without_start_fingerprint_remains_fenced(ledger, identity, monkeypatch):
    from gateway import status
    first = ledger.create_execution('fake-report', source='builtin')
    prepare(ledger, first, identity)
    with ledger._transaction() as conn:
        conn.execute('UPDATE executions SET pid=?, process_started_at=NULL WHERE id=?', (987654, first['id']))
    monkeypatch.setattr(status, '_pid_exists', lambda pid: True)
    second = ledger.create_execution('fake-report', source='builtin')
    assert prepare(ledger, second, identity)['execution_id'] == first['id']


def test_concurrent_requests_share_one_durable_anchor(ledger, identity):
    import concurrent.futures
    import threading
    ready = threading.Barrier(2)
    def claim():
        execution = ledger.create_execution('fake-report', source='builtin')
        ready.wait(timeout=5)
        return prepare(ledger, execution, identity)['execution_id']
    with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
        results = list(pool.map(lambda _: claim(), range(2)))
    assert results[0] == results[1]


def test_receipts_resolve_only_within_current_profile(tmp_path, monkeypatch, identity):
    from hermes_constants import set_hermes_home_override, reset_hermes_home_override
    monkeypatch.setattr(e, 'EXECUTIONS_FILE', None)
    first = set_hermes_home_override(tmp_path / 'one')
    try:
        execution = e.create_execution('fake-report', source='builtin')
        prepare(e, execution, identity)
    finally:
        reset_hermes_home_override(first)
    second = set_hermes_home_override(tmp_path / 'two')
    try:
        assert e.get_artifact_delivery_receipt(identity['token'], 'report') is None
    finally:
        reset_hermes_home_override(second)
    again = set_hermes_home_override(tmp_path / 'one')
    try:
        assert e.get_artifact_delivery_receipt(identity['token'], 'report')['execution_id'] == execution['id']
    finally:
        reset_hermes_home_override(again)


def test_missing_json_index_fences_opt_in_without_breaking_ordinary_jobs(ledger, identity, monkeypatch):
    initialize = ledger._initialize_schema
    def no_index(conn):
        initialize(conn)
        conn.execute('DROP INDEX IF EXISTS idx_delivery_request_key')
    monkeypatch.setattr(ledger, '_initialize_schema', no_index)
    execution = ledger.create_execution('fake-report', source='builtin')
    with pytest.raises(ValueError, match='JSON index support'):
        prepare(ledger, execution, identity)
    assert ledger.finish_execution(execution['id'], success=True)['status'] == 'completed'


@pytest.mark.parametrize('bad_receipt', ['[]', '{"version":1}'])
def test_corrupt_receipt_anchor_returns_explicit_refusal(ledger, identity, bad_receipt):
    execution = ledger.create_execution('fake-report', source='builtin')
    receipt = prepare(ledger, execution, identity)
    with ledger._transaction() as conn:
        conn.execute('UPDATE executions SET delivery_receipt=? WHERE id=?', (bad_receipt, execution['id']))
    with pytest.raises(ValueError, match='corrupt artifact receipt anchor'):
        ledger.claim_delivery_request(execution['id'], receipt['request_sha256'])
