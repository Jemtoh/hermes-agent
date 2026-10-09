"""Durable settlement of an opted-in artifact attempt: CAS, evidence shape, late proof.

The receipt is the only authority. A settle is CAS-bound to the exact execution id,
request digest, minted attempt nonce and owning profile, happens exactly once, and stores
IDs/hashes/targets — never a body, a local path or a credential. A late ``verified`` proof
outranks the outcome a caller computed from the send it saw time out.
"""

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


def _claimed(ledger, identity):
    execution = ledger.create_execution('fake-report', source='builtin')
    receipt = ledger.prepare_delivery_request(execution['id'], execution['job_id'], identity)
    claimed = ledger.claim_delivery_request(execution['id'], receipt['request_sha256'])
    return execution, claimed


def _evidence():
    return {'target': {'platform': 'telegram', 'chat_id': '123', 'thread_id': None},
            'notification': {'sha256': 'd' * 64, 'count': 1,
                             'chunks': [{'index': 0, 'message_id': '7', 'sha256': 'e' * 64}]},
            'artifacts': [{'kind': 'report', 'sha256': 'c' * 64, 'message_id': '8',
                           'method': 'send_document', 'size': 20}]}


def test_settle_is_once_only_and_bound_to_digest_and_attempt_nonce(ledger, identity):
    execution, claimed = _claimed(ledger, identity)

    assert ledger.settle_delivery_request(
        execution['id'], claimed['request_sha256'], attempt_nonce='0' * 4, state='verified') is None
    with pytest.raises(ValueError, match='digest conflict'):
        ledger.settle_delivery_request(
            execution['id'], '0' * 64, attempt_nonce=claimed['attempt_nonce'], state='verified')

    settled = ledger.settle_delivery_request(
        execution['id'], claimed['request_sha256'], attempt_nonce=claimed['attempt_nonce'],
        state='verified', evidence=_evidence())
    assert settled['state'] == 'verified'
    assert settled['evidence']['notification']['chunks'][0]['message_id'] == '7'

    assert ledger.settle_delivery_request(
        execution['id'], claimed['request_sha256'], attempt_nonce=claimed['attempt_nonce'],
        state='unknown') is None
    assert ledger.get_artifact_delivery_receipt(identity['token'], 'report')['state'] == 'verified'


def test_settle_requires_the_minted_nonce_and_a_known_state(ledger, identity):
    execution, claimed = _claimed(ledger, identity)
    with pytest.raises(ValueError, match='attempt nonce'):
        ledger.settle_delivery_request(
            execution['id'], claimed['request_sha256'], attempt_nonce='', state='verified')
    with pytest.raises(ValueError, match='invalid artifact receipt state'):
        ledger.settle_delivery_request(
            execution['id'], claimed['request_sha256'], attempt_nonce=claimed['attempt_nonce'],
            state='sent')


def test_evidence_never_retains_bodies_paths_or_credentials(ledger, identity):
    execution, claimed = _claimed(ledger, identity)
    for bad in ({'path': '/tmp/report.txt'}, {'message': 'full report body'},
                {'token': 'x' * 32}, {'artifact': {'content': 'body'}}):
        with pytest.raises(ValueError):
            ledger.settle_delivery_request(
                execution['id'], claimed['request_sha256'], attempt_nonce=claimed['attempt_nonce'],
                state='verified', evidence=bad)
    assert ledger.get_artifact_delivery_receipt(identity['token'], 'report')['state'] == 'sending'


def test_settle_is_fenced_to_the_owning_profile(ledger, identity):
    execution, claimed = _claimed(ledger, identity)
    assert ledger.settle_delivery_request(
        execution['id'], claimed['request_sha256'], attempt_nonce=claimed['attempt_nonce'],
        state='verified', profile_sha256='f' * 64) is None
    assert ledger.get_artifact_delivery_receipt(identity['token'], 'report')['state'] == 'sending'
    assert ledger.settle_delivery_request(
        execution['id'], claimed['request_sha256'], attempt_nonce=claimed['attempt_nonce'],
        state='verified', profile_sha256=claimed['sender_profile_sha256'])['state'] == 'verified'


def test_reconcile_reads_the_exact_anchor_without_mutating(ledger, identity):
    execution, claimed = _claimed(ledger, identity)
    reread = ledger.reconcile_delivery_request(
        execution['id'], claimed['request_sha256'], attempt_nonce=claimed['attempt_nonce'])
    assert reread['state'] == 'sending'
    with pytest.raises(ValueError, match='digest conflict'):
        ledger.reconcile_delivery_request(execution['id'], '0' * 64)
    with pytest.raises(ValueError, match='nonce conflict'):
        ledger.reconcile_delivery_request(
            execution['id'], claimed['request_sha256'], attempt_nonce='not-the-nonce')
    assert ledger.get_artifact_delivery_receipt(identity['token'], 'report')['state'] == 'sending'


def test_stale_timeout_outcome_cannot_clobber_a_late_verified_receipt(ledger, identity):
    execution, claimed = _claimed(ledger, identity)
    ledger.settle_delivery_request(
        execution['id'], claimed['request_sha256'], attempt_nonce=claimed['attempt_nonce'],
        state='verified')
    record = ledger.finish_execution(execution['id'], success=True, delivery_outcome='unknown')
    assert record['delivery_outcome'] == 'delivered'


def test_inflight_receipt_records_unknown_and_never_a_false_delivery(ledger, identity):
    execution, _claimed_execution = _claimed(ledger, identity)
    assert ledger.finish_execution(
        execution['id'], success=False, error='artifact send timed out',
        delivery_outcome=None)['delivery_outcome'] == 'unknown'


def test_settled_unknown_never_permits_a_resend(ledger, identity):
    execution, claimed = _claimed(ledger, identity)
    ledger.settle_delivery_request(
        execution['id'], claimed['request_sha256'], attempt_nonce=claimed['attempt_nonce'],
        state='unknown', reason='partial provider evidence')
    assert ledger.claim_delivery_request(execution['id'], claimed['request_sha256']) is None
    receipt = ledger.get_artifact_delivery_receipt(identity['token'], 'report')
    assert receipt['state'] == 'unknown'
    assert json.loads(ledger.get_execution(execution['id'])['delivery_receipt'])['state'] == 'unknown'
