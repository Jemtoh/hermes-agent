"""Runtime-owned pre-enqueue crash recovery; invented owner and temp SQLite only."""
import json
import time

import pytest
from cron import executions as e, delivery_queue as q


@pytest.fixture(autouse=True)
def stores(tmp_path, monkeypatch):
    monkeypatch.setattr(e, 'EXECUTIONS_FILE', tmp_path / 'executions.db')
    monkeypatch.setattr(q, 'DELIVERY_DB', tmp_path / 'deliveries.db')
    monkeypatch.setattr(e, '_owner_is_live', lambda *args: False)
    monkeypatch.setattr('gateway.status._pid_exists', lambda pid: False)


def request():
    return {'token': 'a' * 32, 'purpose': 'report',
            'target': {'platform': 'telegram', 'chat_id': '123', 'thread_id': None},
            'message_sha256': 'b' * 64,
            'artifacts': [{'kind': 'report', 'sha256': 'c' * 64, 'transport': 'document', 'size': 20}]}


def prepared_dead_owner():
    execution = e.create_execution('invented', source='test')
    e.prepare_delivery_request(execution['id'], execution['job_id'], request())
    # Simulate a different now-dead worker without killing any actual process.
    with e._transaction(immediate=True) as conn:
        conn.execute('UPDATE executions SET process_id=?,pid=?,process_started_at=? WHERE id=?',
                     ('invented-dead-worker', 987654321, 123.0, execution['id']))
    assert q.get_status(execution['id']) is None
    return execution


@pytest.mark.parametrize('path', ['startup_sweep', 'explicit_dead_owner', 'periodic_reaper'])
def test_runtime_lifecycle_closes_proven_unsent_without_another_prepare(path, monkeypatch):
    execution = prepared_dead_owner()
    def periodic():
        from cron import scheduler
        monkeypatch.setattr(scheduler, '_last_dead_owner_reap_at', {})
        scheduler._maybe_reap_dead_owners()
    actions = {
        'startup_sweep': e.recover_interrupted_executions,
        'explicit_dead_owner': lambda: e.terminalize_dead_owner(execution['id'], reason='invented worker exited'),
        'periodic_reaper': periodic,
    }
    actions[path]()
    assert e.get_execution(execution['id'])['status'] == 'unknown'
    # Recovery is durable and repeated read-only consumer polls preserve its disposition.
    for _ in range(3):
        e.recover_interrupted_executions()
        receipt = e.get_artifact_delivery_receipt('a' * 32, 'report')
    assert q.get_status(execution['id']) is None
    assert receipt['state'] == 'failed_certain', receipt


def test_recovery_allows_new_exact_prepare():
    old = prepared_dead_owner()
    e.recover_interrupted_executions()
    assert e.get_artifact_delivery_receipt('a' * 32, 'report')['state'] == 'failed_certain'
    new = e.create_execution('invented', source='test')
    prepared = e.prepare_delivery_request(new['id'], new['job_id'], request())
    assert prepared['execution_id'] == new['id']
    assert e.execution_delivery_receipt(old['id'])['state'] == 'failed_certain'


@pytest.mark.parametrize('path', ['sweep', 'explicit'])
def test_previously_terminal_orphan_recovers_without_rewriting_run(path):
    old = prepared_dead_owner()
    with e._transaction(immediate=True) as conn:
        conn.execute("UPDATE executions SET status='unknown',error='prior timeout' WHERE id=?", (old['id'],))
    before = e.execution_delivery_receipt(old['id'])
    if path == 'sweep':
        e.recover_interrupted_executions()
    else:
        assert e.terminalize_dead_owner(old['id'], reason='invented exit') is False
    after = e.execution_delivery_receipt(old['id'])
    assert after['state'] == 'failed_certain'
    assert after['request'] == before['request']
    assert after['request_sha256'] == before['request_sha256']
    row = e.get_execution(old['id'])
    assert (row['status'], row['error'], row['delivery_outcome']) == ('unknown', 'prior timeout', 'failed')


@pytest.mark.parametrize('fence', ['live', 'wedged', 'unverifiable', 'handoff', 'nonce', 'evidence',
                                  'sending', 'unknown', 'suppressed'])
def test_recovery_preserves_delivery_fences(fence, monkeypatch):
    old = prepared_dead_owner()
    if fence in ('live', 'wedged'):
        monkeypatch.setattr('gateway.status._pid_exists', lambda pid: True)
        monkeypatch.setattr(e, '_owner_is_live', lambda *args: True)
        monkeypatch.setattr(e, '_live_owner_stale_after_seconds', lambda: 1 if fence == 'wedged' else None)
        if fence == 'wedged':
            monkeypatch.setattr(e, '_stale_age_seconds', lambda *args: 100)
    elif fence == 'unverifiable':
        def unavailable(pid):
            raise OSError('invented unavailable process lookup')
        monkeypatch.setattr('gateway.status._pid_exists', unavailable)
    with e._transaction(immediate=True) as conn:
        if fence == 'handoff':
            conn.execute('UPDATE executions SET handoff_pending=1,handoff_started_at=? WHERE id=?',
                         (time.time(), old['id']))
        receipt = e._decode_receipt(e._fetch(conn, old['id']))
        if fence in ('nonce', 'evidence'):
            receipt['attempt_nonce' if fence == 'nonce' else 'evidence'] = '' if fence == 'nonce' else {}
        elif fence in ('sending', 'unknown', 'suppressed'):
            receipt['state'] = fence
        conn.execute('UPDATE executions SET delivery_receipt=? WHERE id=?', (json.dumps(receipt), old['id']))
    before = e.execution_delivery_receipt(old['id'])
    e.recover_interrupted_executions()
    assert e.execution_delivery_receipt(old['id']) == before


@pytest.mark.parametrize('winner', ['recovery', 'queue_claim'])
def test_old_queue_anchor_and_recovery_share_one_claim_fence(winner):
    old = prepared_dead_owner()
    digest = e.execution_delivery_receipt(old['id'])['request_sha256']
    if winner == 'queue_claim':
        claim = e.claim_delivery_request(old['id'], digest)
        e.recover_interrupted_executions()
        assert e.execution_delivery_receipt(old['id']) == claim
        assert claim['state'] == 'sending'
    else:
        e.recover_interrupted_executions()
        assert e.claim_delivery_request(old['id'], digest) is None
        assert e.execution_delivery_receipt(old['id'])['state'] == 'failed_certain'


def test_admitted_pending_queue_row_never_sends_after_dead_owner_recovery(tmp_path):
    from cron import artifact_transport
    path = tmp_path / 'invented-report.txt'
    path.write_text('invented report')
    import hashlib
    notice = 'Invented report ready'
    envelope = {'version': 1, 'token': 'd' * 32, 'purpose': 'report', 'message': notice,
                'message_sha256': hashlib.sha256(notice.encode()).hexdigest(),
                'artifacts': [{'kind': 'report', 'path': str(path), 'transport': 'document',
                               'sha256': hashlib.sha256(path.read_bytes()).hexdigest()}]}
    old = e.create_execution('invented', source='test')
    job = {'id': old['job_id'], 'execution_id': old['id'], 'deliver': 'origin',
           'origin': {'platform': 'telegram', 'chat_id': '123'}, '_artifact_delivery': envelope}
    _, _, _, receipt = artifact_transport.prepare_artifact_request(job)
    job['_artifact_anchor'] = {'execution_id': old['id'], 'job_id': old['job_id'],
                               'request': receipt['request'], 'request_sha256': receipt['request_sha256']}
    assert q.enqueue(old['id'], job, notice)['status'] == 'pending'
    with e._transaction(immediate=True) as conn:
        conn.execute('UPDATE executions SET process_id=?,pid=?,process_started_at=? WHERE id=?',
                     ('invented-dead-worker', 987654321, 123.0, old['id']))
    e.recover_interrupted_executions()
    sent = []
    assert q.drain(lambda *args: sent.append(args)) == 0
    assert sent == []
    assert q.get_status(old['id'])['status'] == 'failed'


def test_concurrent_queue_claim_and_reaper_cannot_both_win():
    from concurrent.futures import ThreadPoolExecutor
    from threading import Barrier

    old = prepared_dead_owner()
    digest = e.execution_delivery_receipt(old['id'])['request_sha256']
    ready = Barrier(2)
    def claim():
        ready.wait(timeout=5)
        return e.claim_delivery_request(old['id'], digest)
    def recover():
        ready.wait(timeout=5)
        return e.recover_interrupted_executions()
    with ThreadPoolExecutor(max_workers=2) as pool:
        sender = pool.submit(claim)
        reaper = pool.submit(recover)
        claimed = sender.result(timeout=10)
        reaper.result(timeout=10)
    receipt = e.execution_delivery_receipt(old['id'])
    assert receipt['state'] == ('sending' if claimed is not None else 'failed_certain')
    if claimed is not None:
        assert receipt['attempt_nonce'] == claimed['attempt_nonce']
    else:
        assert e.claim_delivery_request(old['id'], digest) is None


def test_explicit_terminalization_preserves_concurrent_worker_adoption(monkeypatch):
    old = e.create_execution('invented-adoption', source='test')
    pending = e.mark_execution_handoff_pending(old['id'])
    monkeypatch.setattr(e, '_PROCESS_ID', 'invented-replacement-scheduler')
    monkeypatch.setattr(e.time, 'time', lambda: pending['handoff_started_at']
                        + e.HANDOFF_ADOPTION_GRACE_SECONDS + 1)
    def adopt_during_liveness_check(*args):
        monkeypatch.setattr(e, '_PROCESS_ID', 'invented-adopting-worker')
        monkeypatch.setattr(e.os, 'getpid', lambda: 4242)
        monkeypatch.setattr(e, '_process_start_time', lambda pid: 9876)
        assert e.adopt_claimed_execution(old['id']) is not None
        return False
    monkeypatch.setattr(e, '_owner_is_live', adopt_during_liveness_check)
    assert e.terminalize_dead_owner(old['id'], reason='invented exit') is False
    row = e.get_execution(old['id'])
    assert (row['status'], row['process_id'], row['pid']) == ('running', 'invented-adopting-worker', 4242)
