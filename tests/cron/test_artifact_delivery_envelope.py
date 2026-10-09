"""Trusted script envelopes bind exact saved bytes before cron dispatch."""
import hashlib
import json

import pytest

from cron import jobs, scheduler as s


def sha(data):
    return hashlib.sha256(data).hexdigest()


@pytest.fixture
def envelope(tmp_path):
    artifact = tmp_path / 'full.txt'
    artifact.write_bytes('full evidence\n'.encode())
    message = 'Report ready.\nMEDIA:/untrusted/prose.txt\n'
    return {'version': 1, 'token': 'a' * 32, 'purpose': 'report',
            'message': message, 'message_sha256': sha(message.encode()),
            'artifacts': [{'kind': 'report', 'path': str(artifact),
                           'sha256': sha(artifact.read_bytes()), 'transport': 'document'}]}


def run_script(monkeypatch, output, *, opt_in=True):
    monkeypatch.setattr(s, '_run_job_script_with_claim_heartbeat', lambda *a, **kw: (True, output))
    monkeypatch.setattr(s, '_resolve_job_workdir', lambda *a: None)
    job = {'id': 'fake-report', 'name': 'Fake report', 'script': 'fake.py', 'no_agent': True}
    if opt_in:
        job['script_output_format'] = 'delivery-v1'
    return job, s._run_no_agent_job(job, job['id'], job['name'], None)


def test_envelope_preserves_exact_notification_and_explicit_artifacts(monkeypatch, envelope):
    job, result = run_script(monkeypatch, json.dumps(envelope))
    assert result[0] is True
    assert result[2] == envelope['message']
    assert job['_artifact_delivery'] == envelope


@pytest.mark.parametrize('corruption', ['message_digest', 'file_digest', 'wakeAgent', 'version_bool',
                                       'duplicate_kind', 'unknown_key', 'token_traversal'])
def test_invalid_envelope_never_becomes_normal_output(monkeypatch, envelope, corruption):
    changes = {
        'message_digest': lambda: envelope.update(message_sha256='0' * 64),
        'file_digest': lambda: envelope['artifacts'][0].update(sha256='0' * 64),
        'wakeAgent': lambda: envelope.update(wakeAgent=False),
        'version_bool': lambda: envelope.update(version=True),
        'duplicate_kind': lambda: envelope['artifacts'].append(dict(envelope['artifacts'][0])),
        'unknown_key': lambda: envelope['artifacts'][0].update(arbitrary='refuse'),
        'token_traversal': lambda: envelope.update(token='../outside'),
    }
    changes[corruption]()
    job, result = run_script(monkeypatch, json.dumps(envelope))
    assert result[0] is False
    assert '_artifact_delivery' not in job
    assert envelope['message'] not in result[2]


def test_duplicate_json_keys_refuse_before_wake_gate(monkeypatch, envelope):
    output = json.dumps(envelope).replace('"version": 1', '"version": 1, "version": 1')
    _, result = run_script(monkeypatch, output)
    assert result[0] is False


def test_legacy_json_remains_verbatim_without_opt_in(monkeypatch, envelope):
    output = json.dumps(envelope)
    job, result = run_script(monkeypatch, output, opt_in=False)
    assert result[0] is True
    assert result[2] == output
    assert '_artifact_delivery' not in job


@pytest.mark.parametrize('output', ['', '[SILENT]'])
def test_opt_in_silence_creates_no_delivery_request(monkeypatch, output):
    job, result = run_script(monkeypatch, output)
    assert result[0] is True
    assert result[2] == s.SILENT_MARKER
    assert '_artifact_delivery' not in job


def test_registered_output_format_requires_script_only_mode(tmp_path):
    with jobs.use_cron_store(tmp_path):
        with pytest.raises(ValueError):
            jobs.create_job(prompt='agent job', schedule='1h', script_output_format='delivery-v1')
        job = jobs.create_job(prompt='', schedule='1h', script='fake.py', no_agent=True,
                              script_output_format='delivery-v1')
        assert jobs.get_job(job['id'])['script_output_format'] == 'delivery-v1'
        with pytest.raises(ValueError):
            jobs.update_job(job['id'], {'no_agent': False})
        with pytest.raises(ValueError):
            jobs.update_job(job['id'], {'script_output_format': 'unknown'})


@pytest.mark.parametrize('corruption', ['text_mismatch', 'redaction_failure', 'denied_path', 'secret'])
def test_request_cannot_attest_different_or_unredacted_message(monkeypatch, envelope, corruption):
    if corruption == 'text_mismatch':
        envelope['artifacts'][0]['transport'] = 'text'
    elif corruption == 'redaction_failure':
        envelope['message'] = '[REDACTED - redaction failed]'
        envelope['message_sha256'] = sha(envelope['message'].encode())
    elif corruption == 'denied_path':
        monkeypatch.setattr('gateway.platforms.base.validate_media_delivery_path', lambda path: None)
    else:
        monkeypatch.setattr('cron.scheduler_delivery._redact_cron_payload', lambda *a: '[redacted]')
    _, result = run_script(monkeypatch, json.dumps(envelope))
    assert result[0] is False


def test_opt_in_transport_refuses_until_complete_receipt_path_exists(monkeypatch, envelope):
    from cron import scheduler_delivery
    job, result = run_script(monkeypatch, json.dumps(envelope))
    assert result[0] is True
    monkeypatch.setattr(scheduler_delivery, '_resolve_delivery_targets',
                        lambda *a, **k: pytest.fail('unverified request reached ordinary sender'))
    assert scheduler_delivery._deliver_result(job, result[2]) is not None


@pytest.mark.parametrize('bad_format', [True, [], 3])
def test_registered_format_refuses_nonstring_values(tmp_path, bad_format):
    with jobs.use_cron_store(tmp_path):
        with pytest.raises(ValueError):
            jobs.create_job(prompt='', schedule='1h', script='fake.py', no_agent=True,
                            script_output_format=bad_format)


def test_authored_import_keeps_format_and_refuses_incompatible_mode():
    from cron.job_definition import merge_job_definition
    local = {'id': 'fake', 'prompt': '', 'script': 'fake.py', 'no_agent': True,
             'schedule': {'kind': 'interval', 'minutes': 60}}
    authored = {**local, 'script_output_format': 'delivery-v1'}
    assert merge_job_definition(local, authored)['script_output_format'] == 'delivery-v1'
    with pytest.raises(ValueError):
        merge_job_definition(local, {**authored, 'no_agent': False})


def test_cron_output_log_contains_notification_without_artifact_paths(monkeypatch, envelope):
    _, result = run_script(monkeypatch, json.dumps(envelope))
    assert envelope['message'] in result[1]
    assert envelope['artifacts'][0]['path'] not in result[1]
