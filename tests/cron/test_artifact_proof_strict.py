"""Exact acceptance proof regressions; invented identities and offline ledger only."""
import copy
import hashlib

import pytest

from cron import artifact_transport as t

TEXT = 'full report\n'
DIGEST = hashlib.sha256(TEXT.encode()).hexdigest()
TARGET = {'platform': 'telegram', 'chat_id': '123', 'thread_id': None}


def result():
    return {'success': True, 'message_id': '7', 'raw_response': {'delivery_receipt': {
        'role': 'notification', 'provider': 'telegram', 'method': 'send_message',
        'complete': True, 'count': 1, 'incoming_size': len(TEXT.encode()), 'incoming_sha256': DIGEST, 'target': TARGET,
        'chunks': [{'index': 0, 'sha256': DIGEST, 'message_id': '7'}]}}}


@pytest.mark.parametrize('mutation', ['failed', 'bool_count', 'bool_index', 'bad_hash',
                                     'null_id', 'provider', 'method', 'hidden'])
def test_notification_refuses_invalid_acceptance(mutation):
    response = copy.deepcopy(result())
    proof = response['raw_response']['delivery_receipt']
    mutations = {
        'failed': lambda: response.update(success=False),
        'bool_count': lambda: proof.update(count=True),
        'bool_index': lambda: proof['chunks'][0].update(index=False),
        'bad_hash': lambda: proof['chunks'][0].update(sha256='garbage'),
        'null_id': lambda: proof['chunks'][0].update(message_id='None'),
        'provider': lambda: proof.update(provider='forged'),
        'method': lambda: proof.update(method='forged'),
        'hidden': lambda: proof.update(hidden='/private/report/body'),
    }
    mutations[mutation]()
    assert t._notification_problem(response, TEXT, TARGET)
