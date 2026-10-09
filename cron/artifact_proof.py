"""Closed provider-proof schema, shared by live transport and durable settlement."""
from cron.artifact_delivery import _hex, _keys


def message_id(value):
    if not isinstance(value, str) or not 1 <= len(value) <= 20 or not value.isascii() or not value.isdigit() or int(value) <= 0:
        raise ValueError('invalid provider message id')


def notification(proof, digest, target):
    fields = ('role', 'provider', 'method', 'complete', 'count', 'incoming_sha256', 'incoming_size', 'target', 'chunks')
    if not isinstance(proof, dict):
        raise ValueError('invalid notification proof')
    rich = proof.get('method') == 'sendRichMessage'
    _keys(proof, (*fields, 'payload_sha256') if rich else fields)
    if (proof['role'] != 'notification' or proof['provider'] != 'telegram'
            or proof['method'] not in ('send_message', 'sendRichMessage') or proof['complete'] is not True
            or proof['incoming_sha256'] != digest or proof['target'] != target):
        raise ValueError('notification identity mismatch')
    if type(proof['incoming_size']) is not int or proof['incoming_size'] < 1:
        raise ValueError('invalid notification size')
    chunks = proof['chunks']
    if (type(proof['count']) is not int or not isinstance(chunks, list)
            or not 1 <= len(chunks) <= 64 or proof['count'] != len(chunks)):
        raise ValueError('notification chunk count mismatch')
    seen = set()
    for index, chunk in enumerate(chunks):
        _keys(chunk, ('index', 'sha256', 'message_id'))
        if type(chunk['index']) is not int or chunk['index'] != index:
            raise ValueError('notification chunk index gap')
        _hex(chunk['sha256'], 64)
        message_id(chunk['message_id'])
        if chunk['message_id'] in seen:
            raise ValueError('duplicate notification message id')
        seen.add(chunk['message_id'])
    if rich:
        _hex(proof['payload_sha256'], 64)
        if len(chunks) != 1 or chunks[0]['sha256'] != proof['payload_sha256']:
            raise ValueError('rich payload mismatch')


def document(proof, artifact, target):
    _keys(proof, ('role', 'provider', 'method', 'sha256', 'size', 'message_id', 'target'))
    if (proof['role'] != 'document' or proof['provider'] != 'telegram'
            or proof['method'] != 'send_document' or proof['sha256'] != artifact['sha256']
            or type(proof['size']) is not int or proof['size'] != artifact['size']
            or proof['target'] != target):
        raise ValueError('native document identity mismatch')
    message_id(proof['message_id'])


def evidence(value, receipt):
    _keys(value, ('execution_id', 'request_sha256', 'attempt_nonce', 'target', 'notification', 'artifacts'))
    for field in ('execution_id', 'request_sha256', 'attempt_nonce'):
        if value[field] != receipt[field]:
            raise ValueError('artifact evidence attempt mismatch')
    request = receipt['request']
    if value['target'] != request['target']:
        raise ValueError('artifact evidence target mismatch')
    notification(value['notification'], request['message_sha256'], request['target'])
    artifacts = value['artifacts']
    if not isinstance(artifacts, list) or len(artifacts) != len(request['artifacts']):
        raise ValueError('artifact evidence inventory mismatch')
    document_ids = set()
    for item, expected in zip(artifacts, request['artifacts']):
        _keys(item, ('kind', 'transport', 'proof'))
        if item['kind'] != expected['kind'] or item['transport'] != expected['transport']:
            raise ValueError('artifact evidence inventory mismatch')
        if expected['transport'] == 'text':
            if (expected['sha256'] != request['message_sha256']
                    or expected['size'] != value['notification']['incoming_size']
                    or item['proof'] != value['notification']):
                raise ValueError('text artifact must use exact notification proof')
        else:
            document(item['proof'], expected, request['target'])
            provider_id = item['proof']['message_id']
            if provider_id in document_ids or any(
                    chunk['message_id'] == provider_id for chunk in value['notification']['chunks']):
                raise ValueError('duplicate artifact provider id')
            document_ids.add(provider_id)
