"""Strict opt-in trusted-script artifact envelopes; transport is separately fenced."""
from __future__ import annotations

import hashlib
import json
import re

_FORMAT = 'delivery-v1'
_HEX = re.compile(r'^[0-9a-f]+$')


def validate_job_format(value, *, no_agent, script):
    if value is None or value == '':
        return None
    if value != _FORMAT or no_agent is not True or not str(script or '').strip():
        raise ValueError('delivery-v1 requires a no-agent script job')
    return value


def normalize_script_output_format(value):
    if value is None or value == '':
        return None
    if not isinstance(value, str) or value != _FORMAT:
        raise ValueError('unsupported script output format')
    return value


def _keys(value, expected):
    if not isinstance(value, dict) or set(value) != set(expected):
        raise ValueError('invalid artifact envelope keys')


def _hex(value, length):
    if not isinstance(value, str) or len(value) != length or not _HEX.fullmatch(value):
        raise ValueError('invalid artifact identity or digest')


def _unique_pairs(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError('duplicate artifact envelope key')
        result[key] = value
    return result


def parse_envelope(output):
    try:
        request = json.loads(output, object_pairs_hook=_unique_pairs)
    except (TypeError, json.JSONDecodeError) as exc:
        raise ValueError('invalid artifact envelope JSON') from exc
    return validate_envelope(request)


def validate_envelope(request, *, verify_files=True):
    from gateway.platforms.base import validate_media_delivery_path
    from cron.scheduler_delivery import _redact_cron_payload

    _keys(request, ('version', 'token', 'purpose', 'message', 'message_sha256', 'artifacts'))
    if type(request['version']) is not int or request['version'] != 1:
        raise ValueError('unsupported artifact envelope version')
    _hex(request['token'], 32)
    _hex(request['message_sha256'], 64)
    if request['purpose'] not in ('report', 'review'):
        raise ValueError('invalid artifact purpose')
    text = request['message']
    if not isinstance(text, str) or not text.strip():
        raise ValueError('artifact notification must be nonempty text')
    if text == '[REDACTED - redaction failed]' or _redact_cron_payload(text, 'artifact notification') != text:
        raise ValueError('artifact notification must be redacted before saving')
    if hashlib.sha256(text.encode('utf-8')).hexdigest() != request['message_sha256']:
        raise ValueError('artifact notification digest mismatch')
    artifacts = request['artifacts']
    if not isinstance(artifacts, list) or not artifacts or len(artifacts) > 3:
        raise ValueError('invalid artifact inventory')
    kinds = set()
    for artifact in artifacts:
        _keys(artifact, ('kind', 'path', 'sha256', 'transport'))
        kind = artifact['kind']
        allowed = ('report',) if request['purpose'] == 'report' else ('review', 'review_result')
        if not isinstance(kind, str) or kind not in allowed or kind in kinds:
            raise ValueError('invalid or duplicate artifact kind')
        kinds.add(kind)
        _hex(artifact['sha256'], 64)
        if artifact['transport'] not in ('text', 'document') or not isinstance(artifact['path'], str):
            raise ValueError('invalid artifact transport or path')
        if not verify_files:
            continue
        path = validate_media_delivery_path(artifact['path'])
        if path is None:
            raise ValueError('artifact path refused by media policy')
        try:
            with open(path, 'rb') as fh:
                digest = hashlib.file_digest(fh, 'sha256').hexdigest()
        except OSError as exc:
            raise ValueError('artifact is unreadable') from exc
        if digest != artifact['sha256']:
            raise ValueError('artifact digest mismatch')
        if artifact['transport'] == 'text' and digest != request['message_sha256']:
            raise ValueError('text artifact must match the exact notification')
    return request


def no_agent_result(job, output, header):
    from cron.scheduler import SILENT_MARKER

    job.pop('_artifact_delivery', None)
    try:
        validate_job_format(job['script_output_format'], no_agent=job.get('no_agent'), script=job.get('script'))
        if output == '' or output == SILENT_MARKER:
            return True, f'{header}**Status:** silent\n', SILENT_MARKER, None
        request = parse_envelope(output)
    except ValueError as exc:
        error = f'Artifact delivery refused: {exc}'
        return False, f'{header}{error}\n', error, error
    job['_artifact_delivery'] = request
    return True, f"{header}\n{request['message']}\n", request['message'], None
