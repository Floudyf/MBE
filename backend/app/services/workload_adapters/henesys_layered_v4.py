"""Henesys raw calldata-derived V4 *projection*. Not EVM storage truth.

Source JSONL is built ONLY from transaction contents and block timestamps.
Receipts are consulted by the exporter for input integrity, never for access selection.
One observed Henesys transaction yields exactly one V4 modeled transaction.
"""
from __future__ import annotations

import gzip
import hashlib
import json
from collections import Counter
from pathlib import Path
from typing import Any, Iterator

from backend.app.services.workload_adapters.base import SourceValidationSummary

SCHEMA = 'mbe_workload_record_v4'
ADAPTER = 'mv_henesys_layered_v4'


def _digest(items: list[dict[str, Any]]) -> str:
    return hashlib.sha256(json.dumps(items, ensure_ascii=False, separators=(',', ':')).encode('utf-8')).hexdigest()


def _access(key: str, semantics: str) -> dict[str, str]:
    return {'key': key, 'mode': 'read_write', 'update_semantics': semantics}


def _address(value: str) -> str:
    v = str(value or '').lower()
    if len(v) != 42 or not v.startswith('0x') or any(c not in '0123456789abcdef' for c in v[2:]):
        raise ValueError('invalid 20-byte EVM address')
    return v


def _word(input_hex: str, index: int) -> str:
    off = 10 + index * 64
    value = input_hex[off:off + 64]
    return value if len(value) == 64 and all(ch in '0123456789abcdef' for ch in value) else ''


def _word_address(value: str) -> str:
    return '0x' + value[-40:] if value and int(value[:-40], 16) == 0 else ''


def project(raw: dict[str, Any], dataset_id: str, idx: int) -> dict[str, Any]:
    sender = _address(raw.get('from'))
    recipient = str(raw.get('to') or '').lower()
    if recipient:
        recipient = _address(recipient)
    txhash = str(raw.get('hash') or '').lower()
    if len(txhash) != 66 or not txhash.startswith('0x') or any(c not in '0123456789abcdef' for c in txhash[2:]):
        raise ValueError('invalid source transaction hash')
    # Source sender nonce is an actual protocol-level sequencing state.
    nonce_key = 'henesys_nonce:' + sender
    access_by_key: dict[str, dict[str, Any]] = {nonce_key: _access(nonce_key, 'henesys_sender_nonce_sequence')}
    code = str(raw.get('calldata_prefix') or '0x').lower()
    selector = code[:10] if len(code) >= 10 else ''
    operation = 'henesys_other_call'
    confidence = 'sender_nonce_only'
    touched_asset = ''
    # Standard ABI call *intent*, not a claim of verified contract storage writes.
    # Do not inspect receipt logs or execution outcome to build the access list.
    if recipient and selector == '0xa9059cbb':  # ERC20 transfer(address,uint256)
        dest = _word_address(_word(code, 0))
        if dest:
            key1 = f'henesys_erc20_balance:{recipient}:{sender}'
            key2 = f'henesys_erc20_balance:{recipient}:{dest}'
            for key in (key1, key2): access_by_key[key] = _access(key, 'henesys_erc20_transfer_intent')
            operation = 'henesys_erc20_transfer_intent'
            confidence = 'standard_abi_intent'
            touched_asset = recipient
    elif recipient and selector in ('0x42842e0e', '0xb88d4fde'):  # ERC721 safeTransferFrom
        src, dst, token = _word_address(_word(code, 0)), _word_address(_word(code, 1)), _word(code, 2)
        if src and dst and token:
            key = f'henesys_erc721_owner:{recipient}:{int(token, 16)}'
            access_by_key[key] = _access(key, 'henesys_erc721_safe_transfer_intent')
            operation = 'henesys_erc721_transfer_intent'
            confidence = 'standard_abi_intent'
            touched_asset = key
    elif recipient and selector == '0xf242432a':  # ERC1155 safeTransferFrom
        src, dst, asset = _word_address(_word(code, 0)), _word_address(_word(code, 1)), _word(code, 2)
        if src and dst and asset:
            asset = str(int(asset, 16))
            for owner in (src, dst):
                key = f'henesys_erc1155_balance:{recipient}:{asset}:{owner}'
                access_by_key[key] = _access(key, 'henesys_erc1155_transfer_intent')
            operation = 'henesys_erc1155_transfer_intent'
            confidence = 'standard_abi_intent'
            touched_asset = recipient + ':' + asset
    # We deliberately DO NOT model 0x23b872dd as NFT transfer: ERC20 and
    # ERC721 share the signature. Unknown contract storage is not fabricated.
    access = [access_by_key[k] for k in sorted(access_by_key)]
    scheduling = [{'key': a['key'], 'mode': a['mode'], 'update_semantics': a['update_semantics']} for a in access]
    timestamp_ms = int(raw['timestamp_ms'])
    if timestamp_ms <= 0: raise ValueError('invalid source block timestamp')
    result = {
        'schema_version': SCHEMA, 'dataset_id': dataset_id,
        'source_row_index': idx, 'source_event_id': txhash, 'source_tx_hash': txhash,
        'timestamp_ms': timestamp_ms, 'sender_id': sender, 'receiver_id': recipient or None,
        'operation_type': operation, 'runtime_value': 1, 'state_keys': sorted(access_by_key),
        'static_access_envelope': {
            'keys': sorted(access_by_key), 'complete': True,
            'policy_id': 'henesys_nonce_abi_intent_projection_v1',
            'construction_method': 'transaction_input_only_no_receipt_log_or_evm_storage_trace',
        },
        'scheduling_access_schema': 'henesys_declared_intent_v4',
        'scheduling_access_source': 'source_transaction_nonce_plus_standard_abi_intent_no_execution_oracle',
        'scheduling_access_list': scheduling, 'scheduling_access_digest': _digest(scheduling),
        'access_list_schema': 'henesys_runtime_intent_v4',
        'access_list_source': 'modeled_transaction_intent_not_observed_contract_storage',
        'access_list': access, 'access_list_digest': _digest(access),
        'routing_source_key': nonce_key,
        'routing_target_key': (next((a['key'] for a in access if a['key'] != nonce_key), nonce_key)),
        'skew_keys': {'sender_nonce': nonce_key, 'call_contract': 'henesys_call:' + (recipient or 'creation'),
                      'asset_hint': touched_asset or nonce_key},
        'metadata': {
            'source_block_number': int(raw['block_number']),
            'source_tx_index': int(raw['transaction_index']),
            'source_nonce': int(raw['nonce']),
            'method_selector': selector, 'contract_address': recipient,
            'modeled_access_scope': confidence,
            'full_contract_storage_readwrite_observed': False,
            'nonmodeled_contract_state': 'unknown_not_added',
            'receipt_status_excluded_from_access_selection': True,
        },
        'provenance': {
            'source_chain': 'henesys', 'source_chain_id': 68414,
            'adapter_id': ADAPTER,
            'source_event_unit': 'one_native_chain_transaction',
            'source_transaction_input_observed': True,
            'source_storage_readwrite_observed': False,
            'projection_policy': 'henesys_nonce_abi_intent_projection_v1',
        },
    }
    return result


class HenesysLayeredV4Adapter:
    adapter_id = ADAPTER

    def iter_canonical_records(self, path: Path, manifest: dict[str, Any]) -> Iterator[dict[str, Any]]:
        with gzip.open(path, 'rt', encoding='utf-8', newline='') as stream:
            previous: tuple[int, int] | None = None
            for index, line in enumerate(stream):
                if not line.strip(): raise ValueError(f'Henesys source row {index}: empty')
                obj = json.loads(line)
                if obj.get('day') != manifest.get('source_day'):
                    raise ValueError(f'Henesys source row {index}: day mismatch')
                current = (int(obj['block_number']), int(obj['transaction_index']))
                if previous is not None and current <= previous:
                    raise ValueError(f'Henesys source row {index}: chain order not strict')
                previous = current
                yield project(obj, manifest['dataset_id'], index)

    def validate_source(self, path: Path, manifest: dict[str, Any], *, expected_sha256: str | None = None) -> SourceValidationSummary:
        hasher = hashlib.sha256()
        with path.open('rb') as raw:
            for chunk in iter(lambda: raw.read(1048576), b''):
                hasher.update(chunk)
        if expected_sha256 and hasher.hexdigest() != expected_sha256.lower():
            raise ValueError('Henesys source SHA256 differs from manifest')
        count = 0
        operations: Counter[str] = Counter()
        hashes: set[str] = set()
        start = end = None
        for item in self.iter_canonical_records(path, manifest):
            count += 1
            operations[item['operation_type']] += 1
            txid = item['source_tx_hash']
            if txid in hashes: raise ValueError('duplicate source tx hash')
            hashes.add(txid)
            ts = item['timestamp_ms']
            start = ts if start is None else min(ts, start)
            end = ts if end is None else max(ts, end)
        expected = int(manifest.get('row_count') or 0)
        if count != expected: raise ValueError(f'Henesys row count mismatch {count} != {expected}')
        return SourceValidationSummary(hasher.hexdigest(), count, len(hashes), start or 0, end or 0, dict(operations))
