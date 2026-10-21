"""Evidence-bounded semantic-v2 adapters. Never claim native storage R/W.

FW: versioned byte-preserving modelled V4 records produced by external installer.
Henesys: same scheduling/runtime accesses as original V4, only observed call grouping.
"""
from __future__ import annotations

import gzip
import hashlib
import json
import re
from collections import Counter
from pathlib import Path
from typing import Any, Iterator

from backend.app.services.workload_adapters.base import SourceValidationSummary

_ADDR = re.compile(r"^0x[0-9a-fA-F]{40}$")
_SEL = re.compile(r"^0x[0-9a-fA-F]{8}")


def _hash_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open('rb') as f:
        for b in iter(lambda: f.read(2 << 20), b''):
            h.update(b)
    return h.hexdigest()


def _jsonl(path: Path):
    with gzip.open(path, 'rt', encoding='utf-8') as f:
        for line in f:
            if line.strip():
                obj = json.loads(line)
                if not isinstance(obj, dict):
                    raise ValueError('Expected JSON object per source row')
                yield obj


class FarmersRoleV2Adapter:
    adapter_id = 'fw_role_semv2'

    def iter_canonical_records(self, path: Path, manifest: dict[str, Any]) -> Iterator[dict[str, Any]]:
        for i, record in enumerate(_jsonl(path)):
            if (record.get('schema_version') != 'mbe_workload_record_v4'
                or record.get('dataset_id') != manifest['dataset_id']
                or record.get('source_row_index') != i
                or not record.get('source_tx_hash')):
                raise ValueError('FW role projection V4 record identity/order invalid at '+str(i))
            if record.get('static_access_envelope', {}).get('policy_id') not in ('fw_role_semv2', 'fw_actor_fallback_semv2'):
                raise ValueError('FW role projection policy missing at '+str(i))
            yield record

    def validate_source(self, path: Path, manifest: dict[str, Any], *, expected_sha256: str | None = None) -> SourceValidationSummary:
        actual = _hash_file(path)
        if expected_sha256 and actual != expected_sha256.lower():
            raise ValueError('FW role source SHA mismatch')
        from backend.app.services.v5_workload_data_plane import _validate_canonical_record
        ops = Counter(); seen = set(); previous = (-1, -1); first = None; last = 0
        for i, row in enumerate(self.iter_canonical_records(path, manifest)):
            _validate_canonical_record(row, dataset_id=manifest['dataset_id'], row_number=i)
            tx = row['source_tx_hash']
            if tx in seen:
                raise ValueError('duplicate FW transaction hash')
            seen.add(tx)
            clock = int(row['timestamp_ms'])
            if (clock,i) < previous:raise ValueError('FW chronology regressed')
            previous = (clock,i)
            if first is None:first = clock
            last = clock
            ops[row['operation_type']] += 1
        if len(seen) != int(manifest['row_count']):
            raise ValueError('FW role row count mismatched')
        return SourceValidationSummary(actual, len(seen), len(seen), first or 0, last, dict(ops))


class HeneCallV2Adapter:
    adapter_id = 'hene_call_semv2'

    @staticmethod
    def _parent(manifest):
        original = dict(manifest)
        original['dataset_id'] = manifest['semantic_parent_dataset_id']
        original['adapter_id'] = manifest['semantic_parent_adapter_id']
        return original

    def iter_canonical_records(self, path: Path, manifest: dict[str, Any]) -> Iterator[dict[str, Any]]:
        from backend.app.services.workload_adapters.registry import get_adapter
        parent = self._parent(manifest)
        if parent['adapter_id'] == self.adapter_id:raise ValueError('Henesys wrapper cannot recurse')
        from itertools import zip_longest
        base = get_adapter(parent['adapter_id']).iter_canonical_records(path, parent)
        for i, (raw, old) in enumerate(zip_longest(_jsonl(path), base)):
            if raw is None or old is None:raise ValueError('Henesys raw/adapter row alignment mismatch')
            txid = str(raw.get('hash') or '').lower()
            record_id = str(old.get('source_tx_hash') or '').lower()
            if not txid or txid != record_id:
                raise ValueError('Henesys transaction hash mismatch at '+str(i))
            contract = str(raw.get('to') or '').lower()
            call = str(raw.get('calldata_prefix') or '').lower()
            contract = contract if _ADDR.fullmatch(contract) else None
            selector = call[:10] if _SEL.match(call) else None
            record = dict(old)
            record['dataset_id'] = manifest['dataset_id']
            record['metadata'] = {**(old.get('metadata') or {}),
                'observed_call_contract': contract,
                'observed_call_selector': selector,
                'observed_call_group': (contract + ':' + selector) if contract and selector else None,
                'semantic_v2_evidence': 'observed_to_and_calldata_prefix_no_abi_or_storage_proof',
                'old_v4_static_access_preserved': True,
                'function_name_proven': False}
            record['provenance'] = {**(old.get('provenance') or {}),
                'semantic_v2_parent_dataset': parent['dataset_id'],
                'semantic_v2_access_policy': 'inherit_original_v4_exactly'}
            yield record

    def validate_source(self, path: Path, manifest: dict[str, Any], *, expected_sha256: str | None = None) -> SourceValidationSummary:
        actual = _hash_file(path)
        if expected_sha256 and actual != expected_sha256.lower():raise ValueError('Henesys source hash mismatch')
        from backend.app.services.v5_workload_data_plane import _validate_canonical_record
        count=0;ops=Counter(); first=None;last=0; previous=(-1,-1); txids=set()
        for i,record in enumerate(self.iter_canonical_records(path,manifest)):
            _validate_canonical_record(record, dataset_id=manifest['dataset_id'], row_number=i)
            clock = int(record['timestamp_ms']); key=(clock,int(record['source_row_index']))
            if key < previous:raise ValueError('Henesys chronology regressed')
            previous=key
            first=clock if first is None else first;last=clock
            ops[record['operation_type']]+=1;count+=1
            if record.get('source_tx_hash'):txids.add(str(record['source_tx_hash']).lower())
        if count != int(manifest['row_count']):raise ValueError('Henesys row count mismatch')
        return SourceValidationSummary(actual,count,len(txids),first or 0,last,dict(ops))
