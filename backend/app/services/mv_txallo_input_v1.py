"""Bounded, provenance-attested TxAllo sidecars for original Axie V4 windows.

Only observed chain block heights are usable for TxAllo source-block epochs.
Do not substitute timestamp buckets, invented block heights, sampled future rows,
or static access oracles for unavailable historical chain facts.
"""
from __future__ import annotations

import gzip
import hashlib
import json
import math
import os
from decimal import Decimal, InvalidOperation
from pathlib import Path  # MBE_MV_FIX_V2
from typing import Any

SUPPORTED = 'axie_day_layered_v4'
V4 = {'axie_day_layered_v4', 'dcl_sales_layered_v4', 'tapos_layered_v4'}
HISTORY_SCHEMA = 'mbe_txallo_history_record_v1'
DYNAMIC_SCHEMA = 'mbe_txallo_dynamic_block_v1'


def _digest(path: Path) -> str:
    h = hashlib.sha256()
    with path.open('rb') as stream:
        for b in iter(lambda: stream.read(1 << 20), b''):
            h.update(b)
    return h.hexdigest()


def _records(path: Path, *, start: int, count: int):
    if not path.is_file():
        raise ValueError('verified canonical/materialized workload is missing: ' + str(path))
    end = start + count
    produced = 0
    with gzip.open(path, 'rt', encoding='utf-8') as stream:
        for index, raw in enumerate(stream):
            if index < start:
                continue
            if index >= end:
                break
            rec = json.loads(raw)
            produced += 1
            yield rec
    if produced != count:
        raise ValueError(f'TxAllo input short read: got {produced}, expected {count}')


def _chain_block(record: dict[str, Any]) -> int:
    # CSV stores actual integer block heights as decimal/scientific strings.
    # Decimal is exact; int(float(...)) would silently round malformed source heights.
    original = (record.get('metadata') or {}).get('block_number_raw')
    try:
        text = str(original).strip()
        if not text or len(text) > 96:
            raise ValueError('missing or oversized block height')
        decimal_height = Decimal(text)
        if not decimal_height.is_finite() or decimal_height != decimal_height.to_integral_value():
            raise ValueError('source block height must be an exact integer')
        number = int(decimal_height)
    except (InvalidOperation, TypeError, ValueError, OverflowError) as err:
        raise ValueError('TxAllo requires a positive integral observed source block_number; cannot invent blocks') from err
    if number <= 0 or number > (2**63 - 1):
        raise ValueError('TxAllo source block_number must fit positive int64')
    return number


def _write_gzip(path: Path, rows: list[dict[str, Any]]) -> str:
    path.parent.mkdir(parents=True, exist_ok=True)
    temp = path.with_name('.' + path.name + f'.{os.getpid()}.tmp')
    try:
        with temp.open('wb') as raw:
            with gzip.GzipFile(fileobj=raw, mode='wb', compresslevel=9, filename='', mtime=0) as packed:
                for row in rows:
                    packed.write((json.dumps(row, sort_keys=True, ensure_ascii=False, separators=(',', ':')) + '\n').encode('utf-8'))
            raw.flush()
            os.fsync(raw.fileno())
        os.replace(temp, path)
    finally:
        temp.unlink(missing_ok=True)
    return _digest(path)


def compile_v4_txallo_inputs(*, run_dir: Path, manifest: dict[str, Any],
                             workload_plan: dict[str, Any], profile: dict[str, dict[str, Any]],
                             cache_root: Path, project_root: Path) -> tuple[dict[str, Any], dict[str, Any]] | None:
    sharding = profile.get('sharding') or {}
    if sharding.get('plugin_id') != 'txallo_account_sharding':
        return None
    dataset = str(workload_plan.get('dataset_id') or '')
    if dataset not in V4:
        return None  # Existing Alien Worlds historical pool and mechanisms stay frozen.
    if dataset != SUPPORTED:
        raise ValueError(
            'TxAllo V4 input blocked for ' + dataset + ': source CSV has no verified '
            'chain block_number identity; generating synthetic source blocks would '
            'change the paper-faithful evidence contract. Other methods remain usable.'
        )
    if (workload_plan.get('variant_mode') != 'original_window'
            or workload_plan.get('selection_mode') != 'contiguous_window'):
        raise ValueError('TxAllo V4 requires original contiguous unresampled source window; derived events are not raw source blocks')
    if not (sharding.get('config') or {}).get('dynamic_a_txallo_runtime'):
        raise ValueError('TxAllo V4 bridge supports the reviewed dynamic A/G profile only')
    start = int(workload_plan.get('start_offset') or 0)
    count = int(workload_plan.get('actual_tx_count') or 0)
    ratio = float((sharding.get('config') or {}).get('history_ratio', 0.10))
    if not (0 < ratio <= 1) or count <= 0 or start <= 0:
        raise ValueError('TxAllo V4 requires positive evaluation count and nonempty pre-evaluation history; choose a later original window')
    requested = max(1, math.ceil(count * ratio))
    history_count = min(start, requested)
    canonical_rel = str(workload_plan.get('canonical_relative_path') or '')
    materialized_rel = str(workload_plan.get('materialized_relative_path') or '')
    # Restrict all canonical source paths to the verified cache root.
    def cache_path(relative: str) -> Path:
        p = (cache_root / relative).resolve()
        if not p.is_relative_to(cache_root.resolve()):
            raise ValueError('unsafe cached workload path')
        return p
    canonical = cache_path(canonical_rel)
    materialized = cache_path(materialized_rel)
    if _digest(canonical) != str(workload_plan.get('canonical_sha256') or '').lower():
        raise ValueError('TxAllo canonical source SHA256 mismatch')
    if _digest(materialized) != str(workload_plan.get('materialized_sha256') or '').lower():
        raise ValueError('TxAllo evaluation source SHA256 mismatch')
    history = []
    for row in _records(canonical, start=start - history_count, count=history_count):
        order = int(row['source_row_index'])
        if order >= start:
            raise ValueError('TxAllo history overlaps evaluation window')
        sender, receiver = str(row['sender_id']).lower(), str(row.get('receiver_id') or '').lower()
        if not sender or not receiver:
            raise ValueError('TxAllo history missing account identity')
        history.append({
            'schema_version': HISTORY_SCHEMA, 'source_order': order,
            'transaction_id': str(row['source_event_id']),
            'timestamp': str(row['timestamp_ms']), 'block_num': _chain_block(row),
            'global_sequence': order + 1,  # Explicit MBE replay index, not native chain sequence.
            'sender_id': sender, 'receiver_id': receiver,
            'accounts': sorted(set([sender, receiver])),
        })
    if any(left['block_num'] > right['block_num'] for left, right in zip(history, history[1:])):
        raise ValueError('TxAllo source history block order regressed')
    if history[-1]['source_order'] != start - 1:
        raise ValueError('TxAllo historical source window is not contiguous with evaluation')
    dynamic = []
    previous_block, previous_order = history[-1]['block_num'], -1
    for index, row in enumerate(_records(materialized, start=0, count=count)):
        order = int(row['source_row_index'])
        if order != start + index:
            raise ValueError('TxAllo dynamic source/order mismatch; sampled/reordered input is not permitted')
        block = _chain_block(row)
        if block < previous_block or order <= previous_order:
            raise ValueError('TxAllo original Axie source block/order regressed')
        previous_block, previous_order = block, order
        dynamic.append({
            'schema_version': DYNAMIC_SCHEMA, 'materialized_index': index,
            'raw_source_row_index': order,
            'transaction_id': str(row.get('logical_event_id') or row['source_event_id']),
            'block_num': block, 'global_sequence': order + 1,
            'sender_id': str(row['sender_id']).lower(),
            'receiver_id': str(row.get('receiver_id') or '').lower(),
        })
    if len(dynamic) != count:
        raise ValueError('TxAllo dynamic evaluation input incomplete')
    workload = run_dir / 'workload'
    history_rel = 'workload/txallo_history.jsonl.gz'
    dynamic_rel = 'workload/txallo_dynamic_blocks.jsonl.gz'
    history_sha = _write_gzip(run_dir / history_rel, history)
    dynamic_sha = _write_gzip(run_dir / dynamic_rel, dynamic)
    hist = {
        'schema_version': 'mbe_txallo_history_selection_v1',
        'selection_policy': 'preceding_ratio_v1', 'history_ratio': ratio,
        'evaluation_transaction_count': count, 'requested_history_count': requested,
        'selected_history_count': history_count, 'history_pool_count': history_count,
        'history_pool_sha256': history_sha, 'selected_history_sha256': history_sha,
        'history_relative_path': history_rel,
        'history_window_start_source_order': start - history_count,
        'history_window_end_source_order': start - 1,
        'evaluation_anchor_raw_row_index': start,
        'accounts_semantics': 'sender_receiver_accounts_only',
        'future_evaluation_transactions_used': 0,
        'g_cache_dir': str((project_root / '.cache' / 'txallo_g').resolve()),
        'source_block_identity': 'observed_axie_block_number_raw',
        'sequence_identity': 'source_row_index_replay_projection_not_native_global_sequence',
        'paper_note': 'Historical G/A core unchanged; only the source block/history adapter differs.',
    }
    epoch_blocks = int((sharding.get('config') or {}).get('a_epoch_blocks') or 15)
    g_multiple = int((sharding.get('config') or {}).get('g_epoch_multiple') or 20)
    if epoch_blocks != 15 or g_multiple != 20:
        raise ValueError('TxAllo source epoch and G-period parameters differ from reviewed MBE profile')
    first, last = dynamic[0], dynamic[-1]
    dyn = {
        'schema_version': 'mbe_txallo_dynamic_block_selection_v1',
        'row_schema_version': DYNAMIC_SCHEMA,
        'relative_path': dynamic_rel, 'sha256': dynamic_sha,
        'selected_count': count, 'evaluation_anchor_raw_row_index': start,
        'evaluation_anchor_transaction_id': first['transaction_id'],
        'first_block_num': first['block_num'], 'last_block_num': last['block_num'],
        'first_global_sequence': first['global_sequence'],
        'last_global_sequence': last['global_sequence'],
        'a_epoch_blocks': epoch_blocks, 'paper_reference_a_epoch_blocks': 300,
        'a_epoch_parameterization': 'mbe_adapted_fixed_15_source_blocks',
        'g_epoch_multiple': g_multiple,
        'source_epoch_count_spanned': ((last['block_num'] - first['block_num']) // epoch_blocks) + 1,
        'selection_semantics': 'contiguous_original_axie_window_exact_source_block_projection',
        'block_identity_source': 'observed_axie_block_number_raw',
        'sequence_identity_source': 'source_row_index_replay_projection_not_native_global_sequence',
        'future_evaluation_transactions_used_for_prior_epoch': 0,
        'paper_note': 'MBE-adapted tau1=15 blocks and tau2/tau1=20, paper core unchanged.',
        'source_raw_relative_path': str(manifest.get('local_raw_relative_path') or ''),
    }
    workload.mkdir(parents=True, exist_ok=True)
    (workload / 'txallo_history_summary.json').write_text(json.dumps(hist, ensure_ascii=False, indent=2, sort_keys=True) + '\n', encoding='utf-8')
    (workload / 'txallo_dynamic_blocks_summary.json').write_text(json.dumps(dyn, ensure_ascii=False, indent=2, sort_keys=True) + '\n', encoding='utf-8')
    return hist, dyn


# MBE_MV_TXALLO_EVENT_CLOCK_V1 -- add an explicitly non-paper, event-order adapter for DCL/Tapos.
# The observed Axie/source-block path remains byte-for-byte unchanged above.
_axie_observed_source_block_compile = compile_v4_txallo_inputs

def compile_v4_txallo_inputs(*, run_dir, manifest, workload_plan, profile, cache_root, project_root):
    if (str(workload_plan.get('dataset_id') or '') in {'dcl_sales_layered_v4', 'tapos_layered_v4'}
            and (profile.get('sharding') or {}).get('plugin_id') == 'txallo_account_sharding'):
        from backend.app.services.mv_txallo_event_v1 import compile_event_inputs
        return compile_event_inputs(run_dir=run_dir, manifest=manifest,
            workload_plan=workload_plan, profile=profile,
            cache_root=cache_root, project_root=project_root)
    return _axie_observed_source_block_compile(run_dir=run_dir, manifest=manifest,
        workload_plan=workload_plan, profile=profile,
        cache_root=cache_root, project_root=project_root)
