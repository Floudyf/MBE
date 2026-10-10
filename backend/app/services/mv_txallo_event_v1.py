"""MBE event-order clock extension for TxAllo on DCL/Tapos V4.

No chain block is inferred. A/G operates on 15 original ordered source EVENTS
per adapted epoch (not 15 blocks). This is a labeled extension, never a direct
source-block/paper-fidelity reproduction. G/A allocation core is unchanged.
"""
from __future__ import annotations

import json
import math
from pathlib import Path
from typing import Any

from backend.app.services.mv_txallo_input_v1 import _digest, _records, _write_gzip, HISTORY_SCHEMA

DATASETS = frozenset({'dcl_sales_layered_v4', 'tapos_layered_v4'})
EVENT_SCHEMA = 'mbe_txallo_event_clock_v1'
CLOCK_KIND = 'mbe_event_order_index'


def compile_event_inputs(*, run_dir: Path, manifest: dict[str, Any],
                         workload_plan: dict[str, Any], profile: dict[str, dict[str, Any]],
                         cache_root: Path, project_root: Path):
    source_id = str(workload_plan.get('dataset_id') or '')
    if source_id not in DATASETS:
        raise ValueError('event-order TxAllo bridge is limited to audited DCL/Tapos V4')
    sharding = profile.get('sharding') or {}
    cfg = sharding.get('config') or {}
    if sharding.get('plugin_id') != 'txallo_account_sharding' or not cfg.get('dynamic_a_txallo_runtime'):
        raise ValueError('event-order bridge requires the reviewed dynamic G/A TxAllo profile')
    if workload_plan.get('variant_mode') != 'original_window' or workload_plan.get('selection_mode') != 'contiguous_window':
        raise ValueError('TxAllo event-order extension requires a contiguous ORIGINAL window, not a resampled/reordered variant')
    start, count = int(workload_plan.get('start_offset') or 0), int(workload_plan.get('actual_tx_count') or 0)
    ratio = float(cfg.get('history_ratio', 0.10))
    if start <= 0 or count <= 0 or not 0 < ratio <= 1:
        raise ValueError('TxAllo event-order extension requires a positive evaluation window with strictly preceding real events')
    a_steps, g_multiple = int(cfg.get('a_epoch_blocks') or 15), int(cfg.get('g_epoch_multiple') or 20)
    if (a_steps, g_multiple) != (15, 20):
        raise ValueError('TxAllo event-order epoch cadence requires reviewed 15-event/20-epoch adapted profile')

    cache_root = cache_root.resolve()
    def checked_path(relative: Any) -> Path:
        rel = str(relative or '')
        if not rel or Path(rel).is_absolute():
            raise ValueError('TxAllo event-order source relative path is absent or absolute')
        path = (cache_root / rel).resolve()
        if not path.is_relative_to(cache_root) or not path.is_file():
            raise ValueError('TxAllo event-order source escapes cache root or is missing')
        return path
    canonical = checked_path(workload_plan.get('canonical_relative_path'))
    materialized = checked_path(workload_plan.get('materialized_relative_path'))
    if _digest(canonical) != str(workload_plan.get('canonical_sha256') or '').lower():
        raise ValueError('TxAllo event-order canonical SHA256 mismatch')
    if _digest(materialized) != str(workload_plan.get('materialized_sha256') or '').lower():
        raise ValueError('TxAllo event-order evaluation SHA256 mismatch')

    wanted = max(1, math.ceil(count * ratio))
    history_count = min(start, wanted)
    history = []
    for index, row in enumerate(_records(canonical, start=start-history_count, count=history_count)):
        order = int(row['source_row_index'])
        if order != start-history_count+index or order >= start:
            raise ValueError('TxAllo event-order historical records are not the exact preceding source window')
        sender = str(row.get('sender_id') or '').strip().lower()
        receiver = str(row.get('receiver_id') or '').strip().lower()
        if not sender or not receiver:
            raise ValueError('TxAllo event-order history missing business account')
        history.append({
            'schema_version': HISTORY_SCHEMA, 'source_order': order,
            'transaction_id': str(row.get('source_event_id') or ''),
            'timestamp': str(row.get('timestamp_ms') or ''),
            'block_num': 0,  # Deliberately no observed source block.
            'global_sequence': order + 1, # MBE source event index, NOT chain sequence.
            'sender_id': sender, 'receiver_id': receiver,
            'accounts': sorted({sender, receiver}),
        })
    if not history or history[-1]['source_order'] != start - 1:
        raise ValueError('TxAllo event-order history did not end immediately before evaluation')

    dynamic = []
    for idx, row in enumerate(_records(materialized, start=0, count=count)):
        order = int(row['source_row_index'])
        if order != start + idx:
            raise ValueError('TxAllo event-order evaluation is not the exact contiguous original source window')
        sender = str(row.get('sender_id') or '').strip().lower()
        receiver = str(row.get('receiver_id') or '').strip().lower()
        logical_id = str(row.get('logical_event_id') or row.get('source_event_id') or '').strip()
        if not sender or not receiver or not logical_id:
            raise ValueError('TxAllo event-order evaluation identity incomplete')
        dynamic.append({
            'schema_version': EVENT_SCHEMA,
            'materialized_index': idx, 'raw_source_row_index': order,
            'transaction_id': logical_id,
            'block_num': 0, # No fake block number: Go reads epoch_coordinate in this mode.
            'epoch_coordinate': order + 1,
            'global_sequence': order + 1,
            'sender_id': sender, 'receiver_id': receiver,
        })
    if len(dynamic) != count:
        raise ValueError('TxAllo event-order evaluation count mismatch')

    history_rel = 'workload/txallo_history.jsonl.gz'
    dynamic_rel = 'workload/txallo_dynamic_blocks.jsonl.gz'
    history_sha = _write_gzip(run_dir / history_rel, history)
    dynamic_sha = _write_gzip(run_dir / dynamic_rel, dynamic)
    hist = {
        'schema_version': 'mbe_txallo_history_selection_v1',
        'selection_policy': 'preceding_ratio_v1', 'history_ratio': ratio,
        'evaluation_transaction_count': count, 'requested_history_count': wanted,
        'selected_history_count': history_count, 'history_pool_count': history_count,
        'history_pool_sha256': history_sha, 'selected_history_sha256': history_sha,
        'history_relative_path': history_rel,
        'history_window_start_source_order': start - history_count,
        'history_window_end_source_order': start-1,
        'evaluation_anchor_raw_row_index': start,
        'accounts_semantics': 'sender_receiver_accounts_only',
        'future_evaluation_transactions_used': 0,
        'g_cache_dir': str((project_root / '.cache' / 'txallo_g').resolve()),
        'source_block_identity': 'not_observed',
        'sequence_identity': 'source_row_index_plus_one_not_native_chain_sequence',
        'epoch_clock_source': CLOCK_KIND,
        'paper_note': 'Historical source events are real; epoch clock uses ordered events, not observed source blocks. MBE extension only.',
    }
    dyn = {
        'schema_version': 'mbe_txallo_dynamic_block_selection_v1',
        'row_schema_version': EVENT_SCHEMA,
        'relative_path': dynamic_rel, 'sha256': dynamic_sha,
        'selected_count': count, 'evaluation_anchor_raw_row_index': start,
        'evaluation_anchor_transaction_id': dynamic[0]['transaction_id'],
        'first_block_num': None, 'last_block_num': None,
        'first_epoch_coordinate': dynamic[0]['epoch_coordinate'],
        'last_epoch_coordinate': dynamic[-1]['epoch_coordinate'],
        'first_global_sequence': dynamic[0]['global_sequence'],
        'last_global_sequence': dynamic[-1]['global_sequence'],
        'a_epoch_blocks': a_steps,  # Legacy Go API count; unit is EVENTS in this adapter.
        'a_epoch_unit': 'source_events_not_chain_blocks',
        'a_epoch_parameterization': 'mbe_adapted_fixed_15_observed_events',
        'paper_reference_a_epoch_blocks': 300,
        'g_epoch_multiple': g_multiple,
        'source_epoch_count_spanned': ((count - 1) // a_steps) + 1,
        'selection_semantics': 'contiguous_original_source_events',
        'epoch_clock_source': CLOCK_KIND,
        'block_identity_source': 'not_observed_no_chain_blocks_invented',
        'sequence_identity_source': 'source_row_index_plus_one',
        'future_evaluation_transactions_used_for_prior_epoch': 0,
        'paper_fidelity_scope': 'MBE_EVENT_ORDER_ADAPTATION_NOT_PAPER_SOURCE_BLOCK_REPRODUCTION',
        'paper_note': 'Same frozen TxAllo G/A core; updates every 15 consecutive observed source events, G every 20 closed epochs. This is not a chain-block or original-paper timing reproduction.',
        'source_raw_relative_path': str(manifest.get('local_raw_relative_path') or ''),
    }
    folder = run_dir / 'workload'
    folder.mkdir(parents=True, exist_ok=True)
    (folder/'txallo_history_summary.json').write_text(json.dumps(hist,ensure_ascii=False,sort_keys=True,indent=2)+'\n',encoding='utf-8')
    (folder/'txallo_dynamic_blocks_summary.json').write_text(json.dumps(dyn,ensure_ascii=False,sort_keys=True,indent=2)+'\n',encoding='utf-8')
    return hist,dyn
