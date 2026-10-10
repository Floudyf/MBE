"""MBE MV_Compat_v1 adapter-only evidence and fail-closed regression tests."""
from __future__ import annotations
import gzip
import hashlib
import json
from pathlib import Path

import pytest
from backend.app.services.mv_txallo_input_v1 import compile_v4_txallo_inputs


def _gzip(path: Path, rows: list[dict]) -> str:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open('wb') as raw:
        with gzip.GzipFile(filename='', mode='wb', mtime=0, fileobj=raw) as output:
            for row in rows:
                output.write((json.dumps(row, separators=(',', ':')) + '\n').encode('utf-8'))
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _case(tmp_path: Path, *, start: int = 5, n: int = 3):
    cache = tmp_path / 'cache'
    rows = []
    for i in range(start + n):
        rows.append({'source_row_index': i, 'source_event_id': 'e' + str(i),
                     'logical_event_id': 'logical_' + str(i), 'sender_id': 'from_a',
                     'receiver_id': 'to_b', 'timestamp_ms': 1700000000000 + i,
                     'metadata': {'block_number_raw': str(1000 + i // 2)}})
    eval_rows = [dict(rows[i], materialized_index=j) for j, i in enumerate(range(start, start + n))]
    canon_sha = _gzip(cache / 'canonical' / 'fixture.gz', rows)
    material_sha = _gzip(cache / 'materialized' / 'fixture.gz', eval_rows)
    plan = {'dataset_id': 'axie_day_layered_v4', 'canonical_relative_path': 'canonical/fixture.gz',
            'materialized_relative_path': 'materialized/fixture.gz',
            'canonical_sha256': canon_sha, 'materialized_sha256': material_sha,
            'start_offset': start, 'actual_tx_count': n,
            'variant_mode': 'original_window', 'selection_mode': 'contiguous_window'}
    profile = {'sharding': {'plugin_id': 'txallo_account_sharding', 'config': {
        'history_ratio': 0.50, 'dynamic_a_txallo_runtime': True,
        'a_epoch_blocks': 15, 'g_epoch_multiple': 20}}}
    return cache, plan, profile


def test_mv_axie_dynamic_history_before_evaluation_only(tmp_path: Path):
    cache, plan, profile = _case(tmp_path)
    run = tmp_path / 'run'
    hist, dyn = compile_v4_txallo_inputs(
        run_dir=run, manifest={'local_raw_relative_path': 'dataset/Axie_Infinity/file.csv'},
        workload_plan=plan, profile=profile, cache_root=cache, project_root=tmp_path)
    assert hist['evaluation_anchor_raw_row_index'] == 5
    assert hist['history_window_end_source_order'] == 4
    assert hist['selected_history_count'] == 2
    assert hist['future_evaluation_transactions_used'] == 0
    assert dyn['selected_count'] == 3
    assert dyn['block_identity_source'] == 'observed_axie_block_number_raw'
    with gzip.open(run / 'workload/txallo_history.jsonl.gz', 'rt', encoding='utf-8') as reader:
        rows = [json.loads(x) for x in reader]
    assert [r['source_order'] for r in rows] == [3, 4]
    assert all(r['source_order'] < 5 for r in rows)
    assert rows[0]['accounts'] == ['from_a', 'to_b']
    with gzip.open(run / 'workload/txallo_dynamic_blocks.jsonl.gz', 'rt', encoding='utf-8') as reader:
        dynamics = [json.loads(x) for x in reader]
    assert [r['raw_source_row_index'] for r in dynamics] == [5, 6, 7]
    assert [r['transaction_id'] for r in dynamics] == ['logical_5', 'logical_6', 'logical_7']


# MBE_MV_TXALLO_EVENT_CLOCK_V1 verified source events, no invented source blockchain height.
@pytest.mark.parametrize('dataset', ['dcl_sales_layered_v4', 'tapos_layered_v4'])
def test_mv_no_invented_chain_block_number(tmp_path: Path, dataset: str):
    cache, plan, profile = _case(tmp_path)
    plan['dataset_id'] = dataset
    hist,dyn = compile_v4_txallo_inputs(run_dir=tmp_path / 'run', manifest={},
        workload_plan=plan, profile=profile, cache_root=cache, project_root=tmp_path)
    assert hist['future_evaluation_transactions_used'] == 0
    assert dyn['epoch_clock_source'] == 'mbe_event_order_index'
    assert dyn['block_identity_source'] == 'not_observed_no_chain_blocks_invented'
    import gzip, json
    with gzip.open(tmp_path / 'run' / dyn['relative_path'], 'rt', encoding='utf-8') as f:
        rows=[json.loads(line) for line in f]
    assert all(row['block_num'] == 0 and row['epoch_coordinate'] == row['raw_source_row_index']+1 for row in rows)


def test_mv_no_future_history_at_anchor_zero(tmp_path: Path):
    cache, plan, profile = _case(tmp_path, start=0)
    with pytest.raises(ValueError, match='nonempty pre-evaluation history'):
        compile_v4_txallo_inputs(run_dir=tmp_path / 'run', manifest={},
            workload_plan=plan, profile=profile, cache_root=cache, project_root=tmp_path)


def test_mv_txallo_never_consumes_reordered_zipf(tmp_path: Path):
    cache, plan, profile = _case(tmp_path)
    plan['variant_mode'] = 'key_zipf'
    with pytest.raises(ValueError, match='original contiguous'):
        compile_v4_txallo_inputs(run_dir=tmp_path / 'run', manifest={},
            workload_plan=plan, profile=profile, cache_root=cache, project_root=tmp_path)


def test_mv_unrelated_method_does_not_need_txallo_sidecar(tmp_path: Path):
    cache, plan, _ = _case(tmp_path)
    assert compile_v4_txallo_inputs(run_dir=tmp_path / 'run', manifest={},
        workload_plan=plan, profile={'sharding': {'plugin_id': 'deterministic_state_key_sharding'}},
        cache_root=cache, project_root=tmp_path) is None


def test_mv_corrupt_materialization_fails_closed(tmp_path: Path):
    cache, plan, profile = _case(tmp_path)
    plan['materialized_sha256'] = '0' * 64
    with pytest.raises(ValueError, match='evaluation source SHA256 mismatch'):
        compile_v4_txallo_inputs(run_dir=tmp_path / 'run', manifest={},
            workload_plan=plan, profile=profile, cache_root=cache, project_root=tmp_path)
