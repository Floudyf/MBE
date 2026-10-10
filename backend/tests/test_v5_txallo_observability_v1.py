from __future__ import annotations
import json
from pathlib import Path
from backend.app.services.v5_txallo_observability_v1 import _timing, _physical, build_summary

def write(p: Path, value):
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(json.dumps(value) + '\n', encoding='utf-8')

def test_epoch_phase_counting_no_overlap(tmp_path: Path):
    write(tmp_path / 'client/txallo_epoch_timing.jsonl', {
        'schema_version': 'mbe_txallo_epoch_timing_v1', 'source_epoch': 0,
        'status': 'passed', 'commit_wait_ns': 300, 'replica_wait_ns': 400,
        'allocation_update_ns': 200, 'mapping_ack_wait_ns': 100, 'total_barrier_ns': 1200})
    got = _timing(tmp_path)
    assert got['status'] == 'passed'
    assert got['phase_ms']['commit_wait_ns'] == 0.0003
    assert got['total_barrier_ms'] == 0.0012

def test_timing_missing_not_zero(tmp_path: Path):
    got = _timing(tmp_path)
    assert got['status'] == 'unavailable'
    assert 'total_barrier_ms' not in got

def test_timing_reject_duplicate(tmp_path: Path):
    p = tmp_path / 'client/txallo_epoch_timing.jsonl'
    row = {'schema_version': 'mbe_txallo_epoch_timing_v1', 'source_epoch': 0, 'status':'passed',
           'commit_wait_ns':0, 'replica_wait_ns':0, 'allocation_update_ns':0,
           'mapping_ack_wait_ns':0, 'total_barrier_ns':1}
    p.parent.mkdir(parents=True)
    p.write_text(json.dumps(row) + '\n' + json.dumps(row) + '\n', encoding='utf-8')
    assert _timing(tmp_path)['status'] == 'failed'

def test_one_leader_per_shard_no_replica_multiplication(tmp_path: Path):
    plan = {'node_configs': [
        {'node_id':'n0','shard_id':'s0','leader':True},
        {'node_id':'n1','shard_id':'s0','leader':False},
        {'node_id':'n4','shard_id':'s1','leader':True}]}
    for node, shard in [('n0','s0'),('n1','s0'),('n4','s1')]:
        write(tmp_path/f'nodes/{node}/block_execution_summary.json', {
            'shard_id':shard, 'blocks': [{'block_hash':node+'b', 'executed_transaction_count':2,
                            'system_delta_drain_block_count':0, 'state_commitment_ms':1}]})
    got = _physical(tmp_path,plan)
    assert got['status'] == 'passed'
    assert got['counts']['physical_blocks'] == 2
    assert got['counts']['physical_transaction_instances'] == 4
    assert got['cost_sum_ms']['state_commitment_ms'] == 2

def test_missing_leader_fails_closed(tmp_path: Path):
    plan = {'node_configs':[{'node_id':'n0','shard_id':'s0','leader':True}]}
    assert _physical(tmp_path,plan)['status'] == 'unavailable'

def test_build_summary_missing_sources_not_success(tmp_path: Path):
    write(tmp_path/'compiled_run_plan.json', {'node_configs':[{'node_id':'n0','shard_id':'s0','leader':True}]})
    res = build_summary(tmp_path)
    assert res['status'] == 'partial'
    assert res['sources_sha256']['client/txallo_epoch_timing.jsonl']['status'] == 'unavailable'
