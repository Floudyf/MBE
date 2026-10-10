from __future__ import annotations
import csv
import gzip
import json
from pathlib import Path

from backend.app.services.v5_txallo_terminal_v14 import build
from backend.app.services.v5_txallo_close_v14 import postprocess


def _j(path, obj):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(obj, ensure_ascii=False), encoding='utf-8')


def _jl(path, rows, zipped=False):
    path.parent.mkdir(parents=True, exist_ok=True)
    data = ''.join(json.dumps(x)+'\n' for x in rows)
    if zipped:
        with gzip.open(path,'wt',encoding='utf-8') as f: f.write(data)
    else: path.write_text(data, encoding='utf-8')


def _csv(path, rows):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open('w', encoding='utf-8', newline='') as f:
        w=csv.DictWriter(f,fieldnames=list(rows[0]));w.writeheader();w.writerows(rows)


def fixture(root):
    _j(root/'compiled_run_plan.json', {'node_configs':[{'node_id':'n0','shard_id':'s0','leader':True,
        'plugin_profile':{'sharding':{'config':{'a_epoch_blocks':15}}}}]})
    _j(root/'workload/txallo_dynamic_blocks_summary.json', {'first_block_num':100})
    _jl(root/'workload/txallo_dynamic_blocks.jsonl.gz', [
        {'materialized_index':0,'block_num':100},
        {'materialized_index':1,'block_num':101},
        {'materialized_index':2,'block_num':116}],True)
    _csv(root/'client/txallo_transaction_placement.csv', [
        {'tx_index':'0','logical_id':'l0','txallo_cross_shard':'false'},
        {'tx_index':'1','logical_id':'l1','txallo_cross_shard':'true'},
        {'tx_index':'2','logical_id':'l2','txallo_cross_shard':'false'}])
    _jl(root/'client/txallo_epoch_timing.jsonl',[{'source_epoch':0,'commit_wait_ns':100000000,'total_barrier_ns':110000000,'status':'passed'}])
    _jl(root/'client/transaction_lifecycle.jsonl',[{'logical_tx_id':'l0','node_id':'mbe-client','stage':'submitted','timestamp_ms':100,'success':True}])
    _jl(root/'nodes/n0/transaction_lifecycle.jsonl', [
        {'logical_tx_id':'l0','node_id':'n0','stage':'received','timestamp_ms':1000,'success':True},
        {'logical_tx_id':'l0','node_id':'n0','stage':'admitted','timestamp_ms':1003,'success':True},
        {'logical_tx_id':'l0','node_id':'n0','stage':'proposed','timestamp_ms':1009,'success':True},
        {'logical_tx_id':'l0','node_id':'n0','stage':'quorum_committed','timestamp_ms':1022,'success':True},
        {'logical_tx_id':'l0','node_id':'n0','stage':'durable_committed','timestamp_ms':1028,'success':True},
        {'logical_tx_id':'l1','node_id':'n0','stage':'admitted','timestamp_ms':1032,'success':True},
        {'logical_tx_id':'l1','node_id':'n0','stage':'proposed','timestamp_ms':1042,'success':True},
        {'logical_tx_id':'l2','node_id':'n0','stage':'admitted','timestamp_ms':1055,'success':True},
    ])


def test_missing_data_is_unavailable_not_zero(tmp_path):
    got=build(tmp_path)
    assert got['status']=='unavailable'
    assert got['missing_inputs']
    assert 'global_critical_path_ms' not in got


def test_terminal_same_node_phase_and_epoch_boundary(tmp_path):
    fixture(tmp_path)
    got=build(tmp_path)
    assert got['status']=='passed_with_scope_limitations'
    assert got['total_logical_transaction_count']==3
    assert got['closed_epoch_count']==1
    assert got['epochs'][0]['cross_shard_transaction_count']==1
    assert got['epochs'][1]['closed_epoch_timing'] is None
    p=got['epochs'][0]['same_node_duration_distribution']
    assert p['received_to_admitted']['p50_ms']==3
    assert p['proposed_to_quorum_committed']['p50_ms']==13
    assert got['cross_node_latency_ms'] is None
    assert got['global_critical_path_ms'] is None


def test_catalog_refresh_and_eligibility_unchanged(tmp_path):
    fixture(tmp_path)
    _j(tmp_path/'aggregate/txallo_evidence_summary.json',{'status':'passed'})
    (tmp_path/'nodes/n0/big_wal.bin').write_bytes(b'original-large-file-no-rehash')
    _j(tmp_path/'artifact_catalog.json',{'schema_version':'mbe_v5_artifact_catalog_v1',
       'run_id':'v5_test','file_count':1,'files':[{'name':'nodes/n0/big_wal.bin','sha256':'preserved_frozen_sha'}]})
    result={'run_id':'v5_test','status':'completed', 'summary':{'formal_eligibility':True}, 'artifacts':[]}
    observations=postprocess(tmp_path, result)
    assert observations['txallo_artifact_reindex_status']=='passed'
    catalog=json.loads((tmp_path/'artifact_catalog.json').read_text())
    names={x['name'] for x in catalog['files']}
    assert 'aggregate/txallo_evidence_summary.json' in names
    assert 'aggregate/txallo_terminal_breakdown.json' in names
    assert {x['name'] for x in result['artifacts']} >= names
    assert result['summary']['formal_eligibility'] is True
    assert 'artifact_gate' not in result['summary']
    assert all(x['sha256'] for x in catalog['files'])
    assert next(x['sha256'] for x in catalog['files'] if x['name']=='nodes/n0/big_wal.bin')=='preserved_frozen_sha'


def test_reindex_failure_cannot_invalidate_formal_result(tmp_path):
    fixture(tmp_path)
    result={'run_id':'invalid','summary':{'formal_eligibility':True},'artifacts':['old']}
    obs=postprocess(tmp_path,result)
    assert obs['txallo_artifact_reindex_status'].startswith('failed:')
    assert result['summary']['formal_eligibility'] is True
    assert result['artifacts']==['old']


def test_source_sidecar_mismatch_is_reported(tmp_path):
    fixture(tmp_path)
    _csv(tmp_path/'client/txallo_transaction_placement.csv',[{'tx_index':'999','logical_id':'l0','txallo_cross_shard':'false'}])
    got=build(tmp_path)
    assert got['status']=='failed'
    assert got['blockers']


def test_negative_same_node_time_never_fabricated(tmp_path):
    fixture(tmp_path)
    _jl(tmp_path/'nodes/n0/transaction_lifecycle.jsonl', [
        {'logical_tx_id':'l0','node_id':'n0','stage':'admitted','timestamp_ms':1200,'success':True},
        {'logical_tx_id':'l0','node_id':'n0','stage':'proposed','timestamp_ms':1100,'success':True}])
    got=build(tmp_path)
    assert got['epochs'][0]['timestamp_order_anomalies']==1
    assert got['epochs'][0]['same_node_duration_distribution']['admitted_to_proposed']['sample_count']==0


def test_absent_node_trace_remains_partial(tmp_path):
    fixture(tmp_path)
    (tmp_path/'nodes/n0/transaction_lifecycle.jsonl').unlink()
    got=build(tmp_path)
    assert got['status']=='partial'
    assert got['epochs'][0]['same_node_duration_distribution']['admitted_to_proposed']['sample_count']==0
