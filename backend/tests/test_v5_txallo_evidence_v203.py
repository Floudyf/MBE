from __future__ import annotations
import csv, gzip, json
from pathlib import Path

from backend.app.services.v5_serial_order_oracle import EMPTY_STATE_ROOT, _business_state_digest, _canonical_digest, _stable_direct_access_value
from backend.app.services.v5_txallo_stateless_oracle import evaluate, _load_entries
from backend.app.services.v5_optme_txallo_method_specific_v18 import compute_writeback_fanout_v18
from backend.app.services.v5_formal_scheduler import _serial_oracle_summary


def _csv(path: Path, fields, rows):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open('w', newline='', encoding='utf-8') as f:
        w=csv.DictWriter(f,fieldnames=fields); w.writeheader(); w.writerows(rows)


def _node(root: Path, name: str, shard: str, txid: str, logical: str, digest: str):
    d=root/'nodes'/name; d.mkdir(parents=True)
    (d/'node_summary.json').write_text(json.dumps({'node_id':name,'shard_id':shard,'business_state_digest':digest}),encoding='utf-8')
    _csv(d/'committed_chain.csv',['height','shard_id','block_hash','parent_hash','tx_count','state_root_before'],[{'height':1,'shard_id':shard,'block_hash':f'b-{shard}','parent_hash':'genesis','tx_count':1,'state_root_before':EMPTY_STATE_ROOT}])
    _csv(d/'transaction_execution_trace.csv',['node_id','shard_id','block_hash','height','tx_id','logical_tx_id','original_index','success'],[{'node_id':name,'shard_id':shard,'block_hash':f'b-{shard}','height':1,'tx_id':txid,'logical_tx_id':logical,'original_index':0,'success':'true'}])


def test_stateless_txallo_multishard_exact_version_serial_replay(tmp_path: Path) -> None:
    key='contract:k'
    v1=_stable_direct_access_value(logical_tx_id='l1',key=key,semantics='set',previous='')
    v2=_stable_direct_access_value(logical_tx_id='l2',key=key,semantics='set',previous=v1)
    empty=_business_state_digest({}); s1=_business_state_digest({'s1::'+key:v2})
    _node(tmp_path,'n0','s0','t1','l1',empty); _node(tmp_path,'n1','s1','t2','l2',s1)
    client=tmp_path/'client'; client.mkdir()
    entries=[
      {'index':0,'logical_id':'l1','tx_id':'t1','execution_shard':'s0','access_list':[{'key':key,'mode':'read_write','update_semantics':'set','delta':0}], 'state_versions':[{'key':key,'required_version':0,'produced_version':1}]},
      {'index':1,'logical_id':'l2','tx_id':'t2','execution_shard':'s1','access_list':[{'key':key,'mode':'read_write','update_semantics':'set','delta':0}], 'state_versions':[{'key':key,'required_version':1,'produced_version':2}]},
    ]
    with gzip.open(client/'resolved_access_lists.jsonl.gz','wt',encoding='utf-8') as f:
        for row in entries: f.write(json.dumps(row)+'\n')
    _csv(client/'placement_plan.csv',['state_key','home_shard'],[{'state_key':key,'home_shard':'s1'}])
    global_digest=_canonical_digest({'s0':empty,'s1':s1})
    got=evaluate(tmp_path,{'global_business_state_digest':global_digest})
    assert got['serial_order_replay_equivalent'] is True, got
    assert got['method_correctness_oracle_valid'] is True
    assert 'multi_shard' in got['serial_order_replay_supported_scope']
    assert got['serial_order_replay_transaction_count']==2


def test_stateless_txallo_replay_fails_closed_when_business_state_version_dependency_missing(tmp_path: Path) -> None:
    key='contract:k'
    empty=_business_state_digest({})
    _node(tmp_path,'n0','s0','t','l',empty)
    client=tmp_path/'client'; client.mkdir()
    with gzip.open(client/'resolved_access_lists.jsonl.gz','wt',encoding='utf-8') as f:
        f.write(json.dumps({'index':0,'logical_id':'l','tx_id':'t','execution_shard':'s0','access_list':[{'key':key,'mode':'read_write','update_semantics':'set','delta':0}]})+'\n')
    _csv(client/'placement_plan.csv',['state_key','home_shard'],[{'state_key':key,'home_shard':'s0'}])
    got=evaluate(tmp_path,{})
    assert got['serial_order_replay_equivalent'] is False
    assert any('state_version_dependency_missing' in x for x in got['serial_order_replay_blockers']), got


def test_stateless_txallo_replay_allows_unversioned_account_and_commutative_state(tmp_path: Path) -> None:
    # Mirrors Go isVersionedStateAccess: account keys and commutative deltas are not exact-versioned.
    client=tmp_path/'client'; client.mkdir(parents=True)
    path=client/'resolved_access_lists.jsonl.gz'
    with gzip.open(path,'wt',encoding='utf-8') as f:
        f.write(json.dumps({'index':0,'logical_id':'l','tx_id':'t','execution_shard':'s0','access_list':[
            {'key':'balance:a','mode':'read_write','update_semantics':'set','delta':0},
            {'key':'nonce:a','mode':'write','update_semantics':'set','delta':0},
            {'key':'market:x','mode':'commutative_delta','update_semantics':'add','delta':1},
        ]})+'\n')
    loaded, _, errs = _load_entries(path)
    assert 't' in loaded
    assert not any('state_version_dependency_missing' in x for x in errs), errs


def test_writeback_method_preserving_origin_forms_exact_five_tuple(tmp_path: Path) -> None:
    _csv(tmp_path/'physical_remote_state_operations.csv',
         ['access_kind','normalized_kind','tx_id','logical_tx_ids','state_key','home_shard','update_semantics','produced_version','apply_origin','success'],
         [{'access_kind':'STATE_DELTA_APPLY','normalized_kind':'writeback','tx_id':'t1','logical_tx_ids':'t1','state_key':'k','home_shard':'s0','update_semantics':'','produced_version':'7','apply_origin':'optme_txallo_versioned_remote_home_v10','success':'true'},
          {'access_kind':'STATE_DELTA_APPLY','normalized_kind':'writeback','tx_id':'t1','logical_tx_ids':'t1','state_key':'k','home_shard':'s0','update_semantics':'','produced_version':'7','apply_origin':'optme_txallo_versioned_remote_home_v10','success':'true'}])
    got=compute_writeback_fanout_v18(tmp_path,{})
    assert got['status']=='available_exact_logical_dedup',got
    assert got['unique_logical_delta_count']==1
    assert got['replica_fanout_ratio']==2


def test_formal_oracle_summary_carries_method_identity() -> None:
    got=_serial_oracle_summary({}, {'method_config_id':'stateless_txallo','comparison_semantics_class':'stateless_remote_home_v1'})
    assert got['method_config_id']=='stateless_txallo'
