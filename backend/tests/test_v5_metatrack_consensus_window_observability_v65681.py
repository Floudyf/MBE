from __future__ import annotations
import json
from pathlib import Path
from backend.app.services.v5_observability_metrics import summarize_metatrack_consensus_windows

def w(path: Path, rows):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(''.join(json.dumps(x)+'\n' for x in rows), encoding='utf-8')

def routing(o,sh,b,wseq,start,end,wn,sn,cp,rnd,preds=None,req=0,rr=0):
    sv=[]
    if req: sv=[{'key':f'k{req}','required_version':req,'required_execution_round':rr}]
    return {'routing_ordinal':o,'execution_shard':sh,'route_batch_sequence':b,
      'consensus_execution_predecessor_ordinals':preds or [],'consensus_execution_round':rnd,'state_versions':sv,
      'consensus_window_sequence':wseq,'consensus_window_start_batch_sequence':start,'consensus_window_end_batch_sequence':end,
      'consensus_window_route_batch_count':end-start+1,'consensus_window_transaction_count':wn,
      'consensus_window_shard_transaction_count':sn,'consensus_window_critical_path':cp}

def tx(t,r): return {'tx_id':t,'execution_routing':r}
def block(h,sh,height,txs): return {'block_hash':h,'shard_id':sh,'height':height,'tx_list':txs}

def test_postrun_durable_reconstruction(tmp_path: Path):
    t1=tx('t1',routing(1,'s0',1,1,1,2,4,2,2,1)); t2=tx('t2',routing(2,'s1',1,1,1,2,4,2,2,1))
    t3=tx('t3',routing(3,'s0',2,1,1,2,4,2,2,2,[1])); t4=tx('t4',routing(4,'s1',2,1,1,2,4,2,2,2,req=2,rr=1))
    t5=tx('t5',routing(5,'s0',3,2,3,3,2,1,2,3,[3])); t6=tx('t6',routing(6,'s1',3,2,3,3,2,1,2,4,[5]))
    bs={'s0':[block('a','s0',1,[t1,t3]),block('b','s0',2,[t5])], 's1':[block('c','s1',1,[t2,t4]),block('d','s1',2,[t6])]}
    for sh,nodes in [('s0',['n0','n1']),('s1',['n4','n5'])]:
        for n in nodes:
            w(tmp_path/'nodes'/n/'blocks.jsonl',bs[sh]); w(tmp_path/'nodes'/n/'commit_markers.jsonl',[{'block_hash':x['block_hash'],'committed':True} for x in bs[sh]])
    (tmp_path/'real_cluster_summary.json').write_text(json.dumps({'configured_block_size':1000}),encoding='utf-8')
    s=summarize_metatrack_consensus_windows(tmp_path)
    assert s['available'] is True
    assert s['truth_scope']=='durable_signed_consensus_window_metadata_post_run_reconstruction_v2'
    assert [x['window_sequence'] for x in s['windows']]==[1,2]
    assert s['windows'][0]['recomputed_critical_path']==2
    assert s['windows'][0]['candidate_critical_path']==4
    assert s['windows'][0]['stop_reason']=='critical_width_not_improved'
    assert s['metrics']['metatrack_consensus_window_signed_reconstruction_match'] is True
    assert s['metrics']['metatrack_consensus_window_observed_pbft_block_count']==4
    assert s['metrics']['metatrack_consensus_window_baseline_route_batch_pbft_block_count']==6
    assert s['metrics']['metatrack_consensus_window_pbft_blocks_saved']==2
    assert (tmp_path/'metatrack_consensus_window_plan.jsonl').is_file()
    assert (tmp_path/'aggregate/metatrack_consensus_window_summary.json').is_file()

def test_asymmetric_shard_projection_counts_are_valid_window_metadata(tmp_path: Path):
    # Global window metadata must agree across shards, while the signed shard-local
    # transaction count is expected to differ (s0=3, s1=1 here).
    t1=tx('t1',routing(1,'s0',1,1,1,1,4,3,1,1))
    t2=tx('t2',routing(2,'s0',1,1,1,1,4,3,1,1))
    t3=tx('t3',routing(3,'s0',1,1,1,1,4,3,1,1))
    t4=tx('t4',routing(4,'s1',1,1,1,1,4,1,1,1))
    bs={'s0':[block('a','s0',1,[t1,t2,t3])], 's1':[block('b','s1',1,[t4])]}
    for sh,nodes in [('s0',['n0','n1']),('s1',['n4','n5'])]:
        for n in nodes:
            w(tmp_path/'nodes'/n/'blocks.jsonl',bs[sh])
            w(tmp_path/'nodes'/n/'commit_markers.jsonl',[{'block_hash':x['block_hash'],'committed':True} for x in bs[sh]])
    (tmp_path/'real_cluster_summary.json').write_text(json.dumps({'configured_block_size':1000}),encoding='utf-8')
    s=summarize_metatrack_consensus_windows(tmp_path)
    assert s['available'] is True
    assert s['windows'][0]['metadata_match'] is True
    assert s['windows'][0]['shard_transaction_count_match'] is True
    assert s['windows'][0]['shard_transaction_counts']=={'s0':3,'s1':1}
    assert s['metrics']['metatrack_consensus_window_signed_reconstruction_match'] is True


def test_inconsistent_count_inside_same_shard_still_fails(tmp_path: Path):
    t1=tx('t1',routing(1,'s0',1,1,1,1,3,2,1,1))
    t2=tx('t2',routing(2,'s0',1,1,1,1,3,1,1,1))  # tampered: should also be 2
    t3=tx('t3',routing(3,'s1',1,1,1,1,3,1,1,1))
    bs={'s0':[block('a','s0',1,[t1,t2])], 's1':[block('b','s1',1,[t3])]}
    for sh,nodes in [('s0',['n0','n1']),('s1',['n4','n5'])]:
        for n in nodes:
            w(tmp_path/'nodes'/n/'blocks.jsonl',bs[sh])
            w(tmp_path/'nodes'/n/'commit_markers.jsonl',[{'block_hash':x['block_hash'],'committed':True} for x in bs[sh]])
    (tmp_path/'real_cluster_summary.json').write_text(json.dumps({'configured_block_size':1000}),encoding='utf-8')
    s=summarize_metatrack_consensus_windows(tmp_path)
    assert s['available'] is True
    assert s['windows'][0]['metadata_match'] is True
    assert s['windows'][0]['shard_transaction_count_match'] is False
    assert s['metrics']['metatrack_consensus_window_signed_reconstruction_match'] is False
