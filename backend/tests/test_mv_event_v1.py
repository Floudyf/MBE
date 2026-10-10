from __future__ import annotations
import gzip,hashlib,json
from pathlib import Path
import pytest

from backend.app.services.mv_txallo_input_v1 import compile_v4_txallo_inputs
from backend.app.services import v5_optme_txallo_paperfaithful_v17 as fidelity


def _gz(path:Path, rows:list[dict]):
    path.parent.mkdir(parents=True,exist_ok=True)
    with gzip.open(path,'wt',encoding='utf-8') as f:
        for row in rows:f.write(json.dumps(row,sort_keys=True)+'\n')
    return hashlib.sha256(path.read_bytes()).hexdigest()

@pytest.mark.parametrize('dataset',["dcl_sales_layered_v4","tapos_layered_v4"])
def test_event_clock_precise_prefix_and_provenance(tmp_path:Path,dataset:str):
    cache=tmp_path/'cache'
    all_rows=[{'source_row_index':i,'source_event_id':f'source{i}',
        'logical_event_id':f'event{i}', 'sender_id':f'a{i%5}',
        'receiver_id':f'b{i%7}', 'timestamp_ms':1234567890000+i} for i in range(48)]
    canonical='canonical/source.jsonl.gz';mat='materialized/run.jsonl.gz'
    ch=_gz(cache/canonical,all_rows)
    mh=_gz(cache/mat,all_rows[23:33])
    plan={'dataset_id':dataset,'variant_mode':'original_window','selection_mode':'contiguous_window',
          'start_offset':23,'actual_tx_count':10,
          'canonical_relative_path':canonical,'canonical_sha256':ch,
          'materialized_relative_path':mat,'materialized_sha256':mh}
    profile={'sharding':{'plugin_id':'txallo_account_sharding','config':{
        'dynamic_a_txallo_runtime':True,'a_epoch_blocks':15,'g_epoch_multiple':20,'history_ratio':0.20}}}
    run=tmp_path/'run'
    hist,dyn=compile_v4_txallo_inputs(run_dir=run,manifest={'local_raw_relative_path':'fixture.csv'},
        workload_plan=plan,profile=profile,cache_root=cache,project_root=tmp_path)
    assert hist['selected_history_count']==2
    assert hist['history_window_end_source_order']==22
    assert hist['future_evaluation_transactions_used']==0
    assert dyn['epoch_clock_source']=='mbe_event_order_index'
    assert dyn['block_identity_source']=='not_observed_no_chain_blocks_invented'
    assert dyn['a_epoch_unit']=='source_events_not_chain_blocks'
    with gzip.open(run/dyn['relative_path'],'rt',encoding='utf-8') as f:
        items=[json.loads(line) for line in f]
    assert [row['epoch_coordinate'] for row in items]==list(range(24,34))
    assert all(row['block_num']==0 and row['schema_version']=='mbe_txallo_event_clock_v1' for row in items)
    with gzip.open(run/hist['history_relative_path'],'rt',encoding='utf-8') as f:
        history=[json.loads(line) for line in f]
    assert [r['source_order'] for r in history]==[21,22]
    assert set(history[-1]['accounts'])=={'a2','b1'}  # unmodified source business identities

    with pytest.raises(ValueError,match='ORIGINAL|contiguous|original'):
        compile_v4_txallo_inputs(run_dir=tmp_path/'bad',manifest={},workload_plan={**plan,'variant_mode':'key_zipf'},
            profile=profile,cache_root=cache,project_root=tmp_path)
    with pytest.raises(ValueError,match='preceding'):
        compile_v4_txallo_inputs(run_dir=tmp_path/'early',manifest={},workload_plan={**plan,'start_offset':0},
            profile=profile,cache_root=cache,project_root=tmp_path)
    broken=[dict(row) for row in all_rows[23:33]]
    broken[2]['source_row_index']=77
    nh=_gz(cache/mat,broken)
    with pytest.raises(ValueError,match='contiguous'):
        compile_v4_txallo_inputs(run_dir=tmp_path/'tampered',manifest={},workload_plan={**plan,'materialized_sha256':nh},
            profile=profile,cache_root=cache,project_root=tmp_path)


def test_fidelity_never_calls_event_clock_paper_source_block(monkeypatch):
    def noop(x):return dict(x)
    monkeypatch.setattr(fidelity.v16,'_canonical_child',noop)
    child=fidelity._canonical_child({
        'method':{'method_id':'stateless_txallo'},
        'metrics':{'txallo_epoch_clock_source':'mbe_event_order_index',
            'v16_logical_transaction_identity_digest':'data-digest',
            'v17_logical_business_state_projection_status':'available',
            'v17_txallo_paper_audit':{},
        },
    })
    assert 'txallo_mbe_event_order_extension_not_paper_source_block_reproduction' in child['v17_fidelity_blockers']
    assert child['v17_paper_fidelity_candidate'] is False
