"""Regression: event-order clock is not an observed chain block height."""
from __future__ import annotations
import csv
import gzip
import json
from pathlib import Path

from backend.app.services.v5_txallo_terminal_v14 import build


def write_json(path: Path, payload):
    path.parent.mkdir(parents=True,exist_ok=True)
    path.write_text(json.dumps(payload,ensure_ascii=False)+'\n',encoding='utf-8')

def write_jsonl(path: Path, rows, compressed=False):
    path.parent.mkdir(parents=True,exist_ok=True)
    text=''.join(json.dumps(row)+'\n' for row in rows)
    if compressed:
        with gzip.open(path,'wt',encoding='utf-8') as output:output.write(text)
    else:
        path.write_text(text,encoding='utf-8')

def event_fixture(root: Path):
    write_json(root/'compiled_run_plan.json',{'node_configs':[{'node_id':'n0','plugin_profile':{'sharding':{'config':{'a_epoch_blocks':15}}}}]})
    write_json(root/'workload/txallo_dynamic_blocks_summary.json',{
        'epoch_clock_source':'mbe_event_order_index',
        'first_block_num':None,'last_block_num':None,
        'first_epoch_coordinate':101,'last_epoch_coordinate':116,
        'a_epoch_unit':'source_events_not_chain_blocks',
    })
    rows=[]
    for idx,coord in enumerate([101,102,116]):
        rows.append({'schema_version':'mbe_txallo_event_clock_v1','materialized_index':idx,
                     'raw_source_row_index':coord-1,'block_num':0,
                     'epoch_coordinate':coord,'global_sequence':coord})
    write_jsonl(root/'workload/txallo_dynamic_blocks.jsonl.gz',rows,True)
    target=root/'client/txallo_transaction_placement.csv'
    target.parent.mkdir(parents=True,exist_ok=True)
    with target.open('w',newline='',encoding='utf-8') as f:
        writer=csv.DictWriter(f,fieldnames=['tx_index','logical_id','txallo_cross_shard'])
        writer.writeheader()
        for i in range(3):writer.writerow({'tx_index':i,'logical_id':f'l{i}','txallo_cross_shard':'false'})
    write_jsonl(root/'client/txallo_epoch_timing.jsonl',[
        {'source_epoch':0,'commit_wait_ns':1000000,'total_barrier_ns':3000000,'status':'passed'}
    ])
    write_jsonl(root/'client/transaction_lifecycle.jsonl',[
        {'logical_tx_id':'l0','node_id':'mbe-client','stage':'submitted','timestamp_ms':100,'success':True}
    ])
    write_jsonl(root/'nodes/n0/transaction_lifecycle.jsonl',[
        {'logical_tx_id':'l0','node_id':'n0','stage':'received','timestamp_ms':1000,'success':True},
        {'logical_tx_id':'l0','node_id':'n0','stage':'admitted','timestamp_ms':1001,'success':True},
    ])


def test_event_clock_no_invented_chain_blocks(tmp_path):
    event_fixture(tmp_path)
    result=build(tmp_path)
    assert result['status']=='passed_with_scope_limitations',result.get('blockers')
    assert result['epoch_clock_source']=='mbe_event_order_index'
    assert result['epoch_width_source_blocks'] is None
    assert result['epoch_width_source_events']==15
    assert result['total_logical_transaction_count']==3
    assert [row['source_epoch'] for row in result['epochs']]==[0,1]
    assert result['epochs'][0]['logical_transaction_count']==2
    assert result['epochs'][1]['logical_transaction_count']==1


def test_event_clock_rejects_fabricated_block_height(tmp_path):
    event_fixture(tmp_path)
    path=tmp_path/'workload/txallo_dynamic_blocks_summary.json'
    data=json.loads(path.read_text(encoding='utf-8'))
    data['first_block_num']=101
    write_json(path,data)
    result=build(tmp_path)
    assert result['status']=='failed'
    assert 'invent' in result['blockers'][0]


def test_event_clock_rejects_wrong_coordinate(tmp_path):
    event_fixture(tmp_path)
    path=tmp_path/'workload/txallo_dynamic_blocks.jsonl.gz'
    rows=[]
    with gzip.open(path,'rt',encoding='utf-8') as f:
        rows=[json.loads(s) for s in f if s.strip()]
    rows[1]['epoch_coordinate']=999
    write_jsonl(path,rows,True)
    result=build(tmp_path)
    assert result['status']=='failed'
    assert 'provenance' in result['blockers'][0]
