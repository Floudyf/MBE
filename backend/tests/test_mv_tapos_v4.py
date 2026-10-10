"""Tapos V4 contract: exact observed write keys and exact indexed windows."""
import csv
import hashlib
import json
from pathlib import Path

import pytest

from backend.app.services.workload_adapters.tapos_layered_v4 import TaposLayeredV4Adapter
from backend.app.services import v5_workload_data_plane as w


def make_source(tmp_path: Path, count: int = 100):
    path = tmp_path / 'tapos.csv'
    fields = ['schema_version','transaction_id','timestamp','sender_id','receiver_id','operation_type','state_keys','entry_function_id','write_key_count','provenance']
    with path.open('w',encoding='utf-8',newline='') as file:
        wr=csv.DictWriter(file,fieldnames=fields)
        wr.writeheader()
        for i in range(count):
            keys=['tapos:key:'+str(i%7), 'tapos:resource:'+str((i+2)%9)]
            wr.writerow(dict(schema_version='mbe_workload_record_v1',transaction_id=str(820410290+i),
                             timestamp='2024-05-25 15:09:00.188',sender_id='0x'+format(i%11,'064x'),
                             receiver_id='0x'+format(i%3,'064x'),operation_type='tapos_play',
                             state_keys=json.dumps(keys),entry_function_id='0x1::tapos::play',
                             write_key_count=2,provenance='aptos_public_source'))
    obj={'dataset_id':'tapos_layered_v4','adapter_id':'mv_tapos_layered_v4', 'row_count':count,
         'source_sha256':hashlib.sha256(path.read_bytes()).hexdigest(),'source_size_bytes':path.stat().st_size,
         'preview_policy':'mv_verified_window_index_v1'}
    return path,obj


def test_tapos_v4_exact_declarations_and_provenance(tmp_path):
    path,manifest=make_source(tmp_path)
    a=TaposLayeredV4Adapter()
    summary=a.validate_source(path,manifest,expected_sha256=manifest['source_sha256'])
    assert summary.row_count==100
    records=list(a.iter_canonical_records(path,manifest))
    assert [r['source_row_index'] for r in records]==list(range(100))
    assert len({r['source_event_id'] for r in records})==100
    for r in records:
        w._validate_canonical_record(r,dataset_id=manifest['dataset_id'],row_number=r['source_row_index'])
        assert r['schema_version']=='mbe_workload_record_v4'
        assert r['source_tx_hash'] is None
        assert r['provenance']['source_read_keys_observed'] is False
        assert r['metadata']['unobserved_read_set']=='not_available_not_inferred'
        assert all(x['mode']=='write' and x['update_semantics']=='tapos_observed_exact_write' for x in r['access_list'])
        assert r['access_list']==r['scheduling_access_list']
        assert r['static_access_envelope']['keys']==r['state_keys']
        assert r['routing_source_key'] in r['state_keys']
        assert r['routing_target_key'] in r['state_keys']


def test_tapos_window_index_equals_full_source_slice(tmp_path):
    path,manifest=make_source(tmp_path)
    a=TaposLayeredV4Adapter()
    expected=list(a.iter_canonical_records(path,manifest))
    offsets=[]
    with path.open('rb') as file:
        file.readline()
        for index in range(100):
            offset=file.tell()
            if index%9==0:offsets.append([index,offset])
            assert file.readline()
    for start,stop in ((0,7),(2,5),(13,31),(76,100)):
        actual=list(a.iter_canonical_window(path,manifest,start=start,stop=stop,sparse_offsets=offsets))
        assert actual==expected[start:stop]


def test_tapos_refuses_fabricated_or_duplicate_write_list(tmp_path):
    path,manifest=make_source(tmp_path)
    text=path.read_text(encoding='utf-8')
    broken=text.replace('""write_key_count""', '""write_key_count""')
    # Deterministic row-field mutation rather than relying on CSV quoting.
    lines=text.splitlines()
    fields=lines[0].split(',')
    import io
    rows=list(csv.DictReader(io.StringIO(text)))
    rows[0]['write_key_count']='3'
    with path.open('w',encoding='utf-8',newline='') as file:
        wr=csv.DictWriter(file,fieldnames=fields);wr.writeheader();wr.writerows(rows)
    with pytest.raises(ValueError,match='write_key_count'):
        list(TaposLayeredV4Adapter().iter_canonical_records(path,manifest))


def test_tapos_source_hash_mismatch_is_fail_closed(tmp_path):
    path,manifest=make_source(tmp_path)
    with pytest.raises(ValueError,match='SHA256'):
        TaposLayeredV4Adapter().validate_source(path,manifest,expected_sha256='0'*64)

@pytest.mark.parametrize('variant_mode,alpha,axis', [
    ('original_window',None,None),
    ('key_zipf',0.8,'primary_write_key'),
    ('key_zipf',1.2,'entry_function'),
])
def test_tapos_fast_preview_same_as_complete_algorithm(tmp_path, variant_mode, alpha, axis):
    from backend.app.services.mv_window_preview_v1 import fast_preview, preview_index_path
    path, manifest=make_source(tmp_path)
    adapter=TaposLayeredV4Adapter()
    offsets=[]
    with path.open('rb') as h:
        h.readline()
        for index in range(100):
            offsets.append([index,h.tell()]);assert h.readline()
    cache=tmp_path/'cache'
    idx=preview_index_path(cache,manifest['dataset_id']);idx.parent.mkdir(parents=True)
    stat=path.stat()
    idx.write_text(json.dumps({
        'policy': 'mv_verified_window_index_v1', 'dataset_id':manifest['dataset_id'],
        'adapter_id':manifest['adapter_id'],'source_sha256':manifest['source_sha256'],
        'source_size_bytes':stat.st_size,'source_mtime_ns':stat.st_mtime_ns,
        'row_count':100,'canonical_sha256':w._canonical_sha256_from_source(path,manifest),
        'csv_offsets':offsets,
    }),encoding='utf-8')
    args=dict(requested_tx_count=25,seed=11,variant_mode=variant_mode,
              target_alpha=alpha,skew_axis=axis,shards=2,
              selection_mode='contiguous_window',supported_counts={25},
              variant_parameters={'target_alpha':alpha,'skew_axis':axis} if alpha is not None else {})
    rapid=fast_preview(csv_path=path,manifest=manifest,cache_root=cache,**args)
    slow=w._selection_preview_from_source(path,
        {k:v for k,v in manifest.items() if k!='preview_policy'},**args)
    for key in ('start_offset','end_offset','selection_digest','base_window_sha256',
                'cross_shard_count','operation_counts','shard_distribution','realized_skew'):
        assert rapid[key]==slow[key], (key,variant_mode)
