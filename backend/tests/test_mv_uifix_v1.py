"""Regression: exact windows, cache invalidation, unchanged original relations."""
from __future__ import annotations
import csv
import hashlib
import json
from pathlib import Path

import pytest
from backend.app.services.v5_workload_data_plane import (
    _selection_preview_from_source, _canonical_sha256_from_source,
)
from backend.app.services.mv_window_preview_v1 import fast_preview, POLICY, preview_index_path

A='0x'+'1'*40
B='0x'+'2'*40
C='0x'+'3'*40
HASH='0x'+'a'*64
DCL_HEADER=['id','tx_hash','buyer','seller','price','timestamp','category','raw_contract_candidates']


def sample_dcl(tmp_path: Path):
    source=tmp_path/'dcl.csv'
    with source.open('w',encoding='utf-8',newline='') as f:
        w=csv.writer(f);w.writerow(DCL_HEADER)
        for i in range(12):
            w.writerow([f'sale-{i}', HASH if i<3 else '0x'+f'{i:064x}',
                        A if i%2 else B,B if i%2 else A,str(i+1),
                        str(1633046400000+i*1000),'wearable' if i%2 else 'emote',C])
    manifest={'dataset_id':'dcl_sales_layered_v4','adapter_id':'mv_dcl_layered_v4',
        'source_sha256':hashlib.sha256(source.read_bytes()).hexdigest(),
        'source_size_bytes':source.stat().st_size,'row_count':12,
        'preview_policy':POLICY}
    return source,manifest


def index_data(path: Path, manifest: dict):
    offsets=[]
    with path.open('rb') as h:
        h.readline()
        for idx in range(12):
            offsets.append([idx,h.tell()]);assert h.readline()
    return {'policy':POLICY,'dataset_id':manifest['dataset_id'],
        'adapter_id':manifest['adapter_id'], 'source_sha256':manifest['source_sha256'],
        'source_size_bytes':path.stat().st_size,'source_mtime_ns':path.stat().st_mtime_ns,
        'row_count':12,'canonical_sha256':_canonical_sha256_from_source(path,manifest),
        'csv_offsets':offsets}


def call_preview(path: Path,manifest: dict,cache: Path,seed: int):
    return fast_preview(csv_path=path,manifest=manifest,cache_root=cache,
        requested_tx_count=4,seed=seed,variant_mode='original_window',target_alpha=None,
        skew_axis=None,shards=2,selection_mode='contiguous_window',
        supported_counts={4},variant_parameters={})


def test_precisely_identical_preview_to_full_canonical_selection(tmp_path: Path):
    path,manifest=sample_dcl(tmp_path)
    cache=tmp_path/'cache'
    idx=preview_index_path(cache,manifest['dataset_id']);idx.parent.mkdir(parents=True)
    idx.write_text(json.dumps(index_data(path,manifest)),encoding='utf-8')
    for seed in (11,12,50011):
        rapid=call_preview(path,manifest,cache,seed)
        slow=_selection_preview_from_source(path,{key:value for key,value in manifest.items() if key!='preview_policy'},
          requested_tx_count=4,seed=seed,variant_mode='original_window',target_alpha=None,
          skew_axis=None,shards=2,selection_mode='contiguous_window',
          supported_counts={4},variant_parameters={})
        for key in ('start_offset','end_offset','selection_digest','base_window_sha256',
                    'cross_shard_count','operation_counts','shard_distribution','realized_skew'):
            assert rapid[key]==slow[key], (key,seed)
        assert rapid['preview_window_truth']=='exact_same_window_as_materializer'


def test_sparse_index_emits_same_exact_original_records(tmp_path: Path):
    from backend.app.services.workload_adapters.mv_layered_v4 import DCLLayeredV4Adapter
    path,manifest=sample_dcl(tmp_path)
    records=list(DCLLayeredV4Adapter().iter_canonical_records(path,manifest))
    subset=list(DCLLayeredV4Adapter().iter_canonical_window(path,manifest,
               start=7,stop=10,sparse_offsets=index_data(path,manifest)['csv_offsets']))
    assert subset==records[7:10]
    assert [row['source_event_id'] for row in subset]==['sale-7','sale-8','sale-9']


def test_preview_index_must_match_source_stat(tmp_path: Path):
    from backend.app.services.v5_workload_data_plane import WorkloadDataError
    path,manifest=sample_dcl(tmp_path);cache=tmp_path/'cache'
    idx=preview_index_path(cache,manifest['dataset_id']);idx.parent.mkdir(parents=True)
    data=index_data(path,manifest);data['source_mtime_ns']+=1
    idx.write_text(json.dumps(data),encoding='utf-8')
    with pytest.raises(WorkloadDataError,match='identity changed'):
        call_preview(path,manifest,cache,11)


def test_formal_picker_disables_superseded_datasets_but_preserves_raw_catalog():
    root=Path(__file__).resolve().parents[2]
    editor=(root/'frontend/src/components/v5/WorkloadSourceEditor.tsx').read_text(encoding='utf-8')
    page=(root/'frontend/src/pages/V5FormalRunPage.tsx').read_text(encoding='utf-8')
    for required in ('axie_day_layered_v4','dcl_sales_layered_v4','alien_worlds_layered_v2_controlled',
                     'alien_worlds_layered_v2_historical','axie_infinity_controlled_prefix_rmw_v1',
                     # MBE_MV_TAPOS_V4_V1 picker regression updated
                     'tapos_layered_v4'):
        assert f'"{required}"' in editor
    assert 'visibleDatasets.map' in editor
    assert 'publishedWorkloadDatasets(datasetResponse, current.mode)' in page
    assert 'nextDataset?.variant_definitions?.[0]' not in editor
