"""MV V4 adapters: same source data for every algorithm; never consume oracle truth."""
from __future__ import annotations

import csv
import json
from pathlib import Path

import pytest

from backend.app.services.v5_workload_data_plane import _validate_canonical_record
from backend.app.services.workload_adapters.registry import get_adapter
from backend.app.services.workload_adapters.mv_layered_v4 import (
    AxieLayeredV4Adapter, DCLLayeredV4Adapter,
)


def _fixture(tmp_path: Path, name: str, columns: list[str], rows: list[list[str]], *, bom: bool = False) -> Path:
    path = tmp_path / name
    with path.open('w', encoding='utf-8-sig' if bom else 'utf-8', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(columns)
        writer.writerows(rows)
    return path


AXIE = list(sorted(AxieLayeredV4Adapter.required_columns))
DCL = ['id','tx_hash','buyer','seller','price','timestamp','category','raw_contract_candidates']
ZERO='0x'+'0'*40
A='0x'+'1'*40
B='0x'+'2'*40
C='0x'+'3'*40
TX='0x'+'a'*64


def test_axie_v4_bom_mint_self_and_transfer(tmp_path: Path):
    rows = []
    for number, src, dst, kind in ((1,ZERO,A,'mint'),(2,A,A,'transfer'),(3,A,B,'transfer')):
        data = {name: '' for name in AXIE}
        data.update(tx_seq=str(number),block_time='2021-10-01 00:00:00.000 UTC',block_number='1',tx_index='0',
                    tx_hash=TX,tx_from=src,tx_to=C,axie_from=src,axie_to=dst,
                    axie_token_id='100',axie_transfer_count='1',event_kind=kind,
                    accesses_marketplace_hotspot='true',operation_class='marketplace')
        rows.append([data[k] for k in AXIE])
    path=_fixture(tmp_path,'axie.csv',AXIE,rows,bom=True)
    obj=AxieLayeredV4Adapter()
    records=list(obj.iter_canonical_records(path,{'dataset_id':'fixture_axie'}))
    assert [r['operation_type'] for r in records]==['axie_mint','axie_self_transfer','axie_transfer']
    assert all(r['schema_version']=='mbe_workload_record_v4' for r in records)
    assert 'axie_account:'+ZERO not in records[0]['state_keys']
    assert len(records[1]['state_keys'])==2
    assert all('axie_marketplace:global' not in r['state_keys'] for r in records)
    assert all(r['source_tx_hash']==TX for r in records)
    assert all(r['metadata']['source_tx_seq']==str(i+1) for i,r in enumerate(records))
    for i,r in enumerate(records):
        _validate_canonical_record(r,dataset_id='fixture_axie',row_number=i)


def test_dcl_v4_preserves_multiple_sale_events_same_tx_hash_and_self_trade(tmp_path: Path):
    path=_fixture(tmp_path,'dcl.csv',DCL,[
        ['sale-1',TX,A,B,'12.2','1633046400000','wearable',C],
        ['sale-2',TX,A,A,'3','1633046400000','emote',C],
        ['sale-3',TX,B,A,'4','1633046400000','wearable',C],
    ])
    rec=list(DCLLayeredV4Adapter().iter_canonical_records(path,{'dataset_id':'fixture_dcl'}))
    assert len(rec)==3 and len({r['source_event_id'] for r in rec})==3
    assert len({r['source_tx_hash'] for r in rec})==1
    assert rec[0]['state_keys'] == sorted(rec[0]['state_keys'])
    assert rec[1]['metadata']['buyer_equals_seller'] is True
    assert 'dcl_balance:'+A in rec[0]['state_keys'] and 'dcl_balance:'+A in rec[2]['state_keys']
    assert not any('nft_token:' in key for row in rec for key in row['state_keys'])
    assert rec[0]['metadata']['price_raw']=='12.2'
    for i,r in enumerate(rec):
        _validate_canonical_record(r,dataset_id='fixture_dcl',row_number=i)


def test_v4_registry_manifests_present_and_immutable_sources():
    root=Path(__file__).resolve().parents[2]
    for dataset_id, expected_adapter, expected_type in (
        ('axie_day_layered_v4','mv_axie_layered_v4',AxieLayeredV4Adapter),
        ('dcl_sales_layered_v4','mv_dcl_layered_v4',DCLLayeredV4Adapter),
    ):
        manifest=json.loads((root/'data/workloads/manifests'/f'{dataset_id}.json').read_text(encoding='utf-8'))
        assert manifest['adapter_id']==expected_adapter
        assert manifest['truth_label']=='real_replay_projected'
        assert manifest['source_layout']=='single_file'
        assert manifest['variant_definitions'][0]['selection_mode']=='contiguous_window'
        assert isinstance(get_adapter(expected_adapter), expected_type)


def test_other_algorithm_core_and_old_dataset_adapters_untouched():
    from backend.app.services.workload_adapters.registry import get_adapter
    assert get_adapter('axie_full_day_v1').adapter_id=='axie_full_day_v1'
    assert get_adapter('decentraland_sales_v1').adapter_id=='decentraland_sales_v1'


def test_bad_csv_or_missing_roles_fail_closed(tmp_path: Path):
    path=_fixture(tmp_path,'bad.csv',DCL,[['sale-1',TX,'not_address',B,'1','1633046400000','wearable',C]])
    with pytest.raises(ValueError):
        list(DCLLayeredV4Adapter().iter_canonical_records(path,{'dataset_id':'fixture_dcl'}))
