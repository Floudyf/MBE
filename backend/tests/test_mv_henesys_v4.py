"""Henesys V4 source-only projection contract: no post-execution read/write oracle."""
from pathlib import Path
import gzip
import hashlib
import json

from backend.app.services.v5_workload_data_plane import _validate_canonical_record
from backend.app.services.workload_adapters.henesys_layered_v4 import HenesysLayeredV4Adapter, project
from backend.app.services.workload_adapters.registry import get_adapter

DID='henesys_20250612_v4'

def _raw(index, sender='0x'+'1'*40, calldata='0x'):
    return dict(day='2025-06-12',hash='0x'+format(index+1,'064x'),block_number=100,
                transaction_index=index,timestamp_ms=1749686400000,nonce=index,
                **{'from':sender,'to':'0x'+'2'*40},calldata_prefix=calldata)


def test_nonce_projection_not_fake_contract_state():
    a = project(_raw(0), DID, 0)
    b = project(_raw(1), DID, 1)
    for i, r in enumerate([a,b]):
        _validate_canonical_record(r,dataset_id=DID,row_number=i)
        assert r['state_keys']==['henesys_nonce:'+'0x'+'1'*40]
        assert r['metadata']['full_contract_storage_readwrite_observed'] is False
    assert a['state_keys']==b['state_keys']
    other=project(_raw(2,'0x'+'3'*40),DID,2)
    assert other['state_keys']!=a['state_keys']


def test_known_abi_uses_input_only_and_is_labeled_intent():
    calldata='0xa9059cbb'+('0'*24+'4'*40)+format(1,'064x')
    a=project(_raw(0,calldata=calldata),DID,0)
    _validate_canonical_record(a,dataset_id=DID,row_number=0)
    assert len(a['state_keys'])==3
    assert a['metadata']['modeled_access_scope']=='standard_abi_intent'
    assert not any(key.startswith('henesys_contract:') for key in a['state_keys'])
    # Do not invent exact NFT ownership from ambiguous ERC20/721 signature.
    ambiguous=project(_raw(1,calldata='0x23b872dd'+'0'*192),DID,1)
    assert len(ambiguous['state_keys'])==1


def test_registry_and_full_prefix(tmp_path):
    assert get_adapter('mv_henesys_layered_v4').adapter_id=='mv_henesys_layered_v4'
    source=tmp_path/'source.jsonl.gz'
    with gzip.open(source,'wt',encoding='utf8') as f:
        f.write(json.dumps(_raw(0))+'\n')
        f.write(json.dumps(_raw(1))+'\n')
    manifest={'dataset_id':DID,'source_day':'2025-06-12','row_count':2}
    adapter=HenesysLayeredV4Adapter()
    rows=list(adapter.iter_canonical_records(source,manifest))
    assert len(rows)==2
    assert adapter.validate_source(source,manifest).row_count==2
    assert len({r['source_tx_hash'] for r in rows})==2
