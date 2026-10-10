from __future__ import annotations
import hashlib
import io
import json
import os
import zipfile
from decimal import Decimal
from pathlib import Path

import pytest
from backend.app.services.mv_txallo_input_v1 import _chain_block
from backend.app.services import v5_optme_txallo_paperfaithful_v17 as fidelity
from backend.app.services import v5_reproducibility_bundle as bundle

@pytest.mark.parametrize('raw,expected', [
    ('7.158073e+06',7158073), ('7158073',7158073), ('7.158073E+6',7158073),
    ('7158073.000',7158073), ('7.158073e+06',7158073),
])
def test_axie_scientific_source_block_v2(raw,expected):
    assert _chain_block({'metadata':{'block_number_raw':raw}})==expected

@pytest.mark.parametrize('raw', ['7.1580731e+06','nan','Infinity','0','-1','1.5','','1e1000','garbage'])
def test_axie_invalid_source_block_v2(raw):
    with pytest.raises(ValueError): _chain_block({'metadata':{'block_number_raw':raw}})

def test_fidelity_peer_failure_does_not_invalidate_local_optme_v2(monkeypatch):
    def canonical(item):return dict(item)
    monkeypatch.setattr(fidelity,'_canonical_child',canonical)
    completed={ 'v17_method_id':'stateful_optme','status':'completed','paper_candidate':True,
        'v16_logical_transaction_identity_digest':'equal','v17_semantic_class':'stateful_local_partition',
        'v17_fidelity_blockers':[] }
    failed={'v17_method_id':'stateless_txallo','status':'failed','paper_candidate':False,
        'v16_logical_transaction_identity_digest':None,'v17_semantic_class':'stateless_global_home',
        'v17_fidelity_blockers':['not_run']}
    children,report=fidelity.apply_group_fidelity_gate([completed,failed],{'performance_comparison_valid':True})
    assert children[0]['paper_candidate'] is True
    assert children[0]['v17_cross_method_comparison_blocked'] is True
    assert children[1]['paper_candidate'] is False
    assert report['performance_comparison_valid'] is False
    assert 'logical_transaction_identity_not_proven_equal' in report['v17_group_fidelity_blockers']

def test_fidelity_equal_complete_pair_still_compares_v2(monkeypatch):
    monkeypatch.setattr(fidelity,'_canonical_child',lambda item:dict(item))
    pair=[{'v17_method_id':name,'status':'completed','paper_candidate':True,
           'v16_logical_transaction_identity_digest':'same','v17_semantic_class':'stateful_local_partition',
           'v17_fidelity_blockers':[]} for name in ['stateful_optme','stateful_txallo']]
    children,report=fidelity.apply_group_fidelity_gate(pair,{'performance_comparison_valid':True})
    assert report['pairwise_logical_transaction_identity_equivalent'] is True
    assert not report['v17_group_fidelity_blockers']
    assert all(child['paper_candidate'] is True for child in children)

def test_bundle_compact_and_full_switch_v2(monkeypatch,tmp_path:Path):
    class FakeStore:
        @staticmethod
        def stream_archived_artifact(root,name):
            yield b'evidence data'
    cold_root=tmp_path/'cold_run'
    entries=[dict(runtime_root=cold_root,artifact_name='nodes/n0/observed_state_access.csv',
                  archive_name='stateful_oracle_evidence/child/nodes/n0/observed_state_access.csv',
                  source='stateful_oracle_cold_archive_evidence',size_bytes=13,
                  sha256=hashlib.sha256(b'evidence data').hexdigest())]
    monkeypatch.setattr(bundle,'_stateful_oracle_archived_evidence',lambda groupdir,group:entries)
    monkeypatch.setattr(bundle,'_failed_runtime_diagnostics',lambda groupdir,group:[])
    monkeypatch.setattr(bundle.v5_artifact_storage,'stream_archived_artifact',FakeStore.stream_archived_artifact)
    group={'run_group_id':'g','execution_backend':'real_cluster','plan':{}}
    for full in [False,True]:
        monkeypatch.setenv('MBE_ARTIFACTS_FULL_ORACLE_EVIDENCE','1' if full else '0')
        folder=tmp_path/('full' if full else 'small');folder.mkdir()
        (folder/'run_group.json').write_text('{}')
        target=bundle.build(folder,group)
        with zipfile.ZipFile(target) as z:
            assert z.testzip() is None
            index=json.loads(z.read('cold_oracle_evidence_index.json'))
            assert len(index['files'])==1 and index['files'][0]['sha256']==entries[0]['sha256']
            assert (entries[0]['archive_name'] in z.namelist()) is full
            manifest=json.loads(z.read('artifact_manifest.json'))
            assert all(e['name'] in z.namelist() for e in manifest['files'])
            for item in manifest['files']:
                assert hashlib.sha256(z.read(item['name'])).hexdigest()==item['sha256']

def test_preview_guard_v2():
    from backend.app.services import v5_compatibility_engine as c
    code=Path(c.__file__).read_text(encoding='utf-8')
    assert 'audited raw data contains no observed chain' in code
    assert 'if txallo_selected:' in code
