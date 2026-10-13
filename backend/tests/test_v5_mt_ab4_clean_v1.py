"""MetaTrack restores exactly four official ablations; SCC experiment retired.
Does not touch real cluster or use synthetic performance claims.
"""
from pathlib import Path
from backend.app.services.v5_formal_plan_validator import BUILTIN_METHODS
ROOT=Path(__file__).resolve().parents[2]
IDS=['metatrack_latest','metatrack_ab_flow','metatrack_ab_track','metatrack_ab_state','metatrack_ab_cons']

def load(path):return (ROOT/path).read_text(encoding='utf-8-sig')

def test_exact_formal_ablation_set_and_real_role():
    cat=load('frontend/src/v5FormalExperimentCatalog.ts')
    s=cat.split('export const METATRACK_ABLATION_METHOD_IDS = [',1)[1].split('] as const;',1)[0]
    import re
    assert set(re.findall(r'"([^"]+)"',s))==set(IDS)
    assert len(re.findall(r'"([^"]+)"',s))==5
    assert BUILTIN_METHODS['metatrack_sh_noco'].role=='ablation'
    assert BUILTIN_METHODS['metatrack_latest'].role=='main'
    for mid in IDS[1:]:assert BUILTIN_METHODS[mid].role=='ablation'

def test_noco_is_not_state_fetch_ablation_and_only_routing_score_differs():
    full=BUILTIN_METHODS['metatrack_latest']
    noco=BUILTIN_METHODS['metatrack_sh_noco']
    assert noco.plugin_overrides==full.plugin_overrides
    assert noco.plugin_config_overrides['block_executor']==full.plugin_config_overrides['block_executor']
    assert noco.plugin_config_overrides['routing']=={
        **full.plugin_config_overrides['routing'],
        'ablation_ignore_coaccess_routing_v661':True,
    }
    assert BUILTIN_METHODS['metatrack_ab_state'].plugin_overrides['state_access']!=full.plugin_overrides['state_access']
    assert 'ablation_no_coaccess_sharding_v675' not in noco.plugin_config_overrides['routing']

def test_scc_method_retired_and_saved_drafts_not_silently_mapped_to_full():
    assert 'metatrack_sccexp' not in BUILTIN_METHODS
    assert not (ROOT/'executor/v5/metatrack_sccexp_v1_test.go').exists()
    assert not (ROOT/'backend/tests/test_v5_metatrack_sccexp_v1.py').exists()
    ui=load('frontend/src/pages/V5FormalRunPage.tsx')
    assert '"metatrack_sh_hash", "metatrack_sh_coacc", "metatrack_sh_noco"' not in ui
    assert '四项正式消融' in ui
    catalog=load('frontend/src/v5FormalExperimentCatalog.ts')
    assert 'metatrack_sccexp' not in catalog
    reg=load('executor/v5/registry.go')
    assert 'SCCArbitrationExpEnabled' not in reg
    assert 'MBE_METATRACK_CLASSREUSE_V1' in reg
