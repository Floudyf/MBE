"""MBE_MT_BLOCKLANE_V1: new cards must not mutate official Full/ablations."""
from pathlib import Path
from backend.app.services.v5_formal_plan_validator import ALL_BUILTIN_METHODS, BUILTIN_METHODS, MT_BLOCKLANE_V1_METHODS


def test_blocklane_profiles_equal_full_except_two_candidate_flags():
    full=BUILTIN_METHODS['metatrack_latest']
    for id,no_bypass in [('metatrack_blocklane',False),('metatrack_blockfifo',True)]:
        method=ALL_BUILTIN_METHODS[id]
        assert method.role=='custom'
        assert method.plugin_overrides==full.plugin_overrides
        expected={k:dict(v) for k,v in full.plugin_config_overrides.items()}
        expected['block_executor']={**expected.get('block_executor',{}),'blocking_lane_candidate_v1':True,'blocking_lane_no_bypass_v1':no_bypass}
        assert method.plugin_config_overrides==expected
    assert all(v.method_id not in MT_BLOCKLANE_V1_METHODS for v in [])


def test_blocklane_ui_cards_and_protected_official_ablation_list():
    front=Path('frontend/src/v5MethodProfile.ts').read_text('utf8')
    catalog=Path('frontend/src/v5FormalExperimentCatalog.ts').read_text('utf8')
    assert 'method_id: id,' in front and 'blocking_lane_no_bypass_v1: noBypass' in front
    for id in ('metatrack_blocklane','metatrack_blockfifo'):
        assert catalog.count('methodId: "'+id+'"')==1
    official=catalog.split('METATRACK_ABLATION_METHOD_IDS = [',1)[1].split('] as const;',1)[0]
    assert 'metatrack_blocklane' not in official and 'metatrack_blockfifo' not in official
    assert 'methodId: "metatrack_ab_track"' in catalog


def test_blocklane_runtime_only_uses_candidate_config():
    go=Path('executor/v5/registry.go').read_text('utf8')
    assert 'blockLaneNoBypassV1 := blockLaneCandidateV1 && boolFromAny(p.config["blocking_lane_no_bypass_v1"])' in go
    assert 'metaTrackBlockingOrderV1(ordered, gateDepsV1)' in go
    assert 'blockLaneGateReadyV1[txID] = true' in go
    assert 'metatrack_blocklane_gate_delayed_ready_count' in go
    assert 'singleConservativeSerialV675 || diagnosticSingleBusinessV28' in go
