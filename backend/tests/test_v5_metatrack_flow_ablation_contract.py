"""Official four-mechanism MetaTrack routing-only ablation contract."""
from pathlib import Path
import re

from backend.app.services.v5_formal_plan_validator import BUILTIN_METHODS

ROOT = Path(__file__).resolve().parents[2]


def read(path):
    return (ROOT / path).read_text(encoding='utf-8')


def test_official_flow_hash_replaces_only_full_routing_choice():
    full = BUILTIN_METHODS['metatrack_latest']
    flow = BUILTIN_METHODS['metatrack_ab_flow']
    assert flow.display_name == '去依赖感知增量分流'
    assert flow.role == 'ablation'
    assert flow.plugin_overrides == full.plugin_overrides
    assert flow.plugin_config_overrides['block_executor'] == full.plugin_config_overrides['block_executor']
    assert set(flow.plugin_config_overrides) == set(full.plugin_config_overrides)
    original = dict(full.plugin_config_overrides['routing'])
    new = dict(flow.plugin_config_overrides['routing'])
    assert original.pop('incremental_exact_continuity_routing_v65') is True
    assert new.pop('incremental_exact_continuity_routing_v65') is False
    assert new.pop('ablation_hash_routing_v662') is True
    assert new == original
    assert full.plugin_overrides['block_producer'] == 'metatrack_nl_window_v669'
    assert full.plugin_overrides['execution'] == 'dual_track_execution'
    assert full.plugin_overrides['state_access'] == 'metatrack_local_exact_access'


def test_official_five_members_and_history_preserved():
    catalog = read('frontend/src/v5FormalExperimentCatalog.ts')
    start = catalog.index('export const METATRACK_ABLATION_METHOD_IDS = [')
    ids = catalog[start:].split('] as const;',1)[0]
    assert re.findall(r'"metatrack_[^"]+"', ids) == [
        '"metatrack_latest"', '"metatrack_ab_flow"', '"metatrack_ab_track"',
        '"metatrack_ab_state"', '"metatrack_ab_cons"'
    ]
    assert '"metatrack_ab_route"' not in ids
    assert '"metatrack_ab_handoff"' not in ids
    formal = read('backend/app/services/v5_formal_plan_validator.py')
    allowed = formal.split('official_metatrack_ids = {',1)[1].split('}',1)[0]
    for name in ('metatrack_latest','metatrack_ab_flow','metatrack_ab_track','metatrack_ab_cons','metatrack_ab_state'):
        assert f'"{name}"' in allowed
    assert '"metatrack_ab_route"' not in allowed
    assert 'metatrack_ab_route' in BUILTIN_METHODS
    assert 'metatrack_sh_noco' in BUILTIN_METHODS


def test_hash_only_route_branch_preserves_signed_exact_version_carrier():
    go = read('executor/v5/metatrack_ablation_v659.go')
    routing = read('executor/v5/registry.go')
    func = go.split('func (p *metaTrackRouting) planHashBaselineAblationV662(',1)[1]
    for token in ('statelessHashRouting', 'state.Capacity', 'IncrementalCapacityForcedChoiceCount',
                  'IncrementalBatchPartitionInvariant', 'IncrementalExactCrossShardStateEdgeCount',
                  'IncrementalHistoryTransactionCountBefore', 'routingPlanDigest'):
        assert token in func
    assert 'if p != nil && boolFromAny(p.config[metaTrackAblationHashRoutingV662])' in routing
    assert 'return p.planHashBaselineAblationV662(input)' in routing
    # The default Full branch has not been replaced or disabled.
    assert 'return p.planIncrementalExactContinuityV650(input)' in routing
    assert 'ablation_ignore_coaccess_routing_v661' not in BUILTIN_METHODS['metatrack_ab_flow'].plugin_config_overrides['routing']


def test_frontend_and_backend_methods_exactly_align():
    front = read('frontend/src/v5MethodProfile.ts')
    back = read('backend/app/services/v5_formal_plan_validator.py')
    assert front.count('method_id: "metatrack_ab_flow"') == 1
    assert back.count('"metatrack_ab_flow": V5FormalMethod(') == 1
    full_front = front.split('method_id: "metatrack_latest"',1)[1].split('method_id: "metatrack_unified"',1)[0]
    flow_front = front.split('method_id: "metatrack_ab_flow"',1)[1].split('method_id: "metatrack_sh_hash"',1)[0]
    for needed in ('routing: "metatrack_coaccess_routing"',
                   'block_producer: "metatrack_nl_window_v669"',
                   'state_access: "metatrack_local_exact_access"',
                   'execution: "dual_track_execution"'):
        assert needed in full_front and needed in flow_front
    assert 'ablation_hash_routing_v662: true' in flow_front
    assert 'ablation_hash_routing_v662: true' not in full_front
