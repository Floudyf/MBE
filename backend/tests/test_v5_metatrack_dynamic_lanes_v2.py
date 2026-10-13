"""Fail-closed integration contract for the replaced MetaTrack candidate."""
from pathlib import Path
from backend.app.services.v5_formal_plan_validator import BUILTIN_METHODS
from backend.app.services.v5_plugin_manifest_store import STORE
from backend.app.services import v5_compatibility_engine


def test_dynamic_candidate_does_not_mutate_full_or_paper_ablation():
    full = BUILTIN_METHODS['metatrack_latest']
    new = BUILTIN_METHODS['metatrack_dyntrack']
    assert new.role == 'custom'
    assert new.plugin_overrides['scheduler'] == 'metatrack_unlock_scheduler'
    expected = dict(full.plugin_overrides)
    expected['scheduler'] = 'metatrack_unlock_scheduler'
    assert expected == new.plugin_overrides
    assert new.plugin_config_overrides == full.plugin_config_overrides
    catalog = Path('frontend/src/v5FormalExperimentCatalog.ts').read_text('utf-8')
    formal = catalog.split('METATRACK_ABLATION_METHOD_IDS = [')[1].split('] as const;')[0]
    assert 'metatrack_dyntrack' not in formal
    assert catalog.count('methodId: "metatrack_dyntrack"') == 1


def test_runtime_has_two_distinct_lane_and_semantic_safety_planes():
    source = Path('executor/v5/registry.go').read_text('utf-8')
    assert 'dynamicRiskByTx[txID] = metaTrackPotentialRemotePublishRisk(item)' in source
    assert 'metaTrackPopDynamicRiskReady' in source
    assert 'dynamic_resource_risk_ready' in source
    assert 'dynamic_normal_ready' in source
    assert 'riskInFlightByTx[txID] = true' in source
    assert 'riskInFlight--' in source
    assert 'decisionByID[txID] = decisionForTx(item, classification.Decisions, nil)' in source
    assert 'metatrack_dynamic_normal_protection_count' in source


def test_compatibility_allows_only_correctly_paired_scheduler():
    source = Path(v5_compatibility_engine.__file__).read_text('utf-8')
    assert source.count('"metatrack_unlock_scheduler"') >= 2
    assert '"metatrack_single_execution"' in source
    manifest = STORE.get('metatrack_unlock_scheduler')
    assert manifest.category == 'scheduler'
    assert 'execution:dual_track_execution' in manifest.requirements
    assert 'logical_domain_frontier_v1 requires' in source
