"""Isolated MetaTrack candidate. Never enters four official ablations."""
from pathlib import Path

from backend.app.services.v5_formal_plan_validator import BUILTIN_METHODS


def test_dyntrack_is_separate_visible_candidate_with_only_scheduler_delta():
    full = BUILTIN_METHODS['metatrack_latest']
    cand = BUILTIN_METHODS['metatrack_dyntrack']
    assert cand.role == 'custom'
    assert cand.plugin_overrides['scheduler'] == 'metatrack_unlock_scheduler'
    expected = dict(full.plugin_overrides)
    expected['scheduler'] = 'metatrack_unlock_scheduler'
    assert cand.plugin_overrides == expected
    assert cand.plugin_config_overrides == full.plugin_config_overrides
    front = Path('frontend/src/v5MethodProfile.ts').read_text(encoding='utf-8')
    catalog = Path('frontend/src/v5FormalExperimentCatalog.ts').read_text(encoding='utf-8')
    assert 'method_id: "metatrack_dyntrack"' in front
    assert 'methodId: "metatrack_dyntrack"' in catalog
    assert 'comparisonVisible: true, mainVisible: true' in catalog
    official = catalog.split('export const METATRACK_ABLATION_METHOD_IDS = [',1)[1].split('] as const;',1)[0]
    assert 'metatrack_dyntrack' not in official


def test_runtime_candidate_is_only_enabled_with_explicit_scheduler():
    source = Path('executor/v5/registry.go').read_text(encoding='utf-8')
    assert 'dynamicUnlockPriority := schedule.ReadyPriorityPolicy == metaTrackUnlockReadyPriorityPolicy' in source
    assert 'unlockApplied = dynamicUnlockPriority && candidates > capacity && capacity > 0' in source
    assert 'metaTrackPopDynamicRiskReady' in source
    assert 'metatrack_unlock_changed_choice_count' in source
    assert 'metatrack_unlock_selected_opportunity_count' in source
    assert 'stateBlocked[txID] = true' in source
    assert 'metatrack_single_fifo_hol_enabled' in source
    assert 'metaTrackUnlockReadySchedulerID' in source


def test_candidate_is_registered_as_a_real_scheduler_plugin():
    # Registration must be in both Python manifest and Go runtime registry.
    import inspect
    from backend.app.services import v5_plugin_manifest_store
    source = inspect.getsource(v5_plugin_manifest_store)
    assert '_manifest("scheduler", "metatrack_unlock_scheduler"' in source
    go = Path('executor/v5/registry.go').read_text(encoding='utf-8')
    assert 'register("scheduler", metaTrackUnlockReadySchedulerID' in go
