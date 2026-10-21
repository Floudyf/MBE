"""Candidate-only H/D + state fetch priority; Full and old candidate frozen."""
from pathlib import Path
from backend.app.services.v5_formal_plan_validator import ALL_BUILTIN_METHODS, BUILTIN_METHODS
from backend.app.services.v5_plugin_manifest_store import STORE


def test_dualprio_clones_full_only_scheduler_delta():
    full = ALL_BUILTIN_METHODS["metatrack_latest"]
    cand = ALL_BUILTIN_METHODS["metatrack_dualprio"]
    assert cand.role == "custom"
    expected = dict(full.plugin_overrides)
    expected["scheduler"] = "metatrack_hd_state_scheduler"
    assert cand.plugin_overrides == expected
    assert cand.plugin_config_overrides == full.plugin_config_overrides
    assert BUILTIN_METHODS["metatrack_dyntrack"].plugin_overrides["scheduler"] == "metatrack_unlock_scheduler"
    assert STORE.get("metatrack_hd_state_scheduler").plugin_id == "metatrack_hd_state_scheduler"


def test_new_visible_card_old_candidate_is_historical():
    catalog = Path("frontend/src/v5FormalExperimentCatalog.ts").read_text(encoding="utf8")
    old = next(line for line in catalog.splitlines() if 'methodId: "metatrack_dyntrack"' in line)
    new = next(line for line in catalog.splitlines() if 'methodId: "metatrack_dualprio"' in line)
    assert "comparisonVisible: false, mainVisible: false" in old
    assert "comparisonVisible: true, mainVisible: true" in new
    source = Path("executor/v5/registry.go").read_text(encoding="utf8")
    assert "criticalStateFetchPriority := schedule.ReadyPriorityPolicy == metaTrackHDStatePriorityPolicy" in source
    assert 'metatrack_candidate_critical_state_fetch_enabled' in source
    assert "register(\"scheduler\", metaTrackHDStateSchedulerID" in source
    assert "MBE_MT_DUALPRIO_V1" in source
