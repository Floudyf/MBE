from backend.app.services.v5_formal_plan_validator import BUILTIN_METHODS


def test_metatrack_middle_profile_is_frozen_verified_v5_0_1_3_current_version():
    initial = BUILTIN_METHODS["metatrack_serial"]
    current = BUILTIN_METHODS["metatrack_full_locality"]
    latest = BUILTIN_METHODS["metatrack_latest"]
    assert initial.plugin_overrides == current.plugin_overrides == latest.plugin_overrides
    assert current.display_name == "MetaTrack（当前版）"
    assert latest.display_name == "Metatrack"
    cfg = current.plugin_config_overrides["block_executor"]
    assert cfg["local_exact_version_handoff"] is True
    assert cfg["batch_entry_state_prefetch"] is True
    assert cfg["batch_remote_writeback"] is True
    assert cfg["safe_state_fold"] is True
    assert cfg["version_liveness"] is True
    assert cfg["final_version_batch_writeback"] is True
    assert cfg.get("dependency_closed_consensus") is None
    assert cfg.get("version_liveness_indexed") is None
    assert cfg.get("single_final_seal") is None
    latest_cfg = latest.plugin_config_overrides["block_executor"]
    assert latest_cfg["version_liveness"] is True
    assert latest_cfg["final_version_batch_writeback"] is True
    assert latest_cfg["dependency_closed_consensus"] is True
    assert latest_cfg["version_liveness_indexed"] is True
    assert latest_cfg["single_final_seal"] is True
