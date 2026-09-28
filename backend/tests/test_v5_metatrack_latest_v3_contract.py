from backend.app.services.v5_cleanup_service import FOUR_METHOD_IDS, LEGACY_FOUR_METHOD_IDS
from backend.app.services.v5_formal_plan_validator import BUILTIN_METHODS
from backend.app.services.v5_plugin_manifest_store import STORE


def test_metatrack_new_v6_is_pure_metatrack_and_extends_frozen_current_v5():
    assert "metatrack_block_stm" not in BUILTIN_METHODS
    initial = BUILTIN_METHODS["metatrack_serial"]
    current = BUILTIN_METHODS["metatrack_full_locality"]
    latest = BUILTIN_METHODS["metatrack_latest"]
    assert initial.plugin_overrides == current.plugin_overrides == latest.plugin_overrides
    assert latest.plugin_overrides["routing"] == "metatrack_coaccess_routing"
    assert latest.plugin_overrides["execution"] == "dual_track_execution"
    assert latest.plugin_overrides["scheduler"] == "fast_first_scheduler"
    assert latest.plugin_overrides["block_executor"] == "metatrack_block_executor"
    assert latest.plugin_overrides["block_executor"] != "block_stm_block_executor"
    assert latest.plugin_config_overrides["routing"]["control_policy"] == "declared_access_frontier_v2"
    cfg = latest.plugin_config_overrides["block_executor"]
    assert cfg["local_exact_version_handoff"] is True
    assert cfg["batch_entry_state_prefetch"] is True
    assert cfg["batch_remote_writeback"] is True
    assert cfg["version_liveness"] is True
    assert cfg["final_version_batch_writeback"] is True
    current_cfg = current.plugin_config_overrides["block_executor"]
    assert current_cfg["version_liveness"] is True
    assert current_cfg["final_version_batch_writeback"] is True
    assert current_cfg.get("dependency_closed_consensus") is None
    assert cfg["dependency_closed_consensus"] is True
    assert cfg["version_liveness_indexed"] is True
    assert cfg["single_final_seal"] is True


def test_metatrack_executor_manifest_defaults_liveness_off_and_accepts_latest_flags():
    manifest = STORE.get("metatrack_block_executor")
    for name in ("local_exact_version_handoff", "batch_entry_state_prefetch", "batch_remote_writeback", "safe_state_fold", "version_liveness", "final_version_batch_writeback"):
        assert manifest.default_config[name] is False
        prop = manifest.config_schema["properties"][name]
        assert prop["type"] == "boolean"
        assert prop["default"] is False


def test_cleanup_keeps_historical_four_method_compatibility_contract():
    assert {"hash_serial", "hash_block_stm", "metatrack_serial", "metatrack_latest"}.issubset(FOUR_METHOD_IDS | LEGACY_FOUR_METHOD_IDS | {"metatrack_latest"})
