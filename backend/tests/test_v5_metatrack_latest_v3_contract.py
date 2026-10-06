from backend.app.services.v5_cleanup_service import FOUR_METHOD_IDS, LEGACY_FOUR_METHOD_IDS
from backend.app.services.v5_formal_plan_validator import BUILTIN_METHODS
from backend.app.services.v5_plugin_manifest_store import STORE


def test_metatrack_latest_is_modular_and_historical_versions_remain_separate():
    latest=BUILTIN_METHODS["metatrack_latest"]
    assert latest.plugin_overrides["routing"]=="metatrack_coaccess_routing"
    assert latest.plugin_overrides["block_producer"]=="metatrack_nl_window_v669"
    assert latest.plugin_overrides["execution"]=="dual_track_execution"
    assert latest.plugin_overrides["scheduler"]=="fast_first_scheduler"
    assert latest.plugin_overrides["block_executor"]=="metatrack_block_executor"
    assert latest.plugin_overrides["state_access"]=="metatrack_local_exact_access"
    cfg=latest.plugin_config_overrides["block_executor"]
    for key in ("batch_entry_state_prefetch","batch_remote_writeback","version_liveness","final_version_batch_writeback","dependency_closed_consensus","version_liveness_indexed","single_final_seal"):
        assert cfg[key] is True
    assert "local_exact_version_handoff" not in cfg
    assert "worker_count" not in cfg


def test_modular_plugins_are_registered_in_manifest_store():
    for pid in ("metatrack_hash_routing","metatrack_single_conservative_execution","metatrack_single_execution","metatrack_dependency_window_producer","metatrack_adaptive_window_producer","metatrack_nl_window_v669","metatrack_route_batch_producer","metatrack_local_exact_access","metatrack_home_exact_access"):
        assert STORE.get(pid).plugin_id==pid


def test_cleanup_keeps_historical_four_method_compatibility_contract():
    assert {"hash_serial", "hash_block_stm", "metatrack_serial", "metatrack_latest"}.issubset(FOUR_METHOD_IDS | LEGACY_FOUR_METHOD_IDS | {"metatrack_latest"})
