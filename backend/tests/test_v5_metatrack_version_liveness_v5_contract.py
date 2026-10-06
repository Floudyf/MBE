from pathlib import Path

from backend.app.services.v5_formal_plan_validator import BUILTIN_METHODS
from backend.app.services.v5_plugin_manifest_store import STORE


def test_metatrack_frontend_keeps_stable_method_and_removes_batch_sensitivity_variants():
    profile = Path("frontend/src/v5MethodProfile.ts").read_text(encoding="utf-8")
    catalog = Path("frontend/src/v5FormalExperimentCatalog.ts").read_text(encoding="utf-8")
    assert 'method_id: "metatrack_latest"' in profile
    assert 'methodId: "metatrack_latest"' in catalog
    for removed in ("metatrack_exp50", "metatrack_exp200", "metatrack_ready_round_control", "metatrack_influence", "metatrack_block_stm"):
        assert f'method_id: "{removed}"' not in profile
        assert f'methodId: "{removed}"' not in catalog

def test_metatrack_current_freezes_v5_liveness_and_new_v6_adds_closure_without_pbft_algorithm_change():
    initial = BUILTIN_METHODS["metatrack_serial"]
    current = BUILTIN_METHODS["metatrack_full_locality"]
    latest = BUILTIN_METHODS["metatrack_latest"]
    assert latest.plugin_overrides["block_producer"] == "metatrack_nl_window_v669"
    assert latest.plugin_overrides["state_access"] == "metatrack_local_exact_access"
    assert latest.plugin_overrides["block_executor"] == "metatrack_block_executor"
    assert latest.plugin_overrides.get("consensus") == initial.plugin_overrides.get("consensus")
    current_cfg = current.plugin_config_overrides["block_executor"]
    latest_cfg = latest.plugin_config_overrides["block_executor"]
    assert current_cfg["local_exact_version_handoff"] is True
    assert current_cfg["batch_entry_state_prefetch"] is True
    assert current_cfg["version_liveness"] is True
    assert current_cfg["final_version_batch_writeback"] is True
    assert current_cfg.get("dependency_closed_consensus") is None
    assert "local_exact_version_handoff" not in latest_cfg
    assert latest_cfg["batch_entry_state_prefetch"] is True
    assert latest_cfg["batch_remote_writeback"] is True
    assert latest_cfg["version_liveness"] is True
    assert latest_cfg["final_version_batch_writeback"] is True
    assert latest_cfg["dependency_closed_consensus"] is True
    assert latest_cfg["version_liveness_indexed"] is True
    assert latest_cfg["single_final_seal"] is True


def test_metatrack_executor_manifest_declares_liveness_flags_off_by_default():
    manifest = STORE.get("metatrack_block_executor")
    for key in ("version_liveness", "final_version_batch_writeback"):
        assert manifest.default_config[key] is False
        assert manifest.config_schema["properties"][key]["type"] == "boolean"
        assert manifest.config_schema["properties"][key]["default"] is False
    assert "version_dependency_liveness" in manifest.capabilities
    assert "final_version_batch_writeback" in manifest.capabilities


def test_liveness_is_signed_at_route_batch_scope_and_runtime_gc_waits_for_actual_successor_completion():
    routing = Path("executor/realism/tx/routing.go").read_text(encoding="utf-8")
    client = Path("executor/v5/client.go").read_text(encoding="utf-8")
    runtime = Path("executor/v5/metatrack_version_liveness_v5.go").read_text(encoding="utf-8")
    assert 'LivenessClass' in routing and 'LivenessDigest' in routing
    assert 'RemoteOrderingSuccessorCount' in routing
    assert 'RequiredLivenessClass' in routing
    assert 'versionLivenessEnabled = boolFromAny(config.Config["version_liveness"])' in client
    assert 'if bindExecutionRouting && versionLivenessEnabled && isMetaTrackRoutingPlugin(plugins.Routing)' in client
    assert 'resealMetaTrackVersionLivenessPlan(records, routePlan)' in client
    assert 'finalizeMetaTrackSignedBatchPlan' in client
    assert 'if bindExecutionRouting && isMetaTrackRoutingPlugin(plugins.Routing)' not in client
    assert client.index('resealMetaTrackVersionLivenessPlan(records, routePlan)') < client.index('routePlanDigest = routePlan.PlanDigest')
    assert 'releaseMetaTrackTransientPredecessors(item)' in runtime
    assert 'metatrack_version_transient_ref_registered_count' in runtime
    assert 'metatrack_version_cross_block_preserved_count' in runtime
    assert 'metaTrackLocalTransientIsSameBlock' in runtime
    assert 'route batch' in runtime.lower() or 'routing batch' in runtime.lower()
