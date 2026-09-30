import pytest

from backend.app.models.v5_experiment_spec import V5ExperimentSpec, V5PluginSelection, V5Topology
from backend.app.models.v5_formal_experiment import V5FormalExperimentPlan, V5FormalMethod, V5FormalRunRequest
from backend.app.services.v5_formal_plan_validator import BUILTIN_METHODS, STATELESS_BUILTIN_METHODS, FormalPlanValidationError, _effective_snapshot, validate_request
from backend.app.services.v5_plugin_manifest_store import CATEGORIES, STORE


def _plan(*, fault_points=None):
    selections = [V5PluginSelection(category=category, plugin_id=next(item.plugin_id for item in STORE.list() if item.category == category), config={}) for category in CATEGORIES]
    workload = next(item for item in selections if item.category == "workload")
    workload.plugin_id = "deterministic_signed_synthetic"
    workload.config = {"cross_shard_ratio": 0, "timeout_every": 0}
    return V5FormalExperimentPlan(name="closure", base_spec=V5ExperimentSpec(name="closure", execution_backend="real_cluster", plugin_selections=selections, topology=V5Topology(nodes=4, shards=1, validators_per_shard=4), tx_count=20, seed=1, duration_ms=9000), methods=[V5FormalMethod(method_id="v5_catalog_default", display_name="forged", plugin_overrides={}, role="main")], suites=["main_experiment"], fault_points=fault_points or [])


def test_catalog_default_is_canonical_baseline_and_alias_snapshot_is_canonical():
    plan = _plan()
    checked = validate_request(V5FormalRunRequest(execution_backend="real_cluster", plan=plan))
    assert checked.plan.methods[0].role == "baseline" and checked.plan.methods[0].display_name == "V5 Catalog Default"
    alias = V5FormalMethod(method_id="x", display_name="x", plugin_overrides={"routing": "hash"})
    canonical = V5FormalMethod(method_id="x", display_name="x", plugin_overrides={"routing": "hash_routing_baseline"})
    assert _effective_snapshot(plan, alias) == _effective_snapshot(plan, canonical)


def test_builtin_methods_are_registry_locked_and_carry_config_overrides():
    plan = _plan()
    current_builtin_ids = [
        "hash_serial",
        "hash_block_stm",
        "hash_aria",
        "hash_groundhog",
        "metatrack_serial",
        "metatrack_full_locality",
        "metatrack_latest",
    ]
    plan.methods = [BUILTIN_METHODS[method_id] for method_id in current_builtin_ids]
    plan.suites = ["comparison_experiment"]
    checked = validate_request(V5FormalRunRequest(execution_backend="real_cluster", plan=plan))
    assert [method.method_id for method in checked.plan.methods] == current_builtin_ids
    assert [method.display_name for method in checked.plan.methods] == [
        "Stateful Hash + Serial Reference",
        "Stateful Hash + Block-STM Reference",
        "Aria",
        "Groundhog",
        "MetaTrack（初始版）",
        "MetaTrack（当前版）",
        "MetaTrack（新版）",
    ]
    assert checked.plan.methods[-1].role == "main"
    assert checked.plan.methods[1].plugin_overrides["block_executor"] == "block_stm_block_executor"
    assert checked.plan.methods[1].plugin_config_overrides["block_executor"]["worker_count"] == 4
    assert checked.plan.methods[1].plugin_config_overrides["block_executor"]["maximum_incarnations"] == 0
    assert checked.plan.methods[2].plugin_overrides["block_executor"] == "aria_block_executor"
    assert checked.plan.methods[2].plugin_config_overrides["block_executor"]["reordering"] is True
    assert checked.plan.methods[3].plugin_overrides["block_producer"] == "groundhog_block_producer"
    assert checked.plan.methods[3].plugin_overrides["block_executor"] == "groundhog_block_executor"
    initial = checked.plan.methods[-3]
    current = checked.plan.methods[-2]
    latest = checked.plan.methods[-1]
    assert initial.plugin_overrides == current.plugin_overrides == latest.plugin_overrides
    assert current.plugin_overrides["block_executor"] == "metatrack_block_executor"
    routing_flag = "incremental_exact_continuity_routing_v65"
    assert initial.plugin_config_overrides["routing"] == current.plugin_config_overrides["routing"]
    assert routing_flag not in initial.plugin_config_overrides["routing"]
    assert routing_flag not in current.plugin_config_overrides["routing"]
    latest_routing = dict(latest.plugin_config_overrides["routing"])
    assert latest_routing.pop(routing_flag, None) is True
    assert latest_routing == current.plugin_config_overrides["routing"]
    assert current.plugin_config_overrides["routing"]["control_policy"] == "declared_access_frontier_v2"
    assert current.plugin_config_overrides["block_executor"]["control_policy"] == "declared_access_frontier_v2"
    assert latest.plugin_config_overrides["block_executor"]["control_policy"] == "declared_access_frontier_v2"
    assert "local_exact_version_handoff" not in initial.plugin_config_overrides["block_executor"]
    assert current.plugin_config_overrides["block_executor"]["local_exact_version_handoff"] is True
    assert latest.plugin_config_overrides["block_executor"]["local_exact_version_handoff"] is True
    assert current.plugin_config_overrides["block_executor"]["version_liveness"] is True
    assert current.plugin_config_overrides["block_executor"]["final_version_batch_writeback"] is True
    assert current.plugin_config_overrides["block_executor"].get("dependency_closed_consensus") is None
    assert latest.plugin_config_overrides["block_executor"]["version_liveness"] is True
    assert latest.plugin_config_overrides["block_executor"]["dependency_closed_consensus"] is True

    forged = BUILTIN_METHODS["hash_block_stm"].model_copy(deep=True)
    forged.plugin_config_overrides["block_executor"]["worker_count"] = 1
    plan.methods = [forged]
    with pytest.raises(FormalPlanValidationError, match="builtin method payload does not match registry"):
        validate_request(V5FormalRunRequest(execution_backend="real_cluster", plan=plan))


def test_optional_stateless_hash_methods_are_registry_locked():
    plan = _plan()
    plan.methods = list(STATELESS_BUILTIN_METHODS.values())
    plan.suites = ["comparison_experiment"]
    checked = validate_request(V5FormalRunRequest(execution_backend="real_cluster", plan=plan))
    assert [method.method_id for method in checked.plan.methods] == ["stateless_hash_serial", "stateless_hash_block_stm"]
    assert all(method.plugin_overrides["routing"] == "stateless_hash_routing" for method in checked.plan.methods)
    assert checked.plan.methods[1].plugin_config_overrides["block_executor"]["maximum_incarnations"] == 0


@pytest.mark.parametrize("point", [{"mode": "delay_only"}, {"mode": "delay_only", "delay_ms": True}, {"mode": "network_drop"}, {"mode": "network_drop", "drop_rate": 0}, {"mode": "network_drop", "drop_every": 3}, {"mode": "kill_node"}, {"mode": "restart_node"}])
def test_unsupported_or_invalid_fault_points_are_rejected(point):
    plan = _plan(fault_points=[{"mode": "disabled"}, point])
    plan.suites = ["fault_recovery_experiment"]
    with pytest.raises(FormalPlanValidationError):
        validate_request(V5FormalRunRequest(execution_backend="real_cluster", plan=plan))


def test_single_shard_network_drop_is_valid_but_cross_shard_is_blocked():
    plan = _plan(fault_points=[{"mode": "disabled"}, {"mode": "network_drop", "drop_rate": 0.2}])
    plan.suites = ["fault_recovery_experiment"]
    assert validate_request(V5FormalRunRequest(execution_backend="real_cluster", plan=plan)).rows
    plan.base_spec.topology = V5Topology(nodes=8, shards=2, validators_per_shard=4)
    next(item for item in plan.base_spec.plugin_selections if item.category == "workload").config["cross_shard_ratio"] = 0.25
    checked = validate_request(V5FormalRunRequest(execution_backend="real_cluster", plan=plan), allow_blocked_rows=True)
    assert any(not row["runnable"] for row in checked.rows)
