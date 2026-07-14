import pytest

from backend.app.models.v5_experiment_spec import V5ExperimentSpec, V5PluginSelection, V5Topology
from backend.app.models.v5_formal_experiment import V5FormalExperimentPlan, V5FormalMethod, V5FormalRunRequest
from backend.app.services.v5_formal_plan_validator import FormalPlanValidationError, _effective_snapshot, validate_request
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
