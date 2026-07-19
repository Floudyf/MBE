from __future__ import annotations

import json
import os
import threading
from pathlib import Path

import pytest

from backend.app.models.v5_experiment_spec import V5ExperimentSpec, V5PluginSelection, V5Topology
from backend.app.models.v5_formal_experiment import V5FormalExperimentPlan, V5FormalMethod
from backend.app.services.v5_experiment_compiler import compile_plan
from backend.app.services.v5_compatibility_engine import validate as validate_compatibility
from backend.app.services.v5_formal_plan_validator import BUILTIN_METHODS, validate_request
from backend.app.services.v5_formal_scheduler import _spec_for, expand
from backend.app.services.v5_plugin_manifest_store import CATEGORIES, STORE


def selections() -> list[V5PluginSelection]:
    return [
        V5PluginSelection(
            category=category,
            plugin_id=next(item.plugin_id for item in STORE.list() if item.category == category),
            config={"cross_shard_ratio": 0.0, "timeout_every": 0} if category == "workload" else {},
        )
        for category in CATEGORIES
    ]


def base_spec() -> V5ExperimentSpec:
    return V5ExperimentSpec(
        name="matrix-propagation",
        execution_backend="real_cluster",
        plugin_selections=selections(),
        topology=V5Topology(nodes=8, shards=2, validators_per_shard=4),
        tx_count=100,
        seed=11,
        duration_ms=3600000,
    )


def method_plan(**kwargs) -> V5FormalExperimentPlan:
    return V5FormalExperimentPlan(
        name="matrix-propagation-plan",
        base_spec=base_spec(),
        methods=[
            V5FormalMethod(method_id="method_a", display_name="A", plugin_overrides={"routing": "metatrack_coaccess_routing"}),
            V5FormalMethod(method_id="method_b", display_name="B", plugin_overrides={"routing": "hash_routing_baseline"}),
        ],
        seeds=[11],
        repeats=1,
        **kwargs,
    )


def compiled(plan: V5FormalExperimentPlan, row: dict, path: Path):
    return compile_plan(_spec_for(plan, row), path)


def test_topology_workload_and_fault_points_propagate_from_natural_rows(tmp_path: Path) -> None:
    topology_plan = method_plan(
        suites=["topology_scaling"],
        topology_points=[
            {"nodes": 8, "shards": 2, "validators_per_shard": 4},
            {"nodes": 16, "shards": 4, "validators_per_shard": 4},
        ],
    )
    topology_rows = expand(topology_plan, "real_cluster")
    topology_a = next(row for row in topology_rows if row["method_config_id"] == "method_a" and row["topology_point"]["nodes"] == 8)
    topology_b = next(row for row in topology_rows if row["method_config_id"] == "method_a" and row["topology_point"]["nodes"] == 16)
    plan_a = compiled(topology_plan, topology_a, tmp_path / "topology-a")
    plan_b = compiled(topology_plan, topology_b, tmp_path / "topology-b")
    assert plan_a.experiment_spec["topology"] != plan_b.experiment_spec["topology"]

    workload_plan = method_plan(
        suites=["workload_sensitivity"],
        workload_points=[
            {"tx_count": 100, "cross_shard_ratio": 0.0},
            {"tx_count": 500, "cross_shard_ratio": 0.25},
        ],
    )
    workload_rows = expand(workload_plan, "real_cluster")
    workload_a = next(row for row in workload_rows if row["method_config_id"] == "method_a" and row["estimated_transactions"] == 100)
    workload_b = next(row for row in workload_rows if row["method_config_id"] == "method_a" and row["estimated_transactions"] == 500)
    workload_plan_a = compiled(workload_plan, workload_a, tmp_path / "workload-a")
    workload_plan_b = compiled(workload_plan, workload_b, tmp_path / "workload-b")
    assert workload_plan_a.workload_plan["tx_count"] == 100
    assert workload_plan_b.workload_plan["tx_count"] == 500
    assert workload_plan_b.workload_plan["requested_cross_shard_count"] == 125

    fault_plan = method_plan(
        suites=["fault_recovery_experiment"],
        fault_points=[{"mode": "disabled"}, {"mode": "network_delay_drop", "delay_ms": 5}],
    )
    fault_rows = expand(fault_plan, "real_cluster")
    fault_a = next(row for row in fault_rows if row["method_config_id"] == "method_a" and row["fault_point"]["mode"] == "disabled")
    fault_b = next(row for row in fault_rows if row["method_config_id"] == "method_a" and row["fault_point"]["mode"] == "network_delay_drop")
    assert compiled(fault_plan, fault_a, tmp_path / "fault-a").fault_plan != compiled(fault_plan, fault_b, tmp_path / "fault-b").fault_plan


def test_scan_suites_expand_every_selected_method_without_base_fallback() -> None:
    workload = method_plan(suites=["workload_sensitivity"], workload_points=[{"tx_count": 10}, {"tx_count": 20}])
    workload_rows = expand(workload, "real_cluster")
    assert len(workload_rows) == 4
    assert all(row["runnable"] for row in workload_rows)
    assert len({row["comparison_group_id"] for row in workload_rows}) == 2
    topology = method_plan(suites=["topology_scaling"], topology_points=[{"nodes": 8, "shards": 2, "validators_per_shard": 4}, {"nodes": 16, "shards": 4, "validators_per_shard": 4}])
    assert len(expand(topology, "real_cluster")) == 4
    faults = method_plan(suites=["fault_recovery_experiment"], fault_points=[{"mode": "disabled"}, {"mode": "delay_only", "delay_ms": 5}])
    assert len(expand(faults, "real_cluster")) == 4
    assert expand(method_plan(suites=["workload_sensitivity"], workload_points=[]), "real_cluster") == []


def test_comparison_rows_change_only_method(tmp_path: Path) -> None:
    plan = method_plan(suites=["comparison_experiment"])
    rows = expand(plan, "real_cluster")
    assert len(rows) == 2
    first, second = [compiled(plan, row, tmp_path / row["method_config_id"]) for row in rows]
    assert first.experiment_spec["topology"] == second.experiment_spec["topology"]
    assert first.workload_plan == second.workload_plan
    assert first.fault_plan == second.fault_plan
    assert first.plugin_snapshot != second.plugin_snapshot


def test_compiled_plan_keeps_formal_and_method_profile_ids(tmp_path: Path) -> None:
    plan = method_plan(suites=["comparison_experiment"])
    rows = expand(plan, "real_cluster")
    first = compile_plan(_spec_for(plan, rows[0], formal_plan_config_id="v3cfg_formal_plan"), tmp_path / "first")
    second = compile_plan(_spec_for(plan, rows[1], formal_plan_config_id="v3cfg_formal_plan"), tmp_path / "second")
    assert first.formal_plan_config_id == second.formal_plan_config_id == "v3cfg_formal_plan"
    assert first.method_config_id != second.method_config_id


def test_method_config_overrides_propagate_to_child_spec() -> None:
    plan = method_plan(suites=["comparison_experiment"])
    plan.methods = [
        V5FormalMethod(
            method_id="hash_block_stm",
            display_name="Hash + Block-STM",
            plugin_overrides={
                "routing": "hash_routing_baseline",
                "execution": "serial_execution_baseline",
                "scheduler": "fifo_serial_scheduler",
                "block_executor": "block_stm_block_executor",
                "commit": "normal_commit",
            },
            plugin_config_overrides={"block_executor": {"worker_count": 4}},
            role="baseline",
        )
    ]
    rows = expand(plan, "real_cluster")
    spec = _spec_for(plan, rows[0])
    block_executor = next(item for item in spec.plugin_selections if item.category == "block_executor")
    assert block_executor.plugin_id == "block_stm_block_executor"
    assert block_executor.config["worker_count"] == 4


def test_method_plugin_override_resets_to_target_plugin_default_config() -> None:
    plan = method_plan(suites=["comparison_experiment"])
    for selection in plan.base_spec.plugin_selections:
        if selection.category == "block_executor":
            selection.plugin_id = "block_stm_block_executor"
            selection.config = {"worker_count": 4}
    plan.methods = [
        V5FormalMethod(
            method_id="hash_serial",
            display_name="Hash + Serial",
            plugin_overrides={"block_executor": "serial_block_executor"},
            role="baseline",
        )
    ]

    rows = expand(plan, "real_cluster")
    spec = _spec_for(plan, rows[0])
    block_executor = next(item for item in spec.plugin_selections if item.category == "block_executor")

    assert block_executor.plugin_id == "serial_block_executor"
    assert block_executor.config["worker_count"] == 1
    assert validate_compatibility(spec).valid is True


def test_builtin_four_method_comparison_preserves_fairness_conditions(tmp_path: Path) -> None:
    plan = method_plan(suites=["comparison_experiment"])
    plan.methods = list(BUILTIN_METHODS.values())
    checked = validate_request(type("Request", (), {"execution_backend": "real_cluster", "plan": plan})())
    rows = checked.rows

    assert [row["method_config_id"] for row in rows] == [
        "hash_serial",
        "hash_block_stm",
        "metatrack_serial",
        "metatrack_block_stm",
    ]
    assert len({row["comparison_group_id"] for row in rows}) == 1
    assert len({row["fairness_key"] for row in rows}) == 1
    assert len({row["workload_snapshot_digest"] for row in rows}) == 1
    assert len({row["topology_snapshot_digest"] for row in rows}) == 1
    assert len({row["fault_snapshot_digest"] for row in rows}) == 1
    assert len({row["seed"] for row in rows}) == 1
    assert len({row["estimated_transactions"] for row in rows}) == 1
    assert len({row["method_snapshot_digest"] for row in rows}) == 4

    compiled_by_method = {
        row["method_config_id"]: compiled(plan, row, tmp_path / row["method_config_id"])
        for row in rows
    }
    common_workload = {item.workload_plan["tx_count"] for item in compiled_by_method.values()}
    common_topology = {json.dumps(item.experiment_spec["topology"], sort_keys=True) for item in compiled_by_method.values()}
    assert common_workload == {100}
    assert len(common_topology) == 1

    def plugin(plan_item, category: str) -> dict:
        return plan_item.node_configs[0].plugin_profile[category]

    assert plugin(compiled_by_method["hash_serial"], "routing")["plugin_id"] == "hash_routing_baseline"
    assert plugin(compiled_by_method["hash_serial"], "block_executor")["plugin_id"] == "serial_block_executor"
    assert plugin(compiled_by_method["hash_serial"], "block_executor")["config"]["worker_count"] == 1
    assert plugin(compiled_by_method["hash_block_stm"], "routing")["plugin_id"] == "hash_routing_baseline"
    assert plugin(compiled_by_method["hash_block_stm"], "block_executor")["plugin_id"] == "block_stm_block_executor"
    assert plugin(compiled_by_method["hash_block_stm"], "block_executor")["config"]["worker_count"] == 4
    assert plugin(compiled_by_method["metatrack_serial"], "routing")["plugin_id"] == "metatrack_coaccess_routing"
    assert plugin(compiled_by_method["metatrack_serial"], "commit")["plugin_id"] == "commutative_hot_update_aggregation"
    assert plugin(compiled_by_method["metatrack_serial"], "block_executor")["plugin_id"] == "serial_block_executor"
    assert plugin(compiled_by_method["metatrack_block_stm"], "routing")["plugin_id"] == "metatrack_coaccess_routing"
    assert plugin(compiled_by_method["metatrack_block_stm"], "commit")["plugin_id"] == "commutative_hot_update_aggregation"
    assert plugin(compiled_by_method["metatrack_block_stm"], "block_executor")["plugin_id"] == "block_stm_block_executor"
    assert plugin(compiled_by_method["metatrack_block_stm"], "block_executor")["config"]["worker_count"] == 4


def test_formal_scheduler_start_records_in_process_worker_thread(monkeypatch) -> None:
    import backend.app.services.v5_formal_scheduler as scheduler

    group_id = "v5grp_threaded_start"
    record = {"run_group_id": group_id, "status": "queued"}
    entered_worker = threading.Event()
    release_worker = threading.Event()

    def fake_read_group(value: str) -> dict:
        assert value == group_id
        return dict(record)

    def fake_write_group(group: dict) -> None:
        record.clear()
        record.update(group)

    def fake_worker(value: str) -> None:
        assert value == group_id
        entered_worker.set()
        release_worker.wait(timeout=2)
        group = fake_read_group(value)
        group["status"] = "completed"
        fake_write_group(group)

    monkeypatch.setattr(scheduler, "read_group", fake_read_group)
    monkeypatch.setattr(scheduler, "write_group", fake_write_group)
    monkeypatch.setattr(scheduler, "_worker", fake_worker)

    scheduler.start(group_id)

    assert entered_worker.wait(timeout=1)
    assert record["worker_pid"] == os.getpid()
    assert record["worker_thread"] == f"v5-formal-worker-{group_id}"
    assert record["status"] == "starting"
    release_worker.set()


def test_unknown_workload_point_is_blocked_and_not_silently_compiled() -> None:
    plan = method_plan(suites=["workload_sensitivity"], workload_points=[{"hotspot": 2}])
    row = expand(plan, "real_cluster")[0]
    assert row["runnable"] is False
    assert "unsupported workload point fields" in row["blockers"][0]
    with pytest.raises(ValueError, match="unsupported workload point fields"):
        _spec_for(plan, row)


def test_single_shard_cross_shard_ratio_is_blocked() -> None:
    spec = base_spec().model_copy(update={"topology": V5Topology(nodes=4, shards=1, validators_per_shard=4)})
    plan = V5FormalExperimentPlan(
        name="single-shard-ratio",
        base_spec=spec,
        suites=["workload_sensitivity"],
        methods=[V5FormalMethod(method_id="method_a", display_name="A", plugin_overrides={})],
        workload_points=[{"cross_shard_ratio": 0.25}],
        seeds=[1],
        repeats=1,
    )
    row = expand(plan, "real_cluster")[0]
    assert row["runnable"] is False
    assert "cross_shard_ratio requires at least 2 shards" in row["blockers"]


@pytest.mark.parametrize("fault", [
    {"mode": "network_delay_drop", "drop_rate": 0.1},
    {"mode": "network_delay_drop", "drop_every": 3},
    {"mode": "kill_node", "kill_node_after_ms": 100},
    {"mode": "restart_node", "restart_node_after_ms": 100},
])
def test_cross_shard_loss_or_restart_fault_is_blocked(fault: dict) -> None:
    spec = base_spec().model_copy(deep=True)
    spec.plugin_selections = [
        item.model_copy(update={"config": item.config | ({"cross_shard_ratio": 0.25} if item.category == "workload" else {})})
        for item in spec.plugin_selections
    ]
    spec.fault_policy = fault
    result = validate_compatibility(spec)
    assert not result.valid
    assert any("reliable retransmission is not implemented" in blocker for blocker in result.blockers)


def test_intra_shard_network_fault_is_not_blocked_by_cross_shard_rule() -> None:
    spec = base_spec().model_copy(deep=True)
    spec.fault_policy = {"mode": "network_delay_drop", "drop_rate": 0.1}
    result = validate_compatibility(spec)
    assert not any("reliable retransmission" in blocker for blocker in result.blockers)


def test_expand_blocks_cross_shard_loss_row_before_scheduling() -> None:
    spec = base_spec().model_copy(deep=True)
    spec.plugin_selections = [
        item.model_copy(update={"config": item.config | ({"cross_shard_ratio": 0.25} if item.category == "workload" else {})})
        for item in spec.plugin_selections
    ]
    plan = V5FormalExperimentPlan(
        name="fault-blocked",
        base_spec=spec,
        suites=["fault_recovery_experiment"],
        methods=[V5FormalMethod(method_id="method_a", display_name="A", plugin_overrides={})],
        seeds=[11],
        repeats=1,
        fault_points=[{"mode": "network_delay_drop", "drop_rate": 0.2}],
    )
    row = expand(plan, "real_cluster")[0]
    assert row["runnable"] is False
    assert "reliable retransmission is not implemented" in row["blockers"][0]
