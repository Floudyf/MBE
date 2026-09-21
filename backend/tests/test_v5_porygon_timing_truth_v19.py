from __future__ import annotations

import json
from pathlib import Path

from backend.app.services.v5_metric_extractor import (
    _apply_common_block_execution_timing,
    _apply_porygon_metrics,
)
from backend.app.services.v5_formal_scheduler import _execution_semantics


def _write(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload), encoding="utf-8")


def _block(**overrides):
    payload = {
        "block_execution_ms": 10,
        "transaction_execution_us": 2_000,
        "transaction_execution_ms": 2,
        "deterministic_materialization_us": 500,
        "state_commitment_ms": 1,
        "porygon_execution_shard_count": 2,
        "porygon_execution_shard_histogram": {"esc_0": 1, "esc_1": 1},
        "porygon_local_business_execution_count": 1,
        "porygon_business_execution_us": 2_000,
        "porygon_result_exchange_wait_us": 1_000,
        "porygon_execution_critical_path_us": 4_000,
    }
    payload.update(overrides)
    return payload


def test_common_execution_timing_separates_cpu_sum_from_parallel_critical_path(tmp_path: Path) -> None:
    _write(tmp_path / "compiled_run_plan.json", {
        "node_configs": [
            {"node_id": "n0", "shard_id": "s0", "leader": True},
            {"node_id": "n1", "shard_id": "s1", "leader": True},
        ]
    })
    _write(tmp_path / "nodes/n0/block_execution_summary.json", {
        "node_id": "n0", "shard_id": "s0", "block_executor_id": "serial_block_executor",
        "blocks": [_block(block_execution_ms=100, transaction_execution_us=80_000)],
    })
    _write(tmp_path / "nodes/n1/block_execution_summary.json", {
        "node_id": "n1", "shard_id": "s1", "block_executor_id": "serial_block_executor",
        "blocks": [_block(block_execution_ms=120, transaction_execution_us=90_000)],
    })
    _write(tmp_path / "nodes/n0/runtime_metrics.json", {"counts": {"consensus_planner_build_us": 3_000}})
    _write(tmp_path / "nodes/n1/runtime_metrics.json", {"counts": {"consensus_planner_build_us": 5_000}})

    metrics: dict = {"source_artifacts": []}
    _apply_common_block_execution_timing(metrics, tmp_path)
    assert metrics["execution_cpu_sum_ms"] == 220
    assert metrics["execution_critical_path_ms"] == 120
    assert metrics["business_execution_cpu_sum_ms"] == 170
    assert metrics["business_execution_critical_path_ms"] == 90
    assert metrics["planner_build_cpu_sum_ms"] == 8
    assert metrics["planner_build_critical_path_ms"] == 5


def test_porygon_metrics_verify_esc_ownership_and_report_separate_timing(tmp_path: Path) -> None:
    nodes = []
    for index in range(8):
        nodes.append({
            "node_id": f"n{index}",
            "shard_id": "porygon-global",
            "execution_shard_id": f"s{index // 4}",
            "consensus_domain_id": "porygon-global",
            "leader": index == 0,
        })
    _write(tmp_path / "compiled_run_plan.json", {"node_configs": nodes})
    for index in range(8):
        local_count = 500
        business_us = 10_000 + index * 100
        _write(tmp_path / f"nodes/n{index}/block_execution_summary.json", {
            "node_id": f"n{index}",
            "shard_id": "porygon-global",
            "block_executor_id": "porygon_block_executor",
            "blocks": [_block(
                porygon_execution_shard_histogram={"esc_0": 500, "esc_1": 500} if index == 0 else {},
                porygon_local_business_execution_count=local_count,
                porygon_business_execution_us=business_us,
                porygon_result_exchange_wait_us=2_000,
                porygon_execution_critical_path_us=20_000 + index * 100,
            )],
        })

    metrics: dict = {"source_artifacts": []}
    _apply_porygon_metrics(metrics, tmp_path)
    assert metrics["porygon_execution_shard_transaction_counts"] == {"esc_0": 500, "esc_1": 500}
    assert metrics["porygon_expected_business_execution_count_by_node"] == {f"n{i}": 500 for i in range(8)}
    assert metrics["porygon_esc_ownership_verified"] is True
    assert metrics["porygon_local_business_execution_count_by_node"] == {f"n{i}": 500 for i in range(8)}
    assert metrics["porygon_business_execution_replica_cpu_sum_ms"] > 80
    # One deterministic representative per ESC: n0 + n4.
    assert metrics["porygon_business_execution_esc_representative_sum_ms"] == 20.4
    assert metrics["porygon_execution_critical_path_ms"] == 20.7


def test_porygon_formal_semantics_do_not_claim_physical_remote_state_plane() -> None:
    semantics = _execution_semantics({"block_executor": "porygon_block_executor"}, "stateless_porygon")
    assert semantics["comparison_semantics_class"] == "porygon_3d_global_ordering_distributed_esc_v4"
    assert semantics["state_access_semantics"] == "global_ordering_distributed_esc_with_logical_state_shards"
    assert semantics["remote_fetch_policy"] == "logical_signed_access_projection_no_physical_fetch"
    assert semantics["remote_writeback_policy"] == "global_deterministic_multi_shard_materialization_no_physical_writeback"
    assert semantics["proof_policy"] == "consensus_bound_porygon_transaction_execution_plan_and_esc_result_certificates"
    assert semantics["legacy_cross_shard_protocol"] is False
