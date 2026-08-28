
from __future__ import annotations

import json
from pathlib import Path

from backend.app.services.v5_fairness_validator import validate as validate_fairness
from backend.app.services.v5_formal_scheduler import (
    _apply_state_equivalence_gate,
    _execution_semantics,
    _state_equivalence_individual_reasons,
    _worker_truth_reasons,
)
from backend.app.services.v5_metric_extractor import _apply_literature_graph_metrics


def _write_leader_summary(root: Path, block: dict) -> None:
    (root / "compiled_run_plan.json").write_text(
        json.dumps({"node_configs": [{"node_id": "n0", "shard_id": "s0", "leader": True, "role": "leader"}]}),
        encoding="utf-8",
    )
    path = root / "nodes/n0/block_execution_summary.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps({
        "node_id": "n0",
        "shard_id": "s0",
        "block_executor_id": "cg_block_executor",
        "blocks": [block],
    }), encoding="utf-8")


def test_cg_retryable_result_is_valid_even_with_attempt_level_abort_evidence() -> None:
    semantics = _execution_semantics({"block_executor": "cg_block_executor", "routing": "hash_routing_baseline"}, "hash_cg")
    assert semantics["comparison_semantics_class"] == "nezha_cg_johnson_retryable_v4"
    child = {
        "status": "completed",
        "method_config_id": "hash_cg",
        "comparison_semantics_class": semantics["comparison_semantics_class"],
        "topology_point": {"worker_count": 8, "shards": 1},
        "metrics": {
            "submitted_unique_tx_count": 1000,
            "terminal_unique_tx_count": 1000,
            "finalized_unique_logical_tx_count": 1000,
            "incomplete_unique_tx_count": 0,
            "cross_shard_failed_unique_count": 0,
            "abort_count": 7696,
            "cg_cycle_abort_count": 7696,
            "lifecycle_complete": True,
            "worker_count": 8,
            "configured_worker_count": 8,
            "cg_execution_worker_count": 8,
            "cg_planning_worker_count": 1,
            "maximum_parallel_width": 8,
        },
    }
    assert _state_equivalence_individual_reasons(child) == []


def test_worker_truth_rejects_requested_execution_mismatch_but_not_underfilled_parallelism() -> None:
    base = {
        "method_config_id": "hash_cg",
        "topology_point": {"worker_count": 32, "shards": 1},
        "metrics": {
            "worker_count": 32,
            "configured_worker_count": 32,
            "cg_execution_worker_count": 32,
            "cg_planning_worker_count": 1,
            "maximum_parallel_width": 11,
        },
    }
    assert _worker_truth_reasons(base) == []
    bad = {**base, "metrics": {**base["metrics"], "cg_execution_worker_count": 8}}
    assert "cg_execution_worker_not_equal_requested" in _worker_truth_reasons(bad)


def test_cg_block_evidence_is_promoted_to_frontend_metrics_and_worker_truth(tmp_path: Path) -> None:
    _write_leader_summary(tmp_path, {
        "worker_count": 32,
        "configured_worker_count": 32,
        "maximum_parallel_width": 11,
        "wave_count": 9,
        "maximum_wave_width": 40,
        "dependency_edge_count": 123,
        "pairwise_conflict_check_count": 4950,
        "graph_color_count": 0,
        "graph_table_construction_ms": 4,
        "sorting_ms": 7,
        "transaction_execution_ms": 13,
        "deterministic_materialization_ms": 2,
        "state_commitment_ms": 3,
        "literature_plan_parse_ms": 5,
        "literature_plan_verify_ms": 6,
        "literature_plan_verify_mode": "preverified_projection",
        "cg_candidate_transaction_count": 100,
        "cg_cycle_abort_count": 20,
        "cg_cycle_resolution_count": 4,
        "cg_cycle_deferred_retry_count": 20,
        "cg_cycle_deferred_tx_ids": ["a", "b", "a"],
        "cg_planning_worker_count": 1,
        "cg_execution_worker_count": 32,
        "cg_validator_mode": "reference_projection",
        "cg_cycle_space_policy": "bounded-johnson",
        "cg_large_rmw_clique_policy": "deterministic-rmw",
        "cg_large_rmw_clique_threshold": 16,
        "cg_johnson_cycle_budget": 10000,
        "cg_johnson_traversal_work_budget": 250000,
        "cg_johnson_plan_work_budget": 1000000,
        "cg_cycle_retry_lifecycle": "fifo_deferred_to_later_block",
        "cg_reference_commit_order_count": 80,
    })
    metrics = {"source_artifacts": [], "submitted_unique_tx_count": 100}
    _apply_literature_graph_metrics(metrics, tmp_path)
    assert metrics["configured_worker_count"] == 32
    assert metrics["worker_count"] == 32
    assert metrics["cg_execution_worker_count"] == 32
    assert metrics["cg_planning_worker_count"] == 1
    assert metrics["cg_worker_truth_valid"] is True
    assert metrics["literature_plan_parse_ms"] == 5
    assert metrics["literature_plan_verify_ms"] == 6
    assert metrics["cg_johnson_cycle_budget"] == 10000
    assert metrics["cg_johnson_traversal_work_budget"] == 250000
    assert metrics["cg_johnson_plan_work_budget"] == 1000000
    assert metrics["cg_cycle_space_policy"] == "bounded-johnson"
    assert metrics["cg_large_rmw_clique_policy"] == "deterministic-rmw"
    assert metrics["cg_large_rmw_clique_threshold"] == 16
    assert metrics["cg_cycle_unique_deferred_tx_count"] == 2


def _fairness_row(method: str, semantic: str) -> dict:
    return {
        "child_run_id": f"child-{method}",
        "method_config_id": method,
        "comparison_group_id": "comparison:11:0:base:",
        "comparison_semantics_class": semantic,
        "seed": 11,
        "repeat_index": 0,
        "execution_backend": "real_cluster",
        "estimated_transactions": 1000,
        "workload_snapshot_digest": "workload",
        "topology_snapshot_digest": "topology",
        "fault_snapshot_digest": "fault",
        "fairness_key": "fairness",
        "block_size": 100,
        "block_interval_ms": 100,
        "topology_point": {"nodes": 8, "shards": 1, "validators_per_shard": 8, "worker_count": 8},
        "state_access_semantics": f"method:{semantic}",
        "state_home_mapping_policy": "execution_shard_local_namespace",
        "remote_fetch_policy": "none",
        "remote_writeback_policy": "none",
        "proof_policy": f"proof:{semantic}",
        "legacy_cross_shard_protocol": method != "hash_batch_si",
        "measurement_boundary": f"client_submit_to_{method}_eventual_finality",
        "runnable": True,
        "blockers": [],
    }


def test_single_shard_cg_acg_batchsi_share_external_performance_contract() -> None:
    rows = [
        _fairness_row("hash_cg", "nezha_cg_johnson_retryable_v4"),
        _fairness_row("hash_acg", "nezha_acg_hs_retryable_v2"),
        _fairness_row("hash_batch_si", "batch_si_common_batch_snapshot_v1"),
    ]
    checked, result = validate_fairness(rows)
    assert result["passed"] is True
    assert result["performance_comparison_valid"] is True
    assert all(row["performance_contract_class"] == "single_shard_stateful_eventual_completion_v1" for row in checked)


def _completed(child_id: str, method: str, semantic: str, state: str) -> dict:
    return {
        "child_run_id": child_id,
        "comparison_group_id": "comparison:11:0:base:",
        "comparison_semantics_class": semantic,
        "performance_contract_class": "single_shard_stateful_eventual_completion_v1",
        "method_config_id": method,
        "status": "completed",
        "paper_candidate": True,
        "topology_point": {"worker_count": 8, "shards": 1},
        "metrics": {
            "submitted_unique_tx_count": 100,
            "terminal_unique_tx_count": 100,
            "finalized_unique_logical_tx_count": 100,
            "incomplete_unique_tx_count": 0,
            "cross_shard_failed_unique_count": 0,
            "lifecycle_complete": True,
            "worker_count": 8,
            "configured_worker_count": 8,
            "serial_order_oracle_status": "passed",
            "serial_order_replay_equivalent": True,
            "serial_order_replay_blockers": [],
            "serial_order_replay_input_digest": "same-logical-workload",
            "serial_order_replay_transaction_count": 100,
            "serial_order_actual_global_business_state_digest": state,
            **({"cg_execution_worker_count": 8, "cg_planning_worker_count": 1, "maximum_parallel_width": 8} if method == "hash_cg" else {}),
        },
        "result": {"summary": {
            "initial_state_digest": "initial",
            "global_final_state_digest": state,
        }},
    }


def test_external_contract_accepts_different_legal_final_states_when_each_matches_its_serial_oracle() -> None:
    items = [
        _completed("cg", "hash_cg", "nezha_cg_johnson_retryable_v4", "cg-final"),
        _completed("acg", "hash_acg", "nezha_acg_hs_retryable_v2", "acg-final"),
        _completed("batch", "hash_batch_si", "batch_si_common_batch_snapshot_v1", "batch-final"),
    ]
    enriched, report = _apply_state_equivalence_gate(items)
    assert report["performance_comparison_valid"] is True
    assert report["external_performance_contract_valid"] is True
    assert report["cross_method_serial_order_oracle_valid"] is True
    external = report["external_performance_contract_reports"][0]
    assert external["cross_method_serial_order_oracle_valid"] is True
    assert external["final_state_digest_equality_required"] is False
    assert external["cross_method_final_state_digest_equal"] is False
    assert all(item["performance_comparison_valid"] is True for item in enriched)


def test_external_contract_fails_closed_when_serial_oracle_or_logical_workload_evidence_fails() -> None:
    items = [
        _completed("cg", "hash_cg", "nezha_cg_johnson_retryable_v4", "cg-final"),
        _completed("acg", "hash_acg", "nezha_acg_hs_retryable_v2", "acg-final"),
        _completed("batch", "hash_batch_si", "batch_si_common_batch_snapshot_v1", "batch-final"),
    ]
    bad_oracle = [dict(item) for item in items]
    bad_oracle[-1] = {**bad_oracle[-1], "metrics": {**bad_oracle[-1]["metrics"], "serial_order_replay_equivalent": False, "serial_order_replay_blockers": ["digest_mismatch"]}}
    _, bad_report = _apply_state_equivalence_gate(bad_oracle)
    assert bad_report["performance_comparison_valid"] is False
    assert bad_report["external_performance_contract_valid"] is False

    bad_workload = [dict(item) for item in items]
    bad_workload[-1] = {**bad_workload[-1], "metrics": {**bad_workload[-1]["metrics"], "serial_order_replay_input_digest": "different-logical-workload"}}
    _, bad_workload_report = _apply_state_equivalence_gate(bad_workload)
    assert bad_workload_report["performance_comparison_valid"] is False
    assert bad_workload_report["external_performance_contract_valid"] is False
