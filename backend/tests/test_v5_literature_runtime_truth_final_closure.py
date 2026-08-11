from __future__ import annotations

import json
from pathlib import Path

from backend.app.services.v5_formal_scheduler import _apply_state_equivalence_gate, _execution_semantics
from backend.app.services.v5_metric_extractor import (
    _apply_literature_graph_metrics,
    _apply_metric_completeness,
    _literature_graph_required_metrics,
)


def _completed(child_id: str, method_id: str, semantic_class: str, final_digest: str) -> dict:
    summary = {
        "initial_state_digest": "initial",
        "global_final_state_digest": final_digest,
        "finality_evidence": {
            "submitted_unique_tx_count": 1000,
            "terminal_unique_tx_count": 1000,
            "finalized_unique_logical_tx_count": 1000,
            "incomplete_unique_tx_count": 0,
            "cross_shard_failed_unique_count": 0,
            "lifecycle_complete": True,
        },
    }
    return {
        "child_run_id": child_id,
        "comparison_group_id": "comparison_experiment:11:0:base:",
        "comparison_semantics_class": semantic_class,
        "method_config_id": method_id,
        "status": "completed",
        "paper_candidate": True,
        "topology_point": {"shards": 1},
        "result": {"summary": summary},
    }


def test_bsx_has_its_own_deterministic_serializable_semantics_class() -> None:
    semantics = _execution_semantics(
        {"routing": "hash_routing_baseline", "block_executor": "bsx_block_executor"},
        "hash_bsx",
    )
    assert semantics["comparison_semantics_class"] == "bsx_deterministic_coloring_serializable_v1"
    assert semantics["proof_policy"] == "consensus_bound_bsx_plan_digest"


def test_single_member_semantic_cohorts_are_valid_without_cross_semantic_digest_equality() -> None:
    legacy = "stateful_local_legacy_v1"
    items = [
        _completed("serial", "hash_serial", legacy, "legacy-state"),
        _completed("blockstm", "hash_block_stm", legacy, "legacy-state"),
        _completed("bsx", "hash_bsx", "bsx_deterministic_coloring_serializable_v1", "bsx-state"),
        _completed("batchsi", "hash_batch_si", "batch_si_common_batch_snapshot_v1", "batchsi-state"),
        _completed("groundhog", "hash_groundhog", "groundhog_typed_commutative_snapshot_v1", "groundhog-state"),
    ]
    enriched, report = _apply_state_equivalence_gate(items)
    by_id = {item["child_run_id"]: item for item in enriched}
    assert all(by_id[item]["paper_candidate"] is True for item in by_id)
    assert report["within_semantic_cohort_state_equivalence_valid"] is True
    assert report["performance_comparison_valid"] is False
    assert by_id["serial"]["pairwise_logical_state_equivalent"] is True
    assert by_id["bsx"]["pairwise_logical_state_equivalent"] is True


def test_literature_graph_metrics_are_read_from_one_leader_per_shard(tmp_path: Path) -> None:
    (tmp_path / "compiled_run_plan.json").write_text(json.dumps({
        "node_configs": [
            {"node_id": "n0", "shard_id": "s0", "leader": True, "role": "leader"},
            {"node_id": "n1", "shard_id": "s0", "leader": False, "role": "validator"},
        ]
    }), encoding="utf-8")
    for node_id in ("n0", "n1"):
        node = tmp_path / "nodes" / node_id
        node.mkdir(parents=True)
        (node / "block_execution_summary.json").write_text(json.dumps({
            "node_id": node_id,
            "shard_id": "s0",
            "block_executor_id": "bsx_block_executor",
            "blocks": [{
                "worker_count": 4,
                "maximum_parallel_width": 3,
                "wave_count": 5,
                "maximum_wave_width": 7,
                "dependency_edge_count": 11,
                "pairwise_conflict_check_count": 0,
                "graph_color_count": 5,
                "transaction_execution_ms": 13,
                "deterministic_materialization_ms": 2,
            }],
        }), encoding="utf-8")
    metrics = {"source_artifacts": []}
    _apply_literature_graph_metrics(metrics, tmp_path)
    assert metrics["literature_graph_metrics_available"] is True
    assert metrics["maximum_parallel_width"] == 3
    assert metrics["wave_count"] == 5
    assert metrics["graph_color_count"] == 5
    assert metrics["transaction_execution_ms"] == 13
    assert metrics["source_artifacts"] == ["nodes/n0/block_execution_summary.json"]
    assert "graph_color_count" in _literature_graph_required_metrics("hash_bsx")
    assert "pairwise_conflict_check_count" in _literature_graph_required_metrics("hash_cg")

def test_metric_requiredness_union_preserves_block_stm_shared_metric_requirements() -> None:
    metrics = {"missing": []}
    _apply_metric_completeness(metrics, method_id="hash_block_stm")
    assert metrics["metric_statuses"]["worker_count"] == "missing"
    assert metrics["metric_statuses"]["maximum_parallel_width"] == "missing"
    assert "metric:worker_count" in metrics["missing"]
    assert "metric:maximum_parallel_width" in metrics["missing"]


def test_metric_requiredness_union_preserves_batch_si_shared_metric_requirements() -> None:
    metrics = {"missing": []}
    _apply_metric_completeness(metrics, method_id="hash_batch_si")
    assert metrics["metric_statuses"]["maximum_parallel_width"] == "missing"
    assert metrics["metric_statuses"]["dependency_edge_count"] == "missing"
    assert "metric:maximum_parallel_width" in metrics["missing"]
    assert "metric:dependency_edge_count" in metrics["missing"]

