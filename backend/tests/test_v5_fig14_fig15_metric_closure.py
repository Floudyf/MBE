from __future__ import annotations

import json
from pathlib import Path

from backend.app.services.v5_metric_extractor import _apply_groundhog_metrics, _apply_literature_graph_metrics
from backend.app.services.v5_formal_scheduler import _execution_semantics, _state_equivalence_individual_reasons
from backend.app.services.v5_paper_exporter import _individual_result_reasons


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload), encoding="utf-8")


def test_fig14_literature_phase_timing_and_cg_abort_export(tmp_path: Path) -> None:
    _write_json(tmp_path / "compiled_run_plan.json", {"node_configs": [{"node_id": "n0", "leader": True}]})
    _write_json(
        tmp_path / "nodes" / "n0" / "block_execution_summary.json",
        {
            "shard_id": "s0",
            "block_executor_id": "cg_block_executor",
            "blocks": [
                {
                    "worker_count": 8,
                    "maximum_parallel_width": 4,
                    "wave_count": 2,
                    "maximum_wave_width": 3,
                    "dependency_edge_count": 9,
                    "pairwise_conflict_check_count": 15,
                    "graph_color_count": 0,
                    "graph_table_construction_ms": 12,
                    "sorting_ms": 7,
                    "transaction_execution_ms": 5,
                    "deterministic_materialization_ms": 2,
                    "state_commitment_ms": 3,
                    "cg_candidate_transaction_count": 6,
                    "cg_cycle_abort_count": 2,
                }
            ],
        },
    )
    metrics: dict = {"source_artifacts": []}
    _apply_literature_graph_metrics(metrics, tmp_path)
    assert metrics["graph_table_construction_ms"] == 12
    assert metrics["sorting_ms"] == 7
    assert metrics["cg_cycle_abort_count"] == 2
    assert metrics["cg_candidate_transaction_count"] == 6
    assert metrics["cg_cycle_abort_rate"] == 2 / 6


def test_groundhog_conflict_abort_alias_excludes_candidate_limit_deferrals(tmp_path: Path) -> None:
    _write_json(tmp_path / "compiled_run_plan.json", {"node_configs": [{"node_id": "n0", "leader": True}]})
    _write_json(
        tmp_path / "nodes" / "n0" / "block_execution_summary.json",
        {"shard_id": "s0", "block_executor_id": "groundhog_block_executor", "blocks": [{"worker_count": 8}]},
    )
    evidence = {
        "algorithm_id": "groundhog_candidate_selection_v1",
        "payload_digest": "d1",
        "payload": {
            "shard_id": "s0",
            "height": 1,
            "candidate_count": 10,
            "selected_count": 6,
            "deferred_count": 4,
            "deferred_tx_ids": ["a", "b", "c", "d"],
            "metrics": {
                "reservation_count": 8,
                "constraint_conflict_count": 2,
                "reservation_rollback_count": 2,
            },
        },
    }
    path = tmp_path / "nodes" / "n0" / "proposal_selection_evidence.jsonl"
    path.write_text(json.dumps(evidence) + "\n", encoding="utf-8")
    metrics: dict = {"source_artifacts": []}
    _apply_groundhog_metrics(metrics, tmp_path)
    assert metrics["groundhog_proposal_deferred_event_count"] == 4
    assert metrics["groundhog_conflict_abort_count"] == 2
    assert metrics["groundhog_conflict_abort_rate"] == 0.2


def test_cg_cycle_retry_semantics_requires_eventual_logical_completion() -> None:
    semantics = _execution_semantics(
        {"routing": "hash_routing_baseline", "block_executor": "cg_block_executor"},
        "hash_cg",
    )
    assert semantics["comparison_semantics_class"] == "nezha_cg_johnson_retryable_v4"
    child = {
        "status": "completed",
        "execution_status": "completed",
        "comparison_semantics_class": "nezha_cg_johnson_retryable_v4",
        "metrics": {
            "submitted_unique_tx_count": 10,
            "terminal_unique_tx_count": 10,
            "finalized_unique_logical_tx_count": 10,
            "incomplete_unique_tx_count": 0,
            "cross_shard_failed_unique_count": 0,
            "abort_count": 2,
            "cg_cycle_abort_count": 2,
            "lifecycle_complete": True,
            "no_fallback": True,
            "state_root_consistent": True,
            "receipt_root_consistent": True,
            "plan_digest_consistent": True,
            "metric_completeness": "complete",
            "end_to_end_tps": 100.0,
            "logical_finality_tps": 100.0,
            "p95_finality_ms": 10.0,
            "p99_finality_ms": 12.0,
        },
        "result": {"summary": {"finality_evidence": {
            "submitted_unique_tx_count": 10,
            "terminal_unique_tx_count": 10,
            "finalized_unique_logical_tx_count": 10,
            "incomplete_unique_tx_count": 0,
            "cross_shard_failed_unique_count": 0,
            "lifecycle_complete": True,
        }}},
    }
    assert _state_equivalence_individual_reasons(child) == []
    assert _individual_result_reasons(child) == []


def test_cg_v2_smoke_terminal_cohort_remains_readable_after_v4() -> None:
    child = {
        "status": "completed",
        "execution_status": "completed",
        "comparison_semantics_class": "cg_cycle_abortable_v2",
        "metrics": {
            "submitted_unique_tx_count": 10,
            "terminal_unique_tx_count": 10,
            "finalized_unique_logical_tx_count": 8,
            "incomplete_unique_tx_count": 0,
            "cross_shard_failed_unique_count": 0,
            "abort_count": 2,
            "cg_cycle_abort_count": 2,
            "lifecycle_complete": True,
            "no_fallback": True,
            "state_root_consistent": True,
            "receipt_root_consistent": True,
            "plan_digest_consistent": True,
            "end_to_end_tps": 100.0,
            "logical_finality_tps": 100.0,
            "p95_finality_ms": 10.0,
            "p99_finality_ms": 12.0,
        },
        "result": {"summary": {"finality_evidence": {
            "submitted_unique_tx_count": 10,
            "terminal_unique_tx_count": 10,
            "finalized_unique_logical_tx_count": 8,
            "incomplete_unique_tx_count": 0,
            "cross_shard_failed_unique_count": 0,
            "lifecycle_complete": True,
        }}},
    }
    assert _state_equivalence_individual_reasons(child) == []
    assert _individual_result_reasons(child) == []

def test_cg_johnson_v1_terminal_cohort_remains_readable_after_v2() -> None:
    child = {
        "status": "completed",
        "execution_status": "completed",
        "comparison_semantics_class": "nezha_cg_johnson_abortable_v1",
        "metrics": {
            "submitted_unique_tx_count": 10,
            "terminal_unique_tx_count": 10,
            "finalized_unique_logical_tx_count": 8,
            "incomplete_unique_tx_count": 0,
            "cross_shard_failed_unique_count": 0,
            "abort_count": 2,
            "cg_cycle_abort_count": 2,
            "lifecycle_complete": True,
            "no_fallback": True,
            "state_root_consistent": True,
            "receipt_root_consistent": True,
            "plan_digest_consistent": True,
            "metric_completeness": "complete",
            "end_to_end_tps": 100.0,
            "logical_finality_tps": 100.0,
            "p95_finality_ms": 10.0,
            "p99_finality_ms": 12.0,
        },
        "result": {"summary": {"finality_evidence": {
            "submitted_unique_tx_count": 10,
            "terminal_unique_tx_count": 10,
            "finalized_unique_logical_tx_count": 8,
            "incomplete_unique_tx_count": 0,
            "cross_shard_failed_unique_count": 0,
            "lifecycle_complete": True,
        }}},
    }
    assert _state_equivalence_individual_reasons(child) == []
    assert _individual_result_reasons(child) == []

