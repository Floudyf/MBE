from __future__ import annotations

import json

from backend.app.services import v5_formal_scheduler, v5_metric_extractor


def _finality_metrics(*, submitted: int, terminal: int, finalized: int, incomplete: int = 0) -> dict:
    return {
        "submitted_unique_tx_count": submitted,
        "terminal_unique_tx_count": terminal,
        "finalized_unique_logical_tx_count": finalized,
        "incomplete_unique_tx_count": incomplete,
        "cross_shard_failed_unique_count": 0,
        "lifecycle_complete": terminal == submitted and incomplete == 0,
    }


def test_nezha_abortable_terminal_outcomes_are_individually_valid() -> None:
    item = {
        "status": "completed",
        "comparison_semantics_class": "nezha_acg_hs_abortable_v1",
        "metrics": {
            **_finality_metrics(submitted=10000, terminal=10000, finalized=7757),
            "abort_count": 2243,
            "nezha_hs_abort_count": 2243,
        },
        "result": {"summary": {}},
    }
    assert v5_formal_scheduler._state_equivalence_individual_reasons(item) == []


def test_non_abortable_semantics_still_require_full_finalization() -> None:
    item = {
        "status": "completed",
        "comparison_semantics_class": "stateful_local_v1",
        "metrics": _finality_metrics(submitted=10000, terminal=10000, finalized=7757),
        "result": {"summary": {}},
    }
    reasons = v5_formal_scheduler._state_equivalence_individual_reasons(item)
    assert "finalized_not_equal_submitted" in reasons


def test_acg_literature_metrics_export_explicit_hs_aborts(tmp_path) -> None:
    (tmp_path / "compiled_run_plan.json").write_text(
        json.dumps({"node_configs": [{"node_id": "n0", "shard_id": "s0", "leader": True}]}),
        encoding="utf-8",
    )
    node_dir = tmp_path / "nodes" / "n0"
    node_dir.mkdir(parents=True)
    (node_dir / "block_execution_summary.json").write_text(
        json.dumps({
            "node_id": "n0",
            "shard_id": "s0",
            "block_executor_id": "acg_block_executor",
            "blocks": [
                {
                    "worker_count": 8,
                    "maximum_parallel_width": 8,
                    "wave_count": 2,
                    "maximum_wave_width": 4,
                    "dependency_edge_count": 7,
                    "pairwise_conflict_check_count": 0,
                    "graph_color_count": 0,
                    "abort_count": 3,
                    "nezha_hs_abort_count": 3,
                    "transaction_execution_ms": 5,
                    "deterministic_materialization_ms": 1,
                    "state_commitment_ms": 1,
                },
                {
                    "worker_count": 8,
                    "maximum_parallel_width": 8,
                    "wave_count": 1,
                    "maximum_wave_width": 2,
                    "dependency_edge_count": 4,
                    "pairwise_conflict_check_count": 0,
                    "graph_color_count": 0,
                    "abort_count": 2,
                    "nezha_hs_abort_count": 2,
                    "transaction_execution_ms": 4,
                    "deterministic_materialization_ms": 1,
                    "state_commitment_ms": 1,
                },
            ],
        }),
        encoding="utf-8",
    )
    metrics = {"source_artifacts": []}
    v5_metric_extractor._apply_literature_graph_metrics(metrics, tmp_path)
    assert metrics["abort_count"] == 5
    assert metrics["nezha_hs_abort_count"] == 5


def test_groundhog_runtime_and_proposal_evidence_use_one_replica_per_shard(tmp_path) -> None:
    (tmp_path / "compiled_run_plan.json").write_text(
        json.dumps({"node_configs": [
            {"node_id": "n0", "shard_id": "s0", "leader": True},
            {"node_id": "n1", "shard_id": "s0", "leader": False},
        ]}),
        encoding="utf-8",
    )
    for node_id in ("n0", "n1"):
        node_dir = tmp_path / "nodes" / node_id
        node_dir.mkdir(parents=True)
        multiplier = 1 if node_id == "n0" else 100
        (node_dir / "block_execution_summary.json").write_text(
            json.dumps({
                "node_id": node_id,
                "shard_id": "s0",
                "block_executor_id": "groundhog_block_executor",
                "blocks": [{
                    "worker_count": 4,
                    "maximum_parallel_width": 4,
                    "groundhog_execution_attempt_count": 10 * multiplier,
                    "groundhog_reservation_count": 10 * multiplier,
                    "groundhog_constraint_conflict_count": 0,
                    "groundhog_reservation_rollback_count": 0,
                    "groundhog_integer_merge_count": 10 * multiplier,
                    "groundhog_bytes_merge_count": 0,
                    "groundhog_ordered_set_merge_count": 0,
                    "groundhog_modified_key_count": 3 * multiplier,
                    "groundhog_reservation_parallel_width": 4,
                    "groundhog_reservation_engine": "object_key_parallel_streaming_proposal_reserve_revert_commit",
                    "groundhog_fallback_mode": "disabled",
                    "groundhog_snapshot_semantics": "block_start_snapshot",
                    "groundhog_typed_modification_semantics": "groundhog_typed_commutative_v1",
                    "transaction_execution_ms": 2,
                    "deterministic_materialization_ms": 1,
                    "state_commitment_ms": 1,
                }],
            }),
            encoding="utf-8",
        )
        evidence = {
            "node_id": node_id,
            "shard_id": "s0",
            "height": 1,
            "algorithm_id": "groundhog_candidate_selection_v1",
            "payload_digest": f"digest-{node_id}",
            "payload": {
                "shard_id": "s0",
                "height": 1,
                "candidate_count": 12 * multiplier,
                "selected_count": 10 * multiplier,
                "deferred_count": 2 * multiplier,
                "deferred_tx_ids": ["d1", "d2"],
                "metrics": {
                    "reservation_count": 10 * multiplier,
                    "constraint_conflict_count": 2 * multiplier,
                    "reservation_rollback_count": 2 * multiplier,
                },
            },
        }
        (node_dir / "proposal_selection_evidence.jsonl").write_text(json.dumps(evidence) + "\n", encoding="utf-8")

    metrics = {"source_artifacts": []}
    v5_metric_extractor._apply_groundhog_metrics(metrics, tmp_path)
    assert metrics["groundhog_metrics_available"] is True
    assert metrics["groundhog_execution_attempt_count"] == 10
    assert metrics["groundhog_reservation_count"] == 10
    assert metrics["groundhog_proposal_evidence_available"] is True
    assert metrics["groundhog_proposal_candidate_count"] == 12
    assert metrics["groundhog_proposal_selected_count"] == 10
    assert metrics["groundhog_proposal_deferred_event_count"] == 2
    assert metrics["groundhog_proposal_constraint_conflict_count"] == 2
    assert metrics["groundhog_proposal_unique_deferred_tx_count"] == 2
    assert metrics["groundhog_reservation_engine"] == "object_key_parallel_streaming_proposal_reserve_revert_commit"


def test_groundhog_metric_completeness_requires_runtime_truth_fields() -> None:
    metrics = {
        "missing": [],
        "block_executor_id": "groundhog_block_executor",
        "end_to_end_tps": 1.0,
        "logical_finality_tps": 1.0,
        "p95_finality_ms": 1,
        "p99_finality_ms": 1,
        "submitted_unique_tx_count": 1,
        "terminal_unique_tx_count": 1,
        "state_root_consistent": True,
        "receipt_root_consistent": True,
        "plan_digest_consistent": True,
        "no_fallback": True,
    }
    v5_metric_extractor._apply_metric_completeness(metrics, method_id="hash_groundhog")
    assert "metric:groundhog_metrics_available" in metrics["metric_missing"]
    assert "groundhog_proposal_evidence_available" in metrics["metric_required"]


def test_acg_metric_completeness_requires_explicit_nezha_abort_evidence() -> None:
    metrics = {
        "missing": [],
        "block_executor_id": "acg_block_executor",
        "end_to_end_tps": 1.0,
        "logical_finality_tps": 1.0,
        "p95_finality_ms": 1,
        "p99_finality_ms": 1,
        "submitted_unique_tx_count": 1,
        "terminal_unique_tx_count": 1,
        "state_root_consistent": True,
        "receipt_root_consistent": True,
        "plan_digest_consistent": True,
        "no_fallback": True,
        "worker_count": 1,
        "maximum_parallel_width": 1,
        "wave_count": 1,
        "maximum_wave_width": 1,
        "dependency_edge_count": 0,
        "transaction_execution_ms": 1,
        "deterministic_materialization_ms": 1,
        "abort_count": 0,
    }
    v5_metric_extractor._apply_metric_completeness(metrics, method_id="hash_acg")
    assert "metric:nezha_hs_abort_count" in metrics["metric_missing"]
    assert metrics["metric_statuses"]["nezha_hs_abort_count"] == "missing"
