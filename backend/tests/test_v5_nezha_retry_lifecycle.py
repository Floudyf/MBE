from __future__ import annotations

import json

from backend.app.services import v5_formal_scheduler, v5_metric_extractor, v5_paper_exporter


def _retryable_child(*, submitted: int = 10000, terminal: int = 10000, finalized: int = 10000) -> dict:
    return {
        "status": "completed",
        "execution_status": "completed",
        "artifact_status": "complete",
        "method_config_id": "hash_acg",
        "comparison_semantics_class": "nezha_acg_hs_retryable_v2",
        "formal_eligibility": True,
        "paper_candidate": True,
        "comparison_eligibility_status": "passed",
        "metrics": {
            "end_to_end_tps": 100.0,
            "logical_finality_tps": 100.0,
            "p95_finality_ms": 10,
            "p99_finality_ms": 20,
            "submitted_unique_tx_count": submitted,
            "terminal_unique_tx_count": terminal,
            "finalized_unique_logical_tx_count": finalized,
            "incomplete_unique_tx_count": submitted - terminal,
            "cross_shard_failed_unique_count": 0,
            "lifecycle_complete": terminal == submitted,
            "no_fallback": True,
            "state_root_consistent": True,
            "receipt_root_consistent": True,
            "plan_digest_consistent": True,
            "abort_count": 920,
            "nezha_hs_abort_count": 920,
        },
        "result": {"summary": {"finality_evidence": {
            "submitted_unique_tx_count": submitted,
            "terminal_unique_tx_count": terminal,
            "finalized_unique_logical_tx_count": finalized,
            "incomplete_unique_tx_count": submitted - terminal,
            "cross_shard_failed_unique_count": 0,
        }}},
    }


def test_retryable_acg_requires_eventual_full_finalization() -> None:
    child = _retryable_child()
    assert v5_formal_scheduler._state_equivalence_individual_reasons(child) == []
    assert v5_paper_exporter._individual_result_reasons(child) == []

    incomplete = _retryable_child(finalized=9999)
    assert "finalized_not_equal_submitted" in v5_formal_scheduler._state_equivalence_individual_reasons(incomplete)
    assert "finalized_not_equal_submitted" in v5_paper_exporter._individual_result_reasons(incomplete)


def test_historical_terminal_abort_semantics_remain_readable() -> None:
    historical = _retryable_child(finalized=9080)
    historical["comparison_semantics_class"] = "nezha_acg_hs_abortable_v1"
    historical["metrics"]["abort_count"] = 920
    historical["metrics"]["nezha_hs_abort_count"] = 920
    assert v5_formal_scheduler._state_equivalence_individual_reasons(historical) == []
    assert v5_paper_exporter._individual_result_reasons(historical) == []


def test_acg_retry_metrics_keep_attempt_and_unique_deferral_evidence(tmp_path) -> None:
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
                    "nezha_hs_candidate_transaction_count": 10,
                    "nezha_hs_accepted_transaction_count": 8,
                    "abort_count": 2,
                    "nezha_hs_abort_count": 2,
                    "nezha_hs_deferred_retry_count": 2,
                    "nezha_hs_deferred_tx_ids": ["a", "b"],
                    "transaction_execution_ms": 5,
                    "deterministic_materialization_ms": 1,
                    "state_commitment_ms": 1,
                },
                {
                    "worker_count": 8,
                    "maximum_parallel_width": 2,
                    "wave_count": 1,
                    "maximum_wave_width": 2,
                    "dependency_edge_count": 1,
                    "pairwise_conflict_check_count": 0,
                    "graph_color_count": 0,
                    "nezha_hs_candidate_transaction_count": 2,
                    "nezha_hs_accepted_transaction_count": 1,
                    "abort_count": 1,
                    "nezha_hs_abort_count": 1,
                    "nezha_hs_deferred_retry_count": 1,
                    "nezha_hs_deferred_tx_ids": ["a"],
                    "transaction_execution_ms": 2,
                    "deterministic_materialization_ms": 1,
                    "state_commitment_ms": 1,
                },
            ],
        }),
        encoding="utf-8",
    )
    metrics = {"source_artifacts": [], "submitted_unique_tx_count": 10}
    v5_metric_extractor._apply_literature_graph_metrics(metrics, tmp_path)
    assert metrics["nezha_hs_abort_decision_count"] == 3
    assert metrics["nezha_hs_candidate_transaction_count"] == 12
    assert metrics["nezha_hs_attempt_abort_rate"] == 3 / 12
    assert metrics["nezha_hs_unique_deferred_tx_count"] == 2
    assert metrics["nezha_hs_unique_deferred_rate"] == 2 / 10
    assert metrics["nezha_hs_retry_lifecycle"] == "fifo_deferred_to_later_block"
