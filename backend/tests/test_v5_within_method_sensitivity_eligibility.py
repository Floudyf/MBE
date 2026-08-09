from __future__ import annotations

from backend.app.services.v5_paper_exporter import _group_rows, _sample_status_for_child


def _batch_si_child(*, eligibility_status: str = "insufficient_valid_runs") -> dict:
    metrics = {
        "end_to_end_tps": 321.5,
        "logical_finality_tps": 330.0,
        "p50_finality_ms": 10.0,
        "p95_finality_ms": 20.0,
        "p99_finality_ms": 30.0,
        "submitted_unique_tx_count": 1000,
        "terminal_unique_tx_count": 1000,
        "finalized_unique_logical_tx_count": 1000,
        "incomplete_unique_tx_count": 0,
        "cross_shard_failed_unique_count": 0,
        "lifecycle_complete": True,
        "no_fallback": True,
        "state_root_consistent": True,
        "receipt_root_consistent": True,
        "plan_digest_consistent": True,
        "metric_completeness": "complete",
        "configured_worker_count": 4,
        "maximum_parallel_width": 4,
        "batch_count": 10,
        "maximum_batch_width": 4,
        "write_opportunity_reuse_count": 2,
        "dependency_edge_count": 3,
        "deferred_transaction_count": 7,
        "batch_snapshot_create_ms": 1,
    }
    finality = {
        "submitted_unique_tx_count": 1000,
        "terminal_unique_tx_count": 1000,
        "finalized_unique_logical_tx_count": 1000,
        "incomplete_unique_tx_count": 0,
        "cross_shard_requested_unique_count": 0,
        "cross_shard_finalized_unique_count": 0,
        "cross_shard_refunded_unique_count": 0,
        "cross_shard_failed_unique_count": 0,
        "lifecycle_complete": True,
    }
    return {
        "child_run_id": "child-theta-08",
        "suite_type": "workload_sensitivity",
        "method_config_id": "hash_batch_si",
        "method": {"display_name": "Batch-SI", "plugin_overrides": {"block_executor": "batch_si_block_executor"}},
        "method_role": "baseline",
        "scan_variable": "target_theta",
        "scan_value": "0.8",
        "workload_point": {"tx_count": 1000, "target_theta": 0.8},
        "topology_point": {"nodes": 8, "shards": 1, "validators_per_shard": 8, "worker_count": 4},
        "fault_point": {"mode": "disabled"},
        "estimated_transactions": 1000,
        "block_size": 100,
        "block_interval_ms": 75,
        "status": "completed",
        "execution_status": "completed",
        "artifact_status": "complete",
        "formal_eligibility": True,
        "paper_candidate": False,
        "comparison_eligibility_status": eligibility_status,
        "metrics": metrics,
        "result": {"summary": {"finality_evidence": finality, "no_fallback": True, "state_root_consistent": True, "receipt_root_consistent": True, "plan_digest_consistent": True}},
    }


def test_single_method_sensitivity_keeps_individually_valid_insufficient_cohort_sample() -> None:
    child = _batch_si_child()
    assert _sample_status_for_child(child) == "comparison_excluded"
    rows = _group_rows({}, [child])
    assert len(rows) == 1
    assert rows[0]["suite_type"] == "workload_sensitivity"
    assert rows[0]["sample_count"] == 1
    assert rows[0]["mean_tps"] == 321.5


def test_sensitivity_does_not_open_other_comparison_exclusion_boundaries() -> None:
    child = _batch_si_child(eligibility_status="hybrid_boundary")
    rows = _group_rows({}, [child])
    assert len(rows) == 1
    assert rows[0]["sample_count"] == 0
    assert rows[0]["mean_tps"] is None


def test_within_method_sensitivity_does_not_change_raw_paper_classification() -> None:
    child = _batch_si_child()
    rows = _group_rows({}, [child])
    assert rows[0]["sample_count"] == 1
    # Direct paper comparison eligibility remains strict and unchanged.
    assert _sample_status_for_child(child) == "comparison_excluded"
