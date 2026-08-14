import json
from pathlib import Path

from backend.app.services.v5_metric_extractor import _apply_aria_metrics


def test_aria_metrics_are_extracted_from_consensus_bound_proposal_evidence(tmp_path: Path):
    (tmp_path / "compiled_run_plan.json").write_text(json.dumps({
        "node_configs": [{"node_id": "n0", "shard_id": "s0", "leader": True, "role": "leader"}]
    }), encoding="utf-8")
    node_dir = tmp_path / "nodes" / "n0"
    node_dir.mkdir(parents=True)
    (node_dir / "block_execution_summary.json").write_text(json.dumps({"node_id": "n0", "shard_id": "s0", "block_executor_id": "aria_block_executor", "blocks": []}), encoding="utf-8")
    payload = {
        "shard_id": "s0",
        "height": 1,
        "metrics": {
            "worker_count": 8,
            "epoch_count": 1,
            "maximum_epoch_width": 100,
            "maximum_parallel_width": 8,
            "execution_attempt_count": 100,
            "committed_transaction_count": 90,
            "finalized_transaction_count": 90,
            "conflict_abort_count": 10,
            "reexecution_count": 0,
            "retryable_nonce_count": 0,
            "waw_dependency_count": 4,
            "raw_dependency_count": 5,
            "war_dependency_count": 6,
            "read_reservation_count": 7,
            "write_reservation_count": 8,
            "read_only_fast_commit_count": 9,
            "application_failure_count": 0,
            "candidate_transaction_count": 100,
            "selected_transaction_count": 90,
            "deferred_transaction_count": 10,
            "fallback_mode": "disabled",
            "batch_lifecycle": "one_consensus_block_per_aria_batch",
            "transaction_execution_ms": 11,
            "deterministic_materialization_ms": 12,
            "state_commitment_ms": 13,
        },
    }
    row = {"algorithm_id": "aria_candidate_selection_v2", "payload_digest": "abc", "payload": payload}
    (node_dir / "proposal_selection_evidence.jsonl").write_text(json.dumps(row) + "\n", encoding="utf-8")
    metrics = {"source_artifacts": []}
    _apply_aria_metrics(metrics, tmp_path)
    assert metrics["aria_metrics_available"] is True
    assert metrics["worker_count"] == 8
    assert metrics["maximum_parallel_width"] == 8
    assert metrics["aria_candidate_transaction_count"] == 100
    assert metrics["aria_selected_transaction_count"] == 90
    assert metrics["aria_deferred_transaction_count"] == 10
    assert metrics["aria_conflict_abort_count"] == 10
    assert metrics["aria_waw_dependency_count"] == 4
    assert metrics["aria_raw_dependency_count"] == 5
    assert metrics["aria_war_dependency_count"] == 6
    assert metrics["aria_fallback_mode"] == "disabled"
