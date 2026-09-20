from __future__ import annotations

import json
from pathlib import Path

from backend.app.services.v5_metric_extractor import extract


def test_v12_metric_extractor_preserves_effective_frontier_evidence(tmp_path: Path) -> None:
    cluster = {
        "block_executor_id": "metatrack_block_executor",
        "block_executor_consistent": True,
        "plan_digest_consistent": True,
        "state_root_consistent": True,
        "receipt_root_consistent": True,
        "orphan_process_count": 0,
        "no_fallback": True,
        "fast_track_count": 953,
        "conservative_track_count": 47,
        "metatrack_effective_frontier_policy": "inflight_exact_value_transitive_reduction_v1",
        "metatrack_effective_frontier_width_zero_count": 100,
        "metatrack_effective_frontier_width_one_count": 853,
        "metatrack_effective_frontier_width_multi_count": 47,
        "metatrack_effective_frontier_width_max": 2,
        "metatrack_effective_frontier_raw_producer_count": 712,
        "metatrack_effective_frontier_reduced_producer_count": 219,
        "metatrack_effective_frontier_track_demotion_count": 47,
        "metatrack_effective_frontier_truth_scope": "replica_deduplicated_by_shard",
    }
    (tmp_path / "real_cluster_summary.json").write_text(json.dumps(cluster), encoding="utf-8")
    (tmp_path / "finality_summary.json").write_text(
        json.dumps(
            {
                "logical_transaction_count": 1000,
                "submitted_unique_tx_count": 1000,
                "finalized_unique_logical_tx_count": 1000,
                "incomplete_unique_tx_count": 0,
                "throughput_tps": 100.0,
                "logical_window_start_ms": 1,
                "logical_window_end_ms": 10001,
                "logical_finality_duration_ms": 10000,
                "logical_finality_tps": 100.0,
                "drain_started_at_ms": 10001,
                "drain_finished_at_ms": 10001,
                "drain_duration_ms": 0,
                "system_delta_drain_block_count": 0,
                "completion_window_start_ms": 1,
                "completion_window_end_ms": 10001,
                "completion_duration_ms": 10000,
                "end_to_end_tps": 100.0,
                "tail_completion_overhead_ms": 0,
                "p95_finality_ms": 10,
                "p99_finality_ms": 20,
            }
        ),
        encoding="utf-8",
    )
    for name in (
        "transaction_lifecycle.jsonl",
        "transaction_finality.csv",
        "client_receipt_log.csv",
        "drain_status.json",
        "throughput_windows.csv",
        "metatrack_batch_plan.jsonl",
        "dependency_graph.csv",
        "track_classification.csv",
        "predicted_remote_access.csv",
        "aggregation_plan.csv",
        "logical_physical_update_mapping.csv",
    ):
        (tmp_path / name).write_text("", encoding="utf-8")

    metrics = extract(tmp_path)

    assert metrics["metatrack_effective_frontier_policy"] == "inflight_exact_value_transitive_reduction_v1"
    assert metrics["metatrack_effective_frontier_width_zero_count"] == 100
    assert metrics["metatrack_effective_frontier_width_one_count"] == 853
    assert metrics["metatrack_effective_frontier_width_multi_count"] == 47
    assert metrics["metatrack_effective_frontier_width_max"] == 2
    assert metrics["metatrack_effective_frontier_raw_producer_count"] == 712
    assert metrics["metatrack_effective_frontier_reduced_producer_count"] == 219
    assert metrics["metatrack_effective_frontier_track_demotion_count"] == 47
    assert metrics["metatrack_effective_frontier_truth_scope"] == "replica_deduplicated_by_shard"
