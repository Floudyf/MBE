from __future__ import annotations

import csv
import json
from pathlib import Path

from backend.app.services.v5_metric_extractor import _apply_batch_si_metrics, _read_scheduler_metrics, extract


def test_batch_si_scheduler_metrics_count_unique_candidate_deferrals(tmp_path: Path) -> None:
    path = tmp_path / "metatrack_scheduler_trace.csv"
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=[
            "block_height", "tx_id", "decision_reason", "blocked", "wakeup", "stolen_work", "local_execution",
            "queue_name", "ready_queue_depth", "fast_queue_depth", "conservative_queue_depth",
            "dependency_wait_ms", "scheduler_idle_ms",
        ])
        writer.writeheader()
        writer.writerows([
            {"block_height": "7", "tx_id": "T1", "decision_reason": "planned:batch_si_accepted", "blocked": "false", "local_execution": "true"},
            {"block_height": "7", "tx_id": "T2", "decision_reason": "planned:batch_si_accepted", "blocked": "false", "local_execution": "true"},
            {"block_height": "7", "tx_id": "T3", "decision_reason": "planned:batch_si_ofas_cycle_deferred", "blocked": "true", "local_execution": "true"},
            # Replica/aggregation duplicates at the same height must not inflate the count.
            {"block_height": "7", "tx_id": "T3", "decision_reason": "planned:batch_si_ofas_cycle_deferred", "blocked": "true", "local_execution": "true"},
            # A retry deferred again in another block is a distinct planning decision.
            {"block_height": "8", "tx_id": "T3", "decision_reason": "planned:batch_si_ofas_cycle_deferred", "blocked": "true", "local_execution": "true"},
        ])
    metrics = _read_scheduler_metrics(path)
    assert metrics["batch_si_accepted_transaction_count"] == 2
    assert metrics["batch_si_deferred_transaction_count"] == 2
    assert metrics["deferred_transaction_count"] == 2
    assert metrics["batch_si_abort_rate"] == 2 / 4


def test_batch_si_block_metrics_use_one_leader_per_shard(tmp_path: Path) -> None:
    plan = {
        "node_configs": [
            {"node_id": "n0", "shard_id": "s0", "leader": True, "role": "leader"},
            {"node_id": "n1", "shard_id": "s0", "leader": False, "role": "validator"},
        ]
    }
    (tmp_path / "compiled_run_plan.json").write_text(json.dumps(plan), encoding="utf-8")
    for node_id in ("n0", "n1"):
        directory = tmp_path / "nodes" / node_id
        directory.mkdir(parents=True)
        (directory / "block_execution_summary.json").write_text(json.dumps({
            "node_id": node_id,
            "shard_id": "s0",
            "block_executor_id": "batch_si_block_executor",
            "blocks": [{
                "configured_worker_count": 4,
                "maximum_parallel_width": 3,
                "batch_count": 2,
                "maximum_batch_width": 3,
                "write_opportunity_reuse_count": 1,
                "dependency_edge_count": 4,
                "deferred_transaction_count": 0,
                "abort_count": 0,
                "planning_iteration_count": 1,
                "batch_snapshot_count": 2,
                "batch_snapshot_create_ms": 5,
                "transaction_execution_ms": 8,
                "deterministic_materialization_ms": 2,
            }],
        }), encoding="utf-8")
    metrics = {"source_artifacts": []}
    _apply_batch_si_metrics(metrics, tmp_path)
    assert metrics["batch_count"] == 2
    assert metrics["dependency_edge_count"] == 4
    assert metrics["maximum_parallel_width"] == 3
    assert metrics["configured_worker_count"] == 4
    assert metrics["deferred_transaction_count"] == 0
    assert metrics["abort_count"] == 0
    assert metrics["batch_si_cross_scheme_algorithm_reuse"] is False
    assert metrics["source_artifacts"] == ["nodes/n0/block_execution_summary.json"]


def test_batch_si_block_metrics_sum_deferred_transactions_across_committed_blocks(tmp_path: Path) -> None:
    plan = {"node_configs": [{"node_id": "n0", "shard_id": "s0", "leader": True, "role": "leader"}]}
    (tmp_path / "compiled_run_plan.json").write_text(json.dumps(plan), encoding="utf-8")
    directory = tmp_path / "nodes" / "n0"
    directory.mkdir(parents=True)
    (directory / "block_execution_summary.json").write_text(json.dumps({
        "node_id": "n0",
        "shard_id": "s0",
        "block_executor_id": "batch_si_block_executor",
        "blocks": [
            {"configured_worker_count": 4, "deferred_transaction_count": 2, "abort_count": 2},
            # Old summaries may only expose abort_count.
            {"configured_worker_count": 4, "abort_count": 1},
            # A real zero remains available rather than being treated as missing.
            {"configured_worker_count": 4, "deferred_transaction_count": 0, "abort_count": 0},
        ],
    }), encoding="utf-8")

    metrics = {"source_artifacts": []}
    _apply_batch_si_metrics(metrics, tmp_path)

    assert metrics["deferred_transaction_count"] == 3
    assert metrics["abort_count"] == 3


def test_batch_si_zero_deferral_is_a_complete_paper_metric_sample(tmp_path: Path) -> None:
    finality = {
        "logical_window_start_ms": 1000,
        "logical_window_end_ms": 2000,
        "logical_finality_duration_ms": 1000,
        "logical_finality_tps": 100.0,
        "drain_started_at_ms": 2000,
        "drain_finished_at_ms": 2010,
        "drain_duration_ms": 10,
        "system_delta_drain_block_count": 0,
        "completion_window_start_ms": 1000,
        "completion_window_end_ms": 2010,
        "completion_duration_ms": 1010,
        "end_to_end_tps": 99.0099009901,
        "throughput_tps": 99.0099009901,
        "tail_completion_overhead_ms": 10,
        "submitted_unique_tx_count": 100,
        "terminal_unique_tx_count": 100,
        "logical_transaction_count": 100,
        "finalized_unique_logical_tx_count": 100,
        "p50_finality_ms": 10,
        "p95_finality_ms": 20,
        "p99_finality_ms": 30,
    }
    cluster = {
        "block_executor_id": "batch_si_block_executor",
        "block_executor_consistent": True,
        "plan_digest_consistent": True,
        "state_root_consistent": True,
        "receipt_root_consistent": True,
        "orphan_process_count": 0,
        "no_fallback": True,
        "artifact_contract_status": "complete",
        "missing_expected_artifacts": [],
        "unexpected_artifacts": [],
    }
    (tmp_path / "finality_summary.json").write_text(json.dumps(finality), encoding="utf-8")
    (tmp_path / "real_cluster_summary.json").write_text(json.dumps(cluster), encoding="utf-8")
    for name in (
        "transaction_lifecycle.jsonl",
        "transaction_finality.csv",
        "client_receipt_log.csv",
        "drain_status.json",
        "throughput_windows.csv",
    ):
        (tmp_path / name).write_text("", encoding="utf-8")
    (tmp_path / "compiled_run_plan.json").write_text(json.dumps({
        "node_configs": [{"node_id": "n0", "shard_id": "s0", "leader": True, "role": "leader"}],
    }), encoding="utf-8")
    node_dir = tmp_path / "nodes" / "n0"
    node_dir.mkdir(parents=True)
    (node_dir / "block_execution_summary.json").write_text(json.dumps({
        "node_id": "n0",
        "shard_id": "s0",
        "block_executor_id": "batch_si_block_executor",
        "blocks": [{
            "configured_worker_count": 4,
            "maximum_parallel_width": 4,
            "batch_count": 3,
            "maximum_batch_width": 40,
            "write_opportunity_reuse_count": 9,
            "dependency_edge_count": 12,
            "deferred_transaction_count": 0,
            "abort_count": 0,
            "batch_snapshot_count": 3,
            "batch_snapshot_create_ms": 1,
        }],
    }), encoding="utf-8")

    metrics = extract(tmp_path, method_id="hash_batch_si")

    assert metrics["deferred_transaction_count"] == 0
    assert metrics["metric_statuses"]["deferred_transaction_count"] == "available"
    assert metrics["metric_missing"] == []
    assert metrics["metric_completeness"] == "complete"
