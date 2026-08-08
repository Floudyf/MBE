from __future__ import annotations

import csv
import json
from pathlib import Path
from typing import Any

from backend.app.services.v5_metric_truth import summarize_remote_operations

FINALITY_REQUIRED_FIELDS = [
    "logical_window_start_ms",
    "logical_window_end_ms",
    "logical_finality_duration_ms",
    "logical_finality_tps",
    "drain_started_at_ms",
    "drain_finished_at_ms",
    "drain_duration_ms",
    "system_delta_drain_block_count",
    "completion_window_start_ms",
    "completion_window_end_ms",
    "completion_duration_ms",
    "end_to_end_tps",
    "tail_completion_overhead_ms",
]

COMMON_REQUIRED_METRICS = [
    "end_to_end_tps",
    "logical_finality_tps",
    "p95_finality_ms",
    "p99_finality_ms",
    "submitted_unique_tx_count",
    "terminal_unique_tx_count",
    "state_root_consistent",
    "receipt_root_consistent",
    "plan_digest_consistent",
    "no_fallback",
]

BLOCK_STM_REQUIRED_METRICS = [
    "worker_count",
    "maximum_parallel_width",
    "abort_count",
    "reexecution_count",
    "validation_failure_count",
    "serial_equivalent",
]

METATRACK_REQUIRED_METRICS = [
    "fast_track_logical_tx_count",
    "conservative_track_logical_tx_count",
    "replica_deduplicated_remote_fetch_count",
    "replica_deduplicated_remote_writeback_count",
    "aggregation_group_count",
    "pre_aggregation_physical_op_count",
    "post_aggregation_physical_op_count",
]

BATCH_SI_REQUIRED_METRICS = [
    "configured_worker_count",
    "maximum_parallel_width",
    "batch_count",
    "maximum_batch_width",
    "write_opportunity_reuse_count",
    "dependency_edge_count",
    "deferred_transaction_count",
    "batch_snapshot_create_ms",
]


def extract(run_dir: Path, method_id: str | None = None) -> dict:
    summary_path = run_dir / "real_cluster_summary.json"
    finality_path = run_dir / "finality_summary.json"
    if not summary_path.is_file() or not finality_path.is_file():
        return {
            "missing": [
                name
                for name, path in {
                    "real_cluster_summary.json": summary_path,
                    "finality_summary.json": finality_path,
                }.items()
                if not path.is_file()
            ]
        }

    cluster = _read_json(summary_path)
    finality = _read_json(finality_path)
    required_artifacts = [
        "transaction_lifecycle.jsonl",
        "transaction_finality.csv",
        "client_receipt_log.csv",
        "finality_summary.json",
        "real_cluster_summary.json",
        "drain_status.json",
        "throughput_windows.csv",
    ]
    missing = [name for name in required_artifacts if not (run_dir / name).is_file()]
    missing.extend(f"finality_summary.json:{field}" for field in FINALITY_REQUIRED_FIELDS if field not in finality)
    if finality.get("throughput_tps") != finality.get("end_to_end_tps"):
        missing.append("finality_summary.json:throughput_tps_must_equal_end_to_end_tps")

    submitted = finality.get("submitted_unique_tx_count", finality.get("logical_transaction_count"))
    terminal = finality.get("terminal_unique_tx_count", finality.get("finalized_unique_logical_tx_count"))
    p95 = finality.get("p95_finality_ms")
    p99 = finality.get("p99_finality_ms")
    metrics: dict[str, Any] = {
        "finalized_tx_count": finality.get("finalized_unique_logical_tx_count"),
        "submitted_unique_tx_count": submitted,
        "terminal_unique_tx_count": terminal,
        "throughput_tps": finality.get("throughput_tps"),
        "logical_finality_tps": finality.get("logical_finality_tps"),
        "end_to_end_tps": finality.get("end_to_end_tps"),
        "logical_window_start_ms": finality.get("logical_window_start_ms"),
        "logical_window_end_ms": finality.get("logical_window_end_ms"),
        "logical_finality_duration_ms": finality.get("logical_finality_duration_ms"),
        "drain_started_at_ms": finality.get("drain_started_at_ms"),
        "drain_finished_at_ms": finality.get("drain_finished_at_ms"),
        "drain_duration_ms": finality.get("drain_duration_ms"),
        "system_delta_drain_block_count": finality.get("system_delta_drain_block_count"),
        "completion_window_start_ms": finality.get("completion_window_start_ms"),
        "completion_window_end_ms": finality.get("completion_window_end_ms"),
        "completion_duration_ms": finality.get("completion_duration_ms"),
        "tail_completion_overhead_ms": finality.get("tail_completion_overhead_ms"),
        "p50_latency_ms": finality.get("p50_finality_ms"),
        "p95_latency_ms": p95,
        "p99_latency_ms": p99,
        "p95_finality_ms": p95,
        "p99_finality_ms": p99,
        "block_executor_id": cluster.get("block_executor_id"),
        "block_executor_consistent": cluster.get("block_executor_consistent"),
        "plan_digest_consistent": cluster.get("plan_digest_consistent"),
        "state_root_consistent": cluster.get("state_root_consistent"),
        "receipt_root_consistent": cluster.get("receipt_root_consistent"),
        "orphan_process_count": cluster.get("orphan_process_count"),
        "no_fallback": cluster.get("no_fallback"),
        "configured_block_size": cluster.get("configured_block_size"),
        "configured_block_interval_ms": cluster.get("configured_block_interval_ms"),
        "actual_committed_block_count": cluster.get("actual_committed_block_count"),
        "actual_average_tx_per_block": cluster.get("actual_average_tx_per_block"),
        "actual_min_tx_per_block": cluster.get("actual_min_tx_per_block"),
        "actual_max_tx_per_block": cluster.get("actual_max_tx_per_block"),
        "actual_block_interval_mean_ms": cluster.get("actual_block_interval_mean_ms"),
        "actual_block_interval_p95_ms": cluster.get("actual_block_interval_p95_ms"),
        "lifecycle_complete": finality.get("logical_transaction_count") == finality.get("finalized_unique_logical_tx_count"),
        "fast_track_count": cluster.get("fast_track_count"),
        "conservative_track_count": cluster.get("conservative_track_count"),
        "aggregation_group_count": cluster.get("aggregation_group_count"),
        "logical_update_count": cluster.get("logical_update_count"),
        "physical_update_count": cluster.get("physical_update_count"),
        "logical_update_count_deprecated": cluster.get("logical_update_count_deprecated"),
        "physical_update_count_deprecated": cluster.get("physical_update_count_deprecated"),
        "executed_logical_transaction_count": cluster.get("executed_logical_transaction_count"),
        "executed_transaction_instance_count": cluster.get("executed_transaction_instance_count"),
        "pre_aggregation_physical_op_count": cluster.get("pre_aggregation_physical_op_count"),
        "post_aggregation_physical_op_count": cluster.get("post_aggregation_physical_op_count"),
        "aggregated_key_count": cluster.get("aggregated_key_count"),
        "aggregated_logical_delta_count": cluster.get("aggregated_logical_delta_count"),
        "physical_ops_saved_count": cluster.get("physical_ops_saved_count"),
        "aggregation_reduction_ratio": cluster.get("aggregation_reduction_ratio"),
        "scheduler_event_count": cluster.get("scheduler_event_count"),
        "scheduler_blocked_count": cluster.get("scheduler_blocked_count"),
        "scheduler_wakeup_count": cluster.get("scheduler_wakeup_count"),
        "scheduler_stolen_work_count": cluster.get("scheduler_stolen_work_count"),
        "scheduler_local_execution_count": cluster.get("scheduler_local_execution_count"),
        "scheduler_ready_queue_max_depth": cluster.get("scheduler_ready_queue_max_depth"),
        "scheduler_fast_queue_max_depth": cluster.get("scheduler_fast_queue_max_depth"),
        "scheduler_conservative_queue_max_depth": cluster.get("scheduler_conservative_queue_max_depth"),
        "scheduler_dependency_wait_ms": cluster.get("scheduler_dependency_wait_ms"),
        "scheduler_idle_ms": cluster.get("scheduler_idle_ms"),
        "scheduler_idle_ratio": cluster.get("scheduler_idle_ratio"),
        "remote_state_access_count": cluster.get("remote_state_access_count"),
        "remote_state_read_count": cluster.get("remote_state_read_count"),
        "remote_state_write_apply_count": cluster.get("remote_state_write_apply_count"),
        "remote_state_access_failed_count": cluster.get("remote_state_access_failed_count"),
        "remote_state_access_avg_latency_ms": cluster.get("remote_state_access_avg_latency_ms"),
        "global_business_state_digest": cluster.get("global_business_state_digest"),
        "metatrack_execution_shard_transaction_counts": cluster.get("metatrack_execution_shard_transaction_counts"),
        "metatrack_execution_shard_count": cluster.get("metatrack_execution_shard_count"),
        "metatrack_max_execution_shard_share": cluster.get("metatrack_max_execution_shard_share"),
        "metatrack_predicted_remote_read_count": cluster.get("metatrack_predicted_remote_read_count"),
        "metatrack_predicted_remote_write_count": cluster.get("metatrack_predicted_remote_write_count"),
        "metatrack_placement_reason_counts": cluster.get("metatrack_placement_reason_counts"),
        "metatrack_placement_score_row_count": cluster.get("metatrack_placement_score_row_count"),
        "state_ready_wait_count": cluster.get("metatrack_state_ready_wait_count"),
        "state_ready_resume_count": cluster.get("metatrack_state_ready_resume_count"),
        "state_prefetch_wait_ms": cluster.get("metatrack_state_prefetch_wait_ms"),
        "remote_state_fetch_count": cluster.get("metatrack_remote_state_fetch_count"),
        "remote_state_fetch_completed_count": cluster.get("metatrack_remote_state_fetch_completed_count"),
        "state_ready_scheduler_mode": cluster.get("metatrack_state_ready_scheduler_mode"),
        "versioned_state_ready_wave_count": cluster.get("versioned_state_ready_wave_count"),
        "versioned_state_ready_wait_observation_count": cluster.get("versioned_state_ready_wait_observation_count"),
        "versioned_state_ready_resolved_token_count": cluster.get("versioned_state_ready_resolved_token_count"),
        "versioned_state_probe_count": cluster.get("versioned_state_probe_count"),
        "versioned_state_probe_latency_ms": cluster.get("versioned_state_probe_latency_ms"),
        "versioned_state_ready_max_wave_width": cluster.get("versioned_state_ready_max_wave_width"),
        "versioned_state_ready_scheduler_mode": cluster.get("versioned_state_ready_scheduler_mode"),
        "source_artifacts": list(required_artifacts),
        "missing": missing,
    }
    _apply_artifact_contract(metrics, cluster)

    _apply_block_stm_metrics(metrics, run_dir)
    _apply_batch_si_metrics(metrics, run_dir)
    _apply_metatrack_artifacts(metrics, run_dir)
    _apply_mechanism_metrics(metrics, run_dir)

    logical_tx_count = _int(finality.get("submitted_unique_tx_count") or finality.get("logical_transaction_count"))
    remote_state_metrics = _read_remote_state_metrics(_remote_state_operations_path(run_dir), logical_tx_count=logical_tx_count)
    if remote_state_metrics:
        metrics.update(remote_state_metrics)

    scheduler_metrics = _read_scheduler_metrics(run_dir / "metatrack_scheduler_trace.csv")
    if scheduler_metrics:
        metrics.update(scheduler_metrics)
        if str(method_id or "").startswith("hash_batch_si") or metrics.get("block_executor_id") == "batch_si_block_executor":
            metrics["abort_count"] = scheduler_metrics.get("batch_si_deferred_transaction_count", 0)

    _derive_update_metrics(metrics)
    _apply_metric_completeness(metrics, method_id=method_id)
    return metrics


def _apply_artifact_contract(metrics: dict[str, Any], cluster: dict[str, Any]) -> None:
    contract = cluster.get("artifact_contract") if isinstance(cluster.get("artifact_contract"), dict) else {}
    status = cluster.get("artifact_contract_status") or contract.get("artifact_contract_status")
    missing = cluster.get("missing_expected_artifacts")
    if missing is None:
        missing = contract.get("missing_expected_artifacts")
    unexpected = cluster.get("unexpected_artifacts")
    if unexpected is None:
        unexpected = contract.get("unexpected_artifacts")

    missing_items = [str(item) for item in missing] if isinstance(missing, list) else []
    unexpected_items = [str(item) for item in unexpected] if isinstance(unexpected, list) else []

    if not status:
        if missing_items:
            status = "incomplete"
        elif contract:
            status = "complete"
        else:
            status = "unknown"

    metrics["artifact_contract_status"] = status
    metrics["missing_expected_artifacts"] = missing_items
    metrics["unexpected_artifact_count"] = len(unexpected_items)
    if contract:
        metrics["expected_artifact_count"] = contract.get("expected_artifact_count")
        metrics["actual_artifact_count"] = contract.get("actual_artifact_count")

    for item in missing_items:
        marker = f"artifact_contract:missing:{item}"
        if marker not in metrics["missing"]:
            metrics["missing"].append(marker)


def _apply_block_stm_metrics(metrics: dict[str, Any], run_dir: Path) -> None:
    block_stm_summary = _read_json(run_dir / "block_stm_summary.json")
    block_stm_metrics = block_stm_summary.get("block_stm_metrics") if isinstance(block_stm_summary.get("block_stm_metrics"), dict) else {}
    if not block_stm_metrics:
        return
    metrics.update(
        {
            "worker_count": block_stm_metrics.get("worker_count"),
            "maximum_parallel_width": block_stm_metrics.get("maximum_parallel_width"),
            "abort_count": block_stm_metrics.get("abort_count"),
            "reexecution_count": block_stm_metrics.get("reexecution_count"),
            "dependency_wait_count": block_stm_metrics.get("dependency_wait_count"),
            "dependency_resume_count": block_stm_metrics.get("dependency_resume_count"),
            "validation_failure_count": block_stm_metrics.get("validation_failure_count"),
            "serial_equivalent": block_stm_summary.get("serial_equivalent"),
        }
    )
    metrics["source_artifacts"].append("block_stm_summary.json")



def _batch_si_leader_summary_paths(run_dir: Path) -> list[Path]:
    plan = _read_json(run_dir / "compiled_run_plan.json")
    node_configs = plan.get("node_configs") if isinstance(plan.get("node_configs"), list) else []
    leader_ids = [
        str(item.get("node_id"))
        for item in node_configs
        if isinstance(item, dict) and (item.get("leader") is True or item.get("role") == "leader") and item.get("node_id")
    ]
    paths = [run_dir / "nodes" / node_id / "block_execution_summary.json" for node_id in leader_ids]
    existing = [path for path in paths if path.is_file()]
    if existing:
        return existing
    # Older compiled plans may omit the leader marker. Select one deterministic
    # replica per shard so mechanism counts are not multiplied by PBFT replicas.
    by_shard: dict[str, Path] = {}
    for path in sorted((run_dir / "nodes").glob("*/block_execution_summary.json")):
        payload = _read_json(path)
        shard_id = str(payload.get("shard_id") or path.parent.name)
        by_shard.setdefault(shard_id, path)
    return list(by_shard.values())


def _apply_batch_si_metrics(metrics: dict[str, Any], run_dir: Path) -> None:
    summaries = [_read_json(path) for path in _batch_si_leader_summary_paths(run_dir)]
    summaries = [item for item in summaries if item.get("block_executor_id") == "batch_si_block_executor"]
    if not summaries:
        return
    blocks = [
        block
        for summary in summaries
        for block in (summary.get("blocks") if isinstance(summary.get("blocks"), list) else [])
        if isinstance(block, dict)
    ]
    if not blocks:
        return
    def total(name: str) -> int:
        return sum(_int(block.get(name)) for block in blocks)

    # OFAS cycle victims are part of the per-block Batch-SI execution summary.
    # Count them from one leader per shard, just like the other Batch-SI plan
    # metrics.  The runtime writes both keys today; the abort_count fallback
    # keeps older result directories readable without treating a real zero as
    # a missing metric.
    deferred_transaction_count = sum(
        _int(
            block.get("deferred_transaction_count")
            if block.get("deferred_transaction_count") is not None
            else block.get("abort_count")
        )
        for block in blocks
    )
    metrics.update({
        "batch_si_metrics_available": True,
        "configured_worker_count": max((_int(block.get("configured_worker_count") or block.get("worker_count")) for block in blocks), default=0),
        "worker_count": max((_int(block.get("configured_worker_count") or block.get("worker_count")) for block in blocks), default=0),
        "maximum_parallel_width": max((_int(block.get("maximum_parallel_width")) for block in blocks), default=0),
        "batch_count": total("batch_count"),
        "maximum_batch_width": max((_int(block.get("maximum_batch_width")) for block in blocks), default=0),
        "write_opportunity_reuse_count": total("write_opportunity_reuse_count"),
        "dependency_edge_count": total("dependency_edge_count"),
        "deferred_transaction_count": deferred_transaction_count,
        "abort_count": deferred_transaction_count,
        "planning_iteration_count": total("planning_iteration_count"),
        "batch_snapshot_count": total("batch_snapshot_count"),
        "batch_snapshot_create_ms": total("batch_snapshot_create_ms"),
        "transaction_execution_ms": total("transaction_execution_ms"),
        "deterministic_materialization_ms": total("deterministic_materialization_ms"),
        "batch_si_cross_scheme_algorithm_reuse": False,
    })
    metrics["source_artifacts"].extend(
        str(path.relative_to(run_dir)).replace("\\", "/")
        for path in _batch_si_leader_summary_paths(run_dir)
        if path.is_file()
    )

def _apply_metatrack_artifacts(metrics: dict[str, Any], run_dir: Path) -> None:
    for name, key in {
        "metatrack_batch_plan.jsonl": "metatrack_batch_plan_available",
        "dependency_graph.csv": "dependency_graph_available",
        "track_classification.csv": "track_classification_available",
        "metatrack_scheduler_trace.csv": "metatrack_scheduler_trace_available",
        "predicted_remote_access.csv": "predicted_remote_access_available",
        "physical_remote_state_operations.csv": "physical_remote_state_operations_available",
        "aggregate/replica_deduplicated_remote_operations.csv": "replica_deduplicated_remote_operations_available",
        "aggregate/remote_state_metrics_summary.json": "remote_state_metrics_summary_available",
        "aggregate/metatrack_aggregate_summary.json": "metatrack_aggregate_summary_available",
        "aggregation_plan.csv": "aggregation_plan_available",
        "logical_physical_update_mapping.csv": "logical_physical_update_mapping_available",
    }.items():
        if (run_dir / name).is_file():
            metrics[key] = True
            metrics["source_artifacts"].append(name)
    if (run_dir / "remote_state_access.csv").is_file():
        metrics["remote_state_access_legacy_available"] = True
        metrics["source_artifacts"].append("remote_state_access.csv")


def _apply_mechanism_metrics(metrics: dict[str, Any], run_dir: Path) -> None:
    mechanism = _read_json(run_dir / "aggregate" / "mechanism_metrics_summary.json")
    if not mechanism:
        return
    metrics["mechanism_metrics_available"] = True
    metrics["source_artifacts"].append("aggregate/mechanism_metrics_summary.json")
    metatrack = mechanism.get("metatrack") if isinstance(mechanism.get("metatrack"), dict) else {}
    if metatrack.get("status") == "available":
        metrics.update(
            {
                "fast_track_logical_tx_count": metatrack.get("fast_track_logical_tx_count"),
                "conservative_track_logical_tx_count": metatrack.get("conservative_track_logical_tx_count"),
                "planning_scheduler_event_count": metatrack.get("planning_scheduler_event_count"),
                "runtime_scheduler_event_count": metatrack.get("runtime_scheduler_event_count"),
                "aggregation_group_count": metatrack.get("aggregation_group_count"),
                "pre_aggregation_physical_op_count": metatrack.get("pre_aggregation_physical_op_count"),
                "post_aggregation_physical_op_count": metatrack.get("post_aggregation_physical_op_count"),
                "physical_ops_saved_count": metatrack.get("physical_ops_saved_count"),
                "aggregation_reduction_ratio": metatrack.get("aggregation_reduction_ratio"),
            }
        )
    block_stm = mechanism.get("block_stm") if isinstance(mechanism.get("block_stm"), dict) else {}
    if block_stm.get("status") == "available":
        metrics.update(
            {
                "worker_count": block_stm.get("worker_count"),
                "maximum_parallel_width": block_stm.get("maximum_parallel_width"),
                "abort_count": block_stm.get("abort_count"),
                "reexecution_count": block_stm.get("reexecution_count"),
                "validation_failure_count": block_stm.get("validation_failure_count"),
                "serial_equivalent": block_stm.get("serial_equivalent"),
            }
        )
    remote_state = mechanism.get("remote_state") if isinstance(mechanism.get("remote_state"), dict) else {}
    if remote_state:
        metrics.update(remote_state)


def _apply_metric_completeness(metrics: dict[str, Any], *, method_id: str | None) -> None:
    uses_block_stm, uses_metatrack, uses_batch_si = _method_traits(metrics, method_id)
    required = list(COMMON_REQUIRED_METRICS)
    if uses_block_stm:
        required.extend(BLOCK_STM_REQUIRED_METRICS)
    if uses_metatrack:
        required.extend(METATRACK_REQUIRED_METRICS)
    if uses_batch_si:
        required.extend(BATCH_SI_REQUIRED_METRICS)

    statuses: dict[str, str] = {}
    metric_missing: list[str] = []
    for name in COMMON_REQUIRED_METRICS:
        statuses[name] = _metric_state(metrics.get(name), required=True)
    for name in BLOCK_STM_REQUIRED_METRICS:
        statuses[name] = _metric_state(metrics.get(name), required=uses_block_stm)
    for name in METATRACK_REQUIRED_METRICS:
        statuses[name] = _metric_state(metrics.get(name), required=uses_metatrack)
    for name in BATCH_SI_REQUIRED_METRICS:
        statuses[name] = _metric_state(metrics.get(name), required=uses_batch_si)
    for name in required:
        if statuses.get(name) == "missing":
            metric_missing.append(f"metric:{name}")

    metrics["metric_required"] = required
    metrics["metric_statuses"] = statuses
    metrics["metric_available"] = sorted(name for name, status in statuses.items() if status == "available")
    metrics["metric_not_applicable"] = sorted(name for name, status in statuses.items() if status == "not_applicable")
    metrics["metric_missing"] = metric_missing
    metrics["metric_completeness"] = "complete" if not metric_missing and not metrics.get("missing") else "incomplete"
    metrics["paper_analysis_status"] = metrics["metric_completeness"]
    metrics["metric_completeness_reason"] = (
        "all_required_metrics_available"
        if metrics["metric_completeness"] == "complete"
        else "missing_required_metrics_or_artifacts"
    )
    for item in metric_missing:
        if item not in metrics["missing"]:
            metrics["missing"].append(item)


def _derive_update_metrics(metrics: dict[str, Any]) -> None:
    pre = metrics.get("pre_aggregation_physical_op_count")
    post = metrics.get("post_aggregation_physical_op_count")
    if metrics.get("physical_ops_saved_count") is None and pre is not None and post is not None:
        metrics["physical_ops_saved_count"] = max(_int(pre) - _int(post), 0)
    if metrics.get("aggregation_reduction_ratio") is None and pre is not None:
        denominator = _int(pre)
        metrics["aggregation_reduction_ratio"] = (float(metrics.get("physical_ops_saved_count") or 0) / denominator) if denominator > 0 else 0


def _method_traits(metrics: dict[str, Any], method_id: str | None) -> tuple[bool, bool, bool]:
    normalized = str(method_id or "").lower()
    uses_block_stm = "block_stm" in normalized or metrics.get("block_executor_id") == "block_stm_block_executor"
    uses_metatrack = "metatrack" in normalized
    if not uses_metatrack:
        uses_metatrack = any(
            metrics.get(key) is not None or metrics.get(key) is True
            for key in (
                "fast_track_logical_tx_count",
                "conservative_track_logical_tx_count",
                "metatrack_batch_plan_available",
                "track_classification_available",
                "remote_state_access_legacy_available",
                "logical_physical_update_mapping_available",
            )
        )
    uses_batch_si = normalized.startswith("hash_batch_si") or metrics.get("block_executor_id") == "batch_si_block_executor" or metrics.get("batch_si_metrics_available") is True
    return uses_block_stm, uses_metatrack, uses_batch_si


def _metric_state(value: object, *, required: bool) -> str:
    if not required:
        return "not_applicable"
    return "missing" if value is None or value == "" else "available"


def _read_json(path: Path) -> dict:
    if not path.is_file():
        return {}
    data = json.loads(path.read_text(encoding="utf-8"))
    return data if isinstance(data, dict) else {}


def _remote_state_operations_path(run_dir: Path) -> Path:
    preferred = run_dir / "physical_remote_state_operations.csv"
    if preferred.is_file():
        return preferred
    return run_dir / "remote_state_access.csv"


def _read_remote_state_metrics(path: Path, *, logical_tx_count: int = 0) -> dict:
    if not path.is_file():
        return {}
    with path.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        if not {"success", "access_kind", "latency_ms"}.issubset(set(reader.fieldnames or [])):
            return {}
        rows = list(reader)
    successful_rows = [row for row in rows if str(row.get("success", "")).lower() in {"true", "1", "yes"}]
    latencies: list[float] = []
    for row in successful_rows:
        try:
            latencies.append(float(row.get("latency_ms") or 0))
        except ValueError:
            continue
    remote_summary = summarize_remote_operations(rows, logical_tx_count=logical_tx_count)
    metrics: dict[str, object] = {
        "remote_state_access_count": len(successful_rows),
        "remote_state_access_failed_count": remote_summary["physical_remote_failed_count"],
        "remote_state_read_count": remote_summary["physical_remote_fetch_count"],
        "remote_state_write_apply_count": remote_summary["physical_remote_writeback_count"],
        "remote_state_operations_artifact": path.name,
        **remote_summary,
    }
    if latencies:
        metrics["remote_state_access_avg_latency_ms"] = sum(latencies) / len(latencies)
        metrics["remote_state_access_max_latency_ms"] = max(latencies)
    return metrics


def _read_scheduler_metrics(path: Path) -> dict:
    if not path.is_file():
        return {}
    with path.open("r", encoding="utf-8", newline="") as handle:
        rows = list(csv.DictReader(handle))
    if not rows:
        return {
            "scheduler_event_count": 0,
            "scheduler_blocked_count": 0,
            "scheduler_wakeup_count": 0,
            "scheduler_stolen_work_count": 0,
            "scheduler_local_execution_count": 0,
            "scheduler_ready_queue_max_depth": 0,
            "scheduler_fast_queue_max_depth": 0,
            "scheduler_conservative_queue_max_depth": 0,
            "scheduler_dependency_wait_ms": 0,
            "scheduler_idle_ms": 0,
            "scheduler_idle_ratio": 0,
            "batch_si_deferred_transaction_count": 0,
            "deferred_transaction_count": 0,
            "batch_si_accepted_transaction_count": 0,
            "batch_si_abort_rate": 0,
        }
    idle_events = sum(1 for row in rows if _numeric(row.get("scheduler_idle_ms")) > 0)
    # Count one logical planning decision per block and transaction. This
    # de-duplicates replicated scheduler rows while preserving repeated OFAS
    # deferrals of the same transaction at different block heights.
    batch_si_deferred_events = {
        (str(row.get("block_height") or ""), str(row.get("tx_id") or ""))
        for row in rows
        if "batch_si_ofas_cycle_deferred" in str(row.get("decision_reason") or "") and str(row.get("tx_id") or "")
    }
    batch_si_accepted_events = {
        (str(row.get("block_height") or ""), str(row.get("tx_id") or ""))
        for row in rows
        if "batch_si_accepted" in str(row.get("decision_reason") or "") and str(row.get("tx_id") or "")
    }
    batch_si_total = len(batch_si_deferred_events) + len(batch_si_accepted_events)
    return {
        "scheduler_event_count": len(rows),
        "scheduler_blocked_count": sum(1 for row in rows if _truthy(row.get("blocked"))),
        "scheduler_wakeup_count": sum(1 for row in rows if _truthy(row.get("wakeup"))),
        "scheduler_stolen_work_count": sum(1 for row in rows if _truthy(row.get("stolen_work"))),
        "scheduler_local_execution_count": sum(1 for row in rows if _truthy(row.get("local_execution"))),
        "scheduler_fast_queue_event_count": sum(1 for row in rows if row.get("queue_name") == "fast_queue"),
        "scheduler_conservative_queue_event_count": sum(1 for row in rows if row.get("queue_name") == "conservative_queue"),
        "scheduler_ready_queue_max_depth": max((_numeric(row.get("ready_queue_depth")) for row in rows), default=0),
        "scheduler_fast_queue_max_depth": max((_numeric(row.get("fast_queue_depth")) for row in rows), default=0),
        "scheduler_conservative_queue_max_depth": max((_numeric(row.get("conservative_queue_depth")) for row in rows), default=0),
        "scheduler_dependency_wait_ms": sum(_numeric(row.get("dependency_wait_ms")) for row in rows),
        "scheduler_idle_ms": sum(_numeric(row.get("scheduler_idle_ms")) for row in rows),
        "scheduler_idle_ratio": idle_events / len(rows),
        "batch_si_deferred_transaction_count": len(batch_si_deferred_events),
        "deferred_transaction_count": len(batch_si_deferred_events),
        "batch_si_accepted_transaction_count": len(batch_si_accepted_events),
        "batch_si_abort_rate": (len(batch_si_deferred_events) / batch_si_total) if batch_si_total else 0,
    }


def _truthy(value: object) -> bool:
    return str(value or "").lower() in {"true", "1", "yes"}


def _numeric(value: object) -> int:
    try:
        return int(float(str(value or "0")))
    except ValueError:
        return 0


def _int(value: object) -> int:
    try:
        return int(float(str(value or "0")))
    except ValueError:
        return 0
