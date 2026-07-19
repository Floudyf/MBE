from __future__ import annotations

import csv
import json
from pathlib import Path


def extract(run_dir: Path) -> dict:
    summary_path = run_dir / "real_cluster_summary.json"
    finality_path = run_dir / "finality_summary.json"
    if not summary_path.is_file() or not finality_path.is_file():
        return {"missing": [name for name, path in {"real_cluster_summary.json": summary_path, "finality_summary.json": finality_path}.items() if not path.is_file()]}
    cluster = json.loads(summary_path.read_text(encoding="utf-8")); finality = json.loads(finality_path.read_text(encoding="utf-8"))
    required = ["transaction_lifecycle.jsonl", "transaction_finality.csv", "client_receipt_log.csv", "finality_summary.json", "real_cluster_summary.json"]
    missing = [name for name in required if not (run_dir / name).is_file()]
    metrics = {"finalized_tx_count": finality.get("finalized_unique_logical_tx_count"), "throughput_tps": finality.get("throughput_tps"), "p50_latency_ms": finality.get("p50_finality_ms"), "p95_latency_ms": finality.get("p95_finality_ms"), "p99_latency_ms": finality.get("p99_finality_ms"), "block_executor_id": cluster.get("block_executor_id"), "block_executor_consistent": cluster.get("block_executor_consistent"), "plan_digest_consistent": cluster.get("plan_digest_consistent"), "state_root_consistent": cluster.get("state_root_consistent"), "orphan_process_count": cluster.get("orphan_process_count"), "no_fallback": cluster.get("no_fallback"), "lifecycle_complete": finality.get("logical_transaction_count") == finality.get("finalized_unique_logical_tx_count"), "fast_track_count": cluster.get("fast_track_count"), "conservative_track_count": cluster.get("conservative_track_count"), "aggregation_group_count": cluster.get("aggregation_group_count"), "logical_update_count": cluster.get("logical_update_count"), "physical_update_count": cluster.get("physical_update_count"), "scheduler_event_count": cluster.get("scheduler_event_count"), "scheduler_blocked_count": cluster.get("scheduler_blocked_count"), "scheduler_wakeup_count": cluster.get("scheduler_wakeup_count"), "scheduler_stolen_work_count": cluster.get("scheduler_stolen_work_count"), "scheduler_local_execution_count": cluster.get("scheduler_local_execution_count"), "scheduler_ready_queue_max_depth": cluster.get("scheduler_ready_queue_max_depth"), "scheduler_fast_queue_max_depth": cluster.get("scheduler_fast_queue_max_depth"), "scheduler_conservative_queue_max_depth": cluster.get("scheduler_conservative_queue_max_depth"), "scheduler_dependency_wait_ms": cluster.get("scheduler_dependency_wait_ms"), "scheduler_idle_ms": cluster.get("scheduler_idle_ms"), "scheduler_idle_ratio": cluster.get("scheduler_idle_ratio"), "remote_state_access_count": cluster.get("remote_state_access_count"), "remote_state_read_count": cluster.get("remote_state_read_count"), "remote_state_write_apply_count": cluster.get("remote_state_write_apply_count"), "remote_state_access_failed_count": cluster.get("remote_state_access_failed_count"), "remote_state_access_avg_latency_ms": cluster.get("remote_state_access_avg_latency_ms"), "source_artifacts": list(required), "missing": missing}
    block_stm_summary = _read_json(run_dir / "block_stm_summary.json")
    block_stm_metrics = block_stm_summary.get("block_stm_metrics") if isinstance(block_stm_summary.get("block_stm_metrics"), dict) else {}
    if block_stm_metrics:
        metrics.update({
            "worker_count": block_stm_metrics.get("worker_count"),
            "maximum_parallel_width": block_stm_metrics.get("maximum_parallel_width"),
            "abort_count": block_stm_metrics.get("abort_count"),
            "reexecution_count": block_stm_metrics.get("reexecution_count"),
            "dependency_wait_count": block_stm_metrics.get("dependency_wait_count"),
            "dependency_resume_count": block_stm_metrics.get("dependency_resume_count"),
            "validation_failure_count": block_stm_metrics.get("validation_failure_count"),
            "serial_equivalent": block_stm_summary.get("serial_equivalent"),
        })
        metrics["source_artifacts"].append("block_stm_summary.json")
    for name, key in {
        "metatrack_batch_plan.jsonl": "metatrack_batch_plan_available",
        "dependency_graph.csv": "dependency_graph_available",
        "track_classification.csv": "track_classification_available",
        "metatrack_scheduler_trace.csv": "metatrack_scheduler_trace_available",
        "remote_state_access.csv": "remote_state_access_available",
        "aggregation_plan.csv": "aggregation_plan_available",
        "logical_physical_update_mapping.csv": "logical_physical_update_mapping_available",
    }.items():
        if (run_dir / name).is_file():
            metrics[key] = True
            metrics["source_artifacts"].append(name)
    remote_state_metrics = _read_remote_state_metrics(run_dir / "remote_state_access.csv")
    if remote_state_metrics:
        metrics.update(remote_state_metrics)
    scheduler_metrics = _read_scheduler_metrics(run_dir / "metatrack_scheduler_trace.csv")
    if scheduler_metrics:
        metrics.update(scheduler_metrics)
    return metrics


def _read_json(path: Path) -> dict:
    if not path.is_file():
        return {}
    data = json.loads(path.read_text(encoding="utf-8"))
    return data if isinstance(data, dict) else {}


def _read_remote_state_metrics(path: Path) -> dict:
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
    metrics: dict[str, object] = {
        "remote_state_access_count": len(successful_rows),
        "remote_state_access_failed_count": max(len(rows) - len(successful_rows), 0),
        "remote_state_read_count": sum(1 for row in successful_rows if row.get("access_kind") != "write_apply"),
        "remote_state_write_apply_count": sum(1 for row in successful_rows if row.get("access_kind") == "write_apply"),
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
        }
    idle_events = sum(1 for row in rows if _numeric(row.get("scheduler_idle_ms")) > 0)
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
    }


def _truthy(value: object) -> bool:
    return str(value or "").lower() in {"true", "1", "yes"}


def _numeric(value: object) -> int:
    try:
        return int(float(str(value or "0")))
    except ValueError:
        return 0
