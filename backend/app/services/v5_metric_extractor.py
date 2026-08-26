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
    "batch_si_first_pass_candidate_count",
    "batch_si_first_pass_accepted_count",
    "batch_si_first_pass_ofas_abort_count",
    "batch_si_first_pass_ofas_abort_rate",
    "batch_snapshot_create_ms",
]

LITERATURE_GRAPH_REQUIRED_METRICS = [
    "worker_count",
    "maximum_parallel_width",
    "wave_count",
    "maximum_wave_width",
    "dependency_edge_count",
    "pairwise_conflict_check_count",
    "graph_color_count",
    "transaction_execution_ms",
    "deterministic_materialization_ms",
]

GROUNDHOG_REQUIRED_METRICS = [
    "groundhog_metrics_available",
    "groundhog_execution_attempt_count",
    "groundhog_reservation_count",
    "groundhog_constraint_conflict_count",
    "groundhog_reservation_rollback_count",
    "groundhog_reservation_parallel_width",
    "groundhog_reservation_engine",
    "groundhog_snapshot_semantics",
    "groundhog_typed_modification_semantics",
    "groundhog_proposal_evidence_available",
    "groundhog_proposal_candidate_count",
    "groundhog_proposal_selected_count",
    "groundhog_proposal_deferred_event_count",
    "groundhog_proposal_constraint_conflict_count",
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
        "finality_semantics_version": finality.get("finality_semantics_version"),
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
        # Lifecycle completion is about every admitted logical transaction reaching
        # a terminal outcome.  Nezha/ACG HS may legitimately terminate a
        # transaction as `nezha_hs_aborted`; those terminal failed no-ops are not
        # committed/finalized state updates, but they still close the lifecycle.
        "lifecycle_complete": (
            finality.get("logical_transaction_count") == terminal
            and finality.get("incomplete_unique_tx_count") == 0
        ),
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
    _apply_workload_replay_metrics(metrics, run_dir)
    _apply_mempool_admission_metrics(metrics, run_dir)
    _apply_common_block_execution_timing(metrics, run_dir)

    _apply_block_stm_metrics(metrics, run_dir)
    _apply_batch_si_metrics(metrics, run_dir)
    _apply_literature_graph_metrics(metrics, run_dir)
    _apply_groundhog_metrics(metrics, run_dir)
    _apply_aria_metrics(metrics, run_dir)
    _apply_observability_metrics(metrics, run_dir)
    _apply_metatrack_artifacts(metrics, run_dir)
    _apply_mechanism_metrics(metrics, run_dir)

    logical_tx_count = _int(finality.get("submitted_unique_tx_count") or finality.get("logical_transaction_count"))
    remote_state_metrics = _read_remote_state_metrics(_remote_state_operations_path(run_dir), logical_tx_count=logical_tx_count)
    if remote_state_metrics:
        metrics.update(remote_state_metrics)

    scheduler_metrics = _read_scheduler_metrics(run_dir / "metatrack_scheduler_trace.csv")
    if scheduler_metrics:
        # Scheduler traces are optional diagnostics.  Preserve consensus-bound
        # Batch-SI accepted/deferred identity evidence when it is available so
        # a truncated/omitted trace cannot change formal unique-deferral counts.
        consensus_deferral_evidence = {
            key: metrics.get(key)
            for key in (
                "batch_si_deferred_identity_evidence_available",
                "batch_si_deferred_event_count",
                "batch_si_unique_deferred_tx_count",
                "batch_si_unique_deferral_rate",
                "batch_si_mean_deferrals_per_finalized_tx",
            )
            if metrics.get("batch_si_deferred_identity_evidence_available") is True
        }
        metrics.update(scheduler_metrics)
        metrics.update(consensus_deferral_evidence)

    _derive_update_metrics(metrics)
    _derive_research_metrics(metrics)
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


def _apply_workload_replay_metrics(metrics: dict[str, Any], run_dir: Path) -> None:
    replay = _read_json(run_dir / "workload_replay_summary.json")
    completion = _read_json(run_dir / "client_submission_complete.json")
    if not replay and not completion:
        return

    def first(name: str, default: object = None) -> object:
        if replay.get(name) is not None:
            return replay.get(name)
        if completion.get(name) is not None:
            return completion.get(name)
        return default

    variant_parameters = replay.get("variant_parameters") if isinstance(replay.get("variant_parameters"), dict) else {}
    audit = replay.get("audit_metadata") if isinstance(replay.get("audit_metadata"), dict) else {}
    target_account_write_theta = audit.get("target_account_write_theta")
    if target_account_write_theta is None:
        target_account_write_theta = variant_parameters.get("target_theta")
    measured_account_touch_theta = audit.get("measured_account_touch_theta")
    if measured_account_touch_theta is None:
        measured_account_touch_theta = audit.get("measured_account_access_theta")
    if measured_account_touch_theta is None:
        measured_account_touch_theta = audit.get("measured_access_theta")

    metrics.update({
        "replay_mode": first("replay_mode"),
        "target_submission_tps": first("target_submission_tps"),
        "observed_submission_tps": first("observed_submission_tps"),
        "submission_duration_ms": first("submission_duration_ms"),
        "pacing_schedule": first("pacing_schedule"),
        "pacing_late_release_count": first("pacing_late_release_count", 0),
        "pacing_max_schedule_lag_ms": first("pacing_max_schedule_lag_ms", 0),
        "target_account_write_theta": target_account_write_theta,
        "measured_account_write_theta": audit.get("measured_account_write_theta"),
        "measured_account_touch_theta": measured_account_touch_theta,
        "measured_account_access_theta": audit.get("measured_account_access_theta"),
        "theta_axis": audit.get("theta_axis"),
    })
    for name in ("workload_replay_summary.json", "client_submission_complete.json"):
        if (run_dir / name).is_file() and name not in metrics["source_artifacts"]:
            metrics["source_artifacts"].append(name)


def _apply_mempool_admission_metrics(metrics: dict[str, Any], run_dir: Path) -> None:
    # Server-side truth for the offered-load experiment: use the first successful
    # mempool admission observed for each logical transaction across replicas.
    # Replica gossip duplicates therefore cannot inflate the admitted rate.
    first_admitted_by_logical_id: dict[str, int] = {}
    lifecycle_paths = sorted((run_dir / "nodes").glob("*/transaction_lifecycle.jsonl"))
    for path in lifecycle_paths:
        try:
            with path.open("r", encoding="utf-8") as handle:
                for line in handle:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        event = json.loads(line)
                    except json.JSONDecodeError:
                        continue
                    if not isinstance(event, dict) or event.get("stage") != "admitted" or event.get("success") is not True:
                        continue
                    logical_id = str(event.get("logical_tx_id") or event.get("tx_id") or "")
                    timestamp_ms = event.get("timestamp_ms")
                    if not logical_id or isinstance(timestamp_ms, bool) or not isinstance(timestamp_ms, (int, float)):
                        continue
                    timestamp = int(timestamp_ms)
                    previous = first_admitted_by_logical_id.get(logical_id)
                    if previous is None or timestamp < previous:
                        first_admitted_by_logical_id[logical_id] = timestamp
        except OSError:
            continue
    if not first_admitted_by_logical_id:
        return
    timestamps = sorted(first_admitted_by_logical_id.values())
    duration_ms = max(0, timestamps[-1] - timestamps[0])
    metrics["mempool_admitted_unique_tx_count"] = len(timestamps)
    metrics["mempool_first_admitted_at_ms"] = timestamps[0]
    metrics["mempool_last_admitted_at_ms"] = timestamps[-1]
    metrics["mempool_admission_duration_ms"] = duration_ms
    if duration_ms > 0 and len(timestamps) > 1:
        metrics["observed_mempool_admission_tps"] = (len(timestamps) - 1) / (duration_ms / 1000.0)
    target = metrics.get("target_submission_tps")
    observed = metrics.get("observed_mempool_admission_tps")
    if isinstance(target, (int, float)) and not isinstance(target, bool) and target > 0 and isinstance(observed, (int, float)) and not isinstance(observed, bool):
        metrics["mempool_admission_target_ratio"] = float(observed) / float(target)
    metrics["mempool_admission_evidence_available"] = True
    # Keep the artifact list compact while still naming the exact evidence family.
    if "nodes/*/transaction_lifecycle.jsonl" not in metrics["source_artifacts"]:
        metrics["source_artifacts"].append("nodes/*/transaction_lifecycle.jsonl")


def _apply_common_block_execution_timing(metrics: dict[str, Any], run_dir: Path) -> None:
    summaries = [_read_json(path) for path in _batch_si_leader_summary_paths(run_dir)]
    summaries = [item for item in summaries if item]
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

    # block_execution_ms is the common runtime wall-clock envelope. The three
    # phase metrics remain separate and are never derived from that envelope.
    metrics["block_execution_ms"] = total("block_execution_ms")
    metrics["transaction_execution_ms"] = total("transaction_execution_ms")
    metrics["deterministic_materialization_ms"] = sum(
        _int(block.get("deterministic_materialization_ms") if block.get("deterministic_materialization_ms") is not None else block.get("deterministic_apply_ms"))
        for block in blocks
    )
    metrics["state_commitment_ms"] = total("state_commitment_ms")
    metrics["common_timing_block_count"] = len(blocks)
    root_versions = sorted({str(block.get("state_root_version")) for block in blocks if block.get("state_root_version")})
    if len(root_versions) == 1:
        metrics["state_root_version"] = root_versions[0]
    executor_ids = sorted({str(summary.get("block_executor_id")) for summary in summaries if summary.get("block_executor_id")})
    if executor_ids:
        metrics["timing_block_executor_ids"] = executor_ids
    metrics["common_block_execution_timing_available"] = True
    for path in _batch_si_leader_summary_paths(run_dir):
        if path.is_file():
            rel = str(path.relative_to(run_dir)).replace("\\", "/")
            if rel not in metrics["source_artifacts"]:
                metrics["source_artifacts"].append(rel)


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

    # Consensus-bound Batch-SI plans persist accepted/deferred identities in
    # each committed block summary.  This evidence survives even when the
    # optional scheduler trace is omitted from a formal artifact bundle.
    identity_evidence_available = bool(blocks) and all(
        isinstance(block.get("batch_si_deferred_tx_ids"), list)
        and isinstance(block.get("batch_si_accepted_tx_ids"), list)
        for block in blocks
    )
    deferred_tx_ids = {
        str(tx_id)
        for block in blocks
        for tx_id in (block.get("batch_si_deferred_tx_ids") or [])
        if str(tx_id)
    } if identity_evidence_available else set()
    accepted_tx_ids = {
        str(tx_id)
        for block in blocks
        for tx_id in (block.get("batch_si_accepted_tx_ids") or [])
        if str(tx_id)
    } if identity_evidence_available else set()

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
        "batch_si_first_pass_candidate_count": total("batch_si_first_pass_candidate_count"),
        "batch_si_first_pass_accepted_count": total("batch_si_first_pass_accepted_count"),
        "batch_si_first_pass_ofas_abort_count": total("batch_si_first_pass_ofas_abort_count"),
        "batch_si_first_pass_ofas_abort_rate": (
            total("batch_si_first_pass_ofas_abort_count") / total("batch_si_first_pass_candidate_count")
            if total("batch_si_first_pass_candidate_count")
            else 0
        ),
        "batch_si_deferred_identity_evidence_available": identity_evidence_available,
        "batch_si_deferred_event_count": total("batch_si_first_pass_ofas_abort_count"),
        "batch_si_unique_deferred_tx_count": len(deferred_tx_ids) if identity_evidence_available else None,
        "batch_si_unique_deferral_rate": (
            len(deferred_tx_ids) / len(accepted_tx_ids)
            if identity_evidence_available and accepted_tx_ids
            else None
        ),
        "batch_si_mean_deferrals_per_finalized_tx": (
            total("batch_si_first_pass_ofas_abort_count") / len(accepted_tx_ids)
            if identity_evidence_available and accepted_tx_ids
            else None
        ),
        # Accepted Batch-SI blocks execute without a second in-block abort.
        # OFAS victims are proposal deferrals and are reported above separately.
        "abort_count": 0,
        "planning_iteration_count": total("planning_iteration_count"),
        "batch_snapshot_count": total("batch_snapshot_count"),
        "batch_snapshot_create_ms": total("batch_snapshot_create_ms"),
        "graph_table_construction_ms": total("graph_table_construction_ms"),
        "sorting_ms": total("sorting_ms"),
        "batch_si_table_construction_ms": total("batch_si_table_construction_ms"),
        "batch_si_sorting_ms": total("batch_si_sorting_ms"),
        "transaction_execution_ms": total("transaction_execution_ms"),
        "deterministic_materialization_ms": total("deterministic_materialization_ms"),
        "state_commitment_ms": total("state_commitment_ms"),
        "batch_si_executor_plan_parse_ms": total("batch_si_executor_plan_parse_ms"),
        "batch_si_executor_plan_verify_ms": total("batch_si_executor_plan_verify_ms"),
        "batch_si_plan_payload_bytes": total("batch_si_plan_payload_bytes"),
        "batch_si_worker_pool_setup_ms": total("batch_si_worker_pool_setup_ms"),
        "batch_si_worker_pool_wait_ms": total("batch_si_worker_pool_wait_ms"),
        "batch_si_executor_full_verify_count": total("batch_si_executor_full_verify_count"),
        "batch_si_executor_full_verify_skip_count": total("batch_si_executor_full_verify_skip_count"),
        "batch_si_cross_scheme_algorithm_reuse": False,
    })
    verification_modes = sorted({
        str(block.get("batch_si_executor_plan_verify_mode"))
        for block in blocks
        if block.get("batch_si_executor_plan_verify_mode") not in (None, "")
    })
    if len(verification_modes) == 1:
        metrics["batch_si_executor_plan_verify_mode"] = verification_modes[0]
    preverified_values = [
        block.get("batch_si_execution_plan_preverified")
        for block in blocks
        if isinstance(block.get("batch_si_execution_plan_preverified"), bool)
    ]
    if preverified_values:
        metrics["batch_si_execution_plan_preverified"] = all(preverified_values)
    metrics["source_artifacts"].extend(
        str(path.relative_to(run_dir)).replace("\\", "/")
        for path in _batch_si_leader_summary_paths(run_dir)
        if path.is_file()
    )

def _apply_literature_graph_metrics(metrics: dict[str, Any], run_dir: Path) -> None:
    summaries = [_read_json(path) for path in _batch_si_leader_summary_paths(run_dir)]
    summaries = [
        item for item in summaries
        if item.get("block_executor_id") in {"cg_block_executor", "acg_block_executor", "bsx_block_executor"}
    ]
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

    executor_ids = sorted({str(item.get("block_executor_id") or "") for item in summaries if item.get("block_executor_id")})
    metrics.update({
        "literature_graph_metrics_available": True,
        "literature_graph_block_executor_ids": executor_ids,
        "worker_count": max((_int(block.get("configured_worker_count") or block.get("worker_count")) for block in blocks), default=0),
        "maximum_parallel_width": max((_int(block.get("maximum_parallel_width")) for block in blocks), default=0),
        "wave_count": total("wave_count"),
        "maximum_wave_width": max((_int(block.get("maximum_wave_width")) for block in blocks), default=0),
        "dependency_edge_count": total("dependency_edge_count"),
        "pairwise_conflict_check_count": total("pairwise_conflict_check_count"),
        "graph_color_count": total("graph_color_count"),
        "graph_table_construction_ms": total("graph_table_construction_ms"),
        "sorting_ms": total("sorting_ms"),
        "transaction_execution_ms": total("transaction_execution_ms"),
        "deterministic_materialization_ms": total("deterministic_materialization_ms"),
        "state_commitment_ms": total("state_commitment_ms"),
        "cg_planning_worker_count": max((_int(block.get("cg_planning_worker_count")) for block in blocks), default=0),
    })
    if executor_ids == ["cg_block_executor"]:
        cg_candidates = total("cg_candidate_transaction_count")
        cg_aborts = total("cg_cycle_abort_count")
        metrics["abort_count"] = cg_aborts
        metrics["cg_candidate_transaction_count"] = cg_candidates
        metrics["cg_cycle_abort_count"] = cg_aborts
        metrics["cg_cycle_resolution_count"] = total("cg_cycle_resolution_count")
        metrics["cg_cycle_abort_rate"] = (cg_aborts / cg_candidates) if cg_candidates else 0
    if executor_ids == ["acg_block_executor"]:
        # Nezha HS aborts are part of the authors' algorithm semantics. Keep the
        # successful-finalization throughput numerator unchanged, but export the
        # explicit abort evidence so a lower finalized count is explainable.
        metrics["abort_count"] = total("abort_count")
        metrics["nezha_hs_abort_count"] = total("nezha_hs_abort_count")

    validator_modes = sorted({str(block.get("cg_validator_mode")) for block in blocks if block.get("cg_validator_mode")})
    if len(validator_modes) == 1:
        metrics["cg_validator_mode"] = validator_modes[0]
    metrics["source_artifacts"].extend(
        str(path.relative_to(run_dir)).replace("\\", "/")
        for path in _batch_si_leader_summary_paths(run_dir)
        if path.is_file()
    )



def _apply_groundhog_metrics(metrics: dict[str, Any], run_dir: Path) -> None:
    leader_summary_paths = _batch_si_leader_summary_paths(run_dir)
    summaries = [_read_json(path) for path in leader_summary_paths]
    summaries = [item for item in summaries if item.get("block_executor_id") == "groundhog_block_executor"]
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

    metrics.update({
        "groundhog_metrics_available": True,
        "worker_count": max((_int(block.get("worker_count")) for block in blocks), default=0),
        "maximum_parallel_width": max((_int(block.get("maximum_parallel_width")) for block in blocks), default=0),
        "groundhog_execution_attempt_count": total("groundhog_execution_attempt_count"),
        "groundhog_reservation_count": total("groundhog_reservation_count"),
        "groundhog_constraint_conflict_count": total("groundhog_constraint_conflict_count"),
        "groundhog_reservation_rollback_count": total("groundhog_reservation_rollback_count"),
        "groundhog_integer_merge_count": total("groundhog_integer_merge_count"),
        "groundhog_bytes_merge_count": total("groundhog_bytes_merge_count"),
        "groundhog_ordered_set_merge_count": total("groundhog_ordered_set_merge_count"),
        "groundhog_modified_key_count": total("groundhog_modified_key_count"),
        "groundhog_reservation_parallel_width": max((_int(block.get("groundhog_reservation_parallel_width")) for block in blocks), default=0),
        "transaction_execution_ms": total("transaction_execution_ms"),
        "deterministic_materialization_ms": total("deterministic_materialization_ms"),
        "state_commitment_ms": total("state_commitment_ms"),
    })
    for name in (
        "groundhog_reservation_engine",
        "groundhog_fallback_mode",
        "groundhog_snapshot_semantics",
        "groundhog_typed_modification_semantics",
    ):
        values = sorted({str(block.get(name)) for block in blocks if block.get(name) not in (None, "")})
        if len(values) == 1:
            metrics[name] = values[0]

    proposal_paths = [path.parent / "proposal_selection_evidence.jsonl" for path in leader_summary_paths]
    proposal_rows: dict[tuple[str, int, str], dict[str, Any]] = {}
    for path in proposal_paths:
        if not path.is_file():
            continue
        try:
            with path.open("r", encoding="utf-8") as handle:
                for line in handle:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        row = json.loads(line)
                    except json.JSONDecodeError:
                        continue
                    if not isinstance(row, dict) or row.get("algorithm_id") != "groundhog_candidate_selection_v1":
                        continue
                    payload = row.get("payload") if isinstance(row.get("payload"), dict) else {}
                    shard_id = str(payload.get("shard_id") or row.get("shard_id") or "")
                    height = _int(payload.get("height") or row.get("height"))
                    digest = str(row.get("payload_digest") or "")
                    proposal_rows[(shard_id, height, digest)] = payload
        except OSError:
            continue

    if proposal_rows:
        proposal_candidate_count = 0
        proposal_selected_count = 0
        proposal_deferred_event_count = 0
        proposal_reservation_count = 0
        proposal_constraint_conflict_count = 0
        proposal_reservation_rollback_count = 0
        unique_deferred: set[str] = set()
        for payload in proposal_rows.values():
            proposal_candidate_count += _int(payload.get("candidate_count"))
            proposal_selected_count += _int(payload.get("selected_count"))
            proposal_deferred_event_count += _int(payload.get("deferred_count"))
            unique_deferred.update(
                str(item)
                for item in (payload.get("deferred_logical_ids") or payload.get("deferred_tx_ids") or [])
                if str(item)
            )
            proposal_metrics = payload.get("metrics") if isinstance(payload.get("metrics"), dict) else {}
            proposal_reservation_count += _int(proposal_metrics.get("reservation_count"))
            proposal_constraint_conflict_count += _int(proposal_metrics.get("constraint_conflict_count"))
            proposal_reservation_rollback_count += _int(proposal_metrics.get("reservation_rollback_count"))
        metrics.update({
            "groundhog_proposal_evidence_available": True,
            "groundhog_proposal_block_count": len(proposal_rows),
            "groundhog_proposal_candidate_count": proposal_candidate_count,
            "groundhog_proposal_selected_count": proposal_selected_count,
            "groundhog_proposal_deferred_event_count": proposal_deferred_event_count,
            "groundhog_proposal_unique_deferred_tx_count": len(unique_deferred),
            "groundhog_proposal_reservation_count": proposal_reservation_count,
            "groundhog_proposal_constraint_conflict_count": proposal_constraint_conflict_count,
            "groundhog_proposal_reservation_rollback_count": proposal_reservation_rollback_count,
            "groundhog_conflict_abort_count": proposal_constraint_conflict_count,
            "groundhog_conflict_abort_rate": (proposal_constraint_conflict_count / proposal_candidate_count) if proposal_candidate_count else 0,
        })
        for path in proposal_paths:
            if path.is_file():
                relative = str(path.relative_to(run_dir)).replace("\\", "/")
                if relative not in metrics["source_artifacts"]:
                    metrics["source_artifacts"].append(relative)

    for path in leader_summary_paths:
        if path.is_file():
            relative = str(path.relative_to(run_dir)).replace("\\", "/")
            if relative not in metrics["source_artifacts"]:
                metrics["source_artifacts"].append(relative)


def _apply_aria_metrics(metrics: dict[str, Any], run_dir: Path) -> None:
    leader_summary_paths = _batch_si_leader_summary_paths(run_dir)
    proposal_paths = [path.parent / "proposal_selection_evidence.jsonl" for path in leader_summary_paths]
    proposal_rows: dict[tuple[str, int, str], dict[str, Any]] = {}
    for path in proposal_paths:
        if not path.is_file():
            continue
        try:
            with path.open("r", encoding="utf-8") as handle:
                for line in handle:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        row = json.loads(line)
                    except json.JSONDecodeError:
                        continue
                    if not isinstance(row, dict) or row.get("algorithm_id") != "aria_candidate_selection_v2":
                        continue
                    payload = row.get("payload") if isinstance(row.get("payload"), dict) else {}
                    shard_id = str(payload.get("shard_id") or row.get("shard_id") or "")
                    height = _int(payload.get("height") or row.get("height"))
                    digest = str(row.get("payload_digest") or "")
                    proposal_rows[(shard_id, height, digest)] = payload
        except OSError:
            continue
    if not proposal_rows:
        return

    raw_metrics = [payload.get("metrics") for payload in proposal_rows.values() if isinstance(payload.get("metrics"), dict)]
    if not raw_metrics:
        return

    def total(name: str) -> int:
        return sum(_int(item.get(name)) for item in raw_metrics)

    metrics.update({
        "aria_metrics_available": True,
        "worker_count": max((_int(item.get("worker_count")) for item in raw_metrics), default=0),
        "maximum_parallel_width": max((_int(item.get("maximum_parallel_width")) for item in raw_metrics), default=0),
        "aria_epoch_count": total("epoch_count"),
        "aria_maximum_epoch_width": max((_int(item.get("maximum_epoch_width")) for item in raw_metrics), default=0),
        "aria_execution_attempt_count": total("execution_attempt_count"),
        "aria_committed_transaction_count": total("committed_transaction_count"),
        "aria_finalized_transaction_count": total("finalized_transaction_count"),
        "aria_conflict_abort_count": total("conflict_abort_count"),
        "aria_reexecution_count": total("reexecution_count"),
        "aria_retryable_nonce_count": total("retryable_nonce_count"),
        "aria_waw_dependency_count": total("waw_dependency_count"),
        "aria_raw_dependency_count": total("raw_dependency_count"),
        "aria_war_dependency_count": total("war_dependency_count"),
        "aria_read_reservation_count": total("read_reservation_count"),
        "aria_write_reservation_count": total("write_reservation_count"),
        "aria_read_only_fast_commit_count": total("read_only_fast_commit_count"),
        "aria_application_failure_count": total("application_failure_count"),
        "aria_candidate_transaction_count": total("candidate_transaction_count"),
        "aria_selected_transaction_count": total("selected_transaction_count"),
        "aria_deferred_transaction_count": total("deferred_transaction_count"),
        "aria_transaction_execution_ms": total("transaction_execution_ms"),
        "aria_deterministic_materialization_ms": total("deterministic_materialization_ms"),
        "aria_state_commitment_ms": total("state_commitment_ms"),
        "aria_proposal_evidence_block_count": len(proposal_rows),
    })
    for name in ("fallback_mode", "batch_lifecycle"):
        values = sorted({str(item.get(name)) for item in raw_metrics if item.get(name) not in (None, "")})
        if len(values) == 1:
            metrics[f"aria_{name}"] = values[0]
    for path in proposal_paths:
        if path.is_file():
            relative = str(path.relative_to(run_dir)).replace("\\", "/")
            if relative not in metrics["source_artifacts"]:
                metrics["source_artifacts"].append(relative)


def _apply_observability_metrics(metrics: dict[str, Any], run_dir: Path) -> None:
    resource = _read_json(run_dir / "resource_usage_summary.json")
    network = _read_json(run_dir / "network_metrics_summary.json")
    if resource:
        values = resource.get("metrics") if isinstance(resource.get("metrics"), dict) else {}
        metrics.update(values)
        metrics["resource_sampling_available"] = resource.get("available") is True
        metrics["resource_sampling"] = {
            "available": resource.get("available"),
            "scope": resource.get("scope"),
            "measurement_boundary": resource.get("measurement_boundary"),
            "sampling_error": resource.get("sampling_error"),
        }
        metrics["source_artifacts"].append("resource_usage_summary.json")
        if (run_dir / "resource_usage_timeseries.csv").is_file():
            metrics["source_artifacts"].append("resource_usage_timeseries.csv")
    if network:
        values = network.get("metrics") if isinstance(network.get("metrics"), dict) else {}
        metrics.update(values)
        metrics["network_metrics_available"] = network.get("available") is True
        metrics["network_categories"] = network.get("categories") if isinstance(network.get("categories"), dict) else {}
        metrics["network_message_types"] = network.get("message_types") if isinstance(network.get("message_types"), dict) else {}
        metrics["source_artifacts"].append("network_metrics_summary.json")
        if (run_dir / "network_message_summary.csv").is_file():
            metrics["source_artifacts"].append("network_message_summary.csv")


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


def _literature_graph_required_metrics(method_id: str | None) -> list[str]:
    normalized = str(method_id or "").lower()
    if normalized not in {"hash_cg", "hash_acg", "hash_bsx"}:
        return []
    required = [
        "worker_count",
        "maximum_parallel_width",
        "wave_count",
        "maximum_wave_width",
        "dependency_edge_count",
        "transaction_execution_ms",
        "deterministic_materialization_ms",
    ]
    if normalized == "hash_cg":
        required.extend(["pairwise_conflict_check_count", "abort_count", "cg_candidate_transaction_count", "cg_cycle_abort_count", "cg_cycle_resolution_count", "cg_cycle_abort_rate"])
    if normalized == "hash_acg":
        required.extend(["abort_count", "nezha_hs_abort_count"])
    if normalized == "hash_bsx":
        required.append("graph_color_count")
    return required


def _apply_metric_completeness(metrics: dict[str, Any], *, method_id: str | None) -> None:
    uses_block_stm, uses_metatrack, uses_batch_si = _method_traits(metrics, method_id)
    normalized_method_id = str(method_id or "").lower()
    uses_groundhog = (
        normalized_method_id == "hash_groundhog"
        or metrics.get("block_executor_id") == "groundhog_block_executor"
        or metrics.get("groundhog_metrics_available") is True
    )
    literature_graph_required = _literature_graph_required_metrics(method_id)
    required = list(COMMON_REQUIRED_METRICS)
    if uses_block_stm:
        required.extend(BLOCK_STM_REQUIRED_METRICS)
    if uses_metatrack:
        required.extend(METATRACK_REQUIRED_METRICS)
    if uses_batch_si:
        required.extend(BATCH_SI_REQUIRED_METRICS)
    if uses_groundhog:
        required.extend(GROUNDHOG_REQUIRED_METRICS)
    required.extend(literature_graph_required)

    # Metric names overlap across methods (for example worker_count and
    # maximum_parallel_width).  Compute requiredness from the union once so a
    # later optional family cannot overwrite an earlier required status.
    required_names = set(required)
    status_names = list(dict.fromkeys(
        COMMON_REQUIRED_METRICS
        + BLOCK_STM_REQUIRED_METRICS
        + METATRACK_REQUIRED_METRICS
        + BATCH_SI_REQUIRED_METRICS
        + LITERATURE_GRAPH_REQUIRED_METRICS
        + GROUNDHOG_REQUIRED_METRICS
        + literature_graph_required
    ))
    statuses: dict[str, str] = {
        name: _metric_state(metrics.get(name), required=name in required_names)
        for name in status_names
    }
    metric_missing: list[str] = []
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



def _derive_research_metrics(metrics: dict[str, Any]) -> None:
    def ratio(numerator: str, denominator: str, target: str) -> None:
        if metrics.get(target) is not None:
            return
        n = metrics.get(numerator)
        d = metrics.get(denominator)
        if isinstance(n, (int, float)) and not isinstance(n, bool) and isinstance(d, (int, float)) and not isinstance(d, bool) and d:
            metrics[target] = float(n) / float(d)

    submitted = metrics.get("submitted_unique_tx_count")
    if isinstance(submitted, (int, float)) and not isinstance(submitted, bool) and submitted:
        denominator = float(submitted)
        for source, target in (
            ("abort_count", "block_stm_abort_events_per_tx"),
            ("reexecution_count", "reexecution_events_per_tx"),
            ("validation_failure_count", "validation_failures_per_tx"),
            ("dependency_wait_count", "dependency_waits_per_tx"),
            ("dependency_edge_count", "dependency_edges_per_tx"),
            ("pairwise_conflict_check_count", "conflict_checks_per_tx"),
            ("write_opportunity_reuse_count", "write_reuse_per_tx"),
            ("groundhog_reservation_count", "groundhog_reservations_per_tx"),
            ("groundhog_modified_key_count", "groundhog_modified_keys_per_tx"),
        ):
            value = metrics.get(source)
            if metrics.get(target) is None and isinstance(value, (int, float)) and not isinstance(value, bool):
                metrics[target] = float(value) / denominator
        if metrics.get("nezha_hs_abort_count") is not None:
            metrics["nezha_hs_abort_rate"] = float(metrics.get("nezha_hs_abort_count") or 0) / denominator

    committed_blocks = metrics.get("actual_committed_block_count")
    if isinstance(committed_blocks, (int, float)) and not isinstance(committed_blocks, bool) and committed_blocks:
        for source, target in (("batch_count", "batch_count_per_block"), ("graph_color_count", "graph_colors_per_block"), ("batch_si_plan_payload_bytes", "batch_si_plan_bytes_per_block")):
            value = metrics.get(source)
            if metrics.get(target) is None and isinstance(value, (int, float)) and not isinstance(value, bool):
                metrics[target] = float(value) / float(committed_blocks)

    ratio("groundhog_constraint_conflict_count", "groundhog_execution_attempt_count", "groundhog_constraint_conflicts_per_attempt")
    ratio("groundhog_reservation_rollback_count", "groundhog_reservation_count", "groundhog_reservation_rollback_rate")
    ratio("groundhog_proposal_deferred_event_count", "groundhog_proposal_candidate_count", "groundhog_proposal_deferral_rate")
    ratio("aria_conflict_abort_count", "aria_candidate_transaction_count", "aria_conflict_abort_rate")
    ratio("aria_reexecution_count", "aria_candidate_transaction_count", "aria_reexecution_rate")

    fast = metrics.get("fast_track_logical_tx_count")
    conservative = metrics.get("conservative_track_logical_tx_count")
    if isinstance(fast, (int, float)) and not isinstance(fast, bool) and isinstance(conservative, (int, float)) and not isinstance(conservative, bool) and float(fast) + float(conservative) > 0:
        metrics["fast_track_ratio"] = float(fast) / (float(fast) + float(conservative))
    logical = metrics.get("submitted_unique_tx_count")
    if isinstance(logical, (int, float)) and not isinstance(logical, bool) and logical:
        fetches = metrics.get("physical_remote_fetch_count")
        writes = metrics.get("physical_remote_writeback_count")
        if isinstance(fetches, (int, float)) and not isinstance(fetches, bool):
            metrics["remote_fetches_per_logical_tx"] = float(fetches) / float(logical)
        if isinstance(writes, (int, float)) and not isinstance(writes, bool):
            metrics["remote_writebacks_per_logical_tx"] = float(writes) / float(logical)
        if isinstance(fetches, (int, float)) and not isinstance(fetches, bool) and isinstance(writes, (int, float)) and not isinstance(writes, bool):
            metrics["remote_operations_per_logical_tx"] = (float(fetches) + float(writes)) / float(logical)


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
    batch_si_unique_deferred_tx_ids = {tx_id for _, tx_id in batch_si_deferred_events}
    batch_si_unique_accepted_tx_ids = {tx_id for _, tx_id in batch_si_accepted_events}
    batch_si_unique_decision_tx_ids = batch_si_unique_deferred_tx_ids | batch_si_unique_accepted_tx_ids
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
        # Backward-compatible aliases remain, but paper analysis should use
        # the explicit event/unique/first-pass fields below.
        "batch_si_deferred_transaction_count": len(batch_si_deferred_events),
        "deferred_transaction_count": len(batch_si_deferred_events),
        "batch_si_accepted_transaction_count": len(batch_si_accepted_events),
        "batch_si_abort_rate": (len(batch_si_deferred_events) / batch_si_total) if batch_si_total else 0,
        "batch_si_deferred_event_count": len(batch_si_deferred_events),
        "batch_si_unique_deferred_tx_count": len(batch_si_unique_deferred_tx_ids),
        "batch_si_deferral_event_rate": (len(batch_si_deferred_events) / batch_si_total) if batch_si_total else 0,
        "batch_si_unique_deferral_rate": (
            len(batch_si_unique_deferred_tx_ids) / len(batch_si_unique_decision_tx_ids)
            if batch_si_unique_decision_tx_ids
            else 0
        ),
        "batch_si_mean_deferrals_per_finalized_tx": (
            len(batch_si_deferred_events) / len(batch_si_unique_accepted_tx_ids)
            if batch_si_unique_accepted_tx_ids
            else 0
        ),
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
