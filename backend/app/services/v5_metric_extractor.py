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

CALVIN_REQUIRED_METRICS = [
    "worker_count",
    "maximum_parallel_width",
    "calvin_lock_request_count",
    "calvin_lock_wait_count",
    "calvin_waiting_transaction_count",
    "calvin_blocked_lock_request_count",
    "calvin_lock_wakeup_count",
    "calvin_remote_read_count",
    "calvin_read_result_message_count",
    "calvin_read_result_physical_message_count",
    "calvin_remote_read_wait_ms",
    "calvin_outcome_message_count",
    "calvin_outcome_physical_message_count",
    "calvin_outcome_wait_ms",
    "abort_count",
    "reexecution_count",
]

CALVIN_STATELESS_REQUIRED_METRICS = [
    "calvin_stateless_remote_fetch_count",
    "calvin_stateless_remote_fetch_physical_count",
    "calvin_stateless_remote_writeback_count",
    "calvin_stateless_remote_writeback_physical_count",
    "calvin_stateless_remote_writeback_wait_ms",
    "calvin_consensus_version_binding_count",
    "calvin_stateless_block_start_read_count",
    "calvin_stateless_exact_predecessor_read_count",
    "calvin_client_state_version_metadata_count",
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
        # a terminal outcome. Historical Nezha/ACG artifacts may contain terminal
        # HS-abort no-ops; the retryable v2 lifecycle instead keeps HS victims
        # non-terminal until a later block actually finalizes them.
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
        "metatrack_classification_conflict_edge_count": cluster.get("metatrack_classification_conflict_edge_count"),
        "metatrack_classification_dependency_chain_max": cluster.get("metatrack_classification_dependency_chain_max"),
        "metatrack_classification_nontrivial_scc_count": cluster.get("metatrack_classification_nontrivial_scc_count"),
        "metatrack_classification_ambiguous_conflict_pair_count": cluster.get("metatrack_classification_ambiguous_conflict_pair_count"),
        "metatrack_classification_semantic_unsafe_unique_count": cluster.get("metatrack_classification_semantic_unsafe_unique_count"),
        "metatrack_classification_truth_scope": cluster.get("metatrack_classification_truth_scope"),
        # MBE_METATRACK_EFFECTIVE_FRONTIER_OBSERVABILITY_V12
        "metatrack_effective_frontier_policy": cluster.get("metatrack_effective_frontier_policy"),
        "metatrack_effective_frontier_width_zero_count": cluster.get("metatrack_effective_frontier_width_zero_count"),
        "metatrack_effective_frontier_width_one_count": cluster.get("metatrack_effective_frontier_width_one_count"),
        "metatrack_effective_frontier_width_multi_count": cluster.get("metatrack_effective_frontier_width_multi_count"),
        "metatrack_effective_frontier_width_max": cluster.get("metatrack_effective_frontier_width_max"),
        "metatrack_effective_frontier_raw_producer_count": cluster.get("metatrack_effective_frontier_raw_producer_count"),
        "metatrack_effective_frontier_reduced_producer_count": cluster.get("metatrack_effective_frontier_reduced_producer_count"),
        "metatrack_effective_frontier_track_demotion_count": cluster.get("metatrack_effective_frontier_track_demotion_count"),
        "metatrack_effective_frontier_truth_scope": cluster.get("metatrack_effective_frontier_truth_scope"),
        "metatrack_frontier_seal_count": cluster.get("metatrack_frontier_seal_count"),
        "metatrack_terminal_access_violation_count": cluster.get("metatrack_terminal_access_violation_count"),
        "metatrack_frontier_required_version_count": cluster.get("metatrack_frontier_required_version_count"),
        "metatrack_frontier_write_slot_count": cluster.get("metatrack_frontier_write_slot_count"),
        "metatrack_version_ticket_issued_count": cluster.get("metatrack_version_ticket_issued_count"),
        "metatrack_version_ticket_released_count": cluster.get("metatrack_version_ticket_released_count"),
        "metatrack_frontier_seal_build_us": cluster.get("metatrack_frontier_seal_build_us"),
        "metatrack_frontier_truth_scope": cluster.get("metatrack_frontier_truth_scope"),
        "versioned_state_ready_wave_count": cluster.get("versioned_state_ready_wave_count"),
        "versioned_state_ready_wait_observation_count": cluster.get("versioned_state_ready_wait_observation_count"),
        "versioned_state_ready_resolved_token_count": cluster.get("versioned_state_ready_resolved_token_count"),
        "versioned_state_probe_count": cluster.get("versioned_state_probe_count"),
        "versioned_state_probe_latency_ms": cluster.get("versioned_state_probe_latency_ms"),
        "versioned_state_ready_max_wave_width": cluster.get("versioned_state_ready_max_wave_width"),
        "versioned_state_ready_scheduler_mode": cluster.get("versioned_state_ready_scheduler_mode"),
        "versioned_wave_execution_policy": cluster.get("versioned_wave_execution_policy"),  # MBE_VERSIONED_WAVE_OBSERVABILITY_CLOSURE_V14B1
        "versioned_wave_delta_only_count": cluster.get("versioned_wave_delta_only_count"),
        "versioned_wave_full_fallback_count": cluster.get("versioned_wave_full_fallback_count"),
        "source_artifacts": list(required_artifacts),
        "missing": missing,
    }
    _apply_artifact_contract(metrics, cluster)
    _apply_workload_replay_metrics(metrics, run_dir)
    _apply_mempool_admission_metrics(metrics, run_dir)
    _apply_common_block_execution_timing(metrics, run_dir)
    configured_block_size = _int(metrics.get("configured_block_size"))
    raw_actual_average_tx_per_block = metrics.get("actual_average_tx_per_block")
    try:
        actual_average_tx_per_block = float(raw_actual_average_tx_per_block)
    except (TypeError, ValueError):
        actual_average_tx_per_block = None
    if configured_block_size > 0 and actual_average_tx_per_block is not None and actual_average_tx_per_block >= 0:
        metrics["actual_block_fill_ratio"] = actual_average_tx_per_block / configured_block_size
        metrics["block_utilization_truth_scope"] = "actual_average_committed_tx_per_block_over_configured_block_size"
    _apply_porygon_metrics(metrics, run_dir)
    _apply_calvin_metrics(metrics, run_dir)

    _apply_block_stm_metrics(metrics, run_dir)
    _apply_stateless_version_frontier_metrics(metrics, run_dir)
    _apply_metatrack_track_observability(metrics, run_dir)
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
    target_access_theta = audit.get("target_access_theta")
    if target_access_theta is None:
        target_access_theta = variant_parameters.get("target_theta")
    measured_access_theta = audit.get("measured_access_theta")
    target_account_write_theta = audit.get("target_account_write_theta")
    if target_account_write_theta is None and str(audit.get("theta_axis") or "").startswith("account"):
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
        "target_access_theta": target_access_theta,
        "measured_access_theta": measured_access_theta,
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

    def total_int(name: str) -> int:
        return sum(_int(block.get(name)) for block in blocks)

    def total_number(name: str) -> float:
        total = 0.0
        for block in blocks:
            value = block.get(name)
            if isinstance(value, bool) or value is None:
                continue
            try:
                total += float(value)
            except (TypeError, ValueError):
                continue
        return total

    metrics["block_execution_ms"] = total_int("block_execution_ms")

    # MBE_PORYGON_ESC_OWNERSHIP_TIMING_TRUTH_V19_20260921: distinguish
    # resource-sum timing from parallel-shard wall-clock critical path.
    per_leader_block_ms: list[float] = []
    per_leader_business_us: list[float] = []
    for summary in summaries:
        summary_blocks = summary.get("blocks") if isinstance(summary.get("blocks"), list) else []
        per_leader_block_ms.append(sum(float(block.get("block_execution_ms") or 0) for block in summary_blocks if isinstance(block, dict)))
        business_us = 0.0
        for block in summary_blocks:
            if not isinstance(block, dict):
                continue
            raw = block.get("transaction_execution_us")
            if raw is not None and not isinstance(raw, bool):
                try:
                    business_us += float(raw)
                    continue
                except (TypeError, ValueError):
                    pass
            business_us += float(_int(block.get("transaction_execution_ms"))) * 1000.0
        per_leader_business_us.append(business_us)
    metrics["execution_cpu_sum_ms"] = sum(per_leader_block_ms)
    metrics["execution_critical_path_ms"] = max(per_leader_block_ms, default=0.0)
    metrics["business_execution_cpu_sum_ms"] = sum(per_leader_business_us) / 1000.0
    metrics["business_execution_critical_path_ms"] = max(per_leader_business_us, default=0.0) / 1000.0
    metrics["execution_critical_path_truth_scope"] = "max_of_per_consensus_domain_leader_sequential_block_sums"

    planner_us_by_leader: list[float] = []
    for path in _batch_si_leader_summary_paths(run_dir):
        runtime_metrics = _read_json(path.parent / "runtime_metrics.json")
        counts = runtime_metrics.get("counts") if isinstance(runtime_metrics.get("counts"), dict) else {}
        planner_us_by_leader.append(float(_int(counts.get("consensus_planner_build_us"))))
    if any(value > 0 for value in planner_us_by_leader):
        metrics["planner_build_cpu_sum_ms"] = sum(planner_us_by_leader) / 1000.0
        metrics["planner_build_critical_path_ms"] = max(planner_us_by_leader, default=0.0) / 1000.0
        metrics["planner_build_timing_truth_scope"] = "proposal_leader_consensus_plan_build_only"

    transaction_us = total_number("transaction_execution_us")
    materialization_us = total_number("deterministic_materialization_us")
    metrics["transaction_execution_ms"] = (
        transaction_us / 1000.0 if transaction_us > 0 else total_int("transaction_execution_ms")
    )
    metrics["deterministic_materialization_ms"] = (
        materialization_us / 1000.0
        if materialization_us > 0
        else sum(
            _int(
                block.get("deterministic_materialization_ms")
                if block.get("deterministic_materialization_ms") is not None
                else block.get("deterministic_apply_ms")
            )
            for block in blocks
        )
    )
    metrics["state_commitment_ms"] = total_int("state_commitment_ms")
    if transaction_us > 0 or materialization_us > 0:
        metrics["execution_phase_timing_precision"] = "microsecond_accumulated_then_reported_ms"
    else:
        metrics["execution_phase_timing_precision"] = "millisecond_executor_fields"

    versioned_execution_ms = total_number("versioned_state_ready_execution_ms")
    if versioned_execution_ms > 0:
        metrics["versioned_state_ready_execution_ms"] = versioned_execution_ms
    native_metatrack_envelope_ms = total_number("metatrack_suspend_resume_execution_ms")
    if native_metatrack_envelope_ms > 0:
        metrics["metatrack_suspend_resume_execution_ms"] = native_metatrack_envelope_ms

    effective_worker_count = max(
        (_int(block.get("configured_worker_count") or block.get("worker_count")) for block in blocks),
        default=0,
    )
    if effective_worker_count > 0:
        metrics["configured_worker_count"] = effective_worker_count
        metrics["worker_count"] = effective_worker_count

    executor_ids = sorted({str(summary.get("block_executor_id")) for summary in summaries if summary.get("block_executor_id")})
    observed_parallel_width = max(
        (
            max(
                _int(block.get("maximum_parallel_width")),
                _int(block.get("max_inflight_business_executions")),
            )
            for block in blocks
        ),
        default=0,
    )
    if executor_ids == ["serial_block_executor"] and blocks:
        observed_parallel_width = max(observed_parallel_width, 1)
    if observed_parallel_width > 0:
        metrics["maximum_parallel_width"] = observed_parallel_width

    metrics["common_timing_block_count"] = len(blocks)
    metrics["common_block_execution_timing_available"] = True
    metrics["common_block_execution_timing_truth_scope"] = "leader_per_shard_runtime_block_evidence"
    root_versions = sorted({str(block.get("state_root_version")) for block in blocks if block.get("state_root_version")})
    if len(root_versions) == 1:
        metrics["state_root_version"] = root_versions[0]
    if executor_ids:
        metrics["timing_block_executor_ids"] = executor_ids
    for path in _batch_si_leader_summary_paths(run_dir):
        if path.is_file():
            rel = str(path.relative_to(run_dir)).replace("\\", "/")
            if rel not in metrics["source_artifacts"]:
                metrics["source_artifacts"].append(rel)

# MBE_PORYGON_PAPER_REPRO_20260921_V8_REFACTOR: derive Porygon v8 mechanism evidence from leader block summaries.
def _apply_porygon_metrics(metrics: dict[str, Any], run_dir: Path) -> None:
    summaries = [_read_json(path) for path in _batch_si_leader_summary_paths(run_dir)]
    summaries = [item for item in summaries if item and item.get("block_executor_id") == "porygon_block_executor"]
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

    def maximum(name: str) -> int:
        return max((_int(block.get(name)) for block in blocks), default=0)

    # MBE_PORYGON_UNIFIED_SHARD_V10_20260921: expose the unified frontend-shard -> Porygon execution-shard topology.
    compiled = _read_json(run_dir / "compiled_run_plan.json")
    node_configs = compiled.get("node_configs") if isinstance(compiled.get("node_configs"), list) else []
    execution_members: dict[str, list[str]] = {}
    consensus_domains: dict[str, list[str]] = {}
    for node in node_configs:
        if not isinstance(node, dict):
            continue
        node_id = str(node.get("node_id") or "")
        execution_shard = str(node.get("execution_shard_id") or node.get("shard_id") or "")
        consensus_domain = str(node.get("consensus_domain_id") or node.get("shard_id") or "")
        if execution_shard and node_id:
            execution_members.setdefault(execution_shard, []).append(node_id)
        if consensus_domain and node_id:
            consensus_domains.setdefault(consensus_domain, []).append(node_id)
    shard_tx_counts: dict[str, int] = {}
    for block in blocks:
        histogram = block.get("porygon_execution_shard_histogram")
        if isinstance(histogram, dict):
            for shard_id, raw in histogram.items():
                shard_tx_counts[str(shard_id)] = shard_tx_counts.get(str(shard_id), 0) + _int(raw)
    total_shard_txs = sum(shard_tx_counts.values())
    max_shard_share = (max(shard_tx_counts.values()) / total_shard_txs) if total_shard_txs and shard_tx_counts else None
    metrics.update({
        "porygon_topology_shard_count": len(execution_members),
        "porygon_ordering_domain_count": len(consensus_domains),
        "porygon_execution_shard_members": execution_members,
        "porygon_consensus_domains": consensus_domains,
        "porygon_execution_shard_transaction_counts": shard_tx_counts,
        "porygon_max_execution_shard_share": max_shard_share,
        "porygon_metrics_available": True,
        "porygon_witnessed_block_count": total("porygon_witnessed_block_count"),
        "porygon_cross_batch_witness_count": total("porygon_cross_batch_witness_count"),
        "porygon_pipeline_overlap_slot_count": total("porygon_pipeline_overlap_slot_count"),
        "porygon_execution_committee_count": maximum("porygon_execution_committee_count"),
        "porygon_execution_shard_count": maximum("porygon_execution_shard_count"),
        "porygon_active_execution_shard_count": maximum("porygon_active_execution_shard_count"),
        "porygon_execution_wave_count": total("porygon_execution_wave_count"),
        "porygon_maximum_wave_width": maximum("porygon_maximum_wave_width"),
        "porygon_intra_shard_transaction_count": total("porygon_intra_shard_transaction_count"),
        "porygon_cross_shard_transaction_count": total("porygon_cross_shard_transaction_count"),
        "porygon_logical_state_cross_shard_transaction_count": total("porygon_logical_state_cross_shard_transaction_count") or total("porygon_cross_shard_transaction_count"),
        "porygon_logical_state_cross_shard_ratio": ((total("porygon_logical_state_cross_shard_transaction_count") or total("porygon_cross_shard_transaction_count")) / total_shard_txs) if total_shard_txs else None,
        "porygon_cross_shard_metric_truth_scope": next((block.get("porygon_cross_shard_metric_truth_scope") for block in blocks if block.get("porygon_cross_shard_metric_truth_scope")), None),
        "porygon_witness_threshold_configured": maximum("porygon_witness_threshold_configured") or maximum("porygon_witness_threshold"),
        "porygon_witness_threshold_enforced": any(bool(block.get("porygon_witness_threshold_enforced")) for block in blocks),
        "porygon_witness_validation_mode": next((block.get("porygon_witness_validation_mode") for block in blocks if block.get("porygon_witness_validation_mode")), None),
        "porygon_single_shard_execution_count": total("porygon_single_shard_execution_count"),
        "porygon_multi_shard_update_count": total("porygon_multi_shard_update_count"),
        "porygon_state_lock_count": total("porygon_state_lock_count"),
        "porygon_meta_remote_state_control_plane_used": any(bool(block.get("porygon_meta_remote_state_control_plane_used")) for block in blocks),
        "porygon_physical_relay_protocol_used": any(bool(block.get("porygon_physical_relay_protocol_used")) for block in blocks),
        "porygon_pipeline_timing_truth_boundary": next((block.get("porygon_pipeline_timing_truth_boundary") for block in blocks if block.get("porygon_pipeline_timing_truth_boundary")), None),
        "porygon_mbe_consensus_adaptation": next((block.get("porygon_mbe_consensus_adaptation") for block in blocks if block.get("porygon_mbe_consensus_adaptation")), None),
        "porygon_storage_node_adaptation": next((block.get("porygon_storage_node_adaptation") for block in blocks if block.get("porygon_storage_node_adaptation")), None),
    })
    for path in _batch_si_leader_summary_paths(run_dir):
        if path.is_file():
            rel = str(path.relative_to(run_dir)).replace("\\", "/")
            if rel not in metrics["source_artifacts"]:
                metrics["source_artifacts"].append(rel)
    # MBE_PORYGON_ESC_OWNERSHIP_TIMING_TRUTH_V19_20260921: audit real ESC
    # business-execution ownership from every replica. Algorithmic timing
    # also keeps one deterministic representative per ESC so PBFT replicas do
    # not multiply the logical execution cost.
    all_porygon_summaries = []
    for path in sorted((run_dir / "nodes").glob("*/block_execution_summary.json")):
        payload = _read_json(path)
        if payload.get("block_executor_id") == "porygon_block_executor":
            all_porygon_summaries.append((path.parent.name, payload))
    local_count_by_node: dict[str, int] = {}
    business_us_by_node: dict[str, float] = {}
    exchange_wait_us_by_node: dict[str, float] = {}
    business_us_by_block: dict[tuple[int, str], list[float]] = {}
    exchange_wait_us_by_block: dict[tuple[int, str], list[float]] = {}
    critical_us_by_block: dict[tuple[int, str], list[float]] = {}
    critical_phase_rows_by_block: dict[tuple[int, str], list[tuple[str, float, float, float]]] = {}
    for node_id, summary in all_porygon_summaries:
        node_blocks = summary.get("blocks") if isinstance(summary.get("blocks"), list) else []
        local_count_by_node[node_id] = sum(_int(block.get("porygon_local_business_execution_count")) for block in node_blocks if isinstance(block, dict))
        business_us_by_node[node_id] = sum(float(block.get("porygon_business_execution_us") or 0) for block in node_blocks if isinstance(block, dict))
        exchange_wait_us_by_node[node_id] = sum(float(block.get("porygon_result_exchange_wait_us") or 0) for block in node_blocks if isinstance(block, dict))
        for block_index, block in enumerate(node_blocks):
            if not isinstance(block, dict):
                continue
            block_key = (_int(block.get("height")), str(block.get("block_hash") or f"index:{block_index}"))
            business_us = float(block.get("porygon_business_execution_us") or 0)
            exchange_wait_us = float(block.get("porygon_result_exchange_wait_us") or 0)
            critical_us = float(block.get("porygon_execution_critical_path_us") or 0)
            business_us_by_block.setdefault(block_key, []).append(business_us)
            exchange_wait_us_by_block.setdefault(block_key, []).append(exchange_wait_us)
            critical_us_by_block.setdefault(block_key, []).append(critical_us)
            critical_phase_rows_by_block.setdefault(block_key, []).append((node_id, critical_us, business_us, exchange_wait_us))
    representative_nodes = [sorted(members)[0] for _, members in sorted(execution_members.items()) if members]

    def sum_block_max_ms(rows: dict[tuple[int, str], list[float]]) -> float:
        return sum(max(values) for values in rows.values() if values) / 1000.0

    business_critical_path_ms = sum_block_max_ms(business_us_by_block)
    exchange_wait_critical_path_ms = sum_block_max_ms(exchange_wait_us_by_block)
    execution_critical_path_ms = sum_block_max_ms(critical_us_by_block)

    aligned_business_us = 0.0
    aligned_exchange_us = 0.0
    aligned_other_us = 0.0
    for rows in critical_phase_rows_by_block.values():
        if not rows:
            continue
        # The additive breakdown must use the same replica that defines the
        # per-block total critical path. Independent per-phase maxima can come
        # from different replicas and therefore are diagnostic only, not additive.
        _, critical_us, business_us, exchange_wait_us = min(rows, key=lambda row: (-row[1], row[0]))
        aligned_business_us += business_us
        aligned_exchange_us += exchange_wait_us
        aligned_other_us += max(0.0, critical_us - business_us - exchange_wait_us)
    aligned_business_ms = aligned_business_us / 1000.0
    aligned_exchange_ms = aligned_exchange_us / 1000.0
    aligned_other_ms = aligned_other_us / 1000.0
    leader_local_transaction_execution_ms = metrics.get("transaction_execution_ms")
    # MBE_PORYGON_ESC_OWNERSHIP_METRIC_KEYFIX_V24_20260921: topology uses s0/s1
    # while historical Porygon execution histograms use esc_0/esc_1. Preserve the
    # published histogram keys, but normalize aliases when deriving expected per-node
    # business execution ownership.
    def ownership_expected_count(execution_shard_id: str) -> int:
        if execution_shard_id in shard_tx_counts:
            return shard_tx_counts[execution_shard_id]
        if execution_shard_id.startswith("s") and execution_shard_id[1:].isdigit():
            return shard_tx_counts.get(f"esc_{execution_shard_id[1:]}", 0)
        if execution_shard_id.startswith("esc_") and execution_shard_id[4:].isdigit():
            return shard_tx_counts.get(f"s{execution_shard_id[4:]}", 0)
        return 0
    expected_by_node = {node_id: ownership_expected_count(shard_id) for shard_id, members in execution_members.items() for node_id in members}
    ownership_verified = bool(local_count_by_node) and bool(expected_by_node) and all(local_count_by_node.get(node_id) == expected for node_id, expected in expected_by_node.items())
    metrics.update({
        "porygon_local_business_execution_count_by_node": local_count_by_node,
        "porygon_expected_business_execution_count_by_node": expected_by_node,
        "porygon_esc_ownership_verified": ownership_verified,
        "porygon_business_execution_replica_cpu_sum_ms": sum(business_us_by_node.values()) / 1000.0,
        "porygon_business_execution_esc_representative_sum_ms": sum(business_us_by_node.get(node_id, 0.0) for node_id in representative_nodes) / 1000.0,
        "porygon_business_execution_critical_path_ms": business_critical_path_ms,
        "porygon_result_exchange_wait_replica_sum_ms": sum(exchange_wait_us_by_node.values()) / 1000.0,
        "porygon_result_exchange_wait_critical_path_ms": exchange_wait_critical_path_ms,
        "porygon_execution_critical_path_ms": execution_critical_path_ms,
        "porygon_execution_critical_path_business_component_ms": aligned_business_ms,
        "porygon_execution_critical_path_exchange_component_ms": aligned_exchange_ms,
        "porygon_execution_critical_path_other_component_ms": aligned_other_ms,
        "porygon_leader_local_transaction_execution_ms": leader_local_transaction_execution_ms,
        "porygon_timing_truth_scope": "per_block_max_replica_critical_path_summed_across_committed_blocks;independent_phase_maxima_are_diagnostic;additive_components_follow_the_same_per_block_critical_replica",
        "porygon_transaction_execution_ms_truth_scope": "normalized_to_porygon_execution_critical_path_ms;raw_global_leader_local_owned_business_time_preserved_as_porygon_leader_local_transaction_execution_ms",
    })
    if execution_critical_path_ms > 0:
        metrics["transaction_execution_ms"] = execution_critical_path_ms
        metrics["transaction_execution_ms_truth_scope"] = metrics["porygon_transaction_execution_ms_truth_scope"]

def _leader_node_ids(run_dir: Path) -> list[str]:
    plan = _read_json(run_dir / "compiled_run_plan.json")
    node_configs = plan.get("node_configs") if isinstance(plan.get("node_configs"), list) else []
    leaders = [
        str(item.get("node_id"))
        for item in node_configs
        if isinstance(item, dict) and item.get("node_id") and (item.get("leader") is True or item.get("role") == "leader")
    ]
    return leaders


def _apply_stateless_version_frontier_metrics(metrics: dict[str, Any], run_dir: Path) -> None:
    leader_ids = _leader_node_ids(run_dir)
    summaries: list[tuple[str, dict[str, Any]]] = []
    for node_id in leader_ids:
        rel = f"nodes/{node_id}/node_summary.json"
        payload = _read_json(run_dir / rel)
        if payload:
            summaries.append((rel, payload))
    if not summaries:
        return

    keys = (
        "stateless_version_admission_candidate_event_count",
        "stateless_version_admission_nonempty_frontier_event_count",
        "stateless_version_admission_zero_frontier_event_count",
        "stateless_version_admission_candidate_tx_count",
        "stateless_version_admission_admitted_tx_count",
        "stateless_version_admission_deferred_event_count",
        "stateless_version_admission_external_exact_dependency_tx_count",
        "stateless_version_admission_external_exact_dependency_edge_count",
        "stateless_version_admission_external_exact_dependency_token_count",
        "stateless_version_admission_external_version_ready_token_count",
        "stateless_version_admission_external_version_not_ready_token_count",
        "stateless_version_admission_internal_candidate_dependency_edge_count",
        "stateless_version_admission_deferred_direct_external_not_ready_count",
        "stateless_version_admission_deferred_internal_propagation_count",
    )
    totals = {key: 0 for key in keys}
    for rel, summary in summaries:
        counts = summary.get("runtime_metric_counts") if isinstance(summary.get("runtime_metric_counts"), dict) else {}
        for key in keys:
            totals[key] += _int(counts.get(key))
        if rel not in metrics["source_artifacts"]:
            metrics["source_artifacts"].append(rel)
    if totals["stateless_version_admission_candidate_event_count"] <= 0:
        return
    metrics.update(totals)
    events = totals["stateless_version_admission_candidate_event_count"]
    candidate = totals["stateless_version_admission_candidate_tx_count"]
    admitted = totals["stateless_version_admission_admitted_tx_count"]
    external_tokens = totals["stateless_version_admission_external_exact_dependency_token_count"]
    nonempty_events = totals["stateless_version_admission_nonempty_frontier_event_count"]
    zero_events = totals["stateless_version_admission_zero_frontier_event_count"]
    metrics["stateless_version_admission_candidate_mean_tx_count"] = candidate / events if events else None
    metrics["stateless_version_admission_mean_frontier_width"] = admitted / events if events else None
    metrics["stateless_version_admission_mean_nonempty_frontier_width"] = admitted / nonempty_events if nonempty_events else 0.0
    metrics["stateless_version_admission_zero_frontier_rate"] = zero_events / events if events else 0.0
    metrics["stateless_version_admission_admission_ratio"] = admitted / candidate if candidate else None
    metrics["stateless_version_admission_external_not_ready_ratio"] = totals["stateless_version_admission_external_version_not_ready_token_count"] / external_tokens if external_tokens else 0.0
    metrics["stateless_version_admission_direct_external_blocked_ratio"] = totals["stateless_version_admission_deferred_direct_external_not_ready_count"] / candidate if candidate else None
    metrics["stateless_version_admission_internal_propagated_blocked_ratio"] = totals["stateless_version_admission_deferred_internal_propagation_count"] / candidate if candidate else None
    metrics["stateless_version_admission_truth_scope"] = "sum_of_preconsensus_leader_candidate_events_across_execution_shards;no_pbft_replica_multiplication"


def _apply_metatrack_track_observability(metrics: dict[str, Any], run_dir: Path) -> None:
    summaries = [_read_json(path) for path in _batch_si_leader_summary_paths(run_dir)]
    summaries = [item for item in summaries if item.get("block_executor_id") == "metatrack_block_executor"]
    blocks = [
        block
        for summary in summaries
        for block in (summary.get("blocks") if isinstance(summary.get("blocks"), list) else [])
        if isinstance(block, dict)
    ]
    if not blocks or not any("metatrack_fast_business_execution_attempt_count" in block for block in blocks):
        return

    count_keys = (
        "metatrack_fast_initial_tx_count",
        "metatrack_conservative_initial_tx_count",
        "metatrack_fast_final_tx_count",
        "metatrack_conservative_final_tx_count",
        "metatrack_fast_business_execution_attempt_count",
        "metatrack_conservative_business_execution_attempt_count",
        "metatrack_fast_fallback_count",
        "metatrack_fast_discarded_tentative_count",
        "metatrack_conservative_reexecution_count",
    )
    float_keys = (
        "metatrack_fast_business_execution_sum_ms",
        "metatrack_conservative_business_execution_sum_ms",
        "metatrack_fast_discarded_execution_ms",
        "metatrack_fast_track_sojourn_sum_ms",
        "metatrack_conservative_track_sojourn_sum_ms",
        "metatrack_fast_state_wait_sum_ms",
        "metatrack_conservative_state_wait_sum_ms",
        "metatrack_fast_dependency_wait_sum_ms",
        "metatrack_conservative_dependency_wait_sum_ms",
        "metatrack_fast_queue_wait_sum_ms",
        "metatrack_conservative_queue_wait_sum_ms",
    )
    totals: dict[str, Any] = {key: sum(_int(block.get(key)) for block in blocks) for key in count_keys}
    for key in float_keys:
        totals[key] = sum(float(block.get(key) or 0.0) for block in blocks)
    metrics.update(totals)
    fast_initial = totals["metatrack_fast_initial_tx_count"]
    fast_attempts = totals["metatrack_fast_business_execution_attempt_count"]
    conservative_attempts = totals["metatrack_conservative_business_execution_attempt_count"]
    metrics["metatrack_fast_fallback_rate"] = totals["metatrack_fast_fallback_count"] / fast_initial if fast_initial else 0.0
    metrics["metatrack_conservative_reexecution_share"] = totals["metatrack_conservative_reexecution_count"] / conservative_attempts if conservative_attempts else 0.0
    metrics["metatrack_fast_business_execution_mean_ms"] = totals["metatrack_fast_business_execution_sum_ms"] / fast_attempts if fast_attempts else 0.0
    metrics["metatrack_conservative_business_execution_mean_ms"] = totals["metatrack_conservative_business_execution_sum_ms"] / conservative_attempts if conservative_attempts else 0.0

    fast_durations_ms: list[float] = []
    conservative_durations_ms: list[float] = []
    fast_sojourn_ms: list[float] = []
    conservative_sojourn_ms: list[float] = []
    per_leader_block_intervals: list[dict[str, list[tuple[int, int]]]] = []
    for path in _batch_si_leader_summary_paths(run_dir):
        business_path = path.parent / "business_execute_invocation_count_by_node.csv"
        block_intervals: dict[str, list[tuple[int, int]]] = {}
        if business_path.is_file():
            try:
                with business_path.open("r", encoding="utf-8", newline="") as handle:
                    for row in csv.DictReader(handle):
                        try:
                            raw_ns = row.get("duration_ns")
                            duration_ms = max(0.0, float(raw_ns) / 1_000_000.0) if raw_ns not in (None, "") else max(0.0, float(row.get("duration_us") or 0.0) / 1000.0)
                            sojourn_ms = max(0.0, float(row.get("sojourn_ns") or 0.0) / 1_000_000.0)
                            start_ns = int(row.get("attempt_start_offset_ns") or 0)
                            end_ns = int(row.get("attempt_end_offset_ns") or 0)
                        except (TypeError, ValueError):
                            continue
                        track = str(row.get("track") or "").strip().lower()
                        if track == "fast":
                            fast_durations_ms.append(duration_ms)
                            fast_sojourn_ms.append(sojourn_ms)
                        elif track == "conservative":
                            conservative_durations_ms.append(duration_ms)
                            conservative_sojourn_ms.append(sojourn_ms)
                        block_hash = str(row.get("block_hash") or "")
                        if block_hash and end_ns >= start_ns and end_ns > 0:
                            block_intervals.setdefault(block_hash, []).append((start_ns, end_ns))
            except OSError:
                pass
            rel_business = str(business_path.relative_to(run_dir)).replace("\\", "/")
            if rel_business not in metrics["source_artifacts"]:
                metrics["source_artifacts"].append(rel_business)
        if block_intervals:
            per_leader_block_intervals.append(block_intervals)
        rel = str(path.relative_to(run_dir)).replace("\\", "/")
        if rel not in metrics["source_artifacts"]:
            metrics["source_artifacts"].append(rel)

    def percentile(values: list[float], q: float) -> float | None:
        if not values:
            return None
        ordered = sorted(values)
        if len(ordered) == 1:
            return ordered[0]
        position = (len(ordered) - 1) * q
        lower = int(position)
        upper = min(lower + 1, len(ordered) - 1)
        weight = position - lower
        return ordered[lower] * (1.0 - weight) + ordered[upper] * weight

    def union_ns(intervals: list[tuple[int, int]]) -> int:
        if not intervals:
            return 0
        ordered = sorted(intervals)
        total = 0
        start, end = ordered[0]
        for next_start, next_end in ordered[1:]:
            if next_start <= end:
                end = max(end, next_end)
            else:
                total += max(0, end - start)
                start, end = next_start, next_end
        total += max(0, end - start)
        return total

    metrics["metatrack_fast_business_execution_p95_ms"] = percentile(fast_durations_ms, 0.95)
    metrics["metatrack_fast_business_execution_p99_ms"] = percentile(fast_durations_ms, 0.99)
    metrics["metatrack_conservative_business_execution_p95_ms"] = percentile(conservative_durations_ms, 0.95)
    metrics["metatrack_conservative_business_execution_p99_ms"] = percentile(conservative_durations_ms, 0.99)
    metrics["metatrack_fast_track_sojourn_mean_ms"] = totals["metatrack_fast_track_sojourn_sum_ms"] / fast_attempts if fast_attempts else 0.0
    metrics["metatrack_conservative_track_sojourn_mean_ms"] = totals["metatrack_conservative_track_sojourn_sum_ms"] / conservative_attempts if conservative_attempts else 0.0
    metrics["metatrack_fast_track_sojourn_p95_ms"] = percentile(fast_sojourn_ms, 0.95)
    metrics["metatrack_fast_track_sojourn_p99_ms"] = percentile(fast_sojourn_ms, 0.99)
    metrics["metatrack_conservative_track_sojourn_p95_ms"] = percentile(conservative_sojourn_ms, 0.95)
    metrics["metatrack_conservative_track_sojourn_p99_ms"] = percentile(conservative_sojourn_ms, 0.99)
    business_cpu_sum_ms = totals["metatrack_fast_business_execution_sum_ms"] + totals["metatrack_conservative_business_execution_sum_ms"]
    if metrics.get("business_execution_cpu_sum_ms") is not None:
        metrics["metatrack_legacy_execution_envelope_business_cpu_sum_ms"] = metrics.get("business_execution_cpu_sum_ms")
    if metrics.get("business_execution_critical_path_ms") is not None:
        metrics["metatrack_legacy_execution_envelope_business_critical_path_ms"] = metrics.get("business_execution_critical_path_ms")
    per_leader_business_active_ms = [sum(union_ns(intervals) for intervals in blocks.values()) / 1_000_000.0 for blocks in per_leader_block_intervals]
    business_critical_path_ms = max(per_leader_business_active_ms, default=0.0)
    metrics["metatrack_business_execution_cpu_sum_ms"] = business_cpu_sum_ms
    metrics["metatrack_business_execution_critical_path_ms"] = business_critical_path_ms
    metrics["business_execution_cpu_sum_ms"] = business_cpu_sum_ms
    metrics["business_execution_critical_path_ms"] = business_critical_path_ms
    metrics["business_execution_truth_scope"] = "metatrack_worker_attempt_duration_sum_and_per_block_interval_union_critical_path"
    metrics["metatrack_attempt_timing_precision"] = "nanosecond_monotonic"
    metrics["metatrack_track_duration_trace_available"] = bool(fast_durations_ms or conservative_durations_ms)
    metrics["metatrack_track_timing_truth_scope"] = "nanosecond_monotonic_attempts_and_track_sojourn_from_execution_runtime;state_and_dependency_wait_components_may_overlap;parallel_sums_are_not_wall_clock"


def _apply_calvin_metrics(metrics: dict[str, Any], run_dir: Path) -> None:
    plan = _read_json(run_dir / "compiled_run_plan.json")
    node_configs = plan.get("node_configs") if isinstance(plan.get("node_configs"), list) else []
    execution_shard_by_node = {
        str(item.get("node_id")): str(item.get("execution_shard_id") or item.get("shard_id") or "")
        for item in node_configs if isinstance(item, dict) and item.get("node_id")
    }
    per_shard: dict[str, dict[str, float]] = {}
    physical_totals = {"calvin_stateless_remote_fetch_physical_count": 0.0, "calvin_stateless_remote_writeback_physical_count": 0.0}
    mode = None
    seen = False
    max_width = 0
    worker_count = 0
    for path in sorted((run_dir / "nodes").glob("*/block_execution_summary.json")):
        payload = _read_json(path)
        executor_id = payload.get("block_executor_id")
        if executor_id not in {"calvin_block_executor", "stateless_calvin_block_executor"}:
            continue
        seen = True
        node_id = path.parent.name
        shard = execution_shard_by_node.get(node_id) or str(payload.get("shard_id") or node_id)
        row = per_shard.setdefault(shard, {})
        blocks = payload.get("blocks") if isinstance(payload.get("blocks"), list) else []
        totals: dict[str, float] = {}
        for block in blocks:
            if not isinstance(block, dict):
                continue
            mode = mode or block.get("calvin_mode")
            worker_count = max(worker_count, _int(block.get("worker_count")))
            max_width = max(max_width, _int(block.get("maximum_parallel_width")))
            for key in (
                "calvin_lock_request_count", "calvin_shared_lock_count", "calvin_exclusive_lock_count",
                "calvin_lock_wait_count", "calvin_waiting_transaction_count", "calvin_blocked_lock_request_count", "calvin_lock_wakeup_count", "calvin_lock_wait_ms",
                "calvin_local_read_count", "calvin_remote_read_count", "calvin_read_result_message_count", "calvin_read_result_physical_message_count",
                "calvin_remote_read_wait_ms", "calvin_outcome_message_count", "calvin_outcome_physical_message_count", "calvin_outcome_wait_ms", "calvin_active_execution_count", "calvin_passive_participant_count",
                "calvin_multi_partition_tx_count", "calvin_stateless_remote_fetch_count", "calvin_stateless_remote_writeback_count", "calvin_stateless_remote_writeback_wait_ms",
                "calvin_consensus_version_binding_count", "calvin_stateless_block_start_read_count", "calvin_stateless_exact_predecessor_read_count", "calvin_client_state_version_metadata_count",
                "abort_count", "reexecution_count",
            ):
                totals[key] = totals.get(key, 0.0) + float(block.get(key) or 0)
            for key in physical_totals:
                physical_totals[key] += float(block.get(key) or 0)
        # PBFT replicas execute the same partition. Keep one per-partition maximum
        # instead of multiplying algorithmic counts by the replica factor.
        for key, value in totals.items():
            row[key] = max(row.get(key, 0.0), value)
        rel = str(path.relative_to(run_dir)).replace("\\", "/")
        if rel not in metrics["source_artifacts"]:
            metrics["source_artifacts"].append(rel)
    if not seen:
        return
    def sum_shards(key: str) -> int:
        return int(sum(row.get(key, 0.0) for row in per_shard.values()))
    metrics.update({
        "calvin_metrics_available": True,
        "calvin_mode": mode,
        "worker_count": worker_count,
        "maximum_parallel_width": max_width,
        "calvin_lock_request_count": sum_shards("calvin_lock_request_count"),
        "calvin_shared_lock_count": sum_shards("calvin_shared_lock_count"),
        "calvin_exclusive_lock_count": sum_shards("calvin_exclusive_lock_count"),
        "calvin_lock_wait_count": sum_shards("calvin_lock_wait_count"),
        "calvin_waiting_transaction_count": sum_shards("calvin_waiting_transaction_count"),
        "calvin_blocked_lock_request_count": sum_shards("calvin_blocked_lock_request_count"),
        "calvin_lock_wakeup_count": sum_shards("calvin_lock_wakeup_count"),
        "calvin_lock_wait_ms": sum_shards("calvin_lock_wait_ms"),
        "calvin_local_read_count": sum_shards("calvin_local_read_count"),
        "calvin_remote_read_count": sum_shards("calvin_remote_read_count"),
        "calvin_read_result_message_count": sum_shards("calvin_read_result_message_count"),
        "calvin_read_result_physical_message_count": sum_shards("calvin_read_result_physical_message_count"),
        "calvin_remote_read_wait_ms": sum_shards("calvin_remote_read_wait_ms"),
        "calvin_outcome_message_count": sum_shards("calvin_outcome_message_count"),
        "calvin_outcome_physical_message_count": sum_shards("calvin_outcome_physical_message_count"),
        "calvin_outcome_wait_ms": sum_shards("calvin_outcome_wait_ms"),
        "calvin_active_execution_count": sum_shards("calvin_active_execution_count"),
        "calvin_passive_participant_count": sum_shards("calvin_passive_participant_count"),
        "calvin_multi_partition_tx_count": sum_shards("calvin_multi_partition_tx_count"),
        "calvin_stateless_remote_fetch_count": sum_shards("calvin_stateless_remote_fetch_count"),
        "calvin_stateless_remote_fetch_physical_count": int(physical_totals["calvin_stateless_remote_fetch_physical_count"]),
        "calvin_stateless_remote_writeback_count": sum_shards("calvin_stateless_remote_writeback_count"),
        "calvin_stateless_remote_writeback_physical_count": int(physical_totals["calvin_stateless_remote_writeback_physical_count"]),
        "calvin_stateless_remote_writeback_wait_ms": sum_shards("calvin_stateless_remote_writeback_wait_ms"),
        "calvin_consensus_version_binding_count": sum_shards("calvin_consensus_version_binding_count"),
        "calvin_stateless_block_start_read_count": sum_shards("calvin_stateless_block_start_read_count"),
        "calvin_stateless_exact_predecessor_read_count": sum_shards("calvin_stateless_exact_predecessor_read_count"),
        "calvin_client_state_version_metadata_count": sum_shards("calvin_client_state_version_metadata_count"),
        "abort_count": sum_shards("abort_count"),
        "reexecution_count": sum_shards("reexecution_count"),
        "calvin_metric_truth_scope": "sum_of_per_execution_shard_replica_maxima",
    })


def _apply_block_stm_metrics(metrics: dict[str, Any], run_dir: Path) -> None:
    aggregate_path = run_dir / "aggregate" / "block_stm_aggregate_summary.json"
    aggregate = _read_json(aggregate_path)
    if aggregate.get("status") == "available":
        rows = aggregate.get("per_validator") if isinstance(aggregate.get("per_validator"), list) else []

        def replica_deduplicated_sum(field: str, aggregate_fallback: str | None = None) -> int:
            by_shard: dict[str, int] = {}
            for row in rows:
                if not isinstance(row, dict):
                    continue
                shard = str(row.get("shard_id") or "")
                if not shard:
                    continue
                value = _int(row.get(field))
                if value > by_shard.get(shard, 0):
                    by_shard[shard] = value
            if by_shard:
                return sum(by_shard.values())
            return _int(aggregate.get(aggregate_fallback or field))

        metrics.update(
            {
                "block_stm_metrics_available": True,
                "worker_count": aggregate.get("worker_count"),
                "maximum_parallel_width": aggregate.get("maximum_parallel_width"),
                "maximum_concurrent_executions": aggregate.get("maximum_concurrent_executions"),
                "abort_count": replica_deduplicated_sum("abort_count"),
                "reexecution_count": replica_deduplicated_sum("reexecution_count"),
                "dependency_wait_count": replica_deduplicated_sum("dependency_wait_count"),
                "dependency_resume_count": replica_deduplicated_sum("dependency_resume_count"),
                "validation_failure_count": replica_deduplicated_sum("validation_failure_count"),
                "maximum_incarnation_observed": _int(aggregate.get("maximum_incarnation")),
                "serial_fallback_count": aggregate.get("serial_fallback_count"),
                "serial_equivalent": aggregate.get("serial_equivalent"),
                "block_stm_metric_truth_scope": "sum_of_per_shard_replica_maxima_from_per_validator_evidence",
            }
        )
        # The stateless exact-version layer can delegate same-block dependencies
        # back to Block-STM. This evidence lives in per-block execution summaries,
        # not in block_stm_aggregate_summary.json. Select one leader summary per
        # shard (with a per-shard max fallback) so PBFT replicas cannot multiply
        # the count.
        delegation_by_shard: dict[str, int] = {}
        delegation_evidence = False
        for summary_path in _batch_si_leader_summary_paths(run_dir):
            summary = _read_json(summary_path)
            if summary.get("block_executor_id") != "block_stm_block_executor":
                continue
            shard_id = str(summary.get("shard_id") or summary_path.parent.name)
            block_total = 0
            block_has_evidence = False
            for block in summary.get("blocks") if isinstance(summary.get("blocks"), list) else []:
                if not isinstance(block, dict):
                    continue
                if "block_stm_internal_version_dependency_delegated_count" not in block:
                    continue
                block_has_evidence = True
                block_total += _int(block.get("block_stm_internal_version_dependency_delegated_count"))
            if block_has_evidence:
                delegation_evidence = True
                delegation_by_shard[shard_id] = max(delegation_by_shard.get(shard_id, 0), block_total)
                rel_summary = str(summary_path.relative_to(run_dir)).replace("\\", "/")
                if rel_summary not in metrics["source_artifacts"]:
                    metrics["source_artifacts"].append(rel_summary)
        if delegation_evidence:
            metrics["block_stm_internal_version_dependency_delegated_count"] = sum(delegation_by_shard.values())
            metrics["block_stm_internal_version_dependency_delegated_truth_scope"] = (
                "sum_of_per_shard_leader_maxima_from_block_execution_evidence"
            )

        unique_aborted = 0
        unique_reexecuted = 0
        unique_evidence = False
        for summary_path in _batch_si_leader_summary_paths(run_dir):
            summary = _read_json(summary_path)
            if summary.get("block_executor_id") != "block_stm_block_executor":
                continue
            for block in summary.get("blocks") if isinstance(summary.get("blocks"), list) else []:
                if not isinstance(block, dict):
                    continue
                block_metrics = block.get("block_stm_metrics") if isinstance(block.get("block_stm_metrics"), dict) else {}
                if "unique_aborted_transaction_count" in block_metrics or "unique_reexecuted_transaction_count" in block_metrics:
                    unique_evidence = True
                    unique_aborted += _int(block_metrics.get("unique_aborted_transaction_count"))
                    unique_reexecuted += _int(block_metrics.get("unique_reexecuted_transaction_count"))
        if unique_evidence:
            submitted = _int(metrics.get("submitted_unique_tx_count"))
            metrics["block_stm_unique_aborted_tx_count"] = unique_aborted
            metrics["block_stm_unique_reexecuted_tx_count"] = unique_reexecuted
            metrics["block_stm_unique_aborted_tx_rate"] = unique_aborted / submitted if submitted else 0.0
            metrics["block_stm_unique_reexecuted_tx_rate"] = unique_reexecuted / submitted if submitted else 0.0
            metrics["block_stm_unique_tx_truth_scope"] = "sum_of_per_execution_shard_leader_block_unique_transaction_counts"

        rel = "aggregate/block_stm_aggregate_summary.json"
        if rel not in metrics["source_artifacts"]:
            metrics["source_artifacts"].append(rel)
        return

    # Historical/single-node fallback.
    block_stm_summary = _read_json(run_dir / "block_stm_summary.json")
    block_stm_metrics = block_stm_summary.get("block_stm_metrics") if isinstance(block_stm_summary.get("block_stm_metrics"), dict) else {}
    if not block_stm_metrics:
        return
    metrics.update(
        {
            "block_stm_metrics_available": True,
            "worker_count": block_stm_metrics.get("worker_count"),
            "maximum_parallel_width": block_stm_metrics.get("maximum_parallel_width"),
            "abort_count": block_stm_metrics.get("abort_count"),
            "reexecution_count": block_stm_metrics.get("reexecution_count"),
            "dependency_wait_count": block_stm_metrics.get("dependency_wait_count"),
            "dependency_resume_count": block_stm_metrics.get("dependency_resume_count"),
            "validation_failure_count": block_stm_metrics.get("validation_failure_count"),
            "maximum_incarnation_observed": block_stm_metrics.get("maximum_incarnation"),
            "serial_equivalent": block_stm_summary.get("serial_equivalent"),
            "block_stm_metric_truth_scope": "single_node_summary",
        }
    )
    if "block_stm_summary.json" not in metrics["source_artifacts"]:
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
        if item.get("block_executor_id") in {"cg_block_executor", "acg_block_executor", "bsx_block_executor", "fabricpp_cg_block_executor"}
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
        "configured_worker_count": max((_int(block.get("configured_worker_count") or block.get("worker_count")) for block in blocks), default=0),
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
        "literature_plan_parse_ms": total("literature_plan_parse_ms"),
        "literature_plan_verify_ms": total("literature_plan_verify_ms"),
        "cg_planning_worker_count": max((_int(block.get("cg_planning_worker_count")) for block in blocks), default=0),
    })
    plan_verify_modes = sorted({
        str(block.get("literature_plan_verify_mode"))
        for block in blocks
        if block.get("literature_plan_verify_mode")
    })
    if len(plan_verify_modes) == 1:
        metrics["literature_plan_verify_mode"] = plan_verify_modes[0]
    if executor_ids == ["cg_block_executor"]:
        # CG cycle victims are algorithmic abort decisions, but retryable-v4
        # returns their signed transactions to the FIFO mempool. Keep attempt
        # abort evidence separate from eventual logical finality.
        cg_candidates = total("cg_candidate_transaction_count")
        cg_aborts = total("cg_cycle_abort_count")
        cg_deferred = total("cg_cycle_deferred_retry_count")
        unique_deferred = {
            str(tx_id)
            for block in blocks
            for tx_id in (block.get("cg_cycle_deferred_tx_ids") or [])
            if str(tx_id)
        }
        metrics["abort_count"] = cg_aborts
        metrics["cg_candidate_transaction_count"] = cg_candidates
        metrics["cg_cycle_abort_count"] = cg_aborts
        metrics["cg_cycle_abort_decision_count"] = cg_aborts
        metrics["cg_cycle_resolution_count"] = total("cg_cycle_resolution_count")
        metrics["deferred_transaction_count"] = cg_deferred
        metrics["cg_cycle_deferred_retry_count"] = cg_deferred
        metrics["cg_cycle_unique_deferred_tx_count"] = len(unique_deferred)
        metrics["cg_cycle_abort_rate"] = (cg_aborts / cg_candidates) if cg_candidates else 0
        metrics["cg_cycle_attempt_abort_rate"] = (cg_aborts / cg_candidates) if cg_candidates else 0
        submitted = _int(metrics.get("submitted_unique_tx_count"))
        metrics["cg_cycle_unique_deferred_rate"] = (len(unique_deferred) / submitted) if submitted else 0
        retry_lifecycles = sorted({
            str(block.get("cg_cycle_retry_lifecycle"))
            for block in blocks
            if block.get("cg_cycle_retry_lifecycle")
        })
        metrics["cg_cycle_retry_lifecycle"] = (
            retry_lifecycles[0] if len(retry_lifecycles) == 1 else "fifo_deferred_to_later_block"
        )
        metrics["cg_reference_commit_order_count"] = total("cg_reference_commit_order_count") or total("cg_serial_commit_count")
        metrics["cg_execution_worker_count"] = max((_int(block.get("cg_execution_worker_count")) for block in blocks), default=0)
        metrics["cg_johnson_cycle_budget"] = max((_int(block.get("cg_johnson_cycle_budget")) for block in blocks), default=0)
        metrics["cg_johnson_traversal_work_budget"] = max((_int(block.get("cg_johnson_traversal_work_budget")) for block in blocks), default=0)
        metrics["cg_johnson_plan_work_budget"] = max((_int(block.get("cg_johnson_plan_work_budget")) for block in blocks), default=0)
        metrics["cg_large_rmw_clique_threshold"] = max((_int(block.get("cg_large_rmw_clique_threshold")) for block in blocks), default=0)
        for source_key, target_key in (
            ("cg_cycle_space_policy", "cg_cycle_space_policy"),
            ("cg_large_rmw_clique_policy", "cg_large_rmw_clique_policy"),
            ("cg_execution_adaptation_mode", "cg_execution_adaptation_mode"),
        ):
            values = sorted({str(block.get(source_key)) for block in blocks if block.get(source_key)})
            if len(values) == 1:
                metrics[target_key] = values[0]
        requested = _int(metrics.get("configured_worker_count") or metrics.get("worker_count"))
        execution_workers = _int(metrics.get("cg_execution_worker_count"))
        planning_workers = _int(metrics.get("cg_planning_worker_count"))
        max_parallel = _int(metrics.get("maximum_parallel_width"))
        metrics["cg_worker_truth_valid"] = bool(
            requested > 0
            and execution_workers == requested
            and planning_workers == 1
            and 0 <= max_parallel <= execution_workers
        )
        commit_modes = sorted({str(block.get("cg_commit_mode")) for block in blocks if block.get("cg_commit_mode")})
        if len(commit_modes) == 1:
            metrics["cg_commit_mode"] = commit_modes[0]
    if executor_ids == ["fabricpp_cg_block_executor"]:
        fabricpp_candidates = total("fabricpp_candidate_transaction_count")
        fabricpp_aborts = total("fabricpp_cycle_abort_count")
        metrics["abort_count"] = fabricpp_aborts
        metrics["fabricpp_candidate_transaction_count"] = fabricpp_candidates
        metrics["fabricpp_conflict_edge_count"] = total("fabricpp_conflict_edge_count")
        metrics["fabricpp_cycle_abort_count"] = fabricpp_aborts
        metrics["fabricpp_cycle_resolution_count"] = total("fabricpp_cycle_resolution_count")
        metrics["fabricpp_cycle_abort_rate"] = (fabricpp_aborts / fabricpp_candidates) if fabricpp_candidates else 0
    if executor_ids == ["acg_block_executor"]:
        # Nezha HS abort decisions remain algorithm evidence, but retryable-v2
        # defers their signed transactions to a later block instead of counting
        # them as terminal failures. Preserve both attempt and unique evidence.
        hs_candidates = total("nezha_hs_candidate_transaction_count")
        hs_aborts = total("nezha_hs_abort_count")
        hs_deferred = total("nezha_hs_deferred_retry_count")
        unique_deferred = {
            str(tx_id)
            for block in blocks
            for tx_id in (block.get("nezha_hs_deferred_tx_ids") or [])
            if str(tx_id)
        }
        metrics["abort_count"] = hs_aborts
        metrics["nezha_hs_abort_count"] = hs_aborts
        metrics["nezha_hs_abort_decision_count"] = hs_aborts
        metrics["nezha_hs_candidate_transaction_count"] = hs_candidates
        metrics["nezha_hs_accepted_transaction_count"] = total("nezha_hs_accepted_transaction_count")
        metrics["deferred_transaction_count"] = hs_deferred
        metrics["nezha_hs_deferred_retry_count"] = hs_deferred
        metrics["nezha_hs_unique_deferred_tx_count"] = len(unique_deferred)
        metrics["nezha_hs_attempt_abort_rate"] = (hs_aborts / hs_candidates) if hs_candidates else 0
        submitted = _int(metrics.get("submitted_unique_tx_count"))
        metrics["nezha_hs_unique_deferred_rate"] = (len(unique_deferred) / submitted) if submitted else 0
        metrics["nezha_hs_retry_lifecycle"] = "fifo_deferred_to_later_block"

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
        metrics["network_scope_categories"] = network.get("scope_categories") if isinstance(network.get("scope_categories"), dict) else {}
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
        # block_stm_aggregate_summary keeps physical-replica totals as raw
        # mechanism evidence. _apply_block_stm_metrics() has already converted
        # the formal Block-STM counters to the paper truth scope
        # (per-shard replica maxima, then cross-shard sum). Never overwrite those
        # canonical values with physical PBFT replica totals here.
        metrics["block_stm_mechanism_metric_truth_scope"] = block_stm.get("metric_truth_scope")
        for source, target in (
            ("abort_count", "block_stm_physical_replica_abort_count"),
            ("reexecution_count", "block_stm_physical_replica_reexecution_count"),
            ("validation_failure_count", "block_stm_physical_replica_validation_failure_count"),
            ("dependency_wait_count", "block_stm_physical_replica_dependency_wait_count"),
            ("dependency_resume_count", "block_stm_physical_replica_dependency_resume_count"),
        ):
            value = block_stm.get(source)
            if value is not None:
                metrics[target] = value
        for key in ("worker_count", "maximum_parallel_width", "serial_equivalent"):
            if metrics.get(key) is None and block_stm.get(key) is not None:
                metrics[key] = block_stm.get(key)
    remote_state = mechanism.get("remote_state") if isinstance(mechanism.get("remote_state"), dict) else {}
    if remote_state:
        metrics.update(remote_state)


def _literature_graph_required_metrics(method_id: str | None) -> list[str]:
    normalized = str(method_id or "").lower()
    if normalized not in {"hash_cg", "hash_acg", "hash_bsx", "hash_fabricpp_cg"}:
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
    if normalized == "hash_fabricpp_cg":
        required.extend(["pairwise_conflict_check_count", "abort_count", "fabricpp_candidate_transaction_count", "fabricpp_cycle_abort_count", "fabricpp_cycle_resolution_count", "fabricpp_cycle_abort_rate"])
    if normalized == "hash_cg":
        required.extend([
            "pairwise_conflict_check_count",
            "abort_count",
            "cg_candidate_transaction_count",
            "cg_cycle_abort_count",
            "cg_cycle_resolution_count",
            "cg_cycle_abort_rate",
            "cg_cycle_deferred_retry_count",
            "cg_cycle_retry_lifecycle",
            "cg_execution_worker_count",
            "cg_planning_worker_count",
            "cg_worker_truth_valid",
            "literature_plan_parse_ms",
            "literature_plan_verify_ms",
            "cg_johnson_cycle_budget",
            "cg_johnson_traversal_work_budget",
            "cg_johnson_plan_work_budget",
            "cg_cycle_space_policy",
            "cg_large_rmw_clique_policy",
            "cg_large_rmw_clique_threshold",
        ])
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
    uses_calvin = (
        normalized_method_id in {"stateful_calvin", "stateless_calvin"}
        or metrics.get("block_executor_id") in {"calvin_block_executor", "stateless_calvin_block_executor"}
        or metrics.get("calvin_metrics_available") is True
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
    if uses_calvin:
        required.extend(CALVIN_REQUIRED_METRICS)
    if normalized_method_id == "stateless_calvin" or metrics.get("block_executor_id") == "stateless_calvin_block_executor":
        required.extend(CALVIN_STATELESS_REQUIRED_METRICS)
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
        + CALVIN_REQUIRED_METRICS
        + CALVIN_STATELESS_REQUIRED_METRICS
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
        if metrics.get("block_executor_id") == "block_stm_block_executor":
            value = metrics.get("abort_count")
            if metrics.get("block_stm_abort_events_per_tx") is None and isinstance(value, (int, float)) and not isinstance(value, bool):
                metrics["block_stm_abort_events_per_tx"] = float(value) / denominator
        for source, target in (
            ("reexecution_count", "reexecution_events_per_tx"),
            ("validation_failure_count", "validation_failures_per_tx"),
            ("dependency_wait_count", "dependency_waits_per_tx"),
            ("block_stm_internal_version_dependency_delegated_count", "block_stm_internal_version_dependencies_delegated_per_tx"),
            ("dependency_edge_count", "dependency_edges_per_tx"),
            ("pairwise_conflict_check_count", "conflict_checks_per_tx"),
            ("write_opportunity_reuse_count", "write_reuse_per_tx"),
            ("groundhog_reservation_count", "groundhog_reservations_per_tx"),
            ("groundhog_modified_key_count", "groundhog_modified_keys_per_tx"),
        ):
            value = metrics.get(source)
            if metrics.get(target) is None and isinstance(value, (int, float)) and not isinstance(value, bool):
                metrics[target] = float(value) / denominator
        if metrics.get("nezha_hs_abort_count") is not None and metrics.get("nezha_hs_abort_rate") is None:
            hs_candidates = metrics.get("nezha_hs_candidate_transaction_count")
            hs_denominator = float(hs_candidates) if isinstance(hs_candidates, (int, float)) and not isinstance(hs_candidates, bool) and hs_candidates else denominator
            metrics["nezha_hs_abort_rate"] = float(metrics.get("nezha_hs_abort_count") or 0) / hs_denominator

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
