from __future__ import annotations

import csv
import json
import math
import shutil
import zipfile
from collections import defaultdict
from pathlib import Path
from typing import Any

import yaml

from backend.app.models.v3_metatrack_formal_benchmark import V3FormalMetatrackBenchmarkRequest
from backend.app.services.artifact_manager import ARTIFACT_ALLOWLIST, get_artifact_path
from backend.app.services.job_manager import JobManager, JobNotFound
from backend.app.services.v3_composer_draft_runner import model_dump
from backend.app.services.v3_composer_draft_validator import validate_v3_composer_draft
from backend.app.services.v3_composer_catalog import CATALOG
from backend.app.services.v3_go_runtime_runner import ROLE_SEPARATED_CHAIN_PROFILE, run_go_v3_runtime
from backend.app.services.v3_metatrack_formal_baselines import get_formal_baseline, validate_formal_baseline_registry
from backend.app.services.v3_runtime_topology import stage_metadata
from backend.app.services.v3_saved_config_store import get_saved_config


ROOT = Path(__file__).resolve().parents[3]
FORMAL_RUNS_ROOT = ROOT / ".cache" / "v3_metatrack_formal_runs"
FORMAL_PLUGIN_PROFILE_ID = "metatrack_formal_single"
TRUTH_BOUNDARY = "local_emulator_controlled_benchmark_not_production_chain"
RESOURCE_LIMITS = {
    "max_run_count": 200,
    "max_total_tx_count": 20_000_000,
    "max_seed_count": 10,
    "max_tx_count_per_run": 1_000_000,
    "max_scan_points": 20,
}
AGGREGATE_METRICS = [
    "throughput_tps",
    "avg_latency_ms",
    "p95_latency_ms",
    "p99_latency_ms",
    "remote_fetch_count",
    "cross_shard_ratio",
    "cross_shard_tx_count",
    "remote_state_access_count",
    "blocked_tx_count",
    "aggregation_ratio",
    "avg_routing_overhead_ms",
    "avg_execution_latency_ms",
    "avg_state_access_latency_ms",
    "avg_commit_latency_ms",
    "consensus_latency_ms",
    "control_overhead_ratio",
]
METRIC_ALIASES = {
    "throughput_tps": ["throughput_tps", "tps", "TPS", "avg_tps", "transactions_per_second"],
    "avg_latency_ms": ["avg_latency_ms", "average_latency_ms", "latency_avg_ms", "avg_latency", "mean_latency_ms"],
    "p95_latency_ms": ["p95_latency_ms", "latency_p95_ms", "p95", "p95_ms"],
    "p99_latency_ms": ["p99_latency_ms", "latency_p99_ms", "p99", "p99_ms"],
    "remote_fetch_count": ["remote_fetch_count", "remote_state_fetch_count"],
    "cross_shard_ratio": ["cross_shard_ratio"],
    "cross_shard_tx_count": ["cross_shard_tx_count"],
    "remote_state_access_count": ["remote_state_access_count", "remote_access_count"],
    "blocked_tx_count": ["blocked_tx_count"],
    "aggregation_ratio": ["aggregation_ratio"],
    "avg_routing_overhead_ms": ["avg_routing_overhead_ms", "routing_overhead_ms"],
    "avg_execution_latency_ms": ["avg_execution_latency_ms", "execution_latency_ms"],
    "avg_state_access_latency_ms": ["avg_state_access_latency_ms", "state_access_latency_ms"],
    "avg_commit_latency_ms": ["avg_commit_latency_ms", "commit_latency_ms"],
    "consensus_latency_ms": ["consensus_latency_ms", "avg_consensus_latency_ms", "pbft_consensus_latency_ms"],
    "control_overhead_ratio": ["control_overhead_ratio"],
}
SUMMARY_SOURCES = [
    ("runtime_summary", None),
    ("summary.json", "json"),
    ("summary.csv", "csv_first"),
    ("metatrack_summary.json", "json"),
    ("metatrack_summary.csv", "csv_first"),
    ("metatrack_mechanism_metrics.csv", "csv_first"),
]
LATENCY_SOURCES = ["tx_results.csv", "metatrack_latency.csv"]
SUCCESS_STATUSES = {"", "success", "completed", "ok", "true", "1"}
RAW_SUMMARY_FIELDS = [
    "run_index",
    "status",
    "error",
    "experiment_type",
    "method_config_name",
    "baseline_id",
    "method_config_id",
    "workload_scenario",
    "seed",
    "formal_tx_count",
    "throughput_tps",
    "avg_latency_ms",
    "p95_latency_ms",
    "p99_latency_ms",
    "cross_shard_ratio",
    "cross_shard_tx_count",
    "remote_fetch_count",
    "remote_state_access_count",
    "blocked_tx_count",
    "aggregation_ratio",
    "avg_routing_overhead_ms",
    "avg_execution_latency_ms",
    "avg_state_access_latency_ms",
    "avg_commit_latency_ms",
    "consensus_latency_ms",
    "control_overhead_ratio",
    "child_output_dir",
    "scan_variable",
    "scan_value",
]
AGGREGATE_SUMMARY_FIELDS = [
    "experiment_type",
    "method_config_name",
    "baseline_id",
    "method_or_baseline_id",
    "workload_scenario",
    "metric",
    "metric_available",
    "mean",
    "ci95",
    "count",
    "std",
    "min",
    "max",
    "scan_variable",
    "scan_value",
    "method_config_id",
    "workload_config_name",
    "workload_config_id",
    "topology_config_name",
    "topology_config_id",
]
FIGURE_FIELDS = ["figure_group", "x_value", "series", "metric", "mean", "ci95", "count"]
EXTRACTION_FIELDS = ["run_index", "child_output_dir", "metric", "value", "source_file", "source_field", "status", "missing_reason"]
MISSING_METRIC_FIELDS = ["experiment_type", "method_config_name", "baseline_id", "workload_scenario", "metric", "missing_count", "group_count", "reason"]
RUN_MATRIX_FIELDS = [
    "run_index",
    "experiment_type",
    "baseline_id",
    "baseline_label",
    "method_config_id",
    "method_config_name",
    "workload_config_id",
    "workload_config_name",
    "topology_config_id",
    "topology_config_name",
    "seed",
    "formal_tx_count",
    "scan_variable",
    "scan_value",
    "workload_scenario",
    "runtime_evidence_mode",
]
CHILD_INDEX_FIELDS = [
    "run_index",
    "baseline_id",
    "method_config_id",
    "workload_scenario",
    "seed",
    "child_output_dir",
    "summary_json_exists",
    "summary_json_path",
    "runtime_log_path",
    "routing_log_exists",
    "execution_log_exists",
    "state_access_log_exists",
    "relay_mvp_summary_exists",
    "state_authenticity_summary_exists",
    "status",
    "error",
]
CHART_PREVIEW_METRICS = [
    "throughput_tps",
    "avg_latency_ms",
    "p95_latency_ms",
    "p99_latency_ms",
    "remote_fetch_count",
    "cross_shard_ratio",
    "aggregation_ratio",
    "control_overhead_ratio",
]
FORMAL_ROOT_ZIP_FILES = [
    "summary.json",
    "formal_benchmark_report.md",
    "formal_benchmark_config.json",
    "formal_matrix_preview.json",
    "formal_run_manifest.json",
    "formal_progress.json",
    "formal_run_matrix.csv",
    "formal_run_index.csv",
    "formal_failed_runs.csv",
    "formal_child_artifact_index.csv",
    "formal_metric_extraction_report.csv",
    "formal_metric_extraction_report.json",
    "formal_missing_metrics.csv",
    "formal_raw_summary.csv",
    "formal_aggregate_summary.csv",
    "formal_workload_comparison.csv",
    "formal_latency_summary.csv",
    "formal_throughput_summary.csv",
    "formal_overhead_summary.csv",
    "formal_confidence_interval.csv",
    "formal_paper_figure_data.csv",
    "formal_chart_preview.json",
    "formal_reproducibility_manifest.json",
]
FORMAL_CHILD_ZIP_FILES = [
    "generated_experiment_profile.json",
    "generated_plugin_profile.json",
    "summary.json",
    "routing_log.csv",
    "execution_log.csv",
    "state_access_log.csv",
    "state_commit_log.csv",
    "relay_mvp_summary.json",
    "state_authenticity_summary.json",
    "runtime.log",
]
FORMAL_WORKLOAD_SCENARIOS = {
    "asset_transfer",
    "avatar_update",
    "scene_hotspot",
    "item_transfer",
    "cross_scene_migration",
    "mixed_metaverse",
}
INHERITED_TOPOLOGY_FIELDS = [
    "node_runtime_mode",
    "process_runtime_mode",
    "network_adapter",
    "network_mode",
    "cross_shard_protocol",
    "state_backend",
    "max_local_processes",
    "enable_committee_epoch",
    "epoch_count",
    "shard_count",
    "validators_per_shard",
    "executors_per_shard",
    "storage_nodes_per_shard",
    "supervisor_enabled",
    "metaverse_suite_enabled",
    "metaverse_scenario",
    "workload_source",
    "user_count",
    "asset_count",
    "item_count",
    "avatar_count",
    "scene_count",
    "metaverse_count",
    "hotspot_ratio",
    "cross_scene_ratio",
    "cross_shard_ratio",
    "burst_rate",
    "read_write_ratio",
    "zipf_alpha",
    "submit_rate",
    "arrival_rate",
    "key_space_size",
    "asset_skew",
    "scene_skew",
    "offchain_confirmation_enabled",
    "offchain_confirm_delay_ms",
    "offchain_failure_ratio",
    "cross_metaverse_enabled",
    "fault_profile",
    "fault_injection_enabled",
    "fault_seed",
    "fault_start_round",
    "fault_duration_rounds",
    "failed_node_count",
    "message_delay_ms",
    "message_drop_ratio",
    "target_congestion_ratio",
    "relay_fault_mode",
    "observability_enabled",
    "observability_level",
    "reproducibility_bundle_enabled",
    "paper_mapping_enabled",
    "final_artifact_catalog_enabled",
]


class FormalBenchmarkNotRunnable(ValueError):
    def __init__(self, preview: dict[str, Any]) -> None:
        super().__init__("Formal MetaTrack benchmark is not runnable")
        self.preview = preview


def preview_formal_metatrack_benchmark(request: V3FormalMetatrackBenchmarkRequest) -> dict[str, Any]:
    normalized = normalize_formal_request(request)
    validation = validate_v3_composer_draft(request.draft)
    errors = list(normalized["errors"]) + list(validation.errors)
    warnings = list(validation.warnings)
    normalized_draft = validation.normalized_draft or {}
    contains_preview_or_planned = _contains_preview_or_planned(normalized_draft)
    if contains_preview_or_planned:
        errors.append("Formal benchmark cannot include preview or planned plugins.")
    topology = normalized_draft.get("topology", {}) if isinstance(normalized_draft, dict) else {}
    if isinstance(topology, dict) and topology.get("workload_source") == "existing_trace_preview":
        errors.append("existing_trace_preview is preview-only and cannot enter formal benchmark by default.")
    if not validation.is_valid:
        errors.append("Composer draft validation failed.")

    matrix = build_formal_experiment_matrix(request, normalized_draft)
    run_count = len(matrix)
    total_tx_count = run_count * request.formal_tx_count
    scan_point_count = len({(row["scan_variable"], str(row["scan_value"])) for row in matrix}) if matrix else 0
    if run_count > request.max_run_count:
        errors.append(f"run_count {run_count} exceeds max_run_count {request.max_run_count}.")
    if total_tx_count > request.max_total_tx_count:
        errors.append(f"total_tx_count {total_tx_count} exceeds max_total_tx_count {request.max_total_tx_count}.")

    preview = {
        **stage_metadata(),
        "is_valid": not normalized["errors"] and validation.is_valid,
        "is_runnable": not errors,
        "errors": _dedupe(errors),
        "warnings": _dedupe(warnings),
        "matrix": matrix,
        "seed_list": normalized["seed_list"],
        "run_count": run_count,
        "total_tx_count": total_tx_count,
        "baseline_count": len(request.baseline_ids),
        "method_count": len({row.get("method_config_id") or row.get("baseline_id") for row in matrix}),
        "workload_count": len({row.get("workload_config_id") or row.get("workload_scenario") or "draft" for row in matrix}),
        "topology_count": len({row.get("topology_config_id") or "draft" for row in matrix}),
        "scan_point_count": scan_point_count,
        "experiment_type": request.experiment_type,
        "formal_tx_count": request.formal_tx_count,
        "baseline_ids": request.baseline_ids,
        "method_config_ids": request.method_config_ids,
        "workload_config_ids": request.workload_config_ids,
        "topology_config_ids": request.topology_config_ids,
        "runtime_evidence_mode": request.runtime_evidence_mode,
        "contains_preview_or_planned_plugin": contains_preview_or_planned,
        "exceeds_recommended_range": run_count > 100 or total_tx_count > 10_000_000,
        "includes_fault_injection": request.enable_faults_for_formal_run,
        "truth_boundary": TRUTH_BOUNDARY,
        "runtime_truth": "logical_single_process_for_main_performance" if request.runtime_evidence_mode == "logical_single_process" else "local_multi_process_validation_not_main_performance",
        "validation": model_dump(validation),
    }
    return preview


def run_formal_metatrack_benchmark(request: V3FormalMetatrackBenchmarkRequest, root: Path = FORMAL_RUNS_ROOT) -> dict[str, Any]:
    preview = preview_formal_metatrack_benchmark(request)
    if not preview["is_runnable"]:
        raise FormalBenchmarkNotRunnable(preview)

    manager = JobManager(root)
    metadata = manager.create_run(
        source="v3_metatrack_formal_benchmark",
        experiment_name=f"formal_{request.experiment_type}",
        data_truth_label="modular_runtime",
        stage=stage_metadata()["current_stage"],
        extra_metadata={
            **stage_metadata(),
            "run_mode": "formal_metatrack_benchmark",
            "experiment_type": request.experiment_type,
            "formal_tx_count": request.formal_tx_count,
            "seed_list": preview["seed_list"],
            "run_count": preview["run_count"],
            "truth_boundary": TRUTH_BOUNDARY,
        },
    )
    run_id = metadata["run_id"]
    run_dir = manager.run_dir(run_id)
    run_dir.mkdir(parents=True, exist_ok=True)
    manager.mark_running(run_id)

    raw_rows: list[dict[str, Any]] = []
    run_index: list[dict[str, Any]] = []
    failed_runs: list[dict[str, Any]] = []
    child_artifact_index: list[dict[str, Any]] = []
    metric_extraction_rows: list[dict[str, Any]] = []
    try:
        write_json(run_dir / "formal_benchmark_config.json", model_dump(request))
        write_json(run_dir / "formal_matrix_preview.json", preview)
        write_csv(run_dir / "formal_run_matrix.csv", preview["matrix"], preferred_fields=RUN_MATRIX_FIELDS)
        write_json(run_dir / "formal_run_manifest.json", {
            "run_id": run_id,
            "run_mode": "formal_metatrack_benchmark",
            "total_runs": preview["run_count"],
            "matrix": preview["matrix"],
            "truth_boundary": TRUTH_BOUNDARY,
            **stage_metadata(),
        })
        _write_progress(run_dir, run_id, preview["run_count"], 0, 0, 0, "", "", "running")
        for row in preview["matrix"]:
            _write_progress(run_dir, run_id, preview["run_count"], len(raw_rows), len(failed_runs), row["run_index"], row.get("method_config_name") or row.get("baseline_label") or row.get("baseline_id", ""), row.get("workload_config_name") or row.get("workload_scenario", ""), "running")
            child_dir = run_dir / _child_dir_name(row)
            child_dir.mkdir(parents=True, exist_ok=True)
            experiment_profile = build_formal_experiment_profile(request, row)
            plugin_profile = build_formal_plugin_profile(row)
            write_json(child_dir / "generated_experiment_profile.json", experiment_profile)
            write_json(child_dir / "generated_plugin_profile.json", plugin_profile)
            write_yaml(child_dir / "generated_experiment_profile.yaml", experiment_profile)
            write_yaml(child_dir / "generated_plugin_profile.yaml", plugin_profile)
            status = "completed"
            error = ""
            summary: dict[str, Any] = {}
            try:
                result = run_go_v3_runtime(
                    experiment_profile_path=child_dir / "generated_experiment_profile.yaml",
                    plugin_profile_path=child_dir / "generated_plugin_profile.yaml",
                    plugin_profile_id=FORMAL_PLUGIN_PROFILE_ID,
                    chain_profile_path=ROLE_SEPARATED_CHAIN_PROFILE,
                    output_dir=child_dir,
                )
                summary = model_dump(result.summary) if hasattr(result.summary, "__dict__") else dict(result.summary)
            except Exception as exc:  # keep root aggregation observable.
                status = "failed"
                error = str(exc)
                failed_runs.append({**row, "status": status, "error": error, "child_output_dir": str(child_dir)})
            normalized_metrics, extraction_rows = extract_child_run_metrics(child_dir, summary, row)
            if status == "failed":
                extraction_rows = [
                    {**item, "status": "failed", "missing_reason": item.get("missing_reason") or error}
                    for item in extraction_rows
                ]
            metric_extraction_rows.extend(extraction_rows)
            raw_row = {**row, **summary, **normalized_metrics, "status": status, "error": error, "child_output_dir": str(child_dir)}
            raw_rows.append(raw_row)
            run_index.append({
                "run_index": row["run_index"],
                "baseline_id": row.get("baseline_id", ""),
                "method_config_id": row.get("method_config_id", ""),
                "seed": row["seed"],
                "scan_variable": row["scan_variable"],
                "scan_value": row["scan_value"],
                "workload_scenario": row.get("workload_scenario", ""),
                "status": status,
                "output_dir": str(child_dir),
                "error": error,
            })
            child_artifact_index.append(_child_artifact_row(row, child_dir, status, error))
            _write_progress(run_dir, run_id, preview["run_count"], len(raw_rows), len(failed_runs), row["run_index"], row.get("method_config_name") or row.get("baseline_label") or row.get("baseline_id", ""), row.get("workload_config_name") or row.get("workload_scenario", ""), "running")

        aggregate_rows, ci_rows, missing_metrics, missing_metric_rows = aggregate_formal_results(raw_rows)
        figure_rows = build_paper_figure_rows(aggregate_rows)
        summary = build_formal_summary(request, preview, raw_rows, aggregate_rows, figure_rows, missing_metrics)
        write_json(run_dir / "formal_chart_preview.json", summary["chart_preview"])
        write_csv(run_dir / "formal_run_index.csv", run_index)
        write_csv(run_dir / "formal_failed_runs.csv", failed_runs)
        write_csv(run_dir / "formal_child_artifact_index.csv", child_artifact_index, preferred_fields=CHILD_INDEX_FIELDS)
        write_csv(run_dir / "formal_metric_extraction_report.csv", metric_extraction_rows, preferred_fields=EXTRACTION_FIELDS)
        write_json(run_dir / "formal_metric_extraction_report.json", {"rows": metric_extraction_rows})
        write_csv(run_dir / "formal_missing_metrics.csv", missing_metric_rows, preferred_fields=MISSING_METRIC_FIELDS)
        write_csv(run_dir / "formal_raw_summary.csv", raw_rows, preferred_fields=RAW_SUMMARY_FIELDS)
        write_csv(run_dir / "formal_aggregate_summary.csv", aggregate_rows, preferred_fields=AGGREGATE_SUMMARY_FIELDS)
        if request.experiment_type == "workload_comparison":
            write_csv(run_dir / "formal_workload_comparison.csv", [row for row in aggregate_rows if row.get("experiment_type") == "workload_comparison" and row.get("metric_available") is True and int(row.get("count") or 0) > 0], preferred_fields=AGGREGATE_SUMMARY_FIELDS)
        write_csv(run_dir / "formal_latency_summary.csv", [row for row in aggregate_rows if "latency" in row.get("metric", "") and row.get("metric_available") is True], preferred_fields=AGGREGATE_SUMMARY_FIELDS)
        write_csv(run_dir / "formal_throughput_summary.csv", [row for row in aggregate_rows if row.get("metric") == "throughput_tps" and row.get("metric_available") is True], preferred_fields=AGGREGATE_SUMMARY_FIELDS)
        write_csv(run_dir / "formal_overhead_summary.csv", [row for row in aggregate_rows if "overhead" in row.get("metric", "") and row.get("metric_available") is True], preferred_fields=AGGREGATE_SUMMARY_FIELDS)
        write_csv(run_dir / "formal_confidence_interval.csv", ci_rows, preferred_fields=AGGREGATE_SUMMARY_FIELDS)
        write_csv(run_dir / "formal_paper_figure_data.csv", figure_rows, preferred_fields=FIGURE_FIELDS)
        write_json(run_dir / "formal_reproducibility_manifest.json", {
            **stage_metadata(),
            "run_id": run_id,
            "seed_list": preview["seed_list"],
            "matrix": preview["matrix"],
            "config": model_dump(request),
            "truth_boundary": TRUTH_BOUNDARY,
        })
        write_report(run_dir / "formal_benchmark_report.md", summary, missing_metrics)
        write_json(run_dir / "summary.json", summary)
        completed_count = int(summary.get("completed_run_count", 0))
        failed_count = int(summary.get("failed_run_count", 0))
        _write_progress(run_dir, run_id, preview["run_count"], completed_count, failed_count, preview["run_count"] - 1 if preview["run_count"] else 0, "", "", "completed" if failed_count == 0 else "completed_with_failures")
        _mirror_latest(run_dir, root / "latest")
        completed = manager.mark_completed(run_id, data_truth_label="modular_runtime")
        return {
            "run_id": run_id,
            "status": "completed",
            "run_mode": "formal_metatrack_benchmark",
            "stage": stage_metadata()["current_stage"],
            **stage_metadata(),
            "output_dir": str(run_dir),
            "summary": summary,
            "preview": preview,
            "artifacts": list_formal_artifacts(run_dir, run_id),
            "run": completed,
        }
    except Exception:
        manager.mark_failed(run_id, "formal benchmark failed")
        raise


def normalize_formal_request(request: V3FormalMetatrackBenchmarkRequest) -> dict[str, Any]:
    errors: list[str] = []
    validate_formal_baseline_registry()
    if not request.method_config_ids and not request.baseline_ids:
        errors.append("baseline_ids or method_config_ids must not be empty.")
    if request.baseline_ids:
        for baseline_id in request.baseline_ids:
            try:
                get_formal_baseline(baseline_id)
            except KeyError:
                errors.append(f"unknown baseline_id: {baseline_id}")
    for config_id, expected_kind in [(config_id, "method") for config_id in request.method_config_ids] + [(config_id, "workload") for config_id in request.workload_config_ids] + [(config_id, "topology") for config_id in request.topology_config_ids]:
        try:
            config = get_saved_config(config_id)
            if config["config_kind"] != expected_kind:
                errors.append(f"{config_id} must be a {expected_kind} config.")
        except Exception as exc:
            errors.append(str(exc))
    for key, values in (
        ("hotspot_ratio_points", request.hotspot_ratio_points),
        ("cross_shard_ratio_points", request.cross_shard_ratio_points),
    ):
        if not values or len(values) > RESOURCE_LIMITS["max_scan_points"]:
            errors.append(f"{key} must contain 1 to {RESOURCE_LIMITS['max_scan_points']} points.")
        for value in values:
            if value < 0.0 or value > 1.0:
                errors.append(f"{key} values must be between 0 and 1.")
    if not request.shard_count_points or len(request.shard_count_points) > RESOURCE_LIMITS["max_scan_points"]:
        errors.append(f"shard_count_points must contain 1 to {RESOURCE_LIMITS['max_scan_points']} points.")
    for value in request.shard_count_points:
        if value < 1 or value > 32:
            errors.append("shard_count_points values must be between 1 and 32.")
    if not request.workload_scenario_points or len(request.workload_scenario_points) > RESOURCE_LIMITS["max_scan_points"]:
        errors.append(f"workload_scenario_points must contain 1 to {RESOURCE_LIMITS['max_scan_points']} points.")
    for value in request.workload_scenario_points:
        if value not in FORMAL_WORKLOAD_SCENARIOS:
            errors.append(f"unknown workload_scenario: {value}")
    if request.max_run_count > RESOURCE_LIMITS["max_run_count"]:
        errors.append(f"max_run_count cannot exceed {RESOURCE_LIMITS['max_run_count']}.")
    if request.max_total_tx_count > RESOURCE_LIMITS["max_total_tx_count"]:
        errors.append(f"max_total_tx_count cannot exceed {RESOURCE_LIMITS['max_total_tx_count']}.")
    return {"errors": errors, "seed_list": [request.seed_base + index for index in range(request.seed_count)]}


def build_formal_experiment_matrix(request: V3FormalMetatrackBenchmarkRequest, normalized_draft: dict[str, Any] | None = None) -> list[dict[str, Any]]:
    seed_list = [request.seed_base + index for index in range(request.seed_count)]
    scan_variable, scan_values = _scan_values(request)
    methods = _method_definitions(request)
    workloads = _workload_definitions(request)
    topologies = _topology_definitions(request, normalized_draft or {})
    matrix: list[dict[str, Any]] = []
    run_index = 0
    for method in methods:
        for workload in workloads:
            for topology in topologies:
                for seed in seed_list:
                    for scan_value in scan_values:
                        row = {
                            "run_index": run_index,
                            "experiment_type": request.experiment_type,
                            "baseline_id": method.get("baseline_id", ""),
                            "baseline_label": method.get("baseline_label", ""),
                            "method_config_id": method.get("method_config_id", ""),
                            "method_config_name": method.get("method_config_name", method.get("baseline_label", "")),
                            "workload_config_id": workload.get("workload_config_id", ""),
                            "workload_config_name": workload.get("workload_config_name", ""),
                            "topology_config_id": topology.get("topology_config_id", ""),
                            "topology_config_name": topology.get("topology_config_name", ""),
                            "seed": seed,
                            "formal_tx_count": request.formal_tx_count,
                            "scan_variable": scan_variable,
                            "scan_value": scan_value,
                            "zipf_alpha": request.zipf_alpha,
                            "runtime_evidence_mode": request.runtime_evidence_mode,
                            "plugins": method["plugins"],
                            "workload_payload": workload.get("payload", {}),
                            "topology_payload": topology.get("payload", {}),
                        }
                        row.update(_topology_overrides(request, scan_variable, scan_value, seed, workload, topology))
                        matrix.append(row)
                        run_index += 1
    return matrix


def build_formal_experiment_profile(request: V3FormalMetatrackBenchmarkRequest, row: dict[str, Any]) -> dict[str, Any]:
    topology = model_dump(request.draft.topology) if request.draft.topology is not None else {}
    topology.update(row.get("topology_payload") or {})
    profile = {
        "profile_id": f"formal_{row['run_index']}",
        "stage": stage_metadata()["current_stage"],
        "type": "formal_metatrack_benchmark",
        "run_mode": "formal_metatrack_benchmark",
        "truth_label": "modular_runtime",
        "backend_type": "modular_research_chain",
        "runtime_mode": "logical_single_process",
        "node_runtime_mode": "logical_single_process" if request.runtime_evidence_mode == "logical_single_process" else "local_multi_process",
        "process_runtime_mode": "dry_run" if request.runtime_evidence_mode == "logical_single_process" else "smoke",
        "runtime_evidence_mode": request.runtime_evidence_mode,
        "experiment_type": request.experiment_type,
        "baseline_id": row["baseline_id"],
        "method_config_id": row.get("method_config_id", ""),
        "method_config_name": row.get("method_config_name", ""),
        "workload_config_id": row.get("workload_config_id", ""),
        "workload_config_name": row.get("workload_config_name", ""),
        "topology_config_id": row.get("topology_config_id", ""),
        "topology_config_name": row.get("topology_config_name", ""),
        "scan_variable": row["scan_variable"],
        "scan_value": row["scan_value"],
        "tx_count": request.formal_tx_count,
        "formal_tx_count": request.formal_tx_count,
        "seed": row["seed"],
        "hotspot_ratio": row.get("hotspot_ratio", 0.2),
        "cross_shard_ratio": row.get("cross_shard_ratio", 0.2),
        "shard_count": row.get("shard_count", 4),
        "zipf_alpha": request.zipf_alpha,
        "fault_injection_enabled": bool(request.enable_faults_for_formal_run),
        "paper_grade_benchmark": False,
        "truth_boundary": TRUTH_BOUNDARY,
        "chain_profile": "single_chain_research_default",
        "submit_rate": 120,
        "block_interval_ms": 100,
        "max_tx_per_block": 500,
        **stage_metadata(),
    }
    for field in INHERITED_TOPOLOGY_FIELDS:
        if field in topology:
            profile[field] = topology[field]
    profile.update(row.get("workload_payload") or {})
    profile.update({
        "node_runtime_mode": "logical_single_process" if request.runtime_evidence_mode == "logical_single_process" else "local_multi_process",
        "process_runtime_mode": profile.get("process_runtime_mode", "dry_run") if request.runtime_evidence_mode == "logical_single_process" else profile.get("process_runtime_mode", "smoke"),
        "runtime_evidence_mode": request.runtime_evidence_mode,
        "tx_count": request.formal_tx_count,
        "formal_tx_count": request.formal_tx_count,
        "seed": row["seed"],
        "hotspot_ratio": row.get("hotspot_ratio", profile.get("hotspot_ratio", 0.2)),
        "cross_shard_ratio": row.get("cross_shard_ratio", profile.get("cross_shard_ratio", 0.2)),
        "shard_count": row.get("shard_count", profile.get("shard_count", 4)),
        "metaverse_scenario": row.get("workload_scenario", profile.get("metaverse_scenario", "mixed_metaverse")),
        "workload_source": row.get("workload_source", profile.get("workload_source", "synthetic")),
        "zipf_alpha": request.zipf_alpha,
        "fault_injection_enabled": bool(request.enable_faults_for_formal_run and profile.get("fault_profile", "none") != "none"),
    })
    return profile


def build_formal_plugin_profile(row: dict[str, Any]) -> dict[str, Any]:
    plugins = row["plugins"]
    return {
        "profile_type": "plugin_profile_collection",
        "version": "v3",
        "stage": stage_metadata()["current_stage"],
        "profiles": [{
            "plugin_profile_id": FORMAL_PLUGIN_PROFILE_ID,
            "label": f"Formal MetaTrack {row['baseline_id']}",
            "domain": "metatrack",
            "status": "runnable",
            "runnable": True,
            "plugins": {
                "WorkloadPlugin": plugins["Workload"],
                "TxPoolPlugin": plugins["TxPool"],
                "BlockProducerPlugin": plugins["BlockProducer"],
                "ConsensusPlugin": plugins["Consensus"],
                "ConsensusRuntimePlugin": plugins["Consensus"],
                "ShardingPlugin": plugins["Routing"],
                "ExecutionSchedulerPlugin": plugins["Execution"],
                "StateAccessPlugin": plugins["StateAccess"],
                "StateStoragePlugin": plugins["StateStorage"],
                "CommitPlugin": plugins["Commit"],
                "MetricsPlugin": plugins["MetricsReport"],
            },
            "module_plugins": plugins,
            "tags": ["formal_metatrack", "controlled_benchmark"],
            "blocking_reasons": [],
        }],
    }


def extract_child_run_metrics(
    child_dir: Path,
    runtime_summary: dict[str, Any],
    row: dict[str, Any],
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    normalized_metrics: dict[str, Any] = {}
    extraction_rows: list[dict[str, Any]] = []
    sources: list[tuple[str, dict[str, Any]]] = []
    missing_by_source: dict[str, str] = {}
    for source_file, source_type in SUMMARY_SOURCES:
        if source_file == "runtime_summary":
            sources.append((source_file, runtime_summary or {}))
            continue
        path = child_dir / source_file
        if not path.is_file():
            missing_by_source[source_file] = "file_missing"
            continue
        try:
            if source_type == "json":
                loaded = _read_json_metric_source(path)
            else:
                loaded = _read_csv_metric_source(path)
            if loaded:
                sources.append((source_file, loaded))
            else:
                missing_by_source[source_file] = "empty_or_non_object_source"
        except Exception as exc:
            missing_by_source[source_file] = f"parse_error: {exc}"

    for metric in AGGREGATE_METRICS:
        found = False
        for source_file, payload in sources:
            value, source_field = _metric_from_payload(metric, payload)
            if value is None:
                continue
            normalized_metrics[metric] = value
            extraction_rows.append(_extraction_row(row, child_dir, metric, value, source_file, source_field, "ok", ""))
            found = True
            break
        if not found:
            extraction_rows.append(_extraction_row(row, child_dir, metric, "", "", "", "missing", _missing_reason(metric, missing_by_source)))

    latency_metrics, latency_rows = _extract_latency_metrics(child_dir, row)
    for metric, value in latency_metrics.items():
        if metric not in normalized_metrics:
            normalized_metrics[metric] = value
            extraction_rows = [item for item in extraction_rows if item["metric"] != metric]
            extraction_rows.append(latency_rows[metric])

    if "throughput_tps" not in normalized_metrics:
        throughput, source_file, source_field, reason = _derive_throughput(sources)
        extraction_rows = [item for item in extraction_rows if item["metric"] != "throughput_tps"]
        if throughput is not None:
            normalized_metrics["throughput_tps"] = throughput
            extraction_rows.append(_extraction_row(row, child_dir, "throughput_tps", throughput, source_file, source_field, "ok", ""))
        else:
            extraction_rows.append(_extraction_row(row, child_dir, "throughput_tps", "", "", "", "missing", reason))

    return normalized_metrics, extraction_rows


def aggregate_formal_results(rows: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[str], list[dict[str, Any]]]:
    grouped: dict[tuple[str, str, str, str, str, str, str, str, str, str, str], list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        method_key = row.get("method_config_id") or row.get("baseline_id", "")
        grouped[(
            row["experiment_type"],
            method_key,
            row.get("method_config_name") or row.get("baseline_label", ""),
            row.get("baseline_id", ""),
            row.get("workload_config_id", ""),
            row.get("workload_config_name", ""),
            row.get("topology_config_id", ""),
            row.get("topology_config_name", ""),
            row["scan_variable"],
            str(row["scan_value"]),
            row.get("workload_scenario", ""),
        )].append(row)
    aggregate_rows: list[dict[str, Any]] = []
    ci_rows: list[dict[str, Any]] = []
    missing_metric_rows: list[dict[str, Any]] = []
    missing: set[str] = set()
    for key, group_rows in grouped.items():
        experiment_type, method_key, method_name, baseline_id, workload_config_id, workload_config_name, topology_config_id, topology_config_name, scan_variable, scan_value, workload_scenario = key
        for metric in AGGREGATE_METRICS:
            values = [_to_float(row.get(metric)) for row in group_rows if _to_float(row.get(metric)) is not None]
            if not values:
                missing.add(metric)
                aggregate_rows.append(_aggregate_row(experiment_type, method_key, method_name, baseline_id, workload_config_id, workload_config_name, topology_config_id, topology_config_name, scan_variable, scan_value, workload_scenario, metric, None, None, None, None, 0, None, False))
                missing_metric_rows.append({
                    "experiment_type": experiment_type,
                    "method_config_name": method_name,
                    "baseline_id": baseline_id,
                    "workload_scenario": workload_scenario,
                    "metric": metric,
                    "missing_count": len(group_rows),
                    "group_count": len(group_rows),
                    "reason": "metric_not_extracted",
                })
                continue
            mean = sum(values) / len(values)
            variance = sum((value - mean) ** 2 for value in values) / (len(values) - 1) if len(values) > 1 else 0.0
            std = math.sqrt(variance)
            ci95 = 1.96 * std / math.sqrt(len(values)) if len(values) > 1 else None
            row = _aggregate_row(experiment_type, method_key, method_name, baseline_id, workload_config_id, workload_config_name, topology_config_id, topology_config_name, scan_variable, scan_value, workload_scenario, metric, mean, std, min(values), max(values), len(values), ci95, True)
            aggregate_rows.append(row)
            ci_rows.append(row)
    return aggregate_rows, ci_rows, sorted(missing), missing_metric_rows


def build_paper_figure_rows(aggregate_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [
        {
            "figure_group": row["experiment_type"],
            "x_value": row["workload_scenario"] if row["experiment_type"] == "workload_comparison" else row["scan_value"],
            "series": row.get("method_config_name") or row.get("baseline_id"),
            "metric": row["metric"],
            "mean": row["mean"],
            "ci95": row["ci95"],
            "count": row.get("count", 0),
        }
        for row in aggregate_rows
        if row.get("metric_available") is True and int(row.get("count") or 0) > 0 and _to_float(row.get("mean")) is not None
    ]


def build_chart_preview(aggregate_rows: list[dict[str, Any]], figure_rows: list[dict[str, Any]]) -> dict[str, Any]:
    groups: list[dict[str, Any]] = []
    available_metrics: list[str] = []
    seen_metrics: set[str] = set()
    for row in aggregate_rows:
        metric = str(row.get("metric", ""))
        mean = _to_float(row.get("mean"))
        if metric not in CHART_PREVIEW_METRICS or mean is None or row.get("metric_available") is not True or int(row.get("count") or 0) <= 0:
            continue
        if metric not in seen_metrics:
            seen_metrics.add(metric)
            available_metrics.append(metric)
        x_value = row.get("workload_scenario") if row.get("experiment_type") == "workload_comparison" else row.get("scan_value")
        groups.append({
            "x": str(x_value if x_value not in (None, "") else row.get("scan_value", "")),
            "series": str(row.get("method_config_name") or row.get("baseline_label") or row.get("baseline_id") or row.get("method_or_baseline_id") or "method"),
            "metric": metric,
            "mean": mean,
            "ci95": _to_float(row.get("ci95")),
            "count": int(_to_float(row.get("count")) or 0),
        })
    figure_metric_count = len({str(row.get("metric", "")) for row in figure_rows if row.get("mean") not in (None, "")})
    return {
        "primary_metric": "throughput_tps" if "throughput_tps" in seen_metrics else (available_metrics[0] if available_metrics else ""),
        "available_metrics": available_metrics,
        "groups": groups,
        "figure_metric_count": figure_metric_count,
        "diagnostics": {} if groups else {
            "reason": "no_available_aggregate_metrics",
            "missing_metrics_file": "formal_missing_metrics.csv",
            "extraction_report_file": "formal_metric_extraction_report.csv",
        },
        "data_files": {
            "figure_data": "formal_paper_figure_data.csv",
            "workload_comparison": "formal_workload_comparison.csv",
            "aggregate_summary": "formal_aggregate_summary.csv",
            "raw_summary": "formal_raw_summary.csv",
            "child_artifact_index": "formal_child_artifact_index.csv",
            "reproducibility_manifest": "formal_reproducibility_manifest.json",
        },
    }


def build_formal_summary(
    request: V3FormalMetatrackBenchmarkRequest,
    preview: dict[str, Any],
    raw_rows: list[dict[str, Any]],
    aggregate_rows: list[dict[str, Any]],
    figure_rows: list[dict[str, Any]],
    missing_metrics: list[str],
) -> dict[str, Any]:
    completed = sum(1 for row in raw_rows if row.get("status") == "completed")
    failed = len(raw_rows) - completed
    reasons: list[str] = []
    if request.formal_tx_count < 10000:
        reasons.append("formal_tx_count is below 10000.")
    if request.seed_count < 3:
        reasons.append("seed_count is below 3.")
    if not {"baseline_hash_serial", "metatrack_full"} <= set(request.baseline_ids):
        reasons.append("baseline_ids must include baseline_hash_serial and metatrack_full.")
    if preview["contains_preview_or_planned_plugin"]:
        reasons.append("preview or planned plugin was included.")
    if not aggregate_rows:
        reasons.append("aggregate statistics are missing.")
    if not figure_rows:
        reasons.append("formal_paper_figure_data.csv was not generated.")
    if preview["run_count"] <= 0:
        reasons.append("run_count must be positive.")
    if failed:
        reasons.append("not all sub-runs completed.")
    paper_candidate = not reasons
    chart_preview = build_chart_preview(aggregate_rows, figure_rows)
    return {
        **stage_metadata(),
        "run_mode": "formal_metatrack_benchmark",
        "experiment_evidence_level": "paper_candidate" if paper_candidate else "controlled_benchmark",
        "paper_candidate_eligible": paper_candidate,
        "paper_candidate_reasons": reasons,
        "formal_tx_count": request.formal_tx_count,
        "seed_list": preview["seed_list"],
        "run_count": preview["run_count"],
        "completed_run_count": completed,
        "failed_run_count": failed,
        "current_run_index": preview["run_count"] - 1 if preview["run_count"] else 0,
        "failed_child_run_count": failed,
        "total_tx_count": preview["total_tx_count"],
        "baseline_ids": request.baseline_ids,
        "method_config_ids": request.method_config_ids,
        "workload_config_ids": request.workload_config_ids,
        "topology_config_ids": request.topology_config_ids,
        "baseline_count": len(request.baseline_ids),
        "method_count": len({row.get("method_config_id") or row.get("baseline_id") for row in preview["matrix"]}),
        "workload_count": len({row.get("workload_config_id") or row.get("workload_scenario") or "draft" for row in preview["matrix"]}),
        "topology_count": len({row.get("topology_config_id") or "draft" for row in preview["matrix"]}),
        "experiment_type": request.experiment_type,
        "runtime_evidence_mode": request.runtime_evidence_mode,
        "scan_variable": _scan_values(request)[0],
        "truth_boundary": TRUTH_BOUNDARY,
        "missing_metrics": missing_metrics,
        "chart_preview": chart_preview,
    }


def list_formal_artifacts(run_dir: Path, run_id: str) -> list[dict[str, Any]]:
    artifacts: list[dict[str, Any]] = []
    for filename in sorted(ARTIFACT_ALLOWLIST):
        path = run_dir / filename
        if path.is_file():
            artifacts.append({
                "name": filename,
                "download_url": f"/api/v3/composer/formal-metatrack/{run_id}/artifacts/{filename}",
                "size_bytes": path.stat().st_size,
            })
    return artifacts


def get_formal_artifact_path(run_id: str, filename: str, root: Path = FORMAL_RUNS_ROOT) -> Path:
    manager = JobManager(root)
    manager.get_run(run_id)
    return get_artifact_path(manager.run_dir(run_id), filename)


def build_formal_artifacts_zip(run_id: str, root: Path = FORMAL_RUNS_ROOT) -> Path:
    manager = JobManager(root)
    manager.get_run(run_id)
    run_dir = manager.run_dir(run_id).resolve()
    zip_path = run_dir / "formal_artifacts.zip"
    with zipfile.ZipFile(zip_path, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        for filename in FORMAL_ROOT_ZIP_FILES:
            if filename not in ARTIFACT_ALLOWLIST:
                continue
            try:
                path = get_artifact_path(run_dir, filename)
            except Exception:
                continue
            archive.write(path, filename)
        for child_index, child_dir in enumerate(_child_run_dirs(run_dir)):
            safe_child_dir = child_dir.resolve()
            try:
                safe_child_dir.relative_to(run_dir)
            except ValueError:
                continue
            for filename in FORMAL_CHILD_ZIP_FILES:
                if filename not in ARTIFACT_ALLOWLIST:
                    continue
                path = (safe_child_dir / filename).resolve()
                try:
                    path.relative_to(run_dir)
                except ValueError:
                    continue
                if path.is_file():
                    archive.write(path, f"child_runs/run_{child_index:03d}/{filename}")
    return zip_path


def list_formal_runs(limit: int = 20, root: Path = FORMAL_RUNS_ROOT) -> list[dict[str, Any]]:
    manager = JobManager(root)
    runs: list[dict[str, Any]] = []
    seen_run_ids: set[str] = set()
    for metadata in manager.list_runs(limit=min(max(limit * 2, limit), 200)):
        run_id = str(metadata.get("run_id", ""))
        if not run_id or run_id in seen_run_ids:
            continue
        seen_run_ids.add(run_id)
        run_dir = manager.run_dir(run_id)
        summary = _read_json_if_exists(run_dir / "summary.json")
        progress = _read_json_if_exists(run_dir / "formal_progress.json")
        runs.append({
            "run_id": run_id,
            "created_at": metadata.get("created_at", ""),
            "updated_at": metadata.get("updated_at", ""),
            "status": progress.get("status") or metadata.get("status", ""),
            "experiment_type": summary.get("experiment_type") or metadata.get("experiment_type", ""),
            "formal_tx_count": summary.get("formal_tx_count") or metadata.get("formal_tx_count", ""),
            "run_count": summary.get("run_count") or metadata.get("run_count", 0),
            "completed_run_count": summary.get("completed_run_count", progress.get("completed_runs", 0)),
            "failed_run_count": summary.get("failed_run_count", progress.get("failed_runs", 0)),
            "total_tx_count": summary.get("total_tx_count", 0),
            "runtime_evidence_mode": summary.get("runtime_evidence_mode", ""),
            "method_count": summary.get("method_count", 0),
            "workload_count": summary.get("workload_count", 0),
            "topology_count": summary.get("topology_count", 0),
            "output_dir": str(run_dir),
            "summary_available": (run_dir / "summary.json").is_file(),
            "chart_preview_available": (run_dir / "formal_chart_preview.json").is_file(),
            "zip_download_url": f"/api/v3/composer/formal-metatrack/{run_id}/artifacts.zip",
        })
        if len(runs) >= max(1, min(limit, 200)):
            break
    return runs


def get_formal_run_result(run_id: str, root: Path = FORMAL_RUNS_ROOT) -> dict[str, Any]:
    manager = JobManager(root)
    metadata = manager.get_run(run_id)
    run_dir = manager.run_dir(run_id)
    summary = _read_json_if_exists(run_dir / "summary.json")
    preview = _read_json_if_exists(run_dir / "formal_matrix_preview.json")
    return {
        "run_id": run_id,
        "status": metadata.get("status", ""),
        "run_mode": "formal_metatrack_benchmark",
        "output_dir": str(run_dir),
        "summary": summary,
        "preview": preview,
        "artifacts": list_formal_artifacts(run_dir, run_id),
        "run": metadata,
    }


def write_json(path: Path, payload: Any) -> None:
    path.write_text(json.dumps(payload, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")


def write_yaml(path: Path, payload: Any) -> None:
    path.write_text(yaml.safe_dump(payload, sort_keys=False, allow_unicode=True), encoding="utf-8")


def write_csv(path: Path, rows: list[dict[str, Any]], preferred_fields: list[str] | None = None) -> None:
    keys = {key for row in rows for key in row}
    preferred = [field for field in (preferred_fields or []) if field in keys or preferred_fields]
    fieldnames = preferred + sorted(keys - set(preferred))
    if not fieldnames:
        fieldnames = ["empty"]
        rows = []
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow({key: _csv_value(row.get(key, "")) for key in fieldnames})


def write_report(path: Path, summary: dict[str, Any], missing_metrics: list[str]) -> None:
    lines = [
        "# MetaTrack Formal Benchmark Report",
        "",
        f"- run_mode: {summary['run_mode']}",
        f"- evidence_level: {summary['experiment_evidence_level']}",
        f"- paper_candidate_eligible: {summary['paper_candidate_eligible']}",
        f"- truth_boundary: {summary['truth_boundary']}",
        "",
        "This is a controlled local emulator benchmark. It is not Fabric/EVM live execution, not BlockEmulator backend, not production networking, and not paper-final evidence by itself.",
        "",
        "## Missing Metrics",
    ]
    lines.extend(f"- {metric}" for metric in missing_metrics)
    if not missing_metrics:
        lines.append("- none")
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def _read_json_if_exists(path: Path) -> dict[str, Any]:
    if not path.is_file():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
        return payload if isinstance(payload, dict) else {}
    except json.JSONDecodeError:
        return {}


def _read_json_metric_source(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if isinstance(payload, dict):
        return payload
    if isinstance(payload, list) and payload and isinstance(payload[0], dict):
        return dict(payload[0])
    return {}


def _read_csv_metric_source(path: Path) -> dict[str, Any]:
    with path.open(encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            return dict(row)
    return {}


def _metric_from_payload(metric: str, payload: dict[str, Any]) -> tuple[float | None, str]:
    for field in METRIC_ALIASES.get(metric, [metric]):
        value = _to_float(payload.get(field))
        if value is not None:
            return value, field
    return None, ""


def _extract_latency_metrics(child_dir: Path, row: dict[str, Any]) -> tuple[dict[str, float], dict[str, dict[str, Any]]]:
    for filename in LATENCY_SOURCES:
        path = child_dir / filename
        if not path.is_file():
            continue
        latencies = _read_latency_values(path)
        if not latencies:
            continue
        metrics = {
            "avg_latency_ms": sum(latencies) / len(latencies),
            "p95_latency_ms": _percentile(latencies, 95),
            "p99_latency_ms": _percentile(latencies, 99),
        }
        rows = {
            metric: _extraction_row(row, child_dir, metric, value, filename, "latency_ms", "ok", f"latency_count={len(latencies)}")
            for metric, value in metrics.items()
        }
        return metrics, rows
    return {}, {}


def _read_latency_values(path: Path) -> list[float]:
    values: list[float] = []
    with path.open(encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            value = _to_float(row.get("latency_ms"))
            if value is None:
                continue
            status = str(row.get("status", "")).strip().lower()
            if status and status not in SUCCESS_STATUSES:
                continue
            values.append(value)
    return values


def _percentile(values: list[float], percentile: int) -> float:
    ordered = sorted(values)
    if not ordered:
        return 0.0
    if len(ordered) == 1:
        return ordered[0]
    rank = (percentile / 100) * (len(ordered) - 1)
    lower = math.floor(rank)
    upper = math.ceil(rank)
    if lower == upper:
        return ordered[int(rank)]
    weight = rank - lower
    return ordered[lower] * (1 - weight) + ordered[upper] * weight


def _derive_throughput(sources: list[tuple[str, dict[str, Any]]]) -> tuple[float | None, str, str, str]:
    count_fields = ["success_count", "successful_tx_count", "completed_tx_count", "tx_count"]
    elapsed_fields = ["elapsed_ms", "duration_ms", "runtime_ms"]
    for source_file, payload in sources:
        count_value = None
        count_field = ""
        for field in count_fields:
            count_value = _to_float(payload.get(field))
            if count_value is not None:
                count_field = field
                break
        elapsed_value = None
        elapsed_field = ""
        for field in elapsed_fields:
            elapsed_value = _to_float(payload.get(field))
            if elapsed_value is not None:
                elapsed_field = field
                break
        if count_value is not None and elapsed_value and elapsed_value > 0:
            return count_value / (elapsed_value / 1000), source_file, f"{count_field}/{elapsed_field}", ""
    return None, "", "", "throughput_tps unavailable and no positive elapsed_ms with success_count"


def _extraction_row(row: dict[str, Any], child_dir: Path, metric: str, value: Any, source_file: str, source_field: str, status: str, missing_reason: str) -> dict[str, Any]:
    return {
        "run_index": row.get("run_index", ""),
        "child_output_dir": str(child_dir),
        "metric": metric,
        "value": value,
        "source_file": source_file,
        "source_field": source_field,
        "status": status,
        "missing_reason": missing_reason,
    }


def _missing_reason(metric: str, missing_by_source: dict[str, str]) -> str:
    if missing_by_source:
        details = "; ".join(f"{source}:{reason}" for source, reason in sorted(missing_by_source.items()))
        return f"{metric} not found in available sources; {details}"
    return f"{metric} not found in runtime_summary"


def _child_run_dirs(run_dir: Path) -> list[Path]:
    index_path = run_dir / "formal_child_artifact_index.csv"
    if index_path.is_file():
        with index_path.open(encoding="utf-8", newline="") as handle:
            return [Path(row.get("child_output_dir", "")) for row in csv.DictReader(handle) if row.get("child_output_dir")]
    return sorted(path for path in run_dir.glob("run_*") if path.is_dir())


def _scan_values(request: V3FormalMetatrackBenchmarkRequest) -> tuple[str, list[Any]]:
    if request.experiment_type == "workload_comparison":
        return "workload_scenario", list(request.workload_scenario_points)
    if request.experiment_type == "hotspot_sensitivity":
        return "hotspot_ratio", list(request.hotspot_ratio_points)
    if request.experiment_type == "cross_shard_sensitivity":
        return "cross_shard_ratio", list(request.cross_shard_ratio_points)
    if request.experiment_type == "shard_scalability":
        return "shard_count", list(request.shard_count_points)
    if request.experiment_type == "control_overhead":
        return "mechanism_combination", ["control_overhead"]
    return "plugin_combination", ["baseline"]


def _topology_overrides(request: V3FormalMetatrackBenchmarkRequest, scan_variable: str, scan_value: Any, seed: int, workload: dict[str, Any] | None = None, topology: dict[str, Any] | None = None) -> dict[str, Any]:
    values = {
        "seed": seed,
        "hotspot_ratio": request.hotspot_ratio_points[0] if request.hotspot_ratio_points else 0.2,
        "cross_shard_ratio": request.cross_shard_ratio_points[0] if request.cross_shard_ratio_points else 0.2,
        "shard_count": request.shard_count_points[0] if request.shard_count_points else 4,
        "workload_scenario": (workload or {}).get("payload", {}).get("metaverse_scenario", request.workload_scenario_points[0] if request.workload_scenario_points else "mixed_metaverse"),
        "workload_source": (workload or {}).get("payload", {}).get("workload_source", "metaverse" if scan_variable == "workload_scenario" else "synthetic"),
    }
    if scan_variable in values:
        values[scan_variable] = scan_value
    if scan_variable == "workload_scenario":
        values["metaverse_scenario"] = scan_value
        values["workload_source"] = "metaverse"
    return values


def _contains_preview_or_planned(normalized_draft: dict[str, Any]) -> bool:
    modules = normalized_draft.get("modules", {})
    if not isinstance(modules, dict):
        return True
    return any(bool(module.get("preview_only") or module.get("planned")) for module in modules.values() if isinstance(module, dict))


def _method_definitions(request: V3FormalMetatrackBenchmarkRequest) -> list[dict[str, Any]]:
    methods: list[dict[str, Any]] = []
    methods.extend([
        {
            "baseline_id": baseline_id,
            "baseline_label": get_formal_baseline(baseline_id)["label"],
            "plugins": get_formal_baseline(baseline_id)["plugins"],
        }
        for baseline_id in request.baseline_ids
    ])
    for config_id in request.method_config_ids:
        config = get_saved_config(config_id)
        plugins = _plugins_from_method_payload(config.get("payload", {}))
        _validate_plugin_combo(config_id, plugins)
        methods.append({
            "method_config_id": config_id,
            "method_config_name": config.get("name", config_id),
            "plugins": plugins,
        })
    return methods


def _workload_definitions(request: V3FormalMetatrackBenchmarkRequest) -> list[dict[str, Any]]:
    if not request.workload_config_ids:
        return [{"payload": {}}]
    workloads: list[dict[str, Any]] = []
    for config_id in request.workload_config_ids:
        config = get_saved_config(config_id)
        payload = dict(config.get("payload", {}))
        if "workload" in payload and isinstance(payload["workload"], dict):
            payload = dict(payload["workload"])
        workloads.append({
            "workload_config_id": config_id,
            "workload_config_name": config.get("name", config_id),
            "payload": payload,
        })
    return workloads


def _topology_definitions(request: V3FormalMetatrackBenchmarkRequest, normalized_draft: dict[str, Any]) -> list[dict[str, Any]]:
    if not request.topology_config_ids:
        return [{"payload": normalized_draft.get("topology", {}) if isinstance(normalized_draft, dict) else {}}]
    topologies: list[dict[str, Any]] = []
    for config_id in request.topology_config_ids:
        config = get_saved_config(config_id)
        payload = dict(config.get("payload", {}))
        if "topology" in payload and isinstance(payload["topology"], dict):
            payload = dict(payload["topology"])
        topologies.append({
            "topology_config_id": config_id,
            "topology_config_name": config.get("name", config_id),
            "payload": payload,
        })
    return topologies


def _plugins_from_method_payload(payload: dict[str, Any]) -> dict[str, str]:
    modules = payload.get("modules")
    if not isinstance(modules, dict):
        draft = payload.get("draft")
        modules = draft.get("modules") if isinstance(draft, dict) else {}
    plugins: dict[str, str] = {}
    for module_id, value in (modules or {}).items():
        if isinstance(value, dict):
            plugins[module_id] = str(value.get("plugin", ""))
        else:
            plugins[module_id] = str(value)
    if not plugins and isinstance(payload.get("plugins"), dict):
        plugins = {str(key): str(value) for key, value in payload["plugins"].items()}
    return plugins


def _validate_plugin_combo(config_id: str, plugins: dict[str, str]) -> None:
    missing = sorted(set(CATALOG) - set(plugins))
    if missing:
        raise ValueError(f"{config_id} missing module plugins: {missing}")
    for module_id, plugin_id in plugins.items():
        module = CATALOG.get(module_id)
        if module is None:
            raise ValueError(f"{config_id} references unknown module {module_id}")
        capability = module.plugins.get(plugin_id)
        if capability is None:
            raise ValueError(f"{config_id} references unknown plugin {plugin_id} for {module_id}")
        if not capability.runnable or capability.preview_only or capability.planned:
            raise ValueError(f"{config_id} uses non-runnable plugin {plugin_id} for {module_id}")


def _write_progress(run_dir: Path, run_id: str, total_runs: int, completed_runs: int, failed_runs: int, current_run_index: int, current_baseline_or_method: str, current_workload: str, status: str) -> None:
    write_json(run_dir / "formal_progress.json", {
        "run_id": run_id,
        "total_runs": total_runs,
        "completed_runs": completed_runs,
        "failed_runs": failed_runs,
        "current_run_index": current_run_index,
        "current_baseline_or_method": current_baseline_or_method,
        "current_workload": current_workload,
        "status": status,
    })


def _child_artifact_row(row: dict[str, Any], child_dir: Path, status: str, error: str) -> dict[str, Any]:
    return {
        "run_index": row["run_index"],
        "baseline_id": row.get("baseline_id", ""),
        "method_config_id": row.get("method_config_id", ""),
        "workload_scenario": row.get("workload_scenario", ""),
        "seed": row.get("seed", ""),
        "child_output_dir": str(child_dir),
        "summary_json_exists": (child_dir / "summary.json").is_file(),
        "summary_json_path": str(child_dir / "summary.json"),
        "runtime_log_path": str(child_dir / "runtime.log"),
        "routing_log_exists": (child_dir / "routing_log.csv").is_file(),
        "execution_log_exists": (child_dir / "execution_log.csv").is_file(),
        "state_access_log_exists": (child_dir / "state_access_log.csv").is_file(),
        "relay_mvp_summary_exists": (child_dir / "relay_mvp_summary.json").is_file(),
        "state_authenticity_summary_exists": (child_dir / "state_authenticity_summary.json").is_file(),
        "status": status,
        "error": error,
    }


def _aggregate_row(experiment_type: str, method_key: str, method_name: str, baseline_id: str, workload_config_id: str, workload_config_name: str, topology_config_id: str, topology_config_name: str, scan_variable: str, scan_value: str, workload_scenario: str, metric: str, mean: float | None, std: float | None, minimum: float | None, maximum: float | None, count: int, ci95: float | None, metric_available: bool) -> dict[str, Any]:
    return {
        "experiment_type": experiment_type,
        "baseline_id": baseline_id,
        "method_config_id": method_key if method_key.startswith("v3cfg_") else "",
        "method_config_name": method_name,
        "method_or_baseline_id": method_key,
        "workload_config_id": workload_config_id,
        "workload_config_name": workload_config_name,
        "topology_config_id": topology_config_id,
        "topology_config_name": topology_config_name,
        "scan_variable": scan_variable,
        "scan_value": scan_value,
        "workload_scenario": workload_scenario,
        "metric": metric,
        "metric_available": metric_available,
        "mean": mean,
        "std": std,
        "min": minimum,
        "max": maximum,
        "count": count,
        "ci95": ci95,
    }


def _to_float(value: Any) -> float | None:
    if value in (None, ""):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _csv_value(value: Any) -> Any:
    if isinstance(value, (dict, list)):
        return json.dumps(value, ensure_ascii=False)
    return value


def _child_dir_name(row: dict[str, Any]) -> str:
    value = str(row["scan_value"]).replace(".", "_").replace("/", "_")
    method = (row.get("method_config_id") or row.get("baseline_id") or "method").replace("/", "_")
    return f"run_{row['run_index']:03d}_{row['experiment_type']}_{method}_seed_{row['seed']}_{row['scan_variable']}_{value}"


def _mirror_latest(run_dir: Path, latest_dir: Path) -> None:
    if latest_dir.exists():
        shutil.rmtree(latest_dir)
    shutil.copytree(run_dir, latest_dir)


def _dedupe(values: list[str]) -> list[str]:
    result: list[str] = []
    seen: set[str] = set()
    for value in values:
        if value in seen:
            continue
        seen.add(value)
        result.append(value)
    return result
