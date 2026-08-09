from __future__ import annotations

import csv
import json
from collections import defaultdict
from pathlib import Path
from typing import Any

from backend.app.services.v5_statistics_service import summarize


GROUP_FIELDS = [
    "suite_type", "method_config_id", "method_name", "method_role", "scan_variable", "scan_value",
    "topology_nodes", "topology_shards", "validators_per_shard", "tx_count", "cross_shard_ratio",
    "timeout_every", "fault_mode", "block_size", "block_interval_ms", "sample_count", "completed_count", "observed_completed_count", "completed_invalid_count", "blocked_count", "failed_count", "missing_count",
    "mean_tps", "median_tps", "std_tps", "min_tps", "max_tps", "ci95_low_tps", "ci95_high_tps",
    "mean_p50_ms", "mean_p95_ms", "mean_p99_ms", "submitted", "terminal", "incomplete",
    "cross_requested", "cross_finalized", "cross_refunded", "cross_failed", "changed_plugin_categories",
]

PAPER_TABLE_FIELDS = ["suite_type", "method_id", "method_name", "method_role", "scan_variable", "scan_value", "nodes", "shards", "validators_per_shard", "tx_count", "cross_shard_ratio", "timeout_every", "fault_mode", "block_size", "block_interval_ms", "sample_count", "success_sample_count", "failed_sample_count", "tps_mean", "tps_std", "tps_min", "tps_max", "latency_p50_mean", "latency_p95_mean", "latency_p99_mean", "terminal_mean", "incomplete_mean", "orphan_mean", "cross_shard_requested_mean", "cross_shard_finalized_mean", "no_fallback_all", "state_root_consistent_all"]

PAPER_ANALYSIS_FIELDS = [
    "view", "metric", "metric_unit", "method_id", "method_name", "sample_status",
    "observed_sample_count", "valid_sample_count", "excluded_sample_count", "sample_status_counts",
    "mean", "median", "std", "min", "max", "ci95_low", "ci95_high", "statistical_note", "source_child_ids",
]


def export(group_dir: Path, group: dict, children: list[dict]) -> dict:
    raw_rows = [_raw_row(child) for child in children]
    grouped = _group_rows(group, children)
    paper = paper_result_analysis(group, children)
    overall = _overall(
        children,
        cross_method_statistics_valid=paper.get("performance_comparison_valid") is True,
    )
    _write(group_dir / "raw_summary.csv", raw_rows, list(raw_rows[0]) if raw_rows else ["child_run_id", "status"])
    _write(group_dir / "aggregate_summary.csv", [_overall_row(overall)], list(_overall_row(overall)))
    _write(group_dir / "confidence_interval.csv", grouped, GROUP_FIELDS)
    _write(group_dir / "comparison_summary.csv", _suite(grouped, "comparison_experiment"), GROUP_FIELDS)
    _write(group_dir / "ablation_summary.csv", _suite(grouped, "ablation_experiment"), GROUP_FIELDS)
    _write(group_dir / "sensitivity_summary.csv", _suite(grouped, "workload_sensitivity"), GROUP_FIELDS)
    _write(group_dir / "scaling_summary.csv", _suite(grouped, "topology_scaling"), GROUP_FIELDS)
    _write(group_dir / "fault_recovery_summary.csv", _suite(grouped, "fault_recovery_experiment"), GROUP_FIELDS)
    _write(group_dir / "paper_figure_data.csv", _figure_rows(grouped), ["suite_type", "x_variable", "x_value", "series", "metric", "value", "ci95_low", "ci95_high"])
    table_rows = _paper_table_rows(grouped)
    _write(group_dir / "paper_table_data.csv", table_rows, list(table_rows[0]) if table_rows else PAPER_TABLE_FIELDS)
    classified = [(item, _sample_status_for_child(item)) for item in children]
    failures = [item for item, status in classified if status == "execution_failed"]
    invalid = [item for item, status in classified if status == "completed_invalid"]
    blocked = [item for item, status in classified if status == "blocked_incompatible"]
    paper_valid = [item for item, status in classified if status == "paper_eligible"]
    _write(group_dir / "failed_children.csv", [{key: item.get(key, "") for key in ("child_run_id", "status", "error")} for item in failures], ["child_run_id", "status", "error"])
    _write(group_dir / "invalid_children.csv", [_classification_row(item, "completed_invalid") for item in invalid], ["child_run_id", "method_config_id", "method_name", "sample_status", "status", "reasons", "observed_tps", "error"])
    _write(group_dir / "blocked_children.csv", [_classification_row(item, "blocked_incompatible") for item in blocked], ["child_run_id", "method_config_id", "method_name", "sample_status", "status", "reasons", "observed_tps", "error"])
    comparison_excluded = [item for item, status in classified if status == "comparison_excluded"]
    _write(group_dir / "comparison_excluded_children.csv", [_classification_row(item, "comparison_excluded") for item in comparison_excluded], ["child_run_id", "method_config_id", "method_name", "sample_status", "status", "reasons", "observed_tps", "error"])
    _write(group_dir / "observed_results.csv", [_raw_row(item) for item in children], list(raw_rows[0]) if raw_rows else ["child_run_id", "status"])
    valid_rows = [_raw_row(item) for item in paper_valid]
    _write(group_dir / "paper_valid_results.csv", valid_rows, list(valid_rows[0]) if valid_rows else (list(raw_rows[0]) if raw_rows else ["child_run_id", "status"]))
    effective = [(item, _effective_metrics(item)) for item in children]
    (group_dir / "missing_metrics.csv").write_text(
        "child_run_id,missing\n"
        + "\n".join(
            f"{item.get('child_run_id')},{json.dumps(metrics.get('missing', []))}"
            for item, metrics in effective
            if metrics.get("missing")
        ),
        encoding="utf-8",
    )
    aggregate_dir = group_dir / "aggregate"
    aggregate_dir.mkdir(parents=True, exist_ok=True)
    (aggregate_dir / "paper_result_analysis.json").write_text(json.dumps(paper, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    _write(aggregate_dir / "paper_result_analysis.csv", _paper_analysis_csv_rows(paper), PAPER_ANALYSIS_FIELDS)
    (group_dir / "run_group_report.md").write_text(
        f"# {group['run_group_id']}\n\n"
        f"Individually valid completed: {overall.get('individually_valid_completed_count', 0)}\n"
        f"Within-semantic paper candidates: {overall.get('paper_valid_count', overall['completed_count'])}\n"
        f"Direct cross-semantic performance comparison valid: {str(overall.get('direct_cross_semantic_performance_comparison_valid', False)).lower()}\n"
        f"Observed completed: {overall.get('observed_completed_count', 0)}\n"
        f"Completed invalid: {overall.get('completed_invalid_count', 0)}\n"
        f"Blocked: {overall.get('blocked_count', 0)}\n"
        f"Failed: {overall['failed_count']}\n",
        encoding="utf-8",
    )
    return overall


def analysis(group: dict, children: list[dict]) -> dict:
    rows = _group_rows(group, children)
    return {"run_group_id": group.get("run_group_id"), "groups": rows, "charts": _charts(rows), "paper_result_analysis": paper_result_analysis(group, children)}


def paper_result_analysis(group: dict, children: list[dict]) -> dict:
    fairness = _fairness_status(group)
    accepted: list[dict] = []
    excluded: list[dict] = []
    sample_statuses: list[dict] = []
    status_by_child: dict[str, str] = {}

    for child in children:
        child_id = str(child.get("child_run_id") or "")
        individual_reasons = _individual_result_reasons(child)
        paper_reasons = _paper_exclusion_reasons(child, individual_reasons)
        status = _sample_status(child, individual_reasons, paper_reasons)
        status_by_child[child_id] = status
        finality = _finality(child)
        metrics = _effective_metrics(child)
        sample_statuses.append({
            "child_run_id": child.get("child_run_id"),
            "method_id": child.get("method_config_id"),
            "method_name": (child.get("method") or {}).get("display_name"),
            "suite_type": child.get("suite_type"),
            "status": status,
            "reasons": paper_reasons,
            "submitted_unique_tx_count": _first_number(metrics, finality, name="submitted_unique_tx_count"),
            "terminal_unique_tx_count": _first_number(metrics, finality, name="terminal_unique_tx_count"),
            "finalized_unique_logical_tx_count": _first_number(metrics, finality, name="finalized_unique_logical_tx_count"),
            "cross_shard_failed_unique_count": _first_number(metrics, finality, name="cross_shard_failed_unique_count"),
        })
        if paper_reasons:
            excluded.append({
                "child_run_id": child.get("child_run_id"),
                "method_id": child.get("method_config_id"),
                "method_name": (child.get("method") or {}).get("display_name"),
                "suite_type": child.get("suite_type"),
                "status": status,
                "reasons": paper_reasons,
            })
        else:
            accepted.append(child)

    metrics = {
        "end_to_end_tps": _metric_rows(accepted, "end_to_end_tps", "tps"),
        "p95_finality_ms": _metric_rows(accepted, "p95_finality_ms", "ms"),
        "p99_finality_ms": _metric_rows(accepted, "p99_finality_ms", "ms"),
    }
    observed_metrics = {
        "end_to_end_tps": _observed_metric_rows(children, status_by_child, "end_to_end_tps", "tps"),
        "p95_finality_ms": _observed_metric_rows(children, status_by_child, "p95_finality_ms", "ms"),
        "p99_finality_ms": _observed_metric_rows(children, status_by_child, "p99_finality_ms", "ms"),
    }
    status_counts = {
        name: sum(1 for item in sample_statuses if item["status"] == name)
        for name in ("execution_failed", "blocked_incompatible", "completed_invalid", "comparison_excluded", "paper_eligible")
    }
    performance_valid = _performance_comparison_valid(group, children)
    status = "complete" if fairness == "passed" and performance_valid and any(metrics.values()) and not excluded else "incomplete"
    if fairness == "passed" and not performance_valid:
        status = "incomparable"
    return {
        "schema_version": "mbe_paper_result_analysis_v2",
        "run_group_id": group.get("run_group_id"),
        "analysis_status": status,
        "fairness_status": fairness,
        "performance_comparison_valid": performance_valid,
        "comparison_note": "" if performance_valid else "execution semantics, fairness, or cross-method logical-state equivalence differs; direct performance uplift is prohibited",
        "metrics": metrics,
        "observed_metrics": observed_metrics,
        "sample_statuses": sample_statuses,
        "status_counts": status_counts,
        "excluded_samples": excluded,
    }



def _performance_comparison_valid(group: dict, children: list[dict]) -> bool:
    if "performance_comparison_valid" in group:
        return group.get("performance_comparison_valid") is True
    equivalence = group.get("state_equivalence_validation") or {}
    if isinstance(equivalence, dict) and "performance_comparison_valid" in equivalence:
        if equivalence.get("performance_comparison_valid") is not True:
            return False
    fairness = group.get("fairness_validation") or group.get("fairness") or {}
    if isinstance(fairness, dict) and "performance_comparison_valid" in fairness:
        return fairness.get("performance_comparison_valid") is True
    values = {
        child.get("performance_comparison_valid")
        for child in children
        if child.get("suite_type") == "comparison_experiment"
        and child.get("performance_comparison_valid") is not None
    }
    if not values:
        return True
    return values == {True}

def _fairness_status(group: dict) -> str:
    fairness = group.get("fairness") or group.get("fairness_validation") or {}
    if isinstance(fairness, dict):
        status = fairness.get("status") or fairness.get("fairness_status")
        if status:
            return "passed" if str(status).lower() in {"passed", "pass", "true"} else "failed"
        if fairness.get("passed") is False:
            return "failed"
    return "passed"


def _individual_result_reasons(child: dict) -> list[str]:
    reasons: list[str] = []
    metrics = _effective_metrics(child)
    finality = _finality(child)
    summary = ((child.get("result") or {}).get("summary") or {})
    if child.get("execution_status", child.get("status")) != "completed":
        reasons.append("execution_status_not_completed")
        return reasons
    if child.get("artifact_status") == "incomplete":
        reasons.append("artifact_status_incomplete")

    submitted = _first_number(metrics, finality, name="submitted_unique_tx_count")
    terminal = _first_number(metrics, finality, name="terminal_unique_tx_count")
    finalized = _first_number(metrics, finality, name="finalized_unique_logical_tx_count")
    incomplete = _first_number(metrics, finality, name="incomplete_unique_tx_count")
    cross_failed = _first_number(metrics, finality, name="cross_shard_failed_unique_count")
    lifecycle_complete = _first_bool(metrics, summary, finality, name="lifecycle_complete")

    if submitted is None or terminal is None or submitted != terminal:
        reasons.append("terminal_not_equal_submitted")
    if submitted is None or finalized is None or submitted != finalized:
        reasons.append("finalized_not_equal_submitted")
    if incomplete is None or incomplete != 0:
        reasons.append("incomplete_not_zero")
    if cross_failed is None or cross_failed != 0:
        reasons.append("cross_shard_failed_not_zero")
    if lifecycle_complete is not True:
        reasons.append("lifecycle_complete_not_true")
    for name in ("no_fallback", "state_root_consistent", "receipt_root_consistent", "plan_digest_consistent"):
        if _first_bool(metrics, summary, name=name) is not True:
            reasons.append(f"{name}_not_true")
    if metrics.get("metric_completeness") != "complete":
        reasons.append("metric_completeness_not_complete")
    if _requires_block_stm(child) and _first_bool(metrics, summary, name="serial_equivalent") is not True:
        reasons.append("serial_equivalent_not_true")
    if _metric_value(child, "end_to_end_tps") is None:
        reasons.append("end_to_end_tps_missing")
    if _metric_value(child, "p95_finality_ms") is None:
        reasons.append("p95_finality_ms_missing")
    if _metric_value(child, "p99_finality_ms") is None:
        reasons.append("p99_finality_ms_missing")
    return reasons


def _paper_exclusion_reasons(child: dict, individual_reasons: list[str] | None = None) -> list[str]:
    reasons = list(individual_reasons if individual_reasons is not None else _individual_result_reasons(child))
    if child.get("formal_eligibility") is False:
        reasons.append("formal_eligibility_false")
    if child.get("pairwise_logical_state_equivalent") is False:
        reasons.append("pairwise_logical_state_equivalent_false")
    # The scheduler owns final paper-candidate eligibility after fairness and
    # within-semantic-cohort state-equivalence validation. A completed sample
    # may be individually correct while still being intentionally excluded from
    # direct paper-comparison aggregates (for example, a one-member semantic
    # cohort such as Groundhog or Batch-SI).
    if child.get("paper_candidate") is False:
        status = str(child.get("comparison_eligibility_status") or "false").strip() or "false"
        reasons.append(f"paper_candidate_false:{status}")
    return list(dict.fromkeys(reasons))


def _sample_status(child: dict, individual_reasons: list[str], paper_reasons: list[str]) -> str:
    if child.get("status") == "blocked" or child.get("execution_status") == "blocked_incompatible_workload":
        return "blocked_incompatible"
    if child.get("execution_status", child.get("status")) != "completed":
        return "execution_failed"
    if individual_reasons:
        return "completed_invalid"
    if paper_reasons:
        return "comparison_excluded"
    return "paper_eligible"



def _sample_status_for_child(child: dict) -> str:
    individual = _individual_result_reasons(child)
    paper = _paper_exclusion_reasons(child, individual)
    return _sample_status(child, individual, paper)


def _classification_row(child: dict, fallback_status: str) -> dict:
    status = _sample_status_for_child(child)
    reasons = _paper_exclusion_reasons(child, _individual_result_reasons(child))
    return {
        "child_run_id": child.get("child_run_id"),
        "method_config_id": child.get("method_config_id"),
        "method_name": (child.get("method") or {}).get("display_name"),
        "sample_status": status or fallback_status,
        "status": child.get("status"),
        "reasons": "|".join(reasons),
        "observed_tps": _metric_value(child, "end_to_end_tps"),
        "error": child.get("error") or "",
    }


def _metric_rows(accepted: list[dict], metric: str, unit: str) -> list[dict]:
    buckets: dict[tuple[str, str], list[tuple[dict, float]]] = defaultdict(list)
    for child in accepted:
        value = _metric_value(child, metric)
        if value is None:
            continue
        key = (str(child.get("method_config_id") or ""), str((child.get("method") or {}).get("display_name") or child.get("method_config_id") or ""))
        buckets[key].append((child, value))
    rows = []
    for key in sorted(buckets, key=lambda item: item[0]):
        entries = buckets[key]
        values = [value for _, value in entries]
        stats = summarize(values, completed_count=len(values), failed_count=0, missing_count=0)
        rows.append(_metric_row_payload(key, metric, unit, entries, stats, "paper_eligible", {"paper_eligible": len(entries)}))
    return rows


def _observed_metric_rows(children: list[dict], status_by_child: dict[str, str], metric: str, unit: str) -> list[dict]:
    buckets: dict[tuple[str, str], list[tuple[dict, float]]] = defaultdict(list)
    all_methods: dict[tuple[str, str], list[dict]] = defaultdict(list)
    for child in children:
        key = (str(child.get("method_config_id") or ""), str((child.get("method") or {}).get("display_name") or child.get("method_config_id") or ""))
        all_methods[key].append(child)
        value = _metric_value(child, metric)
        if child.get("execution_status", child.get("status")) == "completed" and value is not None:
            buckets[key].append((child, value))

    severity = {"paper_eligible": 0, "comparison_excluded": 1, "completed_invalid": 2, "blocked_incompatible": 3, "execution_failed": 4}
    rows = []
    for key in sorted(all_methods, key=lambda item: item[0]):
        entries = buckets.get(key, [])
        values = [value for _, value in entries]
        statuses = [status_by_child.get(str(child.get("child_run_id") or ""), "execution_failed") for child in all_methods[key]]
        counts = {name: statuses.count(name) for name in severity}
        row_status = max(statuses, key=lambda name: severity.get(name, 3)) if statuses else "execution_failed"
        stats = summarize(values, completed_count=len(values), failed_count=counts["execution_failed"], missing_count=max(0, len(all_methods[key]) - len(values)))
        rows.append(_metric_row_payload(key, metric, unit, entries, stats, row_status, counts))
    return rows


def _metric_row_payload(key: tuple[str, str], metric: str, unit: str, entries: list[tuple[dict, float]], stats: dict, sample_status: str, status_counts: dict[str, int]) -> dict:
    values = [value for _, value in entries]
    return {
        "method_id": key[0],
        "method_name": key[1],
        "metric": metric,
        "metric_unit": unit,
        "valid_sample_count": status_counts.get("paper_eligible", 0),
        "observed_sample_count": len(values),
        "excluded_sample_count": sum(value for name, value in status_counts.items() if name != "paper_eligible"),
        "sample_status": sample_status,
        "sample_status_counts": status_counts,
        "raw_values": values,
        "mean": stats["mean"],
        "median": stats["median"],
        "std": stats["std"],
        "min": stats["min"],
        "max": stats["max"],
        "ci95_low": stats["ci95_low"],
        "ci95_high": stats["ci95_high"],
        "statistical_note": "single_sample_no_variance_or_ci" if stats["count"] == 1 else ("no_observed_samples" if stats["count"] == 0 else "multi_sample_ci95"),
        "source_child_ids": [str(child.get("child_run_id")) for child, _ in entries],
    }



def _paper_analysis_csv_rows(paper: dict) -> list[dict]:
    rows: list[dict] = []
    for view, collection in (("paper_eligible", paper.get("metrics") or {}), ("observed", paper.get("observed_metrics") or {})):
        for metric, items in collection.items():
            for item in items:
                rows.append({
                    "view": view,
                    "metric": metric,
                    "metric_unit": item.get("metric_unit"),
                    "method_id": item.get("method_id"),
                    "method_name": item.get("method_name"),
                    "sample_status": item.get("sample_status", "paper_eligible"),
                    "observed_sample_count": item.get("observed_sample_count", len(item.get("raw_values") or [])),
                    "valid_sample_count": item.get("valid_sample_count"),
                    "excluded_sample_count": item.get("excluded_sample_count"),
                    "sample_status_counts": json.dumps(item.get("sample_status_counts") or {}, sort_keys=True),
                    "mean": item.get("mean"),
                    "median": item.get("median"),
                    "std": item.get("std"),
                    "min": item.get("min"),
                    "max": item.get("max"),
                    "ci95_low": item.get("ci95_low"),
                    "ci95_high": item.get("ci95_high"),
                    "statistical_note": item.get("statistical_note"),
                    "source_child_ids": json.dumps(item.get("source_child_ids") or []),
                })
    return rows



def _finality(child: dict) -> dict:
    return ((child.get("result") or {}).get("summary") or {}).get("finality_evidence") or {}


def _first_number(*sources: dict, name: str) -> int | None:
    for source in sources:
        value = source.get(name) if isinstance(source, dict) else None
        if isinstance(value, bool):
            continue
        if isinstance(value, (int, float)):
            return int(value)
    return None


def _first_bool(*sources: dict, name: str) -> bool | None:
    for source in sources:
        value = source.get(name) if isinstance(source, dict) else None
        if isinstance(value, bool):
            return value
    return None


def _metric_value(child: dict, metric: str) -> float | None:
    metrics = _effective_metrics(child)
    if metric == "end_to_end_tps":
        value = metrics.get("end_to_end_tps", metrics.get("throughput_tps"))
    elif metric == "p95_finality_ms":
        value = metrics.get("p95_finality_ms", metrics.get("p95_latency_ms"))
    elif metric == "p99_finality_ms":
        value = metrics.get("p99_finality_ms", metrics.get("p99_latency_ms"))
    else:
        value = metrics.get(metric)
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return None
    return float(value)



_COMMON_REQUIRED_METRICS = (
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
)
_BLOCK_STM_REQUIRED_METRICS = (
    "worker_count",
    "maximum_parallel_width",
    "abort_count",
    "reexecution_count",
    "validation_failure_count",
    "serial_equivalent",
)
_METATRACK_REQUIRED_METRICS = (
    "fast_track_logical_tx_count",
    "conservative_track_logical_tx_count",
    "replica_deduplicated_remote_fetch_count",
    "replica_deduplicated_remote_writeback_count",
    "aggregation_group_count",
    "pre_aggregation_physical_op_count",
    "post_aggregation_physical_op_count",
)
_BATCH_SI_REQUIRED_METRICS = (
    "configured_worker_count",
    "maximum_parallel_width",
    "batch_count",
    "maximum_batch_width",
    "write_opportunity_reuse_count",
    "dependency_edge_count",
    "deferred_transaction_count",
    "batch_snapshot_create_ms",
)
_STALE_PATH_ONLY_MISSING = {"real_cluster_summary.json", "finality_summary.json"}


def _effective_metrics(child: dict) -> dict[str, Any]:
    """Return metrics usable by the result exporter.

    The scheduler normally persists the output of ``v5_metric_extractor``.  A
    historical path-resolution bug could leave only two path-missing markers
    even though the runner already embedded the final cluster, finality and
    mechanism summaries in ``child.result.summary``.  This function merges
    those authoritative embedded summaries without fabricating measurements.
    """
    metrics = dict(child.get("metrics") or {})
    result = child.get("result") if isinstance(child.get("result"), dict) else {}
    summary = result.get("summary") if isinstance(result.get("summary"), dict) else {}
    finality = summary.get("finality_evidence") if isinstance(summary.get("finality_evidence"), dict) else {}

    aliases = {
        "throughput_tps": "throughput_tps",
        "end_to_end_tps": "end_to_end_tps",
        "logical_finality_tps": "logical_finality_tps",
        "p50_finality_ms": "p50_finality_ms",
        "p95_finality_ms": "p95_finality_ms",
        "p99_finality_ms": "p99_finality_ms",
        "submitted_unique_tx_count": "submitted_unique_tx_count",
        "terminal_unique_tx_count": "terminal_unique_tx_count",
        "incomplete_unique_tx_count": "incomplete_unique_tx_count",
        "finalized_unique_logical_tx_count": "finalized_unique_logical_tx_count",
        "cross_shard_requested_unique_count": "cross_shard_requested_unique_count",
        "cross_shard_target_committed_unique_count": "cross_shard_target_committed_unique_count",
        "cross_shard_finalized_unique_count": "cross_shard_finalized_unique_count",
        "cross_shard_refunded_unique_count": "cross_shard_refunded_unique_count",
        "cross_shard_failed_unique_count": "cross_shard_failed_unique_count",
        "completion_duration_ms": "completion_duration_ms",
        "logical_finality_duration_ms": "logical_finality_duration_ms",
    }
    for target, source in aliases.items():
        if metrics.get(target) is None and finality.get(source) is not None:
            metrics[target] = finality[source]

    for target, source in {
        "p50_latency_ms": "p50_finality_ms",
        "p95_latency_ms": "p95_finality_ms",
        "p99_latency_ms": "p99_finality_ms",
    }.items():
        if metrics.get(target) is None and finality.get(source) is not None:
            metrics[target] = finality[source]

    for name in (
        "no_fallback",
        "state_root_consistent",
        "receipt_root_consistent",
        "plan_digest_consistent",
        "orphan_process_count",
        "configured_block_size",
        "configured_block_interval_ms",
        "actual_committed_block_count",
        "actual_average_tx_per_block",
        "actual_min_tx_per_block",
        "actual_max_tx_per_block",
        "actual_block_interval_mean_ms",
        "actual_block_interval_p95_ms",
    ):
        if metrics.get(name) is None and summary.get(name) is not None:
            metrics[name] = summary[name]

    mechanism = summary.get("mechanism_metrics") if isinstance(summary.get("mechanism_metrics"), dict) else {}
    block_stm = mechanism.get("block_stm") if isinstance(mechanism.get("block_stm"), dict) else {}
    if block_stm.get("status") == "available":
        for name in (
            "worker_count",
            "maximum_parallel_width",
            "abort_count",
            "reexecution_count",
            "dependency_wait_count",
            "validation_failure_count",
            "serial_equivalent",
        ):
            if metrics.get(name) is None and block_stm.get(name) is not None:
                metrics[name] = block_stm[name]

    metatrack = mechanism.get("metatrack") if isinstance(mechanism.get("metatrack"), dict) else {}
    if metatrack.get("status") == "available":
        for name in (
            "fast_track_logical_tx_count",
            "conservative_track_logical_tx_count",
            "runtime_scheduler_event_count",
            "aggregation_group_count",
            "pre_aggregation_physical_op_count",
            "post_aggregation_physical_op_count",
            "physical_ops_saved_count",
            "aggregation_reduction_ratio",
            "replica_deduplicated_remote_fetch_count",
            "replica_deduplicated_remote_writeback_count",
        ):
            if metrics.get(name) is None and metatrack.get(name) is not None:
                metrics[name] = metatrack[name]

    remote_state = mechanism.get("remote_state") if isinstance(mechanism.get("remote_state"), dict) else {}
    for name, value in remote_state.items():
        if metrics.get(name) is None and value is not None:
            metrics[name] = value

    replay = summary.get("workload_replay_summary") if isinstance(summary.get("workload_replay_summary"), dict) else {}
    for target, source in {
        "actual_cross_shard_ratio": "actual_cross_shard_ratio",
        "actual_cross_shard_count": "actual_cross_shard_count",
        "materialized_workload_digest": "materialized_sha256",
        "workload_mapping_digest": "mapping_digest",
        "workload_truth_label": "truth_label",
        "workload_variant_id": "variant_id",
    }.items():
        if metrics.get(target) is None and replay.get(source) is not None:
            metrics[target] = replay[source]

    stale = [str(item) for item in (metrics.get("missing") or [])]
    if summary and finality:
        stale = [item for item in stale if item not in _STALE_PATH_ONLY_MISSING]

    required = list(_COMMON_REQUIRED_METRICS)
    if _requires_block_stm(child):
        required.extend(_BLOCK_STM_REQUIRED_METRICS)
    if _requires_metatrack(child):
        required.extend(_METATRACK_REQUIRED_METRICS)
    if _requires_batch_si(child):
        required.extend(_BATCH_SI_REQUIRED_METRICS)
    metric_missing = [f"metric:{name}" for name in required if metrics.get(name) is None]
    missing = list(dict.fromkeys(stale + metric_missing))
    metrics["missing"] = missing
    metrics["metric_completeness"] = "complete" if not missing else "incomplete"
    return metrics


def _requires_metatrack(child: dict) -> bool:
    method_id = str(child.get("method_config_id") or "")
    method = child.get("method") or {}
    overrides = method.get("plugin_overrides") if isinstance(method, dict) else {}
    routing = overrides.get("routing") if isinstance(overrides, dict) else ""
    return "metatrack" in method_id or routing == "metatrack_coaccess_routing"

def _requires_batch_si(child: dict) -> bool:
    method_id = str(child.get("method_config_id") or "")
    method = child.get("method") or {}
    overrides = method.get("plugin_overrides") if isinstance(method, dict) else {}
    executor = overrides.get("block_executor") if isinstance(overrides, dict) else ""
    return method_id.startswith("hash_batch_si") or executor == "batch_si_block_executor"


def _requires_block_stm(child: dict) -> bool:
    method_id = str(child.get("method_config_id") or "")
    method = child.get("method") or {}
    overrides = method.get("plugin_overrides") if isinstance(method, dict) else {}
    executor = overrides.get("block_executor") if isinstance(overrides, dict) else ""
    return "block_stm" in method_id or executor == "block_stm_block_executor"


def _group_rows(group: dict, children: list[dict]) -> list[dict]:
    base_workload = _base_workload(group)
    buckets: dict[tuple, list[dict]] = defaultdict(list)
    for child in children:
        buckets[_group_key(child, base_workload)].append(child)
    return [_aggregate(key, values) for key, values in sorted(buckets.items(), key=lambda item: str(item[0]))]


def _base_workload(group: dict) -> dict:
    selections = ((group.get("plan") or {}).get("base_spec") or {}).get("plugin_selections") or []
    return next((dict(item.get("config") or {}) for item in selections if isinstance(item, dict) and item.get("category") == "workload"), {})


def _group_key(child: dict, base_workload: dict) -> tuple:
    topology = child.get("topology_point") or {}
    workload = {**base_workload, **(child.get("workload_point") or {})}
    metrics = _effective_metrics(child)
    fault = child.get("fault_point") or {}
    method = child.get("method") or {}
    block_size, block_interval_ms = _block_settings(child)
    cross_shard_ratio = workload.get("cross_shard_ratio")
    if cross_shard_ratio is None:
        cross_shard_ratio = metrics.get("actual_cross_shard_ratio")
    return (
        child.get("suite_type", ""), child.get("method_config_id", ""), method.get("display_name", ""),
        child.get("method_role", method.get("role", "custom")), child.get("scan_variable", ""), child.get("scan_value", ""),
        topology.get("nodes"), topology.get("shards"), topology.get("validators_per_shard"), topology.get("worker_count", metrics.get("configured_worker_count", metrics.get("worker_count"))),
        workload.get("tx_count", child.get("estimated_transactions")), cross_shard_ratio, workload.get("timeout_every"),
        fault.get("mode", "disabled"), block_size, block_interval_ms, tuple(child.get("changed_plugin_categories") or []),
    )


def _eligible_for_within_method_sensitivity(child: dict) -> bool:
    # A within-method theta/scale trend does not require a second method in the
    # same semantic cohort.  Keep this exception deliberately narrow: only an
    # otherwise individually valid/formally eligible sample excluded solely
    # because its cohort has insufficient comparison runs is admitted here.
    if child.get("suite_type") != "workload_sensitivity":
        return _sample_status_for_child(child) == "paper_eligible"
    if child.get("execution_status", child.get("status")) != "completed":
        return False
    if child.get("formal_eligibility") is not True:
        return False
    if _individual_result_reasons(child):
        return False
    if child.get("paper_candidate") is not False:
        return True
    return str(child.get("comparison_eligibility_status") or "").strip() == "insufficient_valid_runs"


def _aggregate(key: tuple, entries: list[dict]) -> dict:
    suite, method_id, method_name, role, scan_variable, scan_value, nodes, shards, validators, worker_count, tx_count, ratio, timeout, fault, block_size, block_interval_ms, changed = key
    observed_completed = [entry for entry in entries if entry.get("status") == "completed"]
    if suite == "workload_sensitivity":
        completed = [entry for entry in entries if _eligible_for_within_method_sensitivity(entry)]
    else:
        completed = [entry for entry in entries if _sample_status_for_child(entry) == "paper_eligible"]
    completed_invalid = [entry for entry in observed_completed if _sample_status_for_child(entry) == "completed_invalid"]
    blocked = [entry for entry in entries if entry.get("status") == "blocked" or entry.get("execution_status") == "blocked_incompatible_workload"]
    failed = [entry for entry in entries if _sample_status_for_child(entry) == "execution_failed" and entry not in blocked]
    metrics = [_effective_metrics(entry) for entry in completed]
    finalities = [_finality(entry) for entry in completed]
    stats = summarize(
        [float(item["end_to_end_tps"]) for item in metrics if item.get("end_to_end_tps") is not None],
        completed_count=len(completed),
        failed_count=len(failed),
        missing_count=sum(bool(item.get("missing")) for item in metrics),
    )
    mean = lambda name: _mean([item.get(name) for item in metrics])
    return {
        "suite_type": suite, "method_config_id": method_id, "method_name": method_name, "method_role": role,
        "scan_variable": scan_variable, "scan_value": scan_value, "topology_nodes": nodes, "topology_shards": shards,
        "validators_per_shard": validators, "worker_count": worker_count, "tx_count": tx_count, "cross_shard_ratio": ratio, "timeout_every": timeout,
        "fault_mode": fault, "block_size": block_size, "block_interval_ms": block_interval_ms, "sample_count": stats["count"], "completed_count": stats["completed_count"], "observed_completed_count": len(observed_completed), "completed_invalid_count": len(completed_invalid), "blocked_count": len(blocked), "failed_count": stats["failed_count"], "missing_count": stats["missing_count"],
        "mean_tps": stats["mean"], "median_tps": stats["median"], "std_tps": stats["std"], "min_tps": stats["min"], "max_tps": stats["max"], "ci95_low_tps": stats["ci95_low"], "ci95_high_tps": stats["ci95_high"],
        "mean_p50_ms": mean("p50_finality_ms") or mean("p50_latency_ms"),
        "mean_p95_ms": mean("p95_finality_ms") or mean("p95_latency_ms"),
        "mean_p99_ms": mean("p99_finality_ms") or mean("p99_latency_ms"),
        "submitted": sum(_number(item.get("submitted_unique_tx_count")) for item in finalities), "terminal": sum(_number(item.get("terminal_unique_tx_count")) for item in finalities), "incomplete": sum(_number(item.get("incomplete_unique_tx_count")) for item in finalities),
        "cross_requested": sum(_number(item.get("cross_shard_requested_unique_count")) for item in finalities), "cross_finalized": sum(_number(item.get("cross_shard_finalized_unique_count")) for item in finalities), "cross_refunded": sum(_number(item.get("cross_shard_refunded_unique_count")) for item in finalities), "cross_failed": sum(_number(item.get("cross_shard_failed_unique_count")) for item in finalities),
        "changed_plugin_categories": ",".join(changed),
        "orphan": sum(_number(item.get("orphan_process_count")) for item in metrics),
        "no_fallback_all": bool(metrics) and all(item.get("no_fallback") is True for item in metrics),
        "state_root_consistent_all": bool(metrics) and all(item.get("state_root_consistent") is True for item in metrics),
    }


def _overall(children: list[dict], *, cross_method_statistics_valid: bool = True) -> dict:
    observed_completed = [item for item in children if item.get("status") == "completed"]
    individually_valid_completed = [item for item in observed_completed if not _individual_result_reasons(item)]
    completed = [item for item in children if _sample_status_for_child(item) == "paper_eligible"]
    completed_invalid = [item for item in observed_completed if _sample_status_for_child(item) == "completed_invalid"]
    blocked = [item for item in children if item.get("status") == "blocked" or item.get("execution_status") == "blocked_incompatible_workload"]
    failed = [item for item in children if _sample_status_for_child(item) == "execution_failed" and item not in blocked]
    metrics = [_effective_metrics(item) for item in completed]
    summary = summarize(
        [float(item["end_to_end_tps"]) for item in metrics if item.get("end_to_end_tps") is not None],
        completed_count=len(completed),
        failed_count=len(failed),
        missing_count=sum(bool(item.get("missing")) for item in metrics),
    )
    if not cross_method_statistics_valid:
        # A mean/CI over heterogeneous semantic cohorts has no paper meaning.
        # Keep per-method paper_result_analysis rows, counts, and raw observations,
        # but suppress misleading run-group cross-method aggregate statistics.
        for key in ("mean", "median", "std", "min", "max", "ci95_low", "ci95_high"):
            summary[key] = None
    summary.update({
        "direct_cross_semantic_performance_comparison_valid": cross_method_statistics_valid,
        "observed_completed_count": len(observed_completed),
        "individually_valid_completed_count": len(individually_valid_completed),
        "paper_valid_count": len(completed),
        "completed_invalid_count": len(completed_invalid),
        "blocked_count": len(blocked),
        "execution_failed_count": len(failed),
        "excluded_from_paper_count": len(children) - len(completed),
    })
    return summary


def _overall_row(overall: dict) -> dict:
    return {"scope": "run_group", **overall}


def _raw_row(child: dict) -> dict:
    metrics = _effective_metrics(child)
    individual_result_valid = child.get("status") == "completed" and not _individual_result_reasons(child)
    paper_candidate = child.get("paper_candidate")
    return {
        "child_run_id": child.get("child_run_id"),
        "suite_type": child.get("suite_type"),
        "method_config_id": child.get("method_config_id"),
        "method_name": (child.get("method") or {}).get("display_name"),
        "method_role": child.get("method_role"),
        "seed": child.get("seed"),
        "repeat_index": child.get("repeat_index"),
        "scan_variable": child.get("scan_variable"),
        "scan_value": child.get("scan_value"),
        "status": child.get("status"),
        "sample_status": _sample_status_for_child(child),
        "individual_result_valid": individual_result_valid,
        "paper_candidate": paper_candidate,
        "comparison_eligibility_status": child.get("comparison_eligibility_status"),
        "direct_cross_semantic_performance_comparable": child.get("performance_comparison_valid") is True,
        **metrics,
    }


def _figure_rows(groups: list[dict]) -> list[dict]:
    rows = []
    for item in groups:
        x_variable = item["scan_variable"] or "method"
        x_value = item["scan_value"] or item["method_name"]
        for metric, value, low, high in (("end_to_end_tps", item["mean_tps"], item["ci95_low_tps"], item["ci95_high_tps"]), ("p99_finality_ms", item["mean_p99_ms"], None, None)):
            if value is not None:
                rows.append({"suite_type": item["suite_type"], "x_variable": x_variable, "x_value": x_value, "series": item["method_name"], "metric": metric, "value": value, "ci95_low": low, "ci95_high": high})
    return rows


def _paper_table_rows(groups: list[dict]) -> list[dict]:
    rows = []
    for row in groups:
        rows.append({"suite_type": row["suite_type"], "method_id": row["method_config_id"], "method_name": row["method_name"], "method_role": row["method_role"], "scan_variable": row["scan_variable"], "scan_value": row["scan_value"], "nodes": row["topology_nodes"], "shards": row["topology_shards"], "validators_per_shard": row["validators_per_shard"], "tx_count": row["tx_count"], "cross_shard_ratio": row["cross_shard_ratio"], "timeout_every": row["timeout_every"], "fault_mode": row["fault_mode"], "block_size": row["block_size"], "block_interval_ms": row["block_interval_ms"], "sample_count": row["sample_count"], "success_sample_count": row["completed_count"], "failed_sample_count": row["failed_count"], "tps_mean": row["mean_tps"], "tps_std": row["std_tps"], "tps_min": row["min_tps"], "tps_max": row["max_tps"], "latency_p50_mean": row["mean_p50_ms"], "latency_p95_mean": row["mean_p95_ms"], "latency_p99_mean": row["mean_p99_ms"], "terminal_mean": row["terminal"], "incomplete_mean": row["incomplete"], "orphan_mean": row.get("orphan"), "cross_shard_requested_mean": row["cross_requested"], "cross_shard_finalized_mean": row["cross_finalized"], "no_fallback_all": row.get("no_fallback_all"), "state_root_consistent_all": row.get("state_root_consistent_all")})
    return rows


def _block_settings(child: dict) -> tuple[Any, Any]:
    metrics = child.get("metrics") or {}
    summary = (child.get("result") or {}).get("summary") or {}
    return (
        child.get("block_size") or metrics.get("configured_block_size") or summary.get("configured_block_size"),
        child.get("block_interval_ms") or metrics.get("configured_block_interval_ms") or summary.get("configured_block_interval_ms"),
    )


def _charts(groups: list[dict]) -> list[dict]:
    by_suite: dict[str, list[dict]] = defaultdict(list)
    for row in groups:
        by_suite[row["suite_type"]].append(row)
    kind = {"comparison_experiment": "bar", "ablation_experiment": "bar", "workload_sensitivity": "line", "topology_scaling": "line", "fault_recovery_experiment": "bar"}
    return [{"suite_type": suite, "kind": kind.get(suite, "summary"), "rows": rows} for suite, rows in by_suite.items()]


def _suite(rows: list[dict], suite: str) -> list[dict]:
    return [row for row in rows if row["suite_type"] == suite]


def _mean(values: list[Any]) -> float | None:
    numbers = [float(value) for value in values if isinstance(value, (int, float)) and not isinstance(value, bool)]
    return sum(numbers) / len(numbers) if numbers else None


def _number(value: Any) -> int:
    return int(value) if isinstance(value, (int, float)) and not isinstance(value, bool) else 0


def _write(path: Path, rows: list[dict], fields: list[str]) -> None:
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)
