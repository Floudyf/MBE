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
    "timeout_every", "fault_mode", "block_size", "block_interval_ms", "sample_count", "completed_count", "failed_count", "missing_count",
    "mean_tps", "median_tps", "std_tps", "min_tps", "max_tps", "ci95_low_tps", "ci95_high_tps",
    "mean_p50_ms", "mean_p95_ms", "mean_p99_ms", "submitted", "terminal", "incomplete",
    "cross_requested", "cross_finalized", "cross_refunded", "cross_failed", "changed_plugin_categories",
]

PAPER_TABLE_FIELDS = ["suite_type", "method_id", "method_name", "method_role", "scan_variable", "scan_value", "nodes", "shards", "validators_per_shard", "tx_count", "cross_shard_ratio", "timeout_every", "fault_mode", "block_size", "block_interval_ms", "sample_count", "success_sample_count", "failed_sample_count", "tps_mean", "tps_std", "tps_min", "tps_max", "latency_p50_mean", "latency_p95_mean", "latency_p99_mean", "terminal_mean", "incomplete_mean", "orphan_mean", "cross_shard_requested_mean", "cross_shard_finalized_mean", "no_fallback_all", "state_root_consistent_all"]

PAPER_ANALYSIS_FIELDS = [
    "metric", "metric_unit", "method_id", "method_name", "valid_sample_count", "excluded_sample_count",
    "mean", "median", "std", "min", "max", "ci95_low", "ci95_high", "statistical_note", "source_child_ids",
]


def export(group_dir: Path, group: dict, children: list[dict]) -> dict:
    raw_rows = [_raw_row(child) for child in children]
    grouped = _group_rows(group, children)
    overall = _overall(children)
    paper = paper_result_analysis(group, children)
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
    failures = [item for item in children if item.get("status") != "completed"]
    _write(group_dir / "failed_children.csv", [{key: item.get(key, "") for key in ("child_run_id", "status", "error")} for item in failures], ["child_run_id", "status", "error"])
    (group_dir / "missing_metrics.csv").write_text("child_run_id,missing\n" + "\n".join(f"{item.get('child_run_id')},{json.dumps(item.get('metrics', {}).get('missing', []))}" for item in children if item.get("metrics", {}).get("missing")), encoding="utf-8")
    aggregate_dir = group_dir / "aggregate"
    aggregate_dir.mkdir(parents=True, exist_ok=True)
    (aggregate_dir / "paper_result_analysis.json").write_text(json.dumps(paper, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    _write(aggregate_dir / "paper_result_analysis.csv", _paper_analysis_csv_rows(paper), PAPER_ANALYSIS_FIELDS)
    (group_dir / "run_group_report.md").write_text(f"# {group['run_group_id']}\n\nCompleted: {overall['completed_count']}\nFailed: {overall['failed_count']}\n", encoding="utf-8")
    return overall


def analysis(group: dict, children: list[dict]) -> dict:
    rows = _group_rows(group, children)
    return {"run_group_id": group.get("run_group_id"), "groups": rows, "charts": _charts(rows), "paper_result_analysis": paper_result_analysis(group, children)}


def paper_result_analysis(group: dict, children: list[dict]) -> dict:
    fairness = _fairness_status(group)
    accepted: list[dict] = []
    excluded: list[dict] = []
    for child in children:
        reasons = _paper_exclusion_reasons(child)
        if reasons:
            excluded.append({
                "child_run_id": child.get("child_run_id"),
                "method_id": child.get("method_config_id"),
                "method_name": (child.get("method") or {}).get("display_name"),
                "suite_type": child.get("suite_type"),
                "reasons": reasons,
            })
        else:
            accepted.append(child)

    metrics = {
        "end_to_end_tps": _metric_rows(accepted, excluded, "end_to_end_tps", "tps"),
        "p95_finality_ms": _metric_rows(accepted, excluded, "p95_finality_ms", "ms"),
        "p99_finality_ms": _metric_rows(accepted, excluded, "p99_finality_ms", "ms"),
    }
    return {
        "schema_version": "mbe_paper_result_analysis_v1",
        "run_group_id": group.get("run_group_id"),
        "analysis_status": "complete" if fairness == "passed" and any(metrics.values()) and not excluded else "incomplete",
        "fairness_status": fairness,
        "metrics": metrics,
        "excluded_samples": excluded,
    }


def _fairness_status(group: dict) -> str:
    fairness = group.get("fairness") or group.get("fairness_validation") or {}
    if isinstance(fairness, dict):
        status = fairness.get("status") or fairness.get("fairness_status")
        if status:
            return "passed" if str(status).lower() in {"passed", "pass", "true"} else "failed"
        if fairness.get("passed") is False:
            return "failed"
    return "passed"


def _paper_exclusion_reasons(child: dict) -> list[str]:
    reasons: list[str] = []
    metrics = child.get("metrics") or {}
    finality = _finality(child)
    summary = ((child.get("result") or {}).get("summary") or {})
    if child.get("status") != "completed":
        reasons.append("status_not_completed")
    submitted = _first_number(metrics, finality, name="submitted_unique_tx_count")
    terminal = _first_number(metrics, finality, name="terminal_unique_tx_count")
    incomplete = _first_number(metrics, finality, name="incomplete_unique_tx_count")
    if submitted is None or terminal is None or submitted != terminal:
        reasons.append("terminal_not_equal_submitted")
    if incomplete is None or incomplete != 0:
        reasons.append("incomplete_not_zero")
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


def _metric_rows(accepted: list[dict], excluded: list[dict], metric: str, unit: str) -> list[dict]:
    buckets: dict[tuple[str, str], list[tuple[dict, float]]] = defaultdict(list)
    excluded_by_method: dict[tuple[str, str], int] = defaultdict(int)
    for child in accepted:
        value = _metric_value(child, metric)
        if value is None:
            continue
        key = (str(child.get("method_config_id") or ""), str((child.get("method") or {}).get("display_name") or child.get("method_config_id") or ""))
        buckets[key].append((child, value))
    for sample in excluded:
        key = (str(sample.get("method_id") or ""), str(sample.get("method_name") or sample.get("method_id") or ""))
        excluded_by_method[key] += 1
    rows = []
    for key in sorted(set(buckets) | set(excluded_by_method), key=lambda item: item[0]):
        entries = buckets.get(key, [])
        values = [value for _, value in entries]
        stats = summarize(values, completed_count=len(values), failed_count=0, missing_count=excluded_by_method.get(key, 0))
        rows.append({
            "method_id": key[0],
            "method_name": key[1],
            "metric": metric,
            "metric_unit": unit,
            "valid_sample_count": stats["count"],
            "excluded_sample_count": excluded_by_method.get(key, 0),
            "raw_values": values,
            "mean": stats["mean"],
            "median": stats["median"],
            "std": stats["std"],
            "min": stats["min"],
            "max": stats["max"],
            "ci95_low": stats["ci95_low"],
            "ci95_high": stats["ci95_high"],
            "statistical_note": "single_sample_no_variance_or_ci" if stats["count"] == 1 else ("no_valid_samples" if stats["count"] == 0 else "multi_sample_ci95"),
            "source_child_ids": [str(child.get("child_run_id")) for child, _ in entries],
        })
    return rows


def _paper_analysis_csv_rows(paper: dict) -> list[dict]:
    rows: list[dict] = []
    for metric, items in (paper.get("metrics") or {}).items():
        for item in items:
            rows.append({
                "metric": metric,
                "metric_unit": item.get("metric_unit"),
                "method_id": item.get("method_id"),
                "method_name": item.get("method_name"),
                "valid_sample_count": item.get("valid_sample_count"),
                "excluded_sample_count": item.get("excluded_sample_count"),
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
    metrics = child.get("metrics") or {}
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
    fault = child.get("fault_point") or {}
    method = child.get("method") or {}
    block_size, block_interval_ms = _block_settings(child)
    return (
        child.get("suite_type", ""), child.get("method_config_id", ""), method.get("display_name", ""),
        child.get("method_role", method.get("role", "custom")), child.get("scan_variable", ""), child.get("scan_value", ""),
        topology.get("nodes"), topology.get("shards"), topology.get("validators_per_shard"),
        workload.get("tx_count", child.get("estimated_transactions")), workload.get("cross_shard_ratio"), workload.get("timeout_every"),
        fault.get("mode", "disabled"), block_size, block_interval_ms, tuple(child.get("changed_plugin_categories") or []),
    )


def _aggregate(key: tuple, entries: list[dict]) -> dict:
    suite, method_id, method_name, role, scan_variable, scan_value, nodes, shards, validators, tx_count, ratio, timeout, fault, block_size, block_interval_ms, changed = key
    completed = [entry for entry in entries if entry.get("status") == "completed"]
    metrics = [entry.get("metrics", {}) for entry in completed]
    finalities = [((entry.get("result") or {}).get("summary") or {}).get("finality_evidence", {}) for entry in completed]
    stats = summarize([float(item["throughput_tps"]) for item in metrics if item.get("throughput_tps") is not None], completed_count=len(completed), failed_count=len(entries) - len(completed), missing_count=sum(bool(item.get("missing")) for item in metrics))
    mean = lambda name: _mean([item.get(name) for item in metrics])
    return {
        "suite_type": suite, "method_config_id": method_id, "method_name": method_name, "method_role": role,
        "scan_variable": scan_variable, "scan_value": scan_value, "topology_nodes": nodes, "topology_shards": shards,
        "validators_per_shard": validators, "tx_count": tx_count, "cross_shard_ratio": ratio, "timeout_every": timeout,
        "fault_mode": fault, "block_size": block_size, "block_interval_ms": block_interval_ms, "sample_count": stats["count"], "completed_count": stats["completed_count"], "failed_count": stats["failed_count"], "missing_count": stats["missing_count"],
        "mean_tps": stats["mean"], "median_tps": stats["median"], "std_tps": stats["std"], "min_tps": stats["min"], "max_tps": stats["max"], "ci95_low_tps": stats["ci95_low"], "ci95_high_tps": stats["ci95_high"],
        "mean_p50_ms": mean("p50_latency_ms"), "mean_p95_ms": mean("p95_latency_ms"), "mean_p99_ms": mean("p99_latency_ms"),
        "submitted": sum(_number(item.get("submitted_unique_tx_count")) for item in finalities), "terminal": sum(_number(item.get("terminal_unique_tx_count")) for item in finalities), "incomplete": sum(_number(item.get("incomplete_unique_tx_count")) for item in finalities),
        "cross_requested": sum(_number(item.get("cross_shard_requested_unique_count")) for item in finalities), "cross_finalized": sum(_number(item.get("cross_shard_finalized_unique_count")) for item in finalities), "cross_refunded": sum(_number(item.get("cross_shard_refunded_unique_count")) for item in finalities), "cross_failed": sum(_number(item.get("cross_shard_failed_unique_count")) for item in finalities),
        "changed_plugin_categories": ",".join(changed),
    }


def _overall(children: list[dict]) -> dict:
    completed = [item for item in children if item.get("status") == "completed"]
    metrics = [item.get("metrics", {}) for item in completed]
    return summarize([float(item["throughput_tps"]) for item in metrics if item.get("throughput_tps") is not None], completed_count=len(completed), failed_count=len(children) - len(completed), missing_count=sum(bool(item.get("missing")) for item in metrics))


def _overall_row(overall: dict) -> dict:
    return {"scope": "run_group", **overall}


def _raw_row(child: dict) -> dict:
    metrics = child.get("metrics", {})
    return {"child_run_id": child.get("child_run_id"), "suite_type": child.get("suite_type"), "method_config_id": child.get("method_config_id"), "method_name": (child.get("method") or {}).get("display_name"), "method_role": child.get("method_role"), "seed": child.get("seed"), "repeat_index": child.get("repeat_index"), "scan_variable": child.get("scan_variable"), "scan_value": child.get("scan_value"), "status": child.get("status"), "paper_candidate": child.get("paper_candidate"), **metrics}


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
        rows.append({"suite_type": row["suite_type"], "method_id": row["method_config_id"], "method_name": row["method_name"], "method_role": row["method_role"], "scan_variable": row["scan_variable"], "scan_value": row["scan_value"], "nodes": row["topology_nodes"], "shards": row["topology_shards"], "validators_per_shard": row["validators_per_shard"], "tx_count": row["tx_count"], "cross_shard_ratio": row["cross_shard_ratio"], "timeout_every": row["timeout_every"], "fault_mode": row["fault_mode"], "block_size": row["block_size"], "block_interval_ms": row["block_interval_ms"], "sample_count": row["sample_count"], "success_sample_count": row["completed_count"], "failed_sample_count": row["failed_count"], "tps_mean": row["mean_tps"], "tps_std": row["std_tps"], "tps_min": row["min_tps"], "tps_max": row["max_tps"], "latency_p50_mean": row["mean_p50_ms"], "latency_p95_mean": row["mean_p95_ms"], "latency_p99_mean": row["mean_p99_ms"], "terminal_mean": row["terminal"], "incomplete_mean": row["incomplete"], "orphan_mean": "", "cross_shard_requested_mean": row["cross_requested"], "cross_shard_finalized_mean": row["cross_finalized"], "no_fallback_all": "", "state_root_consistent_all": ""})
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
