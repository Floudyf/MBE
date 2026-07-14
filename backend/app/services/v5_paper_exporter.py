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
    "timeout_every", "fault_mode", "sample_count", "completed_count", "failed_count", "missing_count",
    "mean_tps", "median_tps", "std_tps", "min_tps", "max_tps", "ci95_low_tps", "ci95_high_tps",
    "mean_p50_ms", "mean_p95_ms", "mean_p99_ms", "submitted", "terminal", "incomplete",
    "cross_requested", "cross_finalized", "cross_refunded", "cross_failed", "changed_plugin_categories",
]

PAPER_TABLE_FIELDS = ["suite_type", "method_id", "method_name", "method_role", "scan_variable", "scan_value", "nodes", "shards", "validators_per_shard", "tx_count", "cross_shard_ratio", "timeout_every", "fault_mode", "sample_count", "success_sample_count", "failed_sample_count", "tps_mean", "tps_std", "tps_min", "tps_max", "latency_p50_mean", "latency_p95_mean", "latency_p99_mean", "terminal_mean", "incomplete_mean", "orphan_mean", "cross_shard_requested_mean", "cross_shard_finalized_mean", "no_fallback_all", "state_root_consistent_all"]


def export(group_dir: Path, group: dict, children: list[dict]) -> dict:
    raw_rows = [_raw_row(child) for child in children]
    grouped = _group_rows(group, children)
    overall = _overall(children)
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
    (group_dir / "run_group_report.md").write_text(f"# {group['run_group_id']}\n\nCompleted: {overall['completed_count']}\nFailed: {overall['failed_count']}\n", encoding="utf-8")
    return overall


def analysis(group: dict, children: list[dict]) -> dict:
    rows = _group_rows(group, children)
    return {"run_group_id": group.get("run_group_id"), "groups": rows, "charts": _charts(rows)}


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
    return (
        child.get("suite_type", ""), child.get("method_config_id", ""), method.get("display_name", ""),
        child.get("method_role", method.get("role", "custom")), child.get("scan_variable", ""), child.get("scan_value", ""),
        topology.get("nodes"), topology.get("shards"), topology.get("validators_per_shard"),
        workload.get("tx_count", child.get("estimated_transactions")), workload.get("cross_shard_ratio"), workload.get("timeout_every"),
        fault.get("mode", "disabled"), tuple(child.get("changed_plugin_categories") or []),
    )


def _aggregate(key: tuple, entries: list[dict]) -> dict:
    suite, method_id, method_name, role, scan_variable, scan_value, nodes, shards, validators, tx_count, ratio, timeout, fault, changed = key
    completed = [entry for entry in entries if entry.get("status") == "completed"]
    metrics = [entry.get("metrics", {}) for entry in completed]
    finalities = [((entry.get("result") or {}).get("summary") or {}).get("finality_evidence", {}) for entry in completed]
    stats = summarize([float(item["throughput_tps"]) for item in metrics if item.get("throughput_tps") is not None], completed_count=len(completed), failed_count=len(entries) - len(completed), missing_count=sum(bool(item.get("missing")) for item in metrics))
    mean = lambda name: _mean([item.get(name) for item in metrics])
    return {
        "suite_type": suite, "method_config_id": method_id, "method_name": method_name, "method_role": role,
        "scan_variable": scan_variable, "scan_value": scan_value, "topology_nodes": nodes, "topology_shards": shards,
        "validators_per_shard": validators, "tx_count": tx_count, "cross_shard_ratio": ratio, "timeout_every": timeout,
        "fault_mode": fault, "sample_count": stats["count"], "completed_count": stats["completed_count"], "failed_count": stats["failed_count"], "missing_count": stats["missing_count"],
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
        for metric, value, low, high in (("throughput_tps", item["mean_tps"], item["ci95_low_tps"], item["ci95_high_tps"]), ("p99_latency_ms", item["mean_p99_ms"], None, None)):
            if value is not None:
                rows.append({"suite_type": item["suite_type"], "x_variable": x_variable, "x_value": x_value, "series": item["method_name"], "metric": metric, "value": value, "ci95_low": low, "ci95_high": high})
    return rows


def _paper_table_rows(groups: list[dict]) -> list[dict]:
    rows = []
    for row in groups:
        rows.append({"suite_type": row["suite_type"], "method_id": row["method_config_id"], "method_name": row["method_name"], "method_role": row["method_role"], "scan_variable": row["scan_variable"], "scan_value": row["scan_value"], "nodes": row["topology_nodes"], "shards": row["topology_shards"], "validators_per_shard": row["validators_per_shard"], "tx_count": row["tx_count"], "cross_shard_ratio": row["cross_shard_ratio"], "timeout_every": row["timeout_every"], "fault_mode": row["fault_mode"], "sample_count": row["sample_count"], "success_sample_count": row["completed_count"], "failed_sample_count": row["failed_count"], "tps_mean": row["mean_tps"], "tps_std": row["std_tps"], "tps_min": row["min_tps"], "tps_max": row["max_tps"], "latency_p50_mean": row["mean_p50_ms"], "latency_p95_mean": row["mean_p95_ms"], "latency_p99_mean": row["mean_p99_ms"], "terminal_mean": row["terminal"], "incomplete_mean": row["incomplete"], "orphan_mean": "", "cross_shard_requested_mean": row["cross_requested"], "cross_shard_finalized_mean": row["cross_finalized"], "no_fallback_all": "", "state_root_consistent_all": ""})
    return rows


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
