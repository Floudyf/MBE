from __future__ import annotations

import csv
import json
from pathlib import Path

from backend.app.services.v5_statistics_service import summarize


def export(group_dir: Path, group: dict, children: list[dict]) -> dict:
    metrics = [item.get("metrics", {}) for item in children if item.get("status") == "completed"]
    failures = [item for item in children if item.get("status") != "completed"]
    values = [float(item["throughput_tps"]) for item in metrics if item.get("throughput_tps") is not None]
    aggregate = summarize(values, completed_count=len(metrics), failed_count=len(failures), missing_count=sum(bool(item.get("missing")) for item in metrics))
    fields = ["child_run_id", "suite_type", "method_config_id", "seed", "repeat_index", "status", "paper_candidate", "throughput_tps", "p50_latency_ms", "p95_latency_ms", "p99_latency_ms"]
    with (group_dir / "raw_summary.csv").open("w", newline="", encoding="utf-8") as handle:
        writer=csv.DictWriter(handle, fieldnames=fields); writer.writeheader()
        for child in children:
            combined = {**child, **child.get("metrics", {})}
            writer.writerow({field: combined.get(field, "") for field in fields})
    for filename in ("aggregate_summary.csv", "confidence_interval.csv", "comparison_summary.csv", "ablation_summary.csv", "sensitivity_summary.csv", "scaling_summary.csv", "fault_recovery_summary.csv", "paper_figure_data.csv", "paper_table_data.csv"):
        with (group_dir / filename).open("w", newline="", encoding="utf-8") as handle:
            writer=csv.DictWriter(handle, fieldnames=list(aggregate)); writer.writeheader(); writer.writerow(aggregate)
    with (group_dir / "failed_children.csv").open("w", newline="", encoding="utf-8") as handle:
        writer=csv.DictWriter(handle, fieldnames=["child_run_id","status","error"]); writer.writeheader(); writer.writerows({key:item.get(key,"") for key in ("child_run_id","status","error")} for item in failures)
    (group_dir / "missing_metrics.csv").write_text("child_run_id,missing\n" + "\n".join(f"{item.get('child_run_id')},{json.dumps(item.get('metrics',{}).get('missing',[]))}" for item in children if item.get("metrics",{}).get("missing")), encoding="utf-8")
    (group_dir / "run_group_report.md").write_text(f"# {group['run_group_id']}\n\nCompleted: {len(metrics)}\nFailed: {len(failures)}\n", encoding="utf-8")
    return aggregate
