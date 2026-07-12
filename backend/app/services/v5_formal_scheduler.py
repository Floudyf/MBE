from __future__ import annotations

import multiprocessing
import hashlib
import json
from datetime import UTC, datetime
from uuid import uuid4

from backend.app.models.v5_experiment_spec import V5PluginSelection
from backend.app.models.v5_formal_experiment import V5FormalExperimentPlan
from backend.app.services import v5_real_cluster_runner
from backend.app.services.v5_formal_run_store import children, group_dir, read_group, write_attempt, write_child, write_group
from backend.app.services.v5_fairness_validator import validate as validate_fairness, write_artifacts as write_fairness_artifacts
from backend.app.services.v5_metric_extractor import extract as extract_metrics
from backend.app.services.v5_paper_exporter import export as export_paper
from backend.app.services.v5_reproducibility_bundle import build as build_bundle


def expand(plan: V5FormalExperimentPlan, backend: str) -> list[dict]:
    methods = plan.methods or []
    if not methods:
        methods = [{"method_id": "saved_method", "display_name": "Saved Method", "plugin_overrides": {}}]
    rows = []
    for suite in plan.suites:
        variants = _variants(plan, suite)
        suite_methods = methods if suite in {"comparison_experiment", "ablation_experiment", "main_experiment"} else methods[:1]
        for method in suite_methods:
            for seed in plan.seeds:
                for repeat in range(plan.repeats):
                  for variant in variants:
                    item = method if isinstance(method, dict) else method.model_dump()
                    snapshot = {selection.category: item.get("plugin_overrides", {}).get(selection.category, selection.plugin_id) for selection in plan.base_spec.plugin_selections}
                    full_snapshot = {"plugins": snapshot, "workload": variant["workload_point"], "topology": variant["topology_point"], "fault": variant["fault_point"]}
                    rows.append({
                        "child_run_id": "v5child_" + hashlib.sha256(json.dumps({"suite": suite, "method": item["method_id"], "seed": seed, "repeat": repeat, "variant": variant}, sort_keys=True).encode()).hexdigest()[:16],
                        "suite_type": suite, "method": item, "method_config_id": item["method_id"],
                        "method_snapshot_digest": hashlib.sha256(json.dumps(snapshot, sort_keys=True).encode()).hexdigest(),
                        "workload_snapshot_digest": hashlib.sha256(json.dumps(variant["workload_point"], sort_keys=True).encode()).hexdigest(),
                        "topology_snapshot_digest": hashlib.sha256(json.dumps(variant["topology_point"], sort_keys=True).encode()).hexdigest(),
                        "fault_snapshot_digest": hashlib.sha256(json.dumps(variant["fault_point"], sort_keys=True, default=str).encode()).hexdigest(),
                        "workload_point": variant["workload_point"], "topology_point": variant["topology_point"], "fault_point": variant["fault_point"],
                        "seed": seed, "repeat_index": repeat, "scan_variable": variant["scan_variable"], "scan_value": variant["scan_value"],
                        "fairness_key": hashlib.sha256(json.dumps({"suite": suite, "seed": seed, "repeat": repeat, "snapshot": full_snapshot}, sort_keys=True, default=str).encode()).hexdigest(),
                        "comparison_group_id": f"{suite}:{seed}:{repeat}:{variant['group']}", "execution_backend": backend,
                        "estimated_processes": variant["topology_point"].get("nodes", plan.base_spec.topology.nodes) if backend == "real_cluster" else 0,
                        "estimated_transactions": variant["workload_point"].get("tx_count", plan.base_spec.tx_count), "runnable": backend != "simulation", "blockers": ["V3 simulation adapter pending"] if backend == "simulation" else [], "warnings": [],
                    })
    return validate_fairness(rows)[0]


def _variants(plan: V5FormalExperimentPlan, suite: str) -> list[dict]:
    base = {"workload_point": {}, "topology_point": plan.base_spec.topology.model_dump(), "fault_point": {}, "scan_variable": "", "scan_value": "", "group": "base"}
    if suite == "workload_sensitivity":
        return [{**base, "workload_point": point, "scan_variable": next(iter(point), "workload"), "scan_value": str(next(iter(point.values()), "")), "group": "workload"} for point in plan.workload_points] or [base]
    if suite == "topology_scaling":
        return [{**base, "topology_point": point, "scan_variable": "topology", "scan_value": json.dumps(point, sort_keys=True), "group": "topology"} for point in plan.topology_points] or [base]
    if suite == "fault_recovery_experiment":
        return [{**base, "fault_point": point, "scan_variable": "fault_policy", "scan_value": json.dumps(point, sort_keys=True, default=str), "group": "fault"} for point in plan.fault_points] or [base]
    return [base]


def start(group_id: str) -> None:
    process = multiprocessing.Process(target=_worker, args=(group_id,), daemon=False)
    process.start()
    group = read_group(group_id)
    group["worker_pid"] = process.pid
    group["status"] = "running"
    write_group(group)


def _worker(group_id: str) -> None:
    group = read_group(group_id)
    plan = V5FormalExperimentPlan.model_validate(group["plan"])
    backend = group["execution_backend"]
    rows = group.get("matrix") or expand(plan, backend)
    requested = set(group.pop("retry_requested_child_ids", []))
    if requested:
        rows = [row for row in rows if row["child_run_id"] in requested]
    rows, fairness = validate_fairness(rows)
    write_fairness_artifacts(group_dir(group_id), rows, fairness)
    group["total_child_runs"] = len(rows)
    write_group(group)
    for row in rows:
        group = read_group(group_id)
        if group.get("cancel_requested"):
            group["status"] = "cancelled"
            write_group(group)
            return
        child_id = row["child_run_id"]
        existing_attempt = next((item.get("attempt", 0) for item in children(group_id) if item.get("child_run_id") == child_id), 0)
        attempt_number = existing_attempt + 1
        child = {"child_run_id": child_id, "run_group_id": group_id, "status": "running", "attempt": attempt_number, **row}
        write_child(group_id, child)
        group["completed_child_runs"] = len([item for item in children(group_id) if item.get("status") == "completed"])
        write_group(group)
        write_attempt(group_id, child_id, {"attempt_number": attempt_number, "status": "running", "started_at": datetime.now(UTC).isoformat()})
        try:
            spec = _spec_for(plan, row)
            if backend == "preview":
                result = v5_real_cluster_runner.compile_only(spec)
                child.update({"status": "completed", "result": result, "paper_candidate": False})
            elif backend == "real_cluster":
                result = v5_real_cluster_runner.run(spec)
                metrics = extract_metrics(__import__("pathlib").Path(result["output_dir"]))
                child.update({"status": result["status"], "result": result, "metrics": metrics, "paper_candidate": result["status"] == "completed" and result["summary"].get("no_fallback") is True and not metrics.get("missing")})
            else:
                child.update({"status": "blocked", "error": "simulation dispatch is not yet bound to the V3 logical runtime adapter", "paper_candidate": False})
        except Exception as exc:  # preserve failure evidence for result center and retry policy
            child.update({"status": "failed", "error": str(exc), "paper_candidate": False})
        write_child(group_id, child)
        write_attempt(group_id, child_id, {"attempt_number": attempt_number, "status": child["status"], "finished_at": datetime.now(UTC).isoformat(), "result": child.get("result"), "metrics": child.get("metrics"), "error": child.get("error")})
    finalize(group_id)


def finalize(group_id: str) -> dict:
    group = read_group(group_id)
    items = children(group_id)
    statuses = [item["status"] for item in items]
    group["status"] = "completed" if statuses and all(status == "completed" for status in statuses) else "completed_with_failures"
    group["completed_child_runs"] = sum(status == "completed" for status in statuses)
    group["aggregate"] = export_paper(group_dir(group_id), group, items)
    group["bundle_path"] = str(build_bundle(group_dir(group_id), group))
    write_group(group)
    return group


def _spec_for(plan: V5FormalExperimentPlan, row: dict):
    spec = plan.base_spec.model_copy(deep=True)
    spec.seed = row["seed"]
    method = row["method"]
    overrides = method.get("plugin_overrides", {})
    spec.plugin_selections = [V5PluginSelection(category=item.category, plugin_id=overrides.get(item.category, item.plugin_id), config=item.config) for item in spec.plugin_selections]
    spec.saved_config_id = plan.saved_config_id or spec.saved_config_id
    return spec
