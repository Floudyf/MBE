from __future__ import annotations

import multiprocessing
import hashlib
import json
from datetime import UTC, datetime
from uuid import uuid4

from backend.app.models.v5_experiment_spec import V5PluginSelection, V5Topology
from backend.app.models.v5_formal_experiment import V5FormalExperimentPlan
from backend.app.services import v5_real_cluster_runner
from backend.app.services.v5_formal_run_store import children, group_dir, read_group, write_attempt, write_child, write_group
from backend.app.services.v5_fairness_validator import validate as validate_fairness, write_artifacts as write_fairness_artifacts
from backend.app.services.v5_metric_extractor import extract as extract_metrics
from backend.app.services.v5_compatibility_engine import _cross_shard_fault_unsupported
from backend.app.services.v5_paper_exporter import export as export_paper
from backend.app.services.v5_reproducibility_bundle import build as build_bundle

SUPPORTED_WORKLOAD_POINT_FIELDS = {"tx_count", "cross_shard_ratio", "timeout_every"}


def _workload_blockers(point: dict, topology: dict) -> list[str]:
    blockers = []
    unknown = sorted(set(point) - SUPPORTED_WORKLOAD_POINT_FIELDS)
    if unknown:
        blockers.append(f"unsupported workload point fields: {unknown}")
    if "tx_count" in point and (not isinstance(point["tx_count"], int) or isinstance(point["tx_count"], bool) or point["tx_count"] < 1):
        blockers.append("workload tx_count must be a positive integer")
    if "cross_shard_ratio" in point and (not isinstance(point["cross_shard_ratio"], (int, float)) or not 0 <= point["cross_shard_ratio"] <= 1):
        blockers.append("workload cross_shard_ratio must be between 0 and 1")
    if "timeout_every" in point and (not isinstance(point["timeout_every"], int) or isinstance(point["timeout_every"], bool) or point["timeout_every"] < 0):
        blockers.append("workload timeout_every must be a non-negative integer")
    ratio = point.get("cross_shard_ratio", 0)
    if ratio > 0 and topology.get("shards", 0) < 2:
        blockers.append("cross_shard_ratio requires at least 2 shards")
    return blockers


def _fault_blockers(fault: dict, workload: dict, backend: str) -> list[str]:
    if backend == "real_cluster" and float(workload.get("cross_shard_ratio", 0) or 0) > 0 and _cross_shard_fault_unsupported(fault):
        return ["cross-shard experiments with message loss or node restart are not supported because Relay/SourceFinalize reliable retransmission is not implemented"]
    return []


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
                    blockers = _workload_blockers(variant["workload_point"], variant["topology_point"])
                    base_workload = next((selection.config for selection in plan.base_spec.plugin_selections if selection.category == "workload"), {})
                    effective_workload = {**base_workload, **variant["workload_point"]}
                    blockers.extend(_fault_blockers(variant["fault_point"], effective_workload, backend))
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
                        "estimated_transactions": variant["workload_point"].get("tx_count", plan.base_spec.tx_count), "runnable": backend != "simulation" and not blockers, "blockers": blockers + (["V3 simulation adapter pending"] if backend == "simulation" else []), "warnings": [],
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
            if row.get("blockers"):
                child.update({"status": "blocked", "error": "; ".join(row["blockers"]), "paper_candidate": False})
                write_child(group_id, child)
                write_attempt(group_id, child_id, {"attempt_number": attempt_number, "status": "blocked", "finished_at": datetime.now(UTC).isoformat(), "error": child["error"]})
                continue
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

    topology_point = dict(row.get("topology_point") or {})
    allowed_topology = {"nodes", "shards", "validators_per_shard"}
    unsupported_topology = set(topology_point) - allowed_topology
    if unsupported_topology:
        raise ValueError(f"unsupported topology point fields: {sorted(unsupported_topology)}")
    if topology_point:
        spec.topology = V5Topology(**(spec.topology.model_dump() | topology_point))

    workload_point = dict(row.get("workload_point") or {})
    workload_blockers = _workload_blockers(workload_point, topology_point)
    if workload_blockers:
        raise ValueError("; ".join(workload_blockers))
    if "tx_count" in workload_point:
        spec.tx_count = int(workload_point.pop("tx_count"))
    if workload_point:
        spec.plugin_selections = [
            V5PluginSelection(
                category=item.category,
                plugin_id=item.plugin_id,
                config=(item.config | workload_point) if item.category == "workload" else item.config,
            )
            for item in spec.plugin_selections
        ]

    fault_point = row.get("fault_point") or {}
    if fault_point:
        spec.fault_policy = spec.fault_policy | dict(fault_point)

    spec.seed = row["seed"]
    method = row["method"]
    overrides = method.get("plugin_overrides", {})
    spec.plugin_selections = [V5PluginSelection(category=item.category, plugin_id=overrides.get(item.category, item.plugin_id), config=item.config) for item in spec.plugin_selections]
    spec.saved_config_id = plan.saved_config_id or spec.saved_config_id
    return spec
