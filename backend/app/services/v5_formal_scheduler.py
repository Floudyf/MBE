from __future__ import annotations

import hashlib
import json
import os
import threading
from datetime import UTC, datetime
from pathlib import Path
from uuid import uuid4

from backend.app.core.paths import ROOT
from backend.app.models.v5_experiment_spec import V5PluginSelection, V5Topology
from backend.app.models.v5_formal_experiment import V5FormalExperimentPlan
from backend.app.services import v5_real_cluster_runner
from backend.app.services.v5_formal_run_store import children, group_dir, read_group, write_attempt, write_child, write_group
from backend.app.services.v5_fairness_validator import validate as validate_fairness, write_artifacts as write_fairness_artifacts
from backend.app.services.v5_metric_extractor import extract as extract_metrics
from backend.app.services.v5_compatibility_engine import V5CompatibilityError, _cross_shard_fault_unsupported
from backend.app.services.v5_plugin_manifest_store import STORE
from backend.app.services.v5_paper_exporter import export as export_paper
from backend.app.services.v5_reproducibility_bundle import build as build_bundle
from backend.app.services.v5_workload_data_plane import load_manifest, supported_workload_counts

SUPPORTED_SYNTHETIC_WORKLOAD_POINT_FIELDS = {"tx_count", "cross_shard_ratio", "timeout_every"}
SUPPORTED_DATASET_WORKLOAD_POINT_FIELDS = {"tx_count", "target_alpha", "target_theta", "access_profile", "skew_axis"}


def _workload_blockers(point: dict, topology: dict, source_type: str = "synthetic", supported_counts: set[int] | None = None) -> list[str]:
    blockers = []
    supported = SUPPORTED_DATASET_WORKLOAD_POINT_FIELDS if source_type == "dataset" else SUPPORTED_SYNTHETIC_WORKLOAD_POINT_FIELDS
    unknown = sorted(set(point) - supported)
    if unknown:
        blockers.append(f"unsupported workload point fields: {unknown}")
    if "tx_count" in point and (not isinstance(point["tx_count"], int) or isinstance(point["tx_count"], bool) or point["tx_count"] < 1):
        blockers.append("workload tx_count must be a positive integer")
    elif source_type == "dataset" and "tx_count" in point:
        allowed_counts = set(supported_counts or supported_workload_counts())
        if point["tx_count"] not in allowed_counts:
            blockers.append(f"dataset tx_count must be one of {sorted(allowed_counts)}")
    if source_type == "dataset" and "target_alpha" in point and point["target_alpha"] not in {0.0, 0.2, 0.4, 0.6, 0.8, 1.0, 1.2, 1.4}:
        blockers.append("dataset target_alpha must be one of the supported key Zipf alpha values")
    if "cross_shard_ratio" in point and (not isinstance(point["cross_shard_ratio"], (int, float)) or not 0 <= point["cross_shard_ratio"] <= 1):
        blockers.append("workload cross_shard_ratio must be between 0 and 1")
    if "timeout_every" in point and (not isinstance(point["timeout_every"], int) or isinstance(point["timeout_every"], bool) or point["timeout_every"] < 0):
        blockers.append("workload timeout_every must be a non-negative integer")
    ratio = point.get("cross_shard_ratio", 0)
    if ratio > 0 and topology.get("shards", 0) < 2:
        blockers.append("cross_shard_ratio requires at least 2 shards")
    return blockers


def _dataset_supported_counts(workload_source) -> set[int]:
    counts = set(supported_workload_counts())
    if workload_source is None or getattr(workload_source, "source_type", "synthetic") != "dataset":
        return counts
    dataset_id = str(getattr(workload_source, "dataset_id", "") or "")
    if not dataset_id:
        return counts
    try:
        manifest = load_manifest(dataset_id)
    except Exception:
        return counts
    counts = {int(item) for item in manifest.get("supported_tx_counts") or counts}
    if manifest.get("allow_full_dataset", True) and int(manifest.get("row_count") or 0) > 0:
        counts.add(int(manifest["row_count"]))
    return counts


def _fault_blockers(fault: dict, workload: dict, backend: str) -> list[str]:
    if backend == "real_cluster" and float(workload.get("cross_shard_ratio", 0) or 0) > 0 and _cross_shard_fault_unsupported(fault):
        return ["cross-shard experiments with message loss or node restart are not supported because Relay/SourceFinalize reliable retransmission is not implemented"]
    return []


def expand(plan: V5FormalExperimentPlan, backend: str) -> list[dict]:
    methods = plan.methods or []
    rows = []
    base_workload_identity = _workload_identity(plan)
    for suite in plan.suites:
        variants = _variants(plan, suite)
        # Every selected method participates in every suite.  A suite validator owns
        # cardinality and fairness rules; expansion never silently drops methods.
        suite_methods = methods
        for method in suite_methods:
            for seed in plan.seeds:
                for repeat in range(plan.repeats):
                  for variant in variants:
                    item = method if isinstance(method, dict) else method.model_dump()
                    snapshot = {selection.category: STORE.get(item.get("plugin_overrides", {}).get(selection.category, selection.plugin_id)).plugin_id for selection in plan.base_spec.plugin_selections}
                    method_config_snapshot = dict(item.get("plugin_config_overrides", {}))
                    execution_semantics = _execution_semantics(snapshot)
                    workload_identity = {**base_workload_identity, "point": variant["workload_point"]}
                    block_settings = _block_producer_settings(plan, item)
                    estimated_transactions = variant["workload_point"].get("tx_count", plan.base_spec.tx_count)
                    estimated_block_count = (int(estimated_transactions) + block_settings["block_size"] - 1) // block_settings["block_size"]
                    fairness_snapshot = {"workload": variant["workload_point"], "workload_identity": workload_identity, "topology": variant["topology_point"], "fault": variant["fault_point"], "block_production": block_settings}
                    source_type = plan.base_spec.workload_source.source_type if plan.base_spec.workload_source else "synthetic"
                    blockers = _workload_blockers(variant["workload_point"], variant["topology_point"], source_type, _dataset_supported_counts(plan.base_spec.workload_source))
                    worker_count = variant["topology_point"].get("worker_count")
                    if worker_count is not None and (isinstance(worker_count, bool) or not isinstance(worker_count, int) or not 1 <= worker_count <= 8):
                        blockers.append("topology worker_count must be an integer between 1 and 8")
                    base_workload = next((selection.config for selection in plan.base_spec.plugin_selections if selection.category == "workload"), {})
                    effective_workload = {**base_workload, **variant["workload_point"]} if source_type != "dataset" else {}
                    blockers.extend(_fault_blockers(variant["fault_point"], effective_workload, backend))
                    rows.append({
                        "child_run_id": "v5child_" + hashlib.sha256(json.dumps({"suite": suite, "method": item["method_id"], "seed": seed, "repeat": repeat, "variant": variant, "workload_identity": workload_identity}, sort_keys=True, default=str).encode()).hexdigest()[:16],
                        "suite_type": suite, "method": item, "method_config_id": item["method_id"], "method_role": item.get("role", "custom"),
                        "changed_plugin_categories": _changed_categories(plan, item),
                        "method_snapshot_digest": hashlib.sha256(json.dumps(snapshot, sort_keys=True).encode()).hexdigest(),
                        "method_config_snapshot_digest": hashlib.sha256(json.dumps(method_config_snapshot, sort_keys=True, default=str).encode()).hexdigest(),
                        "workload_snapshot_digest": hashlib.sha256(json.dumps(variant["workload_point"], sort_keys=True).encode()).hexdigest(),
                        "topology_snapshot_digest": hashlib.sha256(json.dumps(variant["topology_point"], sort_keys=True).encode()).hexdigest(),
                        "fault_snapshot_digest": hashlib.sha256(json.dumps(variant["fault_point"], sort_keys=True, default=str).encode()).hexdigest(),
                        "workload_point": variant["workload_point"], "topology_point": variant["topology_point"], "fault_point": variant["fault_point"],
                        "seed": seed, "repeat_index": repeat, "scan_variable": variant["scan_variable"], "scan_value": variant["scan_value"],
                        "fairness_key": hashlib.sha256(json.dumps({"suite": suite, "seed": seed, "repeat": repeat, "snapshot": fairness_snapshot}, sort_keys=True, default=str).encode()).hexdigest(),
                        "comparison_group_id": f"{suite}:{seed}:{repeat}:{variant['group']}:{variant['scan_value']}", "execution_backend": backend,
                        **execution_semantics,
                        "estimated_processes": variant["topology_point"].get("nodes", plan.base_spec.topology.nodes) if backend == "real_cluster" else 0,
                        "estimated_transactions": estimated_transactions, "block_size": block_settings["block_size"], "block_interval_ms": block_settings["block_interval_ms"], "estimated_block_count": estimated_block_count, "runnable": backend != "simulation" and not blockers, "blockers": blockers + (["V3 simulation adapter pending"] if backend == "simulation" else []), "warnings": [],
                    })
    return validate_fairness(rows)[0]



def _execution_semantics(snapshot: dict[str, str]) -> dict[str, object]:
    if snapshot.get("block_executor") == "batch_si_block_executor":
        return {
            "comparison_semantics_class": "batch_si_common_batch_snapshot_v1",
            "state_access_semantics": "sequential_batches_common_batch_snapshot",
            "state_home_mapping_policy": "execution_shard_local_namespace",
            "remote_fetch_policy": "none",
            "remote_writeback_policy": "none",
            "proof_policy": "pre_consensus_batch_plan_digest",
            "legacy_cross_shard_protocol": False,
            "measurement_boundary": "client_submit_to_batch_si_terminal",
        }
    if snapshot.get("block_executor") == "groundhog_block_executor":
        return {
            "comparison_semantics_class": "groundhog_typed_commutative_snapshot_v1",
            "state_access_semantics": "block_start_snapshot_typed_commutative",
            "state_home_mapping_policy": "execution_shard_local_namespace",
            "remote_fetch_policy": "none",
            "remote_writeback_policy": "none",
            "proof_policy": "none",
            "legacy_cross_shard_protocol": False,
            "measurement_boundary": "client_submit_to_groundhog_terminal",
        }
    routing = snapshot.get("routing", "")
    routing_capabilities: set[str] = set()
    if routing:
        try:
            routing_capabilities = set(STORE.get(routing).capabilities or [])
        except ValueError:
            routing_capabilities = set()
    if "stateful_local_execution" in routing_capabilities:
        return {
            "comparison_semantics_class": "stateful_local_legacy_v1",
            "state_access_semantics": "stateful_local",
            "state_home_mapping_policy": "execution_shard_local_namespace",
            "remote_fetch_policy": "none",
            "remote_writeback_policy": "none",
            "proof_policy": "none",
            "legacy_cross_shard_protocol": True,
            "measurement_boundary": "client_submit_to_legacy_terminal",
        }
    if "stateless_direct_execution" in routing_capabilities:
        return {
            "comparison_semantics_class": "stateless_remote_home_v1",
            "state_access_semantics": "stateless_remote_home",
            "state_home_mapping_policy": "deterministic_state_key_sharding",
            "remote_fetch_policy": "home_leader_witness_fetch",
            "remote_writeback_policy": "home_shard_consensus_delta",
            "proof_policy": "state_root_witness_digest",
            "legacy_cross_shard_protocol": False,
            "measurement_boundary": "client_submit_to_stateless_direct_terminal",
        }
    return {
        "comparison_semantics_class": "custom_unknown",
        "state_access_semantics": "unknown",
        "state_home_mapping_policy": "unknown",
        "remote_fetch_policy": "unknown",
        "remote_writeback_policy": "unknown",
        "proof_policy": "unknown",
        "legacy_cross_shard_protocol": False,
        "measurement_boundary": "unknown",
    }

def _variants(plan: V5FormalExperimentPlan, suite: str) -> list[dict]:
    base_topology = plan.base_spec.topology.model_dump() | {"worker_count": plan.worker_count}
    base = {"workload_point": {}, "topology_point": base_topology, "fault_point": {}, "scan_variable": "", "scan_value": "", "group": "base"}
    if suite == "workload_sensitivity":
        return [{**base, "workload_point": point, "scan_variable": _scan_variable(point, "workload"), "scan_value": _scan_value(point), "group": "workload"} for point in plan.workload_points]
    if suite == "topology_scaling":
        return [{**base, "topology_point": ({**point, "worker_count": point.get("worker_count", plan.worker_count)}), "scan_variable": "topology", "scan_value": json.dumps(point, sort_keys=True), "group": "topology"} for point in plan.topology_points]
    if suite == "fault_recovery_experiment":
        return [{**base, "fault_point": point, "scan_variable": "fault_policy", "scan_value": json.dumps(point, sort_keys=True, default=str), "group": "fault"} for point in plan.fault_points]
    return [base]


def _workload_identity(plan: V5FormalExperimentPlan) -> dict:
    source = plan.base_spec.workload_source
    if not source:
        return {"source_type": "synthetic"}
    data = source.model_dump(mode="json")
    return {
        key: data.get(key)
        for key in (
            "source_type",
            "plugin_id",
            "dataset_id",
            "variant_id",
            "variant_mode",
            "materialized_id",
            "source_sha256",
            "requested_tx_count",
            "use_full_dataset",
            "seed",
            "selection_mode",
            "replay_mode",
            "skew_axis",
            "target_alpha",
            "variant_parameters",
        )
        if data.get(key) is not None
    }


def _scan_variable(point: dict, default: str) -> str:
    return "+".join(sorted(point)) or default


def _scan_value(point: dict) -> str:
    return json.dumps(point, sort_keys=True, default=str)


def _block_producer_settings(plan: V5FormalExperimentPlan, method: dict) -> dict[str, int]:
    base = next((selection for selection in plan.base_spec.plugin_selections if selection.category == "block_producer"), None)
    plugin_id = method.get("plugin_overrides", {}).get("block_producer", base.plugin_id if base else "time_or_count_block_producer")
    config = dict(STORE.get(plugin_id).default_config)
    if base:
        base_config = dict(STORE.get(base.plugin_id).default_config) | dict(base.config)
        if plugin_id == base.plugin_id:
            config |= base_config
        else:
            for key in ("block_size", "interval_ms", "block_interval_ms"):
                if key in base_config:
                    config[key] = base_config[key]
    config |= dict(method.get("plugin_config_overrides", {}).get("block_producer", {}))
    block_size = int(config.get("block_size") or 100)
    interval_ms = int(config.get("interval_ms") or config.get("block_interval_ms") or 75)
    return {"block_size": block_size, "block_interval_ms": interval_ms}


def _effective_method_snapshot(plan: V5FormalExperimentPlan, method: dict) -> dict[str, str]:
    snapshot: dict[str, str] = {}
    overrides = method.get("plugin_overrides", {})
    config_overrides = method.get("plugin_config_overrides", {})
    for selection in plan.base_spec.plugin_selections:
        if selection.category in {"workload", "fault_injection"}:
            continue
        plugin_id = overrides.get(selection.category, selection.plugin_id)
        manifest = STORE.get(plugin_id)
        config = dict(manifest.default_config)
        if plugin_id == selection.plugin_id:
            config |= dict(selection.config)
        config |= dict(config_overrides.get(selection.category, {}))
        snapshot[selection.category] = json.dumps(
            {"plugin_id": manifest.plugin_id, "config": config},
            sort_keys=True,
            separators=(",", ":"),
            default=str,
        )
    return snapshot


def _changed_categories(plan: V5FormalExperimentPlan, method: dict) -> list[str]:
    main = next((item.model_dump() if hasattr(item, "model_dump") else item for item in plan.methods if (item.role if hasattr(item, "role") else item.get("role")) == "main"), None)
    if not main:
        return []
    main_snapshot = _effective_method_snapshot(plan, main)
    method_snapshot = _effective_method_snapshot(plan, method)
    return sorted(category for category in sorted(set(main_snapshot) | set(method_snapshot)) if main_snapshot.get(category) != method_snapshot.get(category))


def start(group_id: str) -> None:
    group = read_group(group_id)
    worker_name = f"v5-formal-worker-{group_id}"
    group["status"] = "starting"
    group["worker_pid"] = os.getpid()
    group["worker_thread"] = worker_name
    write_group(group)
    worker = threading.Thread(target=_worker, args=(group_id,), name=worker_name, daemon=True)
    worker.start()


def worker_active(group_id: str) -> bool:
    worker_name = f"v5-formal-worker-{group_id}"
    return any(
        thread.name == worker_name and thread.is_alive()
        for thread in threading.enumerate()
    )


def _worker(group_id: str) -> None:
    try:
        _run_worker(group_id)
    except Exception as exc:
        group = read_group(group_id)
        if group.get("cancel_requested"):
            group["cancel_cleanup_error"] = str(exc)
            write_group(group)
            finalize_cancelled(group_id)
            return
        items = children(group_id)
        group["status"] = "failed"
        group["group_error"] = str(exc)
        group["finished_at"] = datetime.now(UTC).isoformat()
        _refresh_child_counts(group, items)
        try:
            group["aggregate"] = export_paper(group_dir(group_id), group, items)
        except Exception as export_exc:
            group["partial_export_error"] = str(export_exc)
        group["bundle_path"] = str(group_dir(group_id) / "artifacts.zip")
        write_group(group)
        _try_build_bundle(group_dir(group_id), group)


def _run_worker(group_id: str) -> None:
    group = read_group(group_id)
    if group.get("cancel_requested"):
        finalize_cancelled(group_id)
        return
    group["status"] = "running"
    write_group(group)
    plan = V5FormalExperimentPlan.model_validate(group["plan"])
    backend = group["execution_backend"]
    all_rows = group.get("matrix") or expand(plan, backend)
    checked_rows, fairness = validate_fairness(all_rows)
    requested = set(group.pop("retry_requested_child_ids", []))
    rows = [row for row in checked_rows if row["child_run_id"] in requested] if requested else checked_rows
    group["fairness_validation"] = fairness
    group["performance_comparison_valid"] = bool(fairness.get("performance_comparison_valid", False))
    write_fairness_artifacts(group_dir(group_id), checked_rows, fairness)
    if not group.get("total_child_runs"):
        group["total_child_runs"] = len(all_rows)
    if requested:
        group["retry_batch_size"] = len(rows)
        group["retry_attempt"] = int(group.get("retry_attempt", 0)) + 1
    write_group(group)
    for row in rows:
        group = read_group(group_id)
        if group.get("cancel_requested"):
            finalize_cancelled(group_id)
            return
        child_id = row["child_run_id"]
        existing_attempt = next((item.get("attempt", 0) for item in children(group_id) if item.get("child_run_id") == child_id), 0)
        attempt_number = existing_attempt + 1
        child = {
            **row,
            "child_run_id": child_id,
            "run_group_id": group_id,
            "status": "running",
            "attempt": attempt_number,
        }
        write_child(group_id, child)
        _refresh_child_counts(group, children(group_id))
        write_group(group)
        write_attempt(group_id, child_id, {"attempt_number": attempt_number, "status": "running", "started_at": datetime.now(UTC).isoformat()})
        stop_after_child = False
        try:
            if row.get("blockers"):
                child.update({"status": "blocked", "error": "; ".join(row["blockers"]), "paper_candidate": False})
                write_child(group_id, child)
                write_attempt(group_id, child_id, {"attempt_number": attempt_number, "status": "blocked", "finished_at": datetime.now(UTC).isoformat(), "error": child["error"]})
                continue
            spec = _spec_for(plan, row, formal_plan_config_id=group.get("plan_config_id"))
            if backend == "preview":
                result = v5_real_cluster_runner.compile_only(spec)
                child.update({"status": "completed", "result": result, "paper_candidate": False})
            elif backend == "real_cluster":
                result = v5_real_cluster_runner.run(
                    spec,
                    cancel_check=lambda: bool(read_group(group_id).get("cancel_requested")),
                )
                if result["status"] == "cancelled":
                    metrics = {}
                else:
                    result_dir = _physical_result_dir(result)
                    metrics = extract_metrics(result_dir, method_id=row.get("method_config_id"))
                child.update(
                    {
                        "status": result["status"],
                        "execution_status": result.get("summary", {}).get("execution_status", result["status"]),
                        "artifact_status": result.get("summary", {}).get("artifact_status", "pending"),
                        "formal_eligibility": bool(result.get("summary", {}).get("formal_eligibility", False)),
                        "execution_gate": result.get("summary", {}).get("execution_gate"),
                        "artifact_gate": result.get("summary", {}).get("artifact_gate"),
                        "completion_gate": result.get("summary", {}).get("completion_gate"),
                        "artifact_contract_version": result.get("summary", {}).get("artifact_contract_version"),
                        "missing_artifacts": result.get("summary", {}).get("missing_artifacts", []),
                        "unexpected_artifacts": result.get("summary", {}).get("unexpected_artifacts", []),
                        "artifact_contract": result.get("summary", {}).get("artifact_contract"),
                        "result": result,
                        "metrics": metrics,
                        "error": result.get("error") or (result.get("summary") or {}).get("root_failure") or "",
                        "paper_candidate": _is_paper_candidate_result(result, metrics),
                    }
                )
            else:
                child.update({"status": "blocked", "error": "simulation dispatch is not yet bound to the V3 logical runtime adapter", "paper_candidate": False})
        except V5CompatibilityError as exc:
            child.update({
                "status": "blocked",
                "execution_status": "blocked_incompatible_workload",
                "compatibility_code": exc.code,
                "compatibility_blockers": exc.blockers,
                "error": str(exc),
                "paper_candidate": False,
            })
        except v5_real_cluster_runner.V5ResourcePressureError as exc:
            child.update({
                "status": "failed",
                "execution_status": "blocked_resource_disk_pressure",
                "artifact_status": "incomplete",
                "formal_eligibility": False,
                "error": str(exc),
                "resource_pressure": exc.evidence,
                "result": {
                    "run_id": exc.run_id,
                    "status": "failed",
                    "output_dir": exc.output_dir,
                    "summary": {
                        "execution_status": "blocked_resource_disk_pressure",
                        "artifact_status": "incomplete",
                        "formal_eligibility": False,
                        "resource_pressure": exc.evidence,
                    },
                    "error": str(exc),
                    "no_fallback": True,
                },
                "paper_candidate": False,
            })
            group = read_group(group_id)
            group["resource_pressure_stop"] = exc.evidence
            group["resource_pressure_stop_child_id"] = child_id
            write_group(group)
            stop_after_child = True
        except Exception as exc:  # preserve failure evidence for result center and retry policy
            child.update({"status": "failed", "error": str(exc), "paper_candidate": False})
        write_child(group_id, child)
        write_attempt(group_id, child_id, {"attempt_number": attempt_number, "status": child["status"], "finished_at": datetime.now(UTC).isoformat(), "result": child.get("result"), "metrics": child.get("metrics"), "error": child.get("error")})
        if child.get("status") == "cancelled":
            finalize_cancelled(group_id)
            return
        if stop_after_child:
            break
    finalize(group_id)


def _try_build_bundle(directory: Path, group: dict) -> bool:
    # Persist the state that a successful ZIP should contain before creating it.
    # Remove a prior/partial ZIP first so catalog.bundle_ready can never mistake
    # a corrupt archive for a completed reproducibility bundle.
    output = directory / "artifacts.zip"
    try:
        output.unlink(missing_ok=True)
    except OSError:
        pass
    group["bundle_status"] = "ready"
    group.pop("bundle_error", None)
    group.pop("bundle_failed_at", None)
    write_group(group)
    try:
        build_bundle(directory, group)
    except Exception as exc:
        try:
            output.unlink(missing_ok=True)
        except OSError:
            pass
        group["bundle_status"] = "failed"
        group["bundle_error"] = str(exc)
        group["bundle_failed_at"] = datetime.now(UTC).isoformat()
        write_group(group)
        return False
    return True


def finalize_cancelled(group_id: str) -> dict:
    directory = group_dir(group_id)
    group = read_group(group_id)
    items = children(group_id)
    cancelled_at = datetime.now(UTC).isoformat()
    # A forced backend stop can leave the last persisted child marked running
    # even though its worker/process is gone. Close only non-terminal child
    # metadata here; completed/failed/blocked evidence stays untouched.
    for item in items:
        if item.get("status") in {"queued", "starting", "running", "cancelling"}:
            item["status"] = "cancelled"
            item["execution_status"] = "cancelled"
            item["paper_candidate"] = False
            item["cancelled_at"] = cancelled_at
            item["error"] = item.get("error") or "cancelled_by_user"
            write_child(group_id, item)
    group["cancel_requested"] = True
    group["status"] = "cancelled"
    group["finished_at"] = cancelled_at
    _refresh_child_counts(group, items)
    group["performance_comparison_valid"] = False
    group["paper_candidate"] = False
    group["bundle_path"] = str(directory / "artifacts.zip")
    write_group(group)
    _try_build_bundle(directory, group)
    return group


def finalize(group_id: str) -> dict:
    directory = group_dir(group_id)
    group = read_group(group_id)
    items = children(group_id)
    if group.get("cancel_requested"):
        return finalize_cancelled(group_id)

    statuses = [item["status"] for item in items]
    group["status"] = (
        "completed"
        if statuses and all(status == "completed" for status in statuses)
        else "completed_with_failures"
    )

    _refresh_child_counts(group, items)

    items, equivalence = _apply_state_equivalence_gate(items)
    group["state_equivalence_validation"] = equivalence
    group["pairwise_logical_state_equivalent"] = equivalence.get(
        "pairwise_logical_state_equivalent"
    )
    group["within_semantic_cohort_state_equivalence_valid"] = equivalence.get(
        "within_semantic_cohort_state_equivalence_valid"
    )
    group["performance_comparison_valid"] = bool(
        group.get("fairness_validation", {}).get(
            "performance_comparison_valid", False
        )
        and equivalence.get("performance_comparison_valid", False)
    )
    group["direct_cross_semantic_performance_comparison_valid"] = group[
        "performance_comparison_valid"
    ]
    _write_state_equivalence_artifacts(directory, equivalence)
    for item in items:
        write_child(group_id, item)

    # 先生成最终汇总文件。这些文件只依赖当前 group 和最终 child 记录。
    group["aggregate"] = export_paper(directory, group, items)

    # 在打包前写入最终状态，确保 ZIP 内的 run_group.json
    # 与 API 查询到的最终状态完全一致。
    group["finished_at"] = datetime.now(UTC).isoformat()
    group["bundle_path"] = str(directory / "artifacts.zip")
    write_group(group)

    # 此时目录中已经包含最终 run_group.json 和论文汇总文件。
    _try_build_bundle(directory, group)

    return group


def _state_equivalence_individual_reasons(item: dict) -> list[str]:
    reasons: list[str] = []
    if item.get("status") != "completed":
        return ["execution_status_not_completed"]
    metrics = item.get("metrics") if isinstance(item.get("metrics"), dict) else {}
    summary = ((item.get("result") or {}).get("summary") or {})
    finality = summary.get("finality_evidence") if isinstance(summary.get("finality_evidence"), dict) else {}

    def number(name: str):
        for source in (metrics, finality, summary):
            value = source.get(name) if isinstance(source, dict) else None
            if isinstance(value, bool):
                continue
            if isinstance(value, (int, float)):
                return int(value)
        return None

    def boolean(name: str):
        for source in (metrics, summary, finality):
            value = source.get(name) if isinstance(source, dict) else None
            if isinstance(value, bool):
                return value
        return None

    submitted = number("submitted_unique_tx_count")
    terminal = number("terminal_unique_tx_count")
    finalized = number("finalized_unique_logical_tx_count")
    incomplete = number("incomplete_unique_tx_count")
    cross_failed = number("cross_shard_failed_unique_count")
    if submitted is None or terminal != submitted:
        reasons.append("terminal_not_equal_submitted")
    if submitted is None or finalized != submitted:
        reasons.append("finalized_not_equal_submitted")
    if incomplete != 0:
        reasons.append("incomplete_not_zero")
    if cross_failed != 0:
        reasons.append("cross_shard_failed_not_zero")
    if boolean("lifecycle_complete") is not True:
        reasons.append("lifecycle_complete_not_true")
    return reasons


def _state_equivalence_family(item: dict) -> str:
    method_id = str(item.get("method_config_id") or "")
    if method_id.startswith("stateless_hash_"):
        return "stateless_hash_reference"
    if method_id.startswith("metatrack_"):
        return "metatrack"
    return "default"


def _build_state_equivalence_report(
    *,
    comparison_group_id: str,
    semantic_class: str,
    valid_completed: list[dict],
    invalid_completed: list[dict],
    required: list[str],
    required_child_count: int,
    equivalence_scope: str,
    method_family: str = "",
) -> dict:
    missing: dict[str, list[str]] = {}
    mismatches: dict[str, list[str]] = {}
    sufficient = len(valid_completed) >= required_child_count
    if sufficient:
        for field in required:
            missing_ids = [
                str(item.get("child_run_id") or "")
                for item in valid_completed
                if not str(item.get(field) or "")
            ]
            if missing_ids:
                missing[field] = missing_ids
                continue
            values = sorted({str(item.get(field)) for item in valid_completed})
            if len(values) != 1:
                mismatches[field] = values
    passed = sufficient and not missing and not mismatches
    status = "passed" if passed else "failed" if sufficient else "insufficient_valid_runs"
    return {
        "comparison_group_id": comparison_group_id,
        "comparison_semantics_class": semantic_class,
        "equivalence_scope": equivalence_scope,
        "method_family": method_family,
        "status": status,
        "completed_child_count": len(valid_completed) + len(invalid_completed),
        "valid_completed_child_count": len(valid_completed),
        "invalid_completed_child_count": len(invalid_completed),
        "required_child_count": required_child_count,
        "child_run_ids": [item.get("child_run_id") for item in valid_completed],
        "excluded_invalid_child_run_ids": [
            item.get("child_run_id") for item in invalid_completed
        ],
        "required_digests": required,
        "missing_digests": missing,
        "mismatched_digests": mismatches,
        "pairwise_logical_state_equivalent": passed if sufficient else None,
    }


def _build_stateless_reference_report(
    *,
    comparison_group_id: str,
    semantic_class: str,
    reference_report: dict | None,
    target_report: dict | None,
    reference_items: list[dict],
    target_items: list[dict],
    required: list[str],
) -> dict:
    missing: dict[str, list[str]] = {}
    mismatches: dict[str, list[str]] = {}
    sufficient = bool(reference_items and target_items)
    if sufficient and reference_report and target_report:
        sufficient = (
            reference_report.get("status") == "passed"
            and target_report.get("status") == "passed"
        )
    if sufficient:
        for field in required:
            reference_values = sorted(
                {str(item.get(field) or "") for item in reference_items}
            )
            target_values = sorted({str(item.get(field) or "") for item in target_items})
            if not reference_values or not reference_values[0]:
                missing[f"reference_{field}"] = [
                    str(item.get("child_run_id") or "") for item in reference_items
                ]
                continue
            if not target_values or not target_values[0]:
                missing[f"target_{field}"] = [
                    str(item.get("child_run_id") or "") for item in target_items
                ]
                continue
            if reference_values != target_values:
                mismatches[field] = sorted(set(reference_values + target_values))
    passed = sufficient and not missing and not mismatches
    status = "passed" if passed else "failed" if sufficient else "insufficient_valid_runs"
    return {
        "comparison_group_id": comparison_group_id,
        "comparison_semantics_class": semantic_class,
        "equivalence_scope": "reference_equivalence",
        "method_family": "metatrack_vs_stateless_hash_reference",
        "status": status,
        "completed_child_count": len(reference_items) + len(target_items),
        "valid_completed_child_count": len(reference_items) + len(target_items),
        "invalid_completed_child_count": 0,
        "required_child_count": 2,
        "child_run_ids": [
            item.get("child_run_id") for item in reference_items + target_items
        ],
        "excluded_invalid_child_run_ids": [],
        "required_digests": required,
        "missing_digests": missing,
        "mismatched_digests": mismatches,
        "pairwise_logical_state_equivalent": passed if sufficient else None,
    }


def _apply_state_equivalence_gate(items: list[dict]) -> tuple[list[dict], dict]:
    cohorts: dict[tuple[str, str], list[dict]] = {}
    enriched: list[dict] = []
    for item in items:
        summary = ((item.get("result") or {}).get("summary") or {})
        validity_reasons = _state_equivalence_individual_reasons(item)
        next_item = {
            **item,
            "initial_state_digest": summary.get("initial_state_digest", ""),
            "state_home_mapping_digest": summary.get("state_home_mapping_digest", ""),
            "global_final_state_digest": summary.get("global_final_state_digest", ""),
            "individual_result_valid": not validity_reasons,
            "individual_result_validity_reasons": validity_reasons,
        }
        enriched.append(next_item)
        key = (
            str(next_item.get("comparison_group_id") or ""),
            str(next_item.get("comparison_semantics_class") or "custom_unknown"),
        )
        cohorts.setdefault(key, []).append(next_item)

    reports: list[dict] = []
    report_by_child: dict[str, dict] = {}
    reference_report_by_child: dict[str, dict] = {}

    for (comparison_group_id, semantic_class), cohort in sorted(cohorts.items()):
        completed = [item for item in cohort if item.get("status") == "completed"]
        valid_completed = [
            item for item in completed if item.get("individual_result_valid") is True
        ]
        invalid_completed = [
            item for item in completed if item.get("individual_result_valid") is not True
        ]
        required = ["initial_state_digest", "global_final_state_digest"]
        if semantic_class == "stateless_remote_home_v1":
            required.append("state_home_mapping_digest")

        if semantic_class != "stateless_remote_home_v1":
            report = _build_state_equivalence_report(
                comparison_group_id=comparison_group_id,
                semantic_class=semantic_class,
                valid_completed=valid_completed,
                invalid_completed=invalid_completed,
                required=required,
                required_child_count=2,
                equivalence_scope="semantic_cohort",
            )
            reports.append(report)
            for item in cohort:
                report_by_child[str(item.get("child_run_id") or "")] = report
            continue

        families: dict[str, list[dict]] = {}
        invalid_families: dict[str, list[dict]] = {}
        for item in valid_completed:
            families.setdefault(_state_equivalence_family(item), []).append(item)
        for item in invalid_completed:
            invalid_families.setdefault(_state_equivalence_family(item), []).append(item)

        family_reports: dict[str, dict] = {}
        for family in sorted(set(families) | set(invalid_families)):
            report = _build_state_equivalence_report(
                comparison_group_id=comparison_group_id,
                semantic_class=semantic_class,
                valid_completed=families.get(family, []),
                invalid_completed=invalid_families.get(family, []),
                required=required,
                # One valid implementation is enough to establish its own
                # digest; multiple backends, when present, must agree.
                required_child_count=1,
                equivalence_scope="method_family",
                method_family=family,
            )
            family_reports[family] = report
            reports.append(report)
            for item in families.get(family, []) + invalid_families.get(family, []):
                report_by_child[str(item.get("child_run_id") or "")] = report

        reference_items = families.get("stateless_hash_reference", [])
        target_items = families.get("metatrack", [])
        if reference_items or target_items:
            reference_report = _build_stateless_reference_report(
                comparison_group_id=comparison_group_id,
                semantic_class=semantic_class,
                reference_report=family_reports.get("stateless_hash_reference"),
                target_report=family_reports.get("metatrack"),
                reference_items=reference_items,
                target_items=target_items,
                required=required,
            )
            reports.append(reference_report)
            for item in target_items:
                reference_report_by_child[str(item.get("child_run_id") or "")] = (
                    reference_report
                )

    comparable_reports = [
        report
        for report in reports
        if report.get("status") in {"passed", "failed"}
    ]

    for item in enriched:
        child_id = str(item.get("child_run_id") or "")
        report = report_by_child.get(child_id, {})
        reference_report = reference_report_by_child.get(child_id)
        topology = item.get("topology_point") if isinstance(item.get("topology_point"), dict) else {}
        method_id = str(item.get("method_config_id") or "")
        metatrack_multishard_prefetch_barrier = (
            method_id == "metatrack_block_stm"
            and int(topology.get("shards") or 1) > 1
        )
        if item.get("individual_result_valid") is not True:
            equivalent = None
            item["comparison_eligibility_status"] = "individual_result_invalid"
            item["paper_candidate"] = False
        else:
            family_equivalent = report.get("pairwise_logical_state_equivalent")
            reference_equivalent = (
                reference_report.get("pairwise_logical_state_equivalent")
                if reference_report
                else None
            )
            item["reference_state_equivalent"] = reference_equivalent
            equivalent = family_equivalent
            if reference_equivalent is False:
                equivalent = False
            item["comparison_eligibility_status"] = (
                reference_report.get("status")
                if reference_report and reference_report.get("status") == "failed"
                else report.get("status", "unknown")
            )
            if item.get("status") == "completed" and equivalent is not True:
                item["paper_candidate"] = False
            if metatrack_multishard_prefetch_barrier:
                # The MetaTrack+Block-STM hybrid still uses the block-level
                # remote-state prefetch barrier. Native MetaTrack now executes
                # transaction-level StateReady suspend/resume and is eligible
                # when the usual within-semantic correctness gates pass.
                item["comparison_eligibility_status"] = (
                    "metatrack_block_stm_multi_shard_prefetch_barrier_hybrid_boundary"
                )
                item["paper_candidate"] = False
        item["pairwise_logical_state_equivalent"] = equivalent

    performance_valid = bool(comparable_reports) and all(
        report.get("pairwise_logical_state_equivalent") is True
        for report in comparable_reports
    )
    return enriched, {
        "passed": performance_valid,
        # Backward-compatible field retained for existing consumers.  This
        # validation is scoped to within-semantic-cohort state equivalence; it
        # does not by itself authorize direct cross-semantic TPS uplift claims.
        "performance_comparison_valid": performance_valid,
        "within_semantic_cohort_state_equivalence_valid": performance_valid,
        "pairwise_logical_state_equivalent": (
            True
            if comparable_reports
            and all(
                report.get("pairwise_logical_state_equivalent") is True
                for report in comparable_reports
            )
            else False if comparable_reports else None
        ),
        "cohort_count": len(reports),
        "comparable_cohort_count": len(comparable_reports),
        "cohorts": reports,
    }



def _write_state_equivalence_artifacts(root: Path, result: dict) -> None:
    root.mkdir(parents=True, exist_ok=True)
    (root / "state_equivalence_validation.json").write_text(
        json.dumps(result, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    fields = [
        "comparison_group_id",
        "comparison_semantics_class",
        "equivalence_scope",
        "method_family",
        "status",
        "completed_child_count",
        "required_child_count",
        "child_run_ids",
        "required_digests",
        "missing_digests",
        "mismatched_digests",
        "pairwise_logical_state_equivalent",
    ]
    import csv

    with (root / "state_equivalence_validation.csv").open(
        "w", newline="", encoding="utf-8"
    ) as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        for row in result.get("cohorts", []):
            writer.writerow(
                {
                    key: json.dumps(row.get(key), sort_keys=True)
                    if isinstance(row.get(key), (list, dict))
                    else row.get(key, "")
                    for key in fields
                }
            )


def _physical_result_dir(result: dict) -> Path:
    """Resolve the real local run directory without exposing it through the API.

    ``v5_real_cluster_runner`` intentionally returns a logical output path when
    runs live outside the repository.  Metric extraction must use the physical
    run root, so the authoritative run id is resolved through the runner.
    Absolute paths remain supported for test doubles and legacy records.
    """
    run_id = str(result.get("run_id") or "")
    if run_id:
        return v5_real_cluster_runner.run_dir(run_id)

    output_dir = Path(str(result.get("output_dir") or ""))
    if output_dir.is_absolute():
        return output_dir
    return ROOT / output_dir


def _is_paper_candidate_result(result: dict, metrics: dict) -> bool:
    summary = result.get("summary") or {}
    return (
        result.get("status") == "completed"
        and summary.get("ready_to_commit") is True
        and summary.get("no_fallback") is True
        and metrics.get("metric_completeness") == "complete"
        and not metrics.get("missing")
    )


def _refresh_child_counts(group: dict, items: list[dict]) -> None:
    group["total_child_runs"] = group.get("total_child_runs") or len({item.get("child_run_id") for item in items})
    group["completed_child_runs"] = sum(item.get("status") == "completed" for item in items)
    group["failed_child_runs"] = sum(item.get("status") == "failed" for item in items)
    group["blocked_child_runs"] = sum(item.get("status") == "blocked" for item in items)
    group["cancelled_child_runs"] = sum(item.get("status") == "cancelled" for item in items)


def _spec_for(plan: V5FormalExperimentPlan, row: dict, *, formal_plan_config_id: str | None = None):
    spec = plan.base_spec.model_copy(deep=True)

    topology_point = dict(row.get("topology_point") or {})
    allowed_topology = {"nodes", "shards", "validators_per_shard", "worker_count"}
    unsupported_topology = set(topology_point) - allowed_topology
    if unsupported_topology:
        raise ValueError(f"unsupported topology point fields: {sorted(unsupported_topology)}")
    worker_count_override = topology_point.pop("worker_count", None)
    if worker_count_override is not None and (isinstance(worker_count_override, bool) or not isinstance(worker_count_override, int) or not 1 <= worker_count_override <= 8):
        raise ValueError("topology worker_count must be an integer between 1 and 8")
    if topology_point:
        spec.topology = V5Topology(**(spec.topology.model_dump() | topology_point))

    workload_point = dict(row.get("workload_point") or {})
    source_type = spec.workload_source.source_type if spec.workload_source else "synthetic"
    workload_blockers = _workload_blockers(workload_point, topology_point, source_type, _dataset_supported_counts(spec.workload_source))
    if workload_blockers:
        raise ValueError("; ".join(workload_blockers))
    if "tx_count" in workload_point:
        spec.tx_count = int(workload_point.pop("tx_count"))
        if spec.workload_source:
            spec.workload_source.requested_tx_count = spec.tx_count
    if source_type == "dataset" and spec.workload_source:
        if "target_alpha" in workload_point:
            if spec.workload_source.variant_mode == "original_window":
                spec.workload_source.variant_mode = "key_zipf"
            value = float(workload_point.pop("target_alpha"))
            spec.workload_source.target_alpha = value
            spec.workload_source.variant_parameters["target_alpha"] = value
        if "skew_axis" in workload_point:
            value = str(workload_point.pop("skew_axis"))
            spec.workload_source.skew_axis = value
            spec.workload_source.variant_parameters["skew_axis"] = value
        for key in ("target_theta", "access_profile"):
            if key in workload_point:
                spec.workload_source.variant_parameters[key] = workload_point.pop(key)
    if workload_point and source_type != "dataset":
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
    if spec.workload_source:
        spec.workload_source.seed = spec.seed
    method = row["method"]
    overrides = method.get("plugin_overrides", {})
    config_overrides = method.get("plugin_config_overrides", {})
    next_selections: list[V5PluginSelection] = []
    for item in spec.plugin_selections:
        plugin_id = overrides.get(item.category, item.plugin_id)
        if plugin_id != item.plugin_id:
            config = dict(STORE.get(plugin_id).default_config)
            if item.category == "block_producer":
                base_config = dict(STORE.get(item.plugin_id).default_config) | dict(item.config)
                for key in ("block_size", "interval_ms", "block_interval_ms"):
                    if key in base_config:
                        config[key] = base_config[key]
        else:
            config = dict(item.config)
        if item.category == "block_executor" and item.category not in overrides and method.get("method_id") != "v5_catalog_default":
            config["migrated_default"] = True
        config |= dict(config_overrides.get(item.category, {}))
        if item.category == "block_executor" and worker_count_override is not None:
            config["worker_count"] = 1 if plugin_id == "serial_block_executor" else worker_count_override
        next_selections.append(V5PluginSelection(category=item.category, plugin_id=plugin_id, config=config))
    spec.plugin_selections = next_selections
    spec.saved_config_id = plan.saved_config_id or spec.saved_config_id
    spec.formal_plan_config_id = formal_plan_config_id or spec.formal_plan_config_id
    spec.method_config_id = row.get("method_config_id")
    return spec
