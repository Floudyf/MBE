from __future__ import annotations

import hashlib
import json
import os
import threading
import time
import zipfile
from datetime import UTC, datetime
from pathlib import Path
from uuid import uuid4

from backend.app.core.paths import ROOT
from backend.app.models.v5_experiment_spec import V5PluginSelection, V5Topology
from backend.app.models.v5_formal_experiment import V5FormalExperimentPlan
from backend.app.services import v5_formal_artifact_storage, v5_real_cluster_runner
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

DEFAULT_FORMAL_EXECUTION_POLICY = {
    "schema_version": "mbe_v5_formal_execution_policy_v4",
    "child_wall_timeout_seconds": 1800,
    "worker_heartbeat_seconds": 5,
    "stale_worker_timeout_seconds": 30,
    "resource_recovery_wait_seconds": 30,
    "fixed_workload_completion_required_for_formal_tps": True,
    "partial_timeout_metrics_diagnostic_only": True,
    "auto_cold_archive_terminal_children": True,
    "auto_cold_archive_delete_raw": True,
    "auto_cold_archive_compression_level": 10,
}
_ACTIVE_CHILD_STATES = {"queued", "starting", "running", "cancelling"}
_RESUMABLE_CHILD_STATES = {"cancelled", "interrupted"}


def _execution_policy(group: dict) -> dict:
    raw = group.get("execution_policy") if isinstance(group.get("execution_policy"), dict) else {}
    return {**DEFAULT_FORMAL_EXECUTION_POLICY, **raw, "schema_version": DEFAULT_FORMAL_EXECUTION_POLICY["schema_version"]}


def _worker_heartbeat_path(group_id: str) -> Path:
    return group_dir(group_id) / "worker_heartbeat.json"


def _read_worker_heartbeat_sidecar(group_id: str) -> dict:
    # Sidecar telemetry is optional. Legacy tests and internal callers may use
    # synthetic group IDs that intentionally bypass the public ID contract.
    # In that case, preserve the historical reconcile behavior by treating the
    # sidecar as absent instead of raising before stale-group logic runs.
    try:
        path = _worker_heartbeat_path(group_id)
    except ValueError:
        return {}
    if not path.is_file():
        return {}
    for attempt in range(3):
        try:
            return json.loads(path.read_text(encoding="utf-8"))
        except PermissionError:
            if attempt == 2:
                return {}
            time.sleep(0.002)
        except (OSError, json.JSONDecodeError):
            return {}
    return {}


def _overlay_worker_heartbeat(group_id: str, group: dict) -> dict:
    heartbeat = _read_worker_heartbeat_sidecar(group_id)
    if not heartbeat:
        return group
    observed = dict(group)
    if heartbeat.get("worker_heartbeat_at"):
        observed["worker_heartbeat_at"] = heartbeat.get("worker_heartbeat_at")
    active_child = str(heartbeat.get("active_child_run_id") or "")
    active_started = str(heartbeat.get("active_child_started_at") or "")
    if active_child:
        observed["active_child_run_id"] = active_child
        if active_started:
            observed["active_child_started_at"] = active_started
    else:
        observed.pop("active_child_run_id", None)
        observed.pop("active_child_started_at", None)
    return observed


def _write_worker_heartbeat(group_id: str, *, child_id: str = "", child_started_at: str = "") -> None:
    directory = group_dir(group_id)
    directory.mkdir(parents=True, exist_ok=True)
    path = _worker_heartbeat_path(group_id)
    temp_path = path.with_name(f".{path.name}.{uuid4().hex}.tmp")
    payload = {
        "schema_version": "mbe_v5_formal_worker_heartbeat_v1",
        "run_group_id": group_id,
        "worker_heartbeat_at": datetime.now(UTC).isoformat(),
        "worker_pid": os.getpid(),
        "active_child_run_id": child_id or None,
        "active_child_started_at": child_started_at or None,
    }
    try:
        with temp_path.open("w", encoding="utf-8") as handle:
            json.dump(payload, handle, indent=2)
            handle.write("\n")
            handle.flush()
            try:
                os.fsync(handle.fileno())
            except OSError:
                pass
        for attempt in range(20):
            try:
                os.replace(temp_path, path)
                return
            except PermissionError:
                if attempt == 19:
                    raise
                time.sleep(0.005)
    finally:
        try:
            temp_path.unlink()
        except FileNotFoundError:
            pass


def _heartbeat_age_seconds(group: dict) -> float:
    raw = str(group.get("worker_heartbeat_at") or "")
    if not raw:
        return float("inf")
    try:
        stamp = datetime.fromisoformat(raw.replace("Z", "+00:00"))
        if stamp.tzinfo is None:
            stamp = stamp.replace(tzinfo=UTC)
        return max(0.0, (datetime.now(UTC) - stamp).total_seconds())
    except ValueError:
        return float("inf")


def reconcile_stale_group(group_id: str) -> dict:
    group = _overlay_worker_heartbeat(group_id, read_group(group_id))
    if group.get("status") not in {"starting", "running", "cancelling"}:
        return group
    if worker_active(group_id):
        return group
    policy = _execution_policy(group)
    if _heartbeat_age_seconds(group) < float(policy["stale_worker_timeout_seconds"]):
        return group
    if group.get("status") == "cancelling":
        return finalize_cancelled(group_id)
    interrupted_at = datetime.now(UTC).isoformat()
    stale_reap = v5_real_cluster_runner.reap_persisted_supervisors(group_id)
    items = children(group_id)
    for item in items:
        if item.get("status") in _ACTIVE_CHILD_STATES:
            item["status"] = "interrupted"
            item["execution_status"] = "interrupted"
            item["formal_eligibility"] = False
            item["paper_candidate"] = False
            item["interrupted_at"] = interrupted_at
            item["error"] = item.get("error") or "formal_worker_disappeared"
            write_child(group_id, item)
    group["status"] = "interrupted"
    group["interrupted_at"] = interrupted_at
    group["interrupted_reason"] = "stale_worker_heartbeat"
    group["stale_supervisor_reap"] = stale_reap
    group["paper_candidate"] = False
    group.pop("active_child_run_id", None)
    group.pop("active_child_started_at", None)
    group["worker_terminalized_at"] = interrupted_at
    _refresh_child_counts(group, items)
    write_group(group)
    return group


def _resume_candidate_mode(item: dict | None) -> tuple[str, str] | None:
    if item is None:
        return ("resume_unfinished", "not_started")
    status = str(item.get("status") or "")
    if status in _RESUMABLE_CHILD_STATES:
        return ("resume_unfinished", status)
    if status in {"failed", "timed_out"}:
        return ("retry_failed", status)
    return None


def list_resume_candidates(group_id: str) -> dict:
    group = reconcile_stale_group(group_id)
    matrix = list(group.get("matrix") or [])
    if matrix:
        raw_plan = group.get("plan")
        if isinstance(raw_plan, dict):
            plan = V5FormalExperimentPlan.model_validate(raw_plan)
            ordered = _ordered_execution_rows(matrix, plan)
        else:
            # Backward-compatible recovery for historical/minimal RunGroup metadata.
            # Candidate classification only requires the persisted matrix + child states.
            ordered = matrix
    else:
        raw_plan = group.get("plan")
        if not isinstance(raw_plan, dict):
            raise ValueError("formal RunGroup has neither a persisted matrix nor a recoverable plan")
        plan = V5FormalExperimentPlan.model_validate(raw_plan)
        matrix = list(expand(plan, group.get("execution_backend", "real_cluster")))
        ordered = _ordered_execution_rows(matrix, plan)
    existing = {str(item.get("child_run_id") or ""): item for item in children(group_id)}
    output: list[dict] = []
    for row in ordered:
        child_id = str(row.get("child_run_id") or "")
        if not child_id:
            continue
        item = existing.get(child_id)
        classification = _resume_candidate_mode(item)
        if classification is None:
            continue
        mode, status_value = classification
        workload_point = row.get("workload_point") if isinstance(row.get("workload_point"), dict) else {}
        method = row.get("method") if isinstance(row.get("method"), dict) else {}
        output.append({
            "child_run_id": child_id,
            "mode": mode,
            "status": status_value,
            "execution_status": (item or {}).get("execution_status"),
            "attempt": int((item or {}).get("attempt") or 0),
            "method_config_id": row.get("method_config_id"),
            "method_name": method.get("display_name") or row.get("method_config_id"),
            "seed": row.get("seed"),
            "repeat_index": int(row.get("repeat_index") or 0),
            "scan_variable": row.get("scan_variable"),
            "scan_value": row.get("scan_value"),
            "target_theta": workload_point.get("target_theta"),
            "target_alpha": workload_point.get("target_alpha"),
            "estimated_transactions": int(row.get("estimated_transactions") or workload_point.get("tx_count") or 0),
            "estimated_processes": int(row.get("estimated_processes") or 0),
            "error": (item or {}).get("error") or "",
        })
    resume_count = sum(item["mode"] == "resume_unfinished" for item in output)
    retry_count = sum(item["mode"] == "retry_failed" for item in output)
    return {
        "schema_version": "mbe_v5_formal_resume_candidates_v1",
        "run_group_id": group_id,
        "group_status": group.get("status"),
        "worker_active": worker_active(group_id),
        "selection_allowed": not worker_active(group_id),
        "resume_unfinished_count": resume_count,
        "retry_failed_count": retry_count,
        "candidate_count": len(output),
        "candidates": output,
    }


def _start_selected_children(group_id: str, requested: list[str], *, selection_mode: str, candidate_rows: list[dict]) -> dict:
    group = reconcile_stale_group(group_id)
    if worker_active(group_id):
        raise ValueError("formal RunGroup worker is still active")
    if not requested:
        raise ValueError("no child experiments were selected")
    now = datetime.now(UTC).isoformat()
    selected_set = set(requested)
    selected_rows = [item for item in candidate_rows if item.get("child_run_id") in selected_set]
    selection = {
        "schema_version": "mbe_v5_formal_resume_selection_v1",
        "mode": selection_mode,
        "selected_at": now,
        "child_run_ids": list(requested),
        "child_count": len(requested),
        "estimated_transactions": sum(int(item.get("estimated_transactions") or 0) for item in selected_rows),
        "estimated_process_starts": sum(int(item.get("estimated_processes") or 0) for item in selected_rows),
    }
    history = list(group.get("resume_selection_history") or [])
    history.append(selection)
    group["resume_selection_history"] = history[-20:]
    group["resume_selection"] = selection
    group["resume_requested_child_ids"] = list(requested)
    group["resume_requested_count"] = len(requested)
    group["resume_attempt"] = int(group.get("resume_attempt", 0)) + 1
    group["resumed_at"] = now
    group["status"] = "queued"
    group["cancel_requested"] = False
    group["paper_candidate"] = False
    group["performance_comparison_valid"] = False
    group["bundle_status"] = "pending_after_resume"
    group.pop("finished_at", None)
    group.pop("group_error", None)
    group.pop("aggregate", None)
    group.pop("state_equivalence_validation", None)
    group.pop("pairwise_logical_state_equivalent", None)
    # Resume invalidates a terminal bundle because it captured the pre-resume
    # group state.  Bundle invalidation is best-effort: historical tests and
    # migrated metadata may carry a non-canonical group id, which must never
    # prevent the persisted resume request from starting.
    try:
        (group_dir(group_id) / "artifacts.zip").unlink(missing_ok=True)
    except (OSError, ValueError):
        pass
    write_group(group)
    start(group_id)
    return read_group(group_id)


def resume_selected(group_id: str, child_run_ids: list[str], *, mode: str) -> dict:
    if mode not in {"resume_unfinished", "retry_failed"}:
        raise ValueError("resume selection mode must be resume_unfinished or retry_failed")
    payload = list_resume_candidates(group_id)
    eligible = [item for item in payload["candidates"] if item.get("mode") == mode]
    eligible_ids = {str(item.get("child_run_id") or "") for item in eligible}
    requested_set = {str(item) for item in child_run_ids if str(item)}
    invalid = sorted(requested_set - eligible_ids)
    if invalid:
        raise ValueError(f"selected child experiments are not eligible for {mode}: {invalid}")
    requested = [str(item.get("child_run_id")) for item in eligible if str(item.get("child_run_id")) in requested_set]
    return _start_selected_children(group_id, requested, selection_mode=mode, candidate_rows=eligible)


def resume_unfinished(group_id: str, *, include_failed: bool = False, include_timed_out: bool = False) -> dict:
    payload = list_resume_candidates(group_id)
    requested_rows = [item for item in payload["candidates"] if item.get("mode") == "resume_unfinished"]
    if include_failed:
        requested_rows.extend(item for item in payload["candidates"] if item.get("status") == "failed")
    if include_timed_out:
        requested_rows.extend(item for item in payload["candidates"] if item.get("status") == "timed_out")
    requested = [str(item.get("child_run_id")) for item in requested_rows]
    if not requested:
        group = reconcile_stale_group(group_id)
        group["resume_requested_count"] = 0
        write_group(group)
        return group
    return _start_selected_children(group_id, requested, selection_mode="legacy_resume_unfinished", candidate_rows=requested_rows)


def _is_system_resource_error(exc: BaseException) -> bool:
    winerror = getattr(exc, "winerror", None)
    return winerror in {8, 14, 1450} or any(
        token in str(exc).lower()
        for token in ("winerror 1450", "not enough storage", "insufficient system resources")
    )


def _ordered_execution_rows(rows: list[dict], plan: V5FormalExperimentPlan) -> list[dict]:
    if list(plan.suites) != ["workload_sensitivity"]:
        return list(rows)
    method_order = {
        (item.get("method_id") if isinstance(item, dict) else item.method_id): index
        for index, item in enumerate(plan.methods or [])
    }
    def scan_value(row: dict):
        point = row.get("workload_point") or {}
        for key in ("target_theta", "target_alpha", "tx_count"):
            value = point.get(key)
            if isinstance(value, (int, float)) and not isinstance(value, bool):
                return (key, float(value))
        return (str(row.get("scan_variable") or ""), str(row.get("scan_value") or ""))
    return sorted(
        rows,
        key=lambda row: (
            scan_value(row),
            int(row.get("repeat_index") or 0),
            method_order.get(str(row.get("method_config_id") or ""), 10_000),
        ),
    )


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
                    execution_semantics = _execution_semantics(snapshot, str(item.get("method_id") or ""))
                    workload_identity = {**base_workload_identity, "point": variant["workload_point"]}
                    block_settings = _block_producer_settings(plan, item)
                    estimated_transactions = variant["workload_point"].get("tx_count", plan.base_spec.tx_count)
                    estimated_block_count = (int(estimated_transactions) + block_settings["block_size"] - 1) // block_settings["block_size"]
                    fairness_snapshot = {"workload": variant["workload_point"], "workload_identity": workload_identity, "topology": variant["topology_point"], "fault": variant["fault_point"], "block_production": block_settings}
                    source_type = plan.base_spec.workload_source.source_type if plan.base_spec.workload_source else "synthetic"
                    blockers = _workload_blockers(variant["workload_point"], variant["topology_point"], source_type, _dataset_supported_counts(plan.base_spec.workload_source))
                    worker_count = variant["topology_point"].get("worker_count")
                    if worker_count is not None and (isinstance(worker_count, bool) or not isinstance(worker_count, int) or not 1 <= worker_count <= 32):
                        blockers.append("topology worker_count must be an integer between 1 and 32")
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
    return validate_fairness(_ordered_execution_rows(rows, plan))[0]



def _execution_semantics(snapshot: dict[str, str], method_id: str = "") -> dict[str, object]:
    if method_id == "hash_cg" or snapshot.get("block_executor") == "cg_block_executor":
        return {
            "comparison_semantics_class": "nezha_cg_johnson_abortable_v2",
            "state_access_semantics": "stateful_local_nezha_cg_johnson_abortable",
            "state_home_mapping_policy": "execution_shard_local_namespace",
            "remote_fetch_policy": "none",
            "remote_writeback_policy": "none",
            "proof_policy": "consensus_bound_nezha_cg_johnson_plan_digest",
            "legacy_cross_shard_protocol": True,
            "measurement_boundary": "client_submit_to_cg_terminal",
        }
    if method_id == "hash_acg" or snapshot.get("block_executor") == "acg_block_executor":
        return {
            "comparison_semantics_class": "nezha_acg_hs_abortable_v1",
            "state_access_semantics": "stateful_local_nezha_hs_abortable",
            "state_home_mapping_policy": "execution_shard_local_namespace",
            "remote_fetch_policy": "none",
            "remote_writeback_policy": "none",
            "proof_policy": "consensus_bound_nezha_hs_plan_digest",
            "legacy_cross_shard_protocol": True,
            "measurement_boundary": "client_submit_to_nezha_terminal",
        }
    if method_id == "hash_bsx" or snapshot.get("block_executor") == "bsx_block_executor":
        return {
            "comparison_semantics_class": "bsx_deterministic_coloring_serializable_v1",
            "state_access_semantics": "stateful_local_deterministic_serializable_order",
            "state_home_mapping_policy": "execution_shard_local_namespace",
            "remote_fetch_policy": "none",
            "remote_writeback_policy": "none",
            "proof_policy": "consensus_bound_bsx_plan_digest",
            "legacy_cross_shard_protocol": True,
            "measurement_boundary": "client_submit_to_bsx_terminal",
        }
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
            "target_submission_tps",
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
    if (
        str(group.get("execution_backend") or "") == "real_cluster"
        and os.environ.get("MBE_V5_BACKEND_RELOAD", "0").strip() == "1"
    ):
        raise RuntimeError(
            "Formal real-cluster RunGroups cannot start while backend reload is enabled; "
            "restart MBE without --reload"
        )
    worker_name = f"v5-formal-worker-{group_id}"
    group["status"] = "starting"
    group["worker_pid"] = os.getpid()
    group["worker_thread"] = worker_name
    group["worker_heartbeat_at"] = datetime.now(UTC).isoformat()
    group["execution_policy"] = _execution_policy(group)
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
    group["worker_heartbeat_at"] = datetime.now(UTC).isoformat()
    group["execution_policy"] = _execution_policy(group)
    write_group(group)
    plan = V5FormalExperimentPlan.model_validate(group["plan"])
    backend = group["execution_backend"]
    all_rows = group.get("matrix") or expand(plan, backend)
    checked_rows, fairness = validate_fairness(all_rows)
    retry_requested = set(group.pop("retry_requested_child_ids", []))
    resume_requested = set(group.pop("resume_requested_child_ids", []))
    requested = retry_requested | resume_requested
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
        child_started_at = datetime.now(UTC).isoformat()
        _write_worker_heartbeat(group_id, child_id=child_id, child_started_at=child_started_at)
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
        write_attempt(group_id, child_id, {"attempt_number": attempt_number, "status": "running", "started_at": child_started_at})
        stop_after_child = False
        policy = _execution_policy(read_group(group_id))
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
                    timeout_seconds=int(policy["child_wall_timeout_seconds"]),
                    heartbeat_callback=lambda: _write_worker_heartbeat(group_id, child_id=child_id, child_started_at=child_started_at),
                    heartbeat_interval_seconds=float(policy["worker_heartbeat_seconds"]),
                    runtime_context={"run_group_id": group_id, "child_run_id": child_id},
                )
                if result["status"] != "completed":
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
        except OSError as exc:
            if _is_system_resource_error(exc):
                child.update({
                    "status": "failed",
                    "execution_status": "blocked_resource_system_pressure",
                    "artifact_status": "incomplete",
                    "formal_eligibility": False,
                    "error": str(exc),
                    "resource_pressure": {
                        "schema_version": "mbe_v5_system_resource_pressure_v1",
                        "winerror": getattr(exc, "winerror", None),
                        "error": str(exc),
                        "observed_at": datetime.now(UTC).isoformat(),
                    },
                    "paper_candidate": False,
                })
                group = read_group(group_id)
                group["last_system_resource_pressure"] = child["resource_pressure"]
                group["last_system_resource_pressure_child_id"] = child_id
                write_group(group)
                time.sleep(max(0, int(policy["resource_recovery_wait_seconds"])))
            else:
                child.update({"status": "failed", "error": str(exc), "paper_candidate": False})
        except Exception as exc:  # preserve failure evidence for result center and retry policy
            child.update({"status": "failed", "error": str(exc), "paper_candidate": False})
        write_child(group_id, child)
        write_attempt(group_id, child_id, {"attempt_number": attempt_number, "status": child["status"], "finished_at": datetime.now(UTC).isoformat(), "result": child.get("result"), "metrics": child.get("metrics"), "error": child.get("error")})
        if bool(policy.get("auto_cold_archive_terminal_children", True)) and bool(policy.get("auto_cold_archive_delete_raw", True)):
            _write_worker_heartbeat(group_id, child_id=child_id, child_started_at=child_started_at)
            v5_formal_artifact_storage.auto_archive_terminal_child(
                group_id,
                child_id,
                compression_level=int(policy.get("auto_cold_archive_compression_level", 10)),
            )
        _write_worker_heartbeat(group_id)
        group = read_group(group_id)
        _refresh_child_counts(group, children(group_id))
        write_group(group)
        if child.get("status") == "cancelled":
            finalize_cancelled(group_id)
            return
        if stop_after_child:
            break
    finalize(group_id)


def _bundle_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _try_build_bundle(directory: Path, group: dict) -> bool:
    output = directory / "artifacts.zip"
    temp_output = directory / f".artifacts.{uuid4().hex}.tmp.zip"
    for stale in directory.glob(".artifacts.*.tmp.zip"):
        try:
            stale.unlink()
        except OSError:
            pass
    try:
        output.unlink(missing_ok=True)
    except OSError:
        pass

    group["bundle_status"] = "building"
    group["bundle_started_at"] = datetime.now(UTC).isoformat()
    for key in ("bundle_ready_at", "bundle_size_bytes", "bundle_sha256", "bundle_error", "bundle_failed_at"):
        group.pop(key, None)
    write_group(group)

    try:
        try:
            build_bundle(directory, group, output_path=temp_output)
        except TypeError as exc:
            if "output_path" not in str(exc):
                raise
            legacy_output = Path(build_bundle(directory, group))
            if legacy_output.resolve() != temp_output.resolve():
                os.replace(legacy_output, temp_output)

        with zipfile.ZipFile(temp_output, "r") as archive:
            bad_member = archive.testzip()
            if bad_member is not None:
                raise RuntimeError(f"bundle CRC verification failed at {bad_member}")
            names = set(archive.namelist())
            required = {"reproducibility_manifest.json", "artifact_manifest.json", "run_group.json"}
            missing = sorted(required - names)
            if missing:
                raise RuntimeError(f"bundle missing required entries: {missing}")

        os.replace(temp_output, output)
        group["bundle_status"] = "ready"
        group["bundle_ready_at"] = datetime.now(UTC).isoformat()
        group["bundle_size_bytes"] = int(output.stat().st_size)
        group["bundle_sha256"] = _bundle_sha256(output)
        write_group(group)
        return True
    except Exception as exc:
        try:
            temp_output.unlink(missing_ok=True)
        except OSError:
            pass
        try:
            output.unlink(missing_ok=True)
        except OSError:
            pass
        group["bundle_status"] = "failed"
        group["bundle_error"] = str(exc)
        group["bundle_failed_at"] = datetime.now(UTC).isoformat()
        write_group(group)
        return False


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
    group.pop("active_child_run_id", None)
    group.pop("active_child_started_at", None)
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
    group.pop("active_child_run_id", None)
    group.pop("active_child_started_at", None)
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
    abort_count = number("abort_count")
    nezha_hs_abort_count = number("nezha_hs_abort_count")
    cg_cycle_abort_count = number("cg_cycle_abort_count")
    semantic_class = str(item.get("comparison_semantics_class") or "")
    terminal_abort_semantics = semantic_class in {"nezha_acg_hs_abortable_v1", "cg_cycle_abortable_v2", "cg_cycle_abortable_v3", "cg_cycle_abortable_v4", "nezha_cg_johnson_abortable_v1", "nezha_cg_johnson_abortable_v2"}
    semantic_abort_count = nezha_hs_abort_count if semantic_class == "nezha_acg_hs_abortable_v1" else cg_cycle_abort_count if semantic_class in {"cg_cycle_abortable_v2", "cg_cycle_abortable_v3", "cg_cycle_abortable_v4", "nezha_cg_johnson_abortable_v1", "nezha_cg_johnson_abortable_v2"} else None
    if submitted is None or terminal != submitted:
        reasons.append("terminal_not_equal_submitted")
    # Nezha's HS may abort transactions as explicit terminal failed no-ops.
    # For that semantic cohort, successful finalization plus HS aborts must
    # account exactly for every terminal transaction.
    if terminal_abort_semantics:
        if finalized is None or abort_count is None or semantic_abort_count is None:
            reasons.append("terminal_abort_accounting_missing")
        else:
            if terminal is None or finalized + abort_count != terminal:
                reasons.append("finalized_plus_abort_not_equal_terminal")
            if abort_count != semantic_abort_count:
                reasons.append("semantic_abort_count_mismatch")
    elif submitted is None or finalized != submitted:
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
                # A single independently valid implementation establishes its own
                # deterministic semantic result. When a cohort contains multiple
                # methods, all of their required digests must still agree.
                required_child_count=1,
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

    within_semantic_valid = bool(comparable_reports) and all(
        report.get("pairwise_logical_state_equivalent") is True
        for report in comparable_reports
    )
    semantic_classes = {
        str(report.get("comparison_semantics_class") or "custom_unknown")
        for report in comparable_reports
    }
    direct_cross_semantic_valid = (
        within_semantic_valid
        and len(semantic_classes) == 1
        and "custom_unknown" not in semantic_classes
    )
    return enriched, {
        "passed": within_semantic_valid,
        # Paper samples can be individually valid inside their own deterministic
        # semantics even when direct cross-semantic uplift claims remain blocked.
        "performance_comparison_valid": direct_cross_semantic_valid,
        "within_semantic_cohort_state_equivalence_valid": within_semantic_valid,
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
    materialized_ids = {str(item.get("child_run_id") or "") for item in items if item.get("child_run_id")}
    group["total_child_runs"] = group.get("total_child_runs") or len(materialized_ids)
    group["completed_child_runs"] = sum(item.get("status") == "completed" for item in items)
    group["failed_child_runs"] = sum(item.get("status") == "failed" for item in items)
    group["blocked_child_runs"] = sum(item.get("status") == "blocked" for item in items)
    group["cancelled_child_runs"] = sum(item.get("status") == "cancelled" for item in items)
    group["timed_out_child_runs"] = sum(item.get("status") == "timed_out" for item in items)
    group["interrupted_child_runs"] = sum(item.get("status") == "interrupted" for item in items)
    group["not_started_child_runs"] = max(0, int(group.get("total_child_runs") or 0) - len(materialized_ids))


def _spec_for(plan: V5FormalExperimentPlan, row: dict, *, formal_plan_config_id: str | None = None):
    spec = plan.base_spec.model_copy(deep=True)

    topology_point = dict(row.get("topology_point") or {})
    allowed_topology = {"nodes", "shards", "validators_per_shard", "worker_count"}
    unsupported_topology = set(topology_point) - allowed_topology
    if unsupported_topology:
        raise ValueError(f"unsupported topology point fields: {sorted(unsupported_topology)}")
    worker_count_override = topology_point.pop("worker_count", None)
    if worker_count_override is not None and (isinstance(worker_count_override, bool) or not isinstance(worker_count_override, int) or not 1 <= worker_count_override <= 32):
        raise ValueError("topology worker_count must be an integer between 1 and 32")
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

# MBE_FORMAL_RUNTIME_CLOSURE_20260820_V7
