from __future__ import annotations

import json
import hashlib
import fnmatch
from pathlib import Path
from typing import Iterable


def write_run_artifact_catalog(run_dir: Path, *, run_id: str) -> dict:
    catalog = build_run_artifact_catalog(run_dir, run_id=run_id)
    (run_dir / "artifact_catalog.json").write_text(json.dumps(catalog, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return catalog


def build_run_artifact_catalog(run_dir: Path, *, run_id: str) -> dict:
    files = []
    for path in sorted(run_dir.rglob("*")):
        if not path.is_file():
            continue
        rel = path.relative_to(run_dir).as_posix()
        if rel == "artifact_catalog.json":
            continue
        files.append(catalog_entry(run_dir, rel, path.stat().st_size, download_url=f"/api/v5/real-cluster/runs/{run_id}/artifacts/{rel}"))
    return {
        "schema_version": "mbe_v5_artifact_catalog_v1",
        "run_id": run_id,
        "file_count": len(files),
        "files": files,
    }


def evaluate_expected_artifacts(run_dir: Path, expected_artifacts: Iterable[object]) -> dict:
    """Evaluate a version-2, child-root scoped artifact contract.

    Entries may be legacy strings or mappings with ``path_pattern``.  A node
    wildcard is deliberately expanded against the actual node ids so one node
    cannot satisfy evidence required from every node.
    """
    specs = [_contract_spec(value) for value in expected_artifacts]
    specs = [item for item in specs if item]
    actual = sorted(path.relative_to(run_dir).as_posix().replace("\\", "/") for path in run_dir.rglob("*") if path.is_file())
    actual_set = set(actual)
    node_ids = sorted({name.split("/")[1] for name in actual if name.startswith("nodes/") and len(name.split("/")) > 2})
    expected_set: set[str] = set()
    missing: list[str] = []
    for spec in specs:
        pattern = spec["path_pattern"]
        patterns = [pattern]
        if spec.get("per_node") and "nodes/*/" in pattern:
            required_nodes = spec.get("node_ids") or node_ids
            patterns = [pattern.replace("nodes/*/", f"nodes/{node_id}/") for node_id in required_nodes]
        for current in patterns:
            matches = [name for name in actual if fnmatch.fnmatchcase(name, current)]
            expected_set.update(matches)
            if len(matches) < int(spec.get("min_count", 1)):
                missing.append(current)
    unexpected = sorted(name for name in actual if name not in expected_set)
    return {
        "schema_version": "mbe_v5_artifact_contract_v2",
        "artifact_contract_version": 2,
        "artifact_contract_status": "complete" if not missing else "incomplete",
        "expected_artifact_count": len(specs),
        "actual_artifact_count": len(actual),
        "missing_expected_artifacts": missing,
        "unexpected_artifacts": unexpected,
    }


def _contract_spec(value: object) -> dict | None:
    if isinstance(value, str):
        path = _safe_relative_path(value)
        return {"path_pattern": path, "scope": path.split("/", 1)[0] if path else "child_root", "per_node": path.startswith("nodes/*/") if path else False, "required_for_formal_eligibility": True, "min_count": 1} if path else None
    if not isinstance(value, dict):
        return None
    path = _safe_relative_path(value.get("path_pattern"))
    if not path:
        return None
    return {"path_pattern": path, "scope": value.get("scope", "child_root"), "per_node": bool(value.get("per_node", False)), "node_ids": list(value.get("node_ids", [])), "required_for_formal_eligibility": bool(value.get("required_for_formal_eligibility", True)), "min_count": int(value.get("min_count", 1))}


def catalog_entry(run_dir: Path, name: str, size_bytes: int, *, download_url: str, sha256: str | None = None) -> dict:
    role, truth_scope, producer, schema_version = classify_artifact(name)
    return {
        "name": name,
        "size_bytes": size_bytes,
        "artifact_role": role,
        "truth_scope": truth_scope,
        "producer": producer,
        "schema_version": schema_version,
        "sha256": sha256 or file_sha256(run_dir / name),
        "download_url": download_url,
    }


def file_sha256(path: Path) -> str | None:
    if not path.is_file():
        return None
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def classify_artifact(name: str) -> tuple[str, str, str, str]:
    base = name.split("/")[-1]
    if name.startswith("children/"):
        return "child_experiment_record", "child_run_metadata", "v5_formal_scheduler", "mbe_v5_child_record_v1"
    if base in {"formal_matrix.csv", "fairness_matrix.csv", "fairness_validation.json", "run_group_summary.json", "run_group.json"}:
        return "formal_run_group_summary", "formal_comparison_group", "v5_formal_scheduler", "mbe_v5_formal_summary_v1"
    if base in {"comparison_summary.csv", "paper_table_data.csv", "paper_figure_data.csv", "paper_result_analysis.json", "paper_result_analysis.csv", "run_group_report.md"}:
        return "paper_analysis", "formal_run_group_analysis", "v5_paper_exporter", "mbe_v5_paper_analysis_v1"
    if base.startswith("workload_"):
        return "workload_evidence", "selected_workload_window", "v5_workload_data_plane", "mbe_workload_record_v1"
    if base == "predicted_remote_access.csv":
        return "metatrack_remote_state_evidence", "predicted_plan", "metatrack_planner", "mbe_metatrack_remote_prediction_v1"
    if base in {"physical_remote_state_operations.csv", "remote_state_access.csv"}:
        scope = "node_physical_legacy" if base == "remote_state_access.csv" else "node_physical"
        return "metatrack_remote_state_evidence", scope, "v5_real_cluster_runtime", "mbe_remote_state_operations_v2"
    if name == "aggregate/replica_deduplicated_remote_operations.csv":
        return "aggregate_metric", "cluster_replica_deduplicated", "v5_metric_aggregator", "mbe_replica_deduplicated_remote_operations_v1"
    if name == "aggregate/remote_state_metrics_summary.json":
        return "aggregate_metric", "node_physical_and_replica_deduplicated", "v5_metric_aggregator", "mbe_remote_state_metrics_v2"
    if base in {"resource_usage_summary.json", "resource_usage_timeseries.csv", "resource_sampler_summary.json"}:
        producer = "mbe_supervisor" if base != "resource_usage_summary.json" else "v5_observability_metrics"
        return "research_observability", "validator_node_processes_completion_window", producer, "mbe_v5_resource_observability_v1"
    if base in {"network_metrics_summary.json", "network_message_summary.csv"}:
        return "research_observability", "successful_node_receive_completion_window", "v5_observability_metrics", "mbe_v5_network_observability_v1"
    if base == "observability_collection_error.json":
        return "audit_log", "observer_failure_open", "v5_real_cluster_runner", "mbe_v5_observability_error_v1"
    if base == "artifact_storage_summary.json":
        return "storage_metadata", "post_measurement_storage", "v5_artifact_storage", "mbe_v5_artifact_storage_v1"
    if name.startswith("aggregate/"):
        return "aggregate_metric", "cluster_aggregate", "v5_metric_aggregator", "mbe_v5_aggregate_metric_v1"
    if name.startswith("node_") or name.startswith("nodes/"):
        return "node_mechanism_evidence", "node_physical", "v5_real_cluster_runtime", "mbe_v5_node_artifact_v1"
    if name.startswith("client/") or base in {"client_submission_log.csv", "resolved_access_lists.jsonl.gz"}:
        return "client_workload_evidence", "client_logical_submission", "v5_real_cluster_client", "mbe_v5_client_artifact_v1"
    if name.startswith("logs/") or base.startswith("supervisor_"):
        return "audit_log", "supervisor_process", "mbe_supervisor", "mbe_v5_log_v1"
    return "raw_artifact", "unspecified", "unknown", "mbe_v5_artifact_v1"


def _safe_relative_path(value: object) -> str | None:
    if not isinstance(value, str):
        return None
    name = value.replace("\\", "/").strip()
    if not name or name.startswith("/") or ":" in name:
        return None
    parts = name.split("/")
    if any(part in {"", ".", ".."} for part in parts):
        return None
    return name
