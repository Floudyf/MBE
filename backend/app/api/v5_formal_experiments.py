from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import FileResponse
from backend.app.services.v5_formal_run_store import ROOT_DIR, group_dir

from backend.app.models.v5_formal_experiment import V5FormalRunRequest
from backend.app.services.v5_formal_run_store import children, create_group, read_child, read_group, write_group
from backend.app.services.v5_formal_scheduler import DEFAULT_FORMAL_EXECUTION_POLICY, finalize_cancelled, list_resume_candidates, reconcile_stale_group, resume_selected, resume_unfinished, start, worker_active
from backend.app.services.v5_formal_artifact_catalog import read_catalog, safe_artifact_name
from backend.app.services.v5_formal_dto import V5FormalChildResponse, V5FormalRunGroupDetailResponse, child_detail as child_detail_dto, child_summary, group_detail, group_summary
from backend.app.services.v5_formal_plan_validator import FormalPlanValidationError, validate_request
from backend.app.services.v5_reproducibility_bundle import build as build_reproducibility_bundle
from backend.app.services import v5_cleanup_service, v5_formal_artifact_storage


router = APIRouter(prefix="/api/v5/formal", tags=["v5"])


@router.post("/preview")
def preview(payload: V5FormalRunRequest) -> dict:
    try:
        checked = validate_request(payload, allow_blocked_rows=True)
    except FormalPlanValidationError as exc:
        raise HTTPException(422, str(exc)) from exc
    return {"execution_backend": payload.execution_backend, "rows": checked.rows, "paper_candidate": False}


@router.post("/run-groups")
def create_run_group(payload: V5FormalRunRequest) -> dict:
    try:
        checked = validate_request(payload)
    except FormalPlanValidationError as exc:
        raise HTTPException(422, str(exc)) from exc
    plan = checked.plan.model_dump()
    group = create_group(
        {
            "execution_backend": payload.execution_backend,
            "runtime_truth": "v5_real_cluster_candidate",
            "formal_experiment_profile": _formal_experiment_profile(plan, checked.rows),
            "plan": plan,
            "matrix": checked.rows,
            "total_child_runs": len(checked.rows),
            "completed_child_runs": 0,
            "failed_child_runs": 0,
            "blocked_child_runs": 0,
            "cancelled_child_runs": 0,
            "timed_out_child_runs": 0,
            "interrupted_child_runs": 0,
            "not_started_child_runs": len(checked.rows),
            "execution_policy": dict(DEFAULT_FORMAL_EXECUTION_POLICY),
            "max_concurrent_real_clusters": 1,
        }
    )
    start(group["run_group_id"])
    return group_summary(group, children=[])


@router.get("/run-groups/summaries")
def list_run_group_summaries(
    limit: int = Query(20, ge=1, le=100), offset: int = Query(0, ge=0), status: str | None = None,
    method_id: str | None = None, suite: str | None = None, search: str | None = None, include_tests: bool = False,
) -> dict:
    summaries = [_summary_for_group(group) for group in _groups()]
    def matches(item: dict) -> bool:
        if not include_tests and item["is_test"]:
            return False
        if status and item.get("status") != status:
            return False
        if method_id and method_id not in item.get("method_ids", []):
            return False
        if suite and suite not in item.get("suite_names", []):
            return False
        if search:
            needle = search.lower()
            return needle in str(item.get("run_group_id", "")).lower() or needle in str(item.get("plan_name", "")).lower() or any(needle in str(name).lower() for name in item.get("method_names", []))
        return True
    filtered = [item for item in summaries if matches(item)]
    page = filtered[offset:offset + limit]
    next_offset = offset + limit if offset + limit < len(filtered) else None
    return {"items": page, "total": len(filtered), "next_cursor": str(next_offset) if next_offset is not None else None}


@router.get("/run-groups/{group_id}", response_model=V5FormalRunGroupDetailResponse)
def get_run_group(group_id: str) -> dict:
    try:
        group = reconcile_stale_group(group_id)
        return group_detail(group, children(group_id))
    except (FileNotFoundError, ValueError) as exc:
        raise HTTPException(404, "unknown formal run group") from exc


@router.get("/run-groups")
def list_run_groups() -> list[dict]:
    return [_summary_for_group(group) for group in _groups()]


@router.post("/run-groups/{group_id}/delete")
def delete_run_group(group_id: str, dry_run: bool = Query(True)) -> dict:
    try:
        return _persist_cleanup_report(
            "delete_run_group",
            v5_cleanup_service.delete_run_group(group_id, dry_run=dry_run),
        )
    except ValueError as exc:
        raise HTTPException(404, "unknown formal run group") from exc


@router.post("/cleanup/run-groups/selected")
def delete_selected_run_groups(payload: dict, dry_run: bool = Query(True)) -> dict:
    run_group_ids = payload.get("run_group_ids")
    if not isinstance(run_group_ids, list) or not all(isinstance(item, str) for item in run_group_ids):
        raise HTTPException(422, "run_group_ids must be a list of strings")
    return _persist_cleanup_report(
        "delete_selected_run_groups",
        v5_cleanup_service.delete_selected_run_groups(run_group_ids, dry_run=dry_run),
    )


@router.post("/cleanup/run-groups/failed")
def delete_failed_run_groups(dry_run: bool = Query(True)) -> dict:
    return _persist_cleanup_report(
        "delete_failed_run_groups",
        v5_cleanup_service.delete_failed_run_groups(dry_run=dry_run),
    )


@router.post("/cleanup/run-groups/old-unpinned")
def delete_old_unpinned_run_groups(payload: dict, dry_run: bool = Query(True)) -> dict:
    before_time = payload.get("before_time")
    if not isinstance(before_time, str):
        raise HTTPException(422, "before_time must be an ISO datetime string")
    try:
        parsed = datetime.fromisoformat(before_time.replace("Z", "+00:00"))
    except ValueError as exc:
        raise HTTPException(422, "before_time must be an ISO datetime string") from exc
    return _persist_cleanup_report(
        "delete_old_unpinned_run_groups",
        v5_cleanup_service.delete_old_unpinned_run_groups(parsed, dry_run=dry_run),
    )


@router.get("/cleanup/orphan-real-cluster-dirs")
def scan_orphan_real_cluster_dirs(min_age_hours: int = Query(24, ge=1)) -> dict:
    return v5_cleanup_service.scan_orphan_real_cluster_dirs(min_age_hours=min_age_hours)


@router.post("/cleanup/orphan-real-cluster-dirs")
def cleanup_orphan_real_cluster_dirs(dry_run: bool = Query(True), min_age_hours: int = Query(24, ge=1)) -> dict:
    return _persist_cleanup_report(
        "cleanup_orphan_real_cluster_dirs",
        v5_cleanup_service.cleanup_orphan_real_cluster_dirs(dry_run=dry_run, min_age_hours=min_age_hours),
    )


@router.get("/cleanup/legacy-saved-configs")
def scan_legacy_saved_configs() -> dict:
    return v5_cleanup_service.scan_legacy_saved_configs()


@router.post("/cleanup/legacy-saved-configs")
def cleanup_legacy_saved_configs(dry_run: bool = Query(True)) -> dict:
    return _persist_cleanup_report(
        "cleanup_legacy_saved_configs",
        v5_cleanup_service.cleanup_legacy_saved_configs(dry_run=dry_run),
    )


@router.get("/run-groups/{group_id}/children", response_model=list[V5FormalChildResponse])
def list_children(group_id: str) -> list[dict]:
    try: return [child_summary(item) for item in children(group_id)]
    except (FileNotFoundError, ValueError) as exc: raise HTTPException(404, "unknown formal run group") from exc


@router.get("/run-groups/{group_id}/children/{child_id}", response_model=V5FormalChildResponse)
def child_detail(group_id: str, child_id: str) -> dict:
    try: return child_detail_dto(read_child(group_id, child_id))
    except (FileNotFoundError, ValueError) as exc: raise HTTPException(404, "unknown child run") from exc


@router.get("/run-groups/{group_id}/metrics")
def group_metrics(group_id: str) -> dict:
    try: return read_group(group_id).get("aggregate", {})
    except (FileNotFoundError, ValueError) as exc: raise HTTPException(404, "unknown formal run group") from exc


@router.get("/run-groups/{group_id}/analysis")
def group_analysis(group_id: str) -> dict:
    try:
        from backend.app.services.v5_paper_exporter import analysis
        return analysis(read_group(group_id), children(group_id))
    except (FileNotFoundError, ValueError) as exc:
        raise HTTPException(404, "unknown formal run group") from exc


@router.get("/run-groups/{group_id}/artifacts")
def group_artifacts(group_id: str) -> dict:
    try:
        read_group(group_id)
        return read_catalog(group_dir(group_id), group_id)
    except (FileNotFoundError, ValueError) as exc:
        raise HTTPException(404, "unknown formal run group") from exc


@router.get("/run-groups/{group_id}/storage")
def group_storage(group_id: str) -> dict:
    try:
        return v5_formal_artifact_storage.status(group_id)
    except (FileNotFoundError, ValueError) as exc:
        raise HTTPException(404, "unknown formal run group") from exc


@router.post("/run-groups/{group_id}/storage/compact")
def compact_group_storage(group_id: str) -> dict:
    try:
        return v5_formal_artifact_storage.compact(group_id)
    except (FileNotFoundError, ValueError) as exc:
        raise HTTPException(404, "unknown formal run group") from exc
    except v5_formal_artifact_storage.FormalArtifactStorageError as exc:
        raise HTTPException(409, str(exc)) from exc


@router.post("/run-groups/{group_id}/storage/archive")
def archive_group_storage(group_id: str, payload: dict | None = None) -> dict:
    payload = payload or {}
    delete_raw = payload.get("delete_raw", True)
    level = payload.get("compression_level", 3)
    if not isinstance(delete_raw, bool):
        raise HTTPException(422, "delete_raw must be boolean")
    if isinstance(level, bool) or not isinstance(level, int) or not 1 <= level <= 19:
        raise HTTPException(422, "compression_level must be an integer between 1 and 19")
    try:
        return v5_formal_artifact_storage.start_archive_job(group_id, delete_raw=delete_raw, compression_level=level)
    except (FileNotFoundError, ValueError) as exc:
        raise HTTPException(404, "unknown formal run group") from exc
    except v5_formal_artifact_storage.FormalArtifactStorageError as exc:
        raise HTTPException(409, str(exc)) from exc


@router.post("/run-groups/{group_id}/storage/restore")
def restore_group_storage(group_id: str) -> dict:
    try:
        return v5_formal_artifact_storage.restore(group_id)
    except (FileNotFoundError, ValueError) as exc:
        raise HTTPException(404, "unknown formal run group") from exc
    except v5_formal_artifact_storage.FormalArtifactStorageError as exc:
        raise HTTPException(409, str(exc)) from exc


@router.get("/run-groups/{group_id}/storage/children/{child_id}/archive")
def download_child_cold_archive(group_id: str, child_id: str):
    try:
        path = v5_formal_artifact_storage.child_archive_path(group_id, child_id)
    except (FileNotFoundError, ValueError) as exc:
        raise HTTPException(404, "cold archive not available") from exc
    except v5_formal_artifact_storage.FormalArtifactStorageError as exc:
        raise HTTPException(409, str(exc)) from exc
    return FileResponse(path, filename=path.name, media_type="application/zstd")


@router.get("/run-groups/{group_id}/bundle")
def bundle(group_id: str):
    try: path = group_dir(group_id) / "artifacts.zip"
    except ValueError as exc: raise HTTPException(404, "unknown formal run group") from exc
    if not path.is_file(): raise HTTPException(404, "bundle not ready")
    return FileResponse(path, filename="artifacts.zip")


@router.post("/run-groups/{group_id}/bundle/rebuild")
def rebuild_bundle(group_id: str) -> dict:
    try:
        group = read_group(group_id)
        directory = group_dir(group_id)
    except (FileNotFoundError, ValueError) as exc:
        raise HTTPException(404, "unknown formal run group") from exc
    if group.get("status") not in {"completed", "completed_with_failures", "failed", "cancelled"}:
        raise HTTPException(409, "bundle can only be rebuilt for a terminal RunGroup")
    output = directory / "artifacts.zip"
    try:
        output.unlink(missing_ok=True)
    except OSError:
        pass
    group["bundle_status"] = "ready"
    group["bundle_path"] = str(output)
    group.pop("bundle_error", None)
    write_group(group)
    try:
        path = build_reproducibility_bundle(directory, group)
    except Exception as exc:
        try:
            output.unlink(missing_ok=True)
        except OSError:
            pass
        group["bundle_status"] = "failed"
        group["bundle_error"] = str(exc)
        write_group(group)
        raise HTTPException(507, f"bundle rebuild failed: {exc}") from exc
    return {"run_group_id": group_id, "bundle_status": "ready", "bundle_path": path.name}


@router.get("/run-groups/{group_id}/artifacts/{artifact_path:path}")
def download_artifact(group_id: str, artifact_path: str):
    safe_name = safe_artifact_name(artifact_path)
    if safe_name is None:
        raise HTTPException(404, "unknown artifact")
    try:
        read_group(group_id)
        directory = group_dir(group_id).resolve()
    except (FileNotFoundError, ValueError) as exc:
        raise HTTPException(404, "unknown formal run group") from exc
    catalog = read_catalog(directory, group_id)
    if not any(item.get("name") == safe_name for item in catalog.get("files", [])):
        raise HTTPException(404, "unknown artifact")
    path = (directory / safe_name).resolve()
    if directory != path and directory not in path.parents:
        raise HTTPException(404, "unknown artifact")
    if not path.is_file():
        raise HTTPException(404, "unknown artifact")
    return FileResponse(path, filename=Path(safe_name).name)


@router.post("/run-groups/{group_id}/cancel")
def cancel_run_group(group_id: str) -> dict:
    try:
        group = read_group(group_id)
    except (FileNotFoundError, ValueError) as exc:
        raise HTTPException(404, "unknown formal run group") from exc
    if group.get("status") in {"completed", "completed_with_failures", "failed", "cancelled"}:
        return _summary_for_group(ensure_persisted_child_counts(group))
    group["cancel_requested"] = True
    group["cancel_requested_at"] = datetime.now(UTC).isoformat()
    # The worker/runner owns process-tree termination and only publishes the
    # terminal cancelled state after the active supervisor has been reaped.
    group["status"] = "cancelling"
    group = ensure_persisted_child_counts(group)
    write_group(group)
    if not worker_active(group_id):
        # A backend restart / forced stop can leave persisted metadata at
        # "running" after the daemon worker thread is already gone. There is no
        # local supervisor process object left to reap in that case, so close
        # the persisted RunGroup truth immediately and preserve partial
        # diagnostics instead of leaving the UI stuck at "cancelling".
        group["cancel_cleanup_mode"] = "stale_worker_metadata_recovery"
        write_group(group)
        return _summary_for_group(finalize_cancelled(group_id))
    return _summary_for_group(group)


@router.get("/run-groups/{group_id}/resume-candidates")
def resume_candidates_run_group(group_id: str) -> dict:
    try:
        return list_resume_candidates(group_id)
    except FileNotFoundError as exc:
        raise HTTPException(404, "unknown formal run group") from exc
    except ValueError as exc:
        raise HTTPException(409, str(exc)) from exc


@router.post("/run-groups/{group_id}/resume-selected")
def resume_selected_run_group(group_id: str, payload: dict) -> dict:
    mode = str(payload.get("mode") or "")
    raw_ids = payload.get("child_run_ids")
    if not isinstance(raw_ids, list) or not all(isinstance(item, str) and item for item in raw_ids):
        raise HTTPException(422, "child_run_ids must be a non-empty string list")
    try:
        group = resume_selected(group_id, raw_ids, mode=mode)
        return _summary_for_group(group)
    except FileNotFoundError as exc:
        raise HTTPException(404, "unknown formal run group") from exc
    except ValueError as exc:
        raise HTTPException(409, str(exc)) from exc


@router.post("/run-groups/{group_id}/resume-unfinished")
def resume_unfinished_run_group(
    group_id: str,
    include_failed: bool = Query(False),
    include_timed_out: bool = Query(False),
) -> dict:
    try:
        group = resume_unfinished(
            group_id,
            include_failed=include_failed,
            include_timed_out=include_timed_out,
        )
        return _summary_for_group(group)
    except FileNotFoundError as exc:
        raise HTTPException(404, "unknown formal run group") from exc
    except ValueError as exc:
        raise HTTPException(409, str(exc)) from exc


@router.post("/run-groups/{group_id}/retry-failed")
def retry_failed(group_id: str) -> dict:
    try: group = read_group(group_id)
    except (FileNotFoundError, ValueError) as exc: raise HTTPException(404, "unknown formal run group") from exc
    failed = [item for item in children(group_id) if item.get("status") == "failed"]
    if not failed: return {"run_group_id": group_id, "retried": 0}
    group["retry_requested_child_ids"] = [item["child_run_id"] for item in failed]
    group["status"] = "queued"; group["cancel_requested"] = False
    write_group(group); start(group_id)
    return {"run_group_id": group_id, "retried": len(failed)}


def _groups() -> list[dict]:
    if not ROOT_DIR.is_dir():
        return []
    return [reconcile_stale_group(path.name) for path in sorted(ROOT_DIR.glob("v5grp_*"), reverse=True) if (path / "run_group.json").is_file()]


def _summary_for_group(group: dict) -> dict:
    return group_summary(ensure_persisted_child_counts(group))


def ensure_persisted_child_counts(group: dict) -> dict:
    if all(group.get(key) is not None for key in ("failed_child_runs", "blocked_child_runs", "cancelled_child_runs")):
        return group
    items = children(group["run_group_id"])
    group["total_child_runs"] = group.get("total_child_runs") or len({item.get("child_run_id") for item in items})
    group["completed_child_runs"] = sum(item.get("status") == "completed" for item in items)
    group["failed_child_runs"] = sum(item.get("status") == "failed" for item in items)
    group["blocked_child_runs"] = sum(item.get("status") == "blocked" for item in items)
    group["cancelled_child_runs"] = sum(item.get("status") == "cancelled" for item in items)
    group["timed_out_child_runs"] = sum(item.get("status") == "timed_out" for item in items)
    group["interrupted_child_runs"] = sum(item.get("status") == "interrupted" for item in items)
    materialized = {str(item.get("child_run_id") or "") for item in items if item.get("child_run_id")}
    group["not_started_child_runs"] = max(0, int(group.get("total_child_runs") or 0) - len(materialized))
    write_group(group)
    return group


def _formal_experiment_profile(plan: dict, rows: list[dict]) -> dict:
    base_spec = plan.get("base_spec") if isinstance(plan.get("base_spec"), dict) else {}
    workload = base_spec.get("workload_source") if isinstance(base_spec.get("workload_source"), dict) else {}
    topology = base_spec.get("topology") if isinstance(base_spec.get("topology"), dict) else {}
    methods = [item for item in plan.get("methods", []) if isinstance(item, dict)]
    block_producer = next(
        (
            item
            for item in base_spec.get("plugin_selections", [])
            if isinstance(item, dict) and item.get("category") == "block_producer"
        ),
        {},
    )
    block_config = block_producer.get("config") if isinstance(block_producer.get("config"), dict) else {}
    requested_worker_count = int(plan.get("worker_count") or 1)
    worker_execution_truth: dict[str, dict] = {}
    for item in methods:
        method_id = str(item.get("method_id", ""))
        overrides = item.get("plugin_config_overrides") or {}
        registered = dict(overrides.get("block_executor") or {})
        executor_id = str((item.get("plugin_overrides") or {}).get("block_executor") or "")
        counts: set[int] = set()
        for row in rows:
            row_method = row.get("method") if isinstance(row.get("method"), dict) else {}
            row_method_id = str(row_method.get("method_id") or row.get("method_config_id") or "")
            if row_method_id != method_id:
                continue
            topology_point = row.get("topology_point") if isinstance(row.get("topology_point"), dict) else {}
            row_workers = topology_point.get("worker_count", requested_worker_count)
            if isinstance(row_workers, int) and not isinstance(row_workers, bool) and row_workers > 0:
                counts.add(1 if executor_id == "serial_block_executor" else row_workers)
        if not counts:
            counts.add(1 if executor_id == "serial_block_executor" else requested_worker_count)
        effective_counts = sorted(counts)
        worker_execution_truth[method_id] = {
            "registered_default_worker_count": registered.get("worker_count"),
            "requested_worker_count": requested_worker_count,
            "effective_worker_count": effective_counts[0] if len(effective_counts) == 1 else None,
            "effective_worker_counts": effective_counts,
        }
    now = datetime.now().isoformat()
    return {
        "schema_version": "v5_formal_experiment_profile_v2",
        "profile_id": "formal_four_method_comparison",
        "method_ids": [str(item.get("method_id", "")) for item in methods],
        "workload_settings": {
            "source_type": workload.get("source_type", "synthetic"),
            "dataset_id": workload.get("dataset_id"),
            "variant_mode": workload.get("variant_mode"),
            "requested_tx_count": workload.get("requested_tx_count", base_spec.get("tx_count")),
            "seed": workload.get("seed", base_spec.get("seed")),
            "selection_mode": workload.get("selection_mode"),
            "replay_mode": workload.get("replay_mode"),
            "target_submission_tps": workload.get("target_submission_tps"),
            "skew_axis": workload.get("skew_axis"),
            "target_alpha": workload.get("target_alpha"),
        },
        "topology": {
            "nodes": topology.get("nodes"),
            "shards": topology.get("shards"),
            "validators_per_shard": topology.get("validators_per_shard"),
        },
        "block_size": block_config.get("block_size", 100),
        "block_interval_ms": block_config.get("interval_ms", block_config.get("block_interval_ms", 75)),
        "worker_settings": {
            str(item.get("method_id", "")): (item.get("plugin_config_overrides") or {}).get("block_executor", {})
            for item in methods
        },
        "worker_execution_truth": worker_execution_truth,
        "repeat_settings": {
            "seeds": list(plan.get("seeds") or []),
            "repeats": plan.get("repeats", 1),
            "suites": list(plan.get("suites") or []),
        },
        "compatibility_snapshot": {
            "row_count": len(rows),
            "runnable_row_count": sum(1 for row in rows if row.get("runnable") is True),
            "blocked_row_count": sum(1 for row in rows if row.get("blockers")),
        },
        "created_at": now,
        "updated_at": now,
    }


def _persist_cleanup_report(action: str, report: dict) -> dict:
    output_dir = v5_cleanup_service.cleanup_report_output_dir(action)
    v5_cleanup_service.write_cleanup_report(report, output_dir)
    enriched = dict(report)
    enriched["cleanup_report"] = {
        "report_id": output_dir.name,
        "action": action,
        "json": _relative_to_root(output_dir / "cleanup_report.json"),
        "csv": _relative_to_root(output_dir / "cleanup_report.csv"),
    }
    return enriched


def _relative_to_root(path: Path) -> str:
    try:
        return path.resolve().relative_to(ROOT_DIR.parent.resolve()).as_posix()
    except ValueError:
        return path.name
