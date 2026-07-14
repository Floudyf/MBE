from __future__ import annotations

from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import FileResponse
from backend.app.services.v5_formal_run_store import ROOT_DIR, group_dir

from backend.app.models.v5_formal_experiment import V5FormalRunRequest
from backend.app.services.v5_formal_run_store import children, create_group, read_child, read_group, write_group
from backend.app.services.v5_formal_scheduler import start
from backend.app.services.v5_formal_artifact_catalog import read_catalog
from backend.app.services.v5_formal_dto import child_detail as child_detail_dto, child_summary, group_detail, group_summary
from backend.app.services.v5_formal_plan_validator import FormalPlanValidationError, validate_request
from backend.app.services.v3_saved_config_store import create_saved_config
from backend.app.models.v3_saved_config import V3SavedConfigCreateRequest


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
    saved = create_saved_config(V3SavedConfigCreateRequest(config_kind="formal_plan", name=checked.plan.name, payload=checked.plan.model_dump(), validation_status="valid", source="user_saved"))
    group = create_group({"execution_backend": payload.execution_backend, "runtime_truth": "v5_real_cluster_candidate", "plan_config_id": saved["config_id"], "plan": checked.plan.model_dump(), "matrix": checked.rows, "total_child_runs": len(checked.rows), "completed_child_runs": 0, "failed_child_runs": 0, "max_concurrent_real_clusters": 1})
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


@router.get("/run-groups/{group_id}")
def get_run_group(group_id: str) -> dict:
    try:
        return group_detail(read_group(group_id), children(group_id))
    except (FileNotFoundError, ValueError) as exc:
        raise HTTPException(404, "unknown formal run group") from exc


@router.get("/run-groups")
def list_run_groups() -> list[dict]:
    return [_summary_for_group(group) for group in _groups()]


@router.get("/run-groups/{group_id}/children")
def list_children(group_id: str) -> list[dict]:
    try: return [child_summary(item) for item in children(group_id)]
    except (FileNotFoundError, ValueError) as exc: raise HTTPException(404, "unknown formal run group") from exc


@router.get("/run-groups/{group_id}/children/{child_id}")
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


@router.get("/run-groups/{group_id}/bundle")
def bundle(group_id: str):
    try: path = group_dir(group_id) / "artifacts.zip"
    except ValueError as exc: raise HTTPException(404, "unknown formal run group") from exc
    if not path.is_file(): raise HTTPException(404, "bundle not ready")
    return FileResponse(path, filename="artifacts.zip")


@router.post("/run-groups/{group_id}/cancel")
def cancel_run_group(group_id: str) -> dict:
    try:
        group = read_group(group_id)
    except (FileNotFoundError, ValueError) as exc:
        raise HTTPException(404, "unknown formal run group") from exc
    group["cancel_requested"] = True
    group["status"] = "cancelled"
    group = ensure_persisted_child_counts(group)
    write_group(group)
    return _summary_for_group(group)


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
    return [read_group(path.name) for path in sorted(ROOT_DIR.glob("v5grp_*"), reverse=True) if (path / "run_group.json").is_file()]


def _summary_for_group(group: dict) -> dict:
    return group_summary(ensure_persisted_child_counts(group))


def ensure_persisted_child_counts(group: dict) -> dict:
    if group.get("failed_child_runs") is not None:
        return group
    items = children(group["run_group_id"])
    group["total_child_runs"] = group.get("total_child_runs") or len({item.get("child_run_id") for item in items})
    group["completed_child_runs"] = sum(item.get("status") == "completed" for item in items)
    group["failed_child_runs"] = sum(item.get("status") in {"failed", "blocked", "cancelled"} for item in items)
    write_group(group)
    return group
