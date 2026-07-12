from __future__ import annotations

from fastapi import APIRouter, HTTPException
from fastapi.responses import FileResponse
from backend.app.services.v5_formal_run_store import ROOT_DIR, group_dir

from backend.app.models.v5_formal_experiment import V5FormalRunRequest
from backend.app.services.v5_formal_run_store import children, create_group, read_child, read_group, write_group
from backend.app.services.v5_formal_scheduler import expand, start
from backend.app.services.v3_saved_config_store import create_saved_config
from backend.app.models.v3_saved_config import V3SavedConfigCreateRequest


router = APIRouter(prefix="/api/v5/formal", tags=["v5"])


@router.post("/preview")
def preview(payload: V5FormalRunRequest) -> dict:
    return {"execution_backend": payload.execution_backend, "rows": expand(payload.plan, payload.execution_backend), "paper_candidate": False}


@router.post("/run-groups")
def create_run_group(payload: V5FormalRunRequest) -> dict:
    saved = create_saved_config(V3SavedConfigCreateRequest(config_kind="formal_plan", name=payload.plan.name, payload=payload.plan.model_dump(), validation_status="valid", source="user_saved"))
    matrix = expand(payload.plan, payload.execution_backend)
    group = create_group({"execution_backend": payload.execution_backend, "runtime_truth": "v5_real_cluster_candidate" if payload.execution_backend == "real_cluster" else "preview_or_simulation", "plan_config_id": saved["config_id"], "plan": payload.plan.model_dump(), "matrix": matrix, "total_child_runs": len(matrix), "completed_child_runs": 0, "max_concurrent_real_clusters": 1})
    start(group["run_group_id"])
    return group


@router.get("/run-groups/{group_id}")
def get_run_group(group_id: str) -> dict:
    try:
        return {"group": read_group(group_id), "children": children(group_id)}
    except (FileNotFoundError, ValueError) as exc:
        raise HTTPException(404, "unknown formal run group") from exc


@router.get("/run-groups")
def list_run_groups() -> list[dict]:
    if not ROOT_DIR.is_dir(): return []
    return [read_group(path.name) for path in sorted(ROOT_DIR.glob("v5grp_*"), reverse=True) if (path / "run_group.json").is_file()]


@router.get("/run-groups/{group_id}/children")
def list_children(group_id: str) -> list[dict]:
    try: return children(group_id)
    except (FileNotFoundError, ValueError) as exc: raise HTTPException(404, "unknown formal run group") from exc


@router.get("/run-groups/{group_id}/children/{child_id}")
def child_detail(group_id: str, child_id: str) -> dict:
    try: return read_child(group_id, child_id)
    except (FileNotFoundError, ValueError) as exc: raise HTTPException(404, "unknown child run") from exc


@router.get("/run-groups/{group_id}/metrics")
def group_metrics(group_id: str) -> dict:
    try: return read_group(group_id).get("aggregate", {})
    except (FileNotFoundError, ValueError) as exc: raise HTTPException(404, "unknown formal run group") from exc


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
    write_group(group)
    return group


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
