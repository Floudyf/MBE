from __future__ import annotations

from fastapi import APIRouter, HTTPException
from fastapi.responses import FileResponse

from backend.app.models.v4_realism import V4RealismSmokeRequest
from backend.app.services import v4_realism_runner

router = APIRouter()


@router.get("/api/v4/realism/status")
def v4_realism_status() -> dict:
    return v4_realism_runner.status()


@router.post("/api/v4/realism/smoke")
def v4_realism_smoke(payload: V4RealismSmokeRequest) -> dict:
    result = v4_realism_runner.run_smoke(payload)
    return result


@router.get("/api/v4/realism/runs/{run_id}/summary")
def v4_realism_run_summary(run_id: str) -> dict:
    try:
        return v4_realism_runner.get_summary(run_id)
    except ValueError as exc:
        raise HTTPException(400, str(exc)) from exc


@router.get("/api/v4/realism/runs/{run_id}/artifacts")
def v4_realism_run_artifacts(run_id: str) -> dict:
    try:
        return {"run_id": run_id, "artifacts": v4_realism_runner.list_artifacts(run_id)}
    except ValueError as exc:
        raise HTTPException(400, str(exc)) from exc


@router.get("/api/v4/realism/runs/{run_id}/artifacts/{filename:path}")
def v4_realism_artifact(run_id: str, filename: str) -> FileResponse:
    try:
        return FileResponse(v4_realism_runner.artifact_path(run_id, filename))
    except (ValueError, FileNotFoundError) as exc:
        raise HTTPException(404, str(exc)) from exc

