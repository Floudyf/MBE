from __future__ import annotations

import subprocess
from pathlib import Path
from urllib.parse import quote

from fastapi import APIRouter, HTTPException
from fastapi.responses import FileResponse, StreamingResponse

from backend.app.models.v5_experiment_spec import V5ExperimentSpec
from backend.app.services import v5_artifact_storage, v5_real_cluster_artifacts, v5_real_cluster_runner


router = APIRouter(prefix="/api/v5/real-cluster", tags=["v5"])


@router.get("/status")
def status() -> dict:
    return v5_real_cluster_runner.status()


@router.post("/run")
def run(payload: V5ExperimentSpec) -> dict:
    try:
        return v5_real_cluster_runner.run(payload)
    except (ValueError, subprocess.TimeoutExpired) as exc:
        raise HTTPException(400, str(exc)) from exc


@router.get("/runs/{run_id}/summary")
def summary(run_id: str) -> dict:
    try:
        return v5_real_cluster_artifacts.read_summary(v5_real_cluster_runner.run_dir(run_id))
    except (ValueError, FileNotFoundError) as exc:
        raise HTTPException(404, str(exc)) from exc


@router.get("/runs/{run_id}/artifacts")
def artifacts(run_id: str) -> dict:
    try:
        return {"run_id": run_id, "artifacts": v5_real_cluster_artifacts.list_artifacts(v5_real_cluster_runner.run_dir(run_id), run_id)}
    except ValueError as exc:
        raise HTTPException(400, str(exc)) from exc


@router.get("/runs/{run_id}/artifacts/{filename:path}")
def artifact(run_id: str, filename: str):
    try:
        run_dir = v5_real_cluster_runner.run_dir(run_id)
        return FileResponse(v5_real_cluster_artifacts.artifact_path(run_dir, filename))
    except ValueError as exc:
        raise HTTPException(404, str(exc)) from exc
    except FileNotFoundError:
        try:
            run_dir = v5_real_cluster_runner.run_dir(run_id)
            info = v5_artifact_storage.archived_artifact_info(run_dir, filename)
            headers = {
                "Content-Disposition": f"attachment; filename*=UTF-8''{quote(Path(filename).name)}",
                "Content-Length": str(int(info["size_bytes"])),
                "X-MBE-Artifact-Storage": "tar.zst",
                "X-MBE-Artifact-SHA256": str(info.get("sha256") or ""),
            }
            return StreamingResponse(
                v5_artifact_storage.stream_archived_artifact(run_dir, filename),
                media_type="application/octet-stream",
                headers=headers,
            )
        except FileNotFoundError as exc:
            raise HTTPException(404, str(exc)) from exc
        except v5_artifact_storage.ArtifactStorageError as exc:
            raise HTTPException(409, f"archived artifact integrity/storage error: {exc}") from exc
