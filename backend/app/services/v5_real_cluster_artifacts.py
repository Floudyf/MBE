from __future__ import annotations

import json
from pathlib import Path

from backend.app.services import v5_artifact_storage


def _frozen_catalog(run_dir: Path) -> list[dict]:
    path = run_dir / "artifact_catalog.json"
    if not path.is_file():
        return []
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return []
    files = value.get("files") if isinstance(value, dict) else None
    return [dict(item) for item in files if isinstance(item, dict)] if isinstance(files, list) else []


def list_artifacts(run_dir: Path, run_id: str) -> list[dict]:
    root = run_dir.resolve()
    storage = v5_artifact_storage.read_storage_summary(root)
    if storage.get("storage_state") in {"archived", "archive_online", "archive_verified_pending_raw_cleanup", "archive_verified_raw_cleanup_incomplete"}:
        frozen = _frozen_catalog(root)
        if frozen:
            return [
                {
                    **item,
                    "truth_category": item.get("truth_category") or item.get("artifact_role") or "runtime_artifact",
                    "download_url": item.get("download_url") or f"/api/v5/real-cluster/runs/{run_id}/artifacts/{item.get('name', '')}",
                }
                for item in frozen
                if item.get("name")
            ]
    items: list[dict] = []
    archive_root = (root / v5_artifact_storage.ARCHIVE_DIR_NAME).resolve()
    for path in sorted(root.rglob("*")):
        if not path.is_file():
            continue
        resolved = path.resolve()
        if resolved == archive_root or archive_root in resolved.parents:
            continue
        rel = path.relative_to(root).as_posix()
        items.append({"name": rel, "size_bytes": path.stat().st_size, "truth_category": "runtime_artifact", "download_url": f"/api/v5/real-cluster/runs/{run_id}/artifacts/{rel}"})
    return items


def artifact_path(run_dir: Path, filename: str) -> Path:
    root = run_dir.resolve()
    candidate = (root / filename).resolve()
    try:
        candidate.relative_to(root)
    except ValueError as exc:
        raise ValueError("artifact path escapes run directory") from exc
    archive_root = (root / v5_artifact_storage.ARCHIVE_DIR_NAME).resolve()
    if candidate == archive_root or archive_root in candidate.parents:
        raise FileNotFoundError(filename)
    if not candidate.is_file():
        raise FileNotFoundError(filename)
    return candidate


def read_summary(run_dir: Path) -> dict:
    path = run_dir / "real_cluster_summary.json"
    return json.loads(path.read_text(encoding="utf-8")) if path.is_file() else {}
