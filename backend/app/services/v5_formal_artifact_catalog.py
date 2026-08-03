from __future__ import annotations

import json
from pathlib import Path
import re
from urllib.parse import quote

from backend.app.services.v5_artifact_contract import catalog_entry


def read_catalog(group_dir: Path, run_group_id: str) -> dict:
    manifest_path = group_dir / "artifact_manifest.json"
    bundle_path = group_dir / "artifacts.zip"
    if not manifest_path.is_file():
        return {
            "run_group_id": run_group_id,
            "status": "pending",
            "bundle_ready": False,
            "bundle_size_bytes": 0,
            "file_count": 0,
            "files": [],
        }

    try:
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        manifest = {}
    files = []
    seen = set()
    for item in manifest.get("files", []) if isinstance(manifest, dict) and isinstance(manifest.get("files"), list) else []:
        if not isinstance(item, dict):
            continue
        name = safe_artifact_name(item.get("name"))
        size = item.get("size_bytes")
        if name is None or type(size) is not int or size < 0 or name in seen:
            continue
        seen.add(name)
        files.append(_catalog_entry(group_dir, run_group_id, name, size, item))
    return {
        "run_group_id": run_group_id,
        "status": "ready",
        "bundle_ready": bundle_path.is_file(),
        "bundle_size_bytes": bundle_path.stat().st_size if bundle_path.is_file() else 0,
        "file_count": len(files),
        "files": files,
    }


def safe_artifact_name(value: object) -> str | None:
    if not isinstance(value, str):
        return None
    name = value.replace("\\", "/")
    if not name or name.startswith("/") or re.match(r"^[A-Za-z]:", name):
        return None
    parts = name.split("/")
    if any(part in {"", ".", ".."} for part in parts):
        return None
    return name


def _catalog_entry(group_dir: Path, run_group_id: str, name: str, size: int, item: dict) -> dict:
    sha256 = item.get("sha256") if isinstance(item.get("sha256"), str) else None
    return catalog_entry(group_dir, name, size, download_url=f"/api/v5/formal/run-groups/{quote(run_group_id, safe='')}/artifacts/{quote(name, safe='/')}", sha256=sha256)
