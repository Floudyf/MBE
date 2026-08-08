from __future__ import annotations

import json
import zipfile
from pathlib import Path

from backend.app.services import v5_real_cluster_runner
from backend.app.services.v5_artifact_contract import file_sha256
from backend.app.services.v5_formal_run_store import children

_DIAGNOSTIC_EXTENSIONS = {".json", ".jsonl", ".csv", ".log", ".txt", ".md", ".yaml", ".yml", ".toml", ".trace"}
_MAX_DIAGNOSTIC_FILE_BYTES = 128 * 1024 * 1024
_MAX_DIAGNOSTIC_TOTAL_BYTES = 512 * 1024 * 1024


def build(group_dir: Path, group: dict) -> Path:
    group_files = [path for path in group_dir.rglob("*") if path.is_file() and path.name not in {"artifacts.zip", "reproducibility_manifest.json", "artifact_manifest.json"}]
    archive_entries: list[tuple[Path, str, str]] = [
        (path, path.relative_to(group_dir).as_posix(), "run_group") for path in group_files
    ]
    archive_entries.extend(_failed_runtime_diagnostics(group_dir, group))
    manifest = {
        "run_group_id": group["run_group_id"],
        "file_count": len(archive_entries),
        "files": [
            {
                "name": archive_name,
                "source": source,
                "size_bytes": path.stat().st_size,
                "sha256": file_sha256(path),
            }
            for path, archive_name, source in archive_entries
        ],
    }
    reproducibility_manifest = group_dir / "reproducibility_manifest.json"
    artifact_manifest = group_dir / "artifact_manifest.json"
    reproducibility_manifest.write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")
    artifact_manifest.write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")
    output = group_dir / "artifacts.zip"
    with zipfile.ZipFile(output, "w", zipfile.ZIP_DEFLATED, allowZip64=True) as archive:
        for path, archive_name, _ in archive_entries:
            if path.is_file():
                archive.write(path, archive_name)
        archive.write(reproducibility_manifest, reproducibility_manifest.name)
        archive.write(artifact_manifest, artifact_manifest.name)
    return output


def _failed_runtime_diagnostics(group_dir: Path, group: dict) -> list[tuple[Path, str, str]]:
    entries: list[tuple[Path, str, str]] = []
    total = 0
    group_id = str(group.get("run_group_id") or group_dir.name)
    for child in children(group_id):
        if child.get("status") == "completed":
            continue
        result = child.get("result") if isinstance(child.get("result"), dict) else {}
        run_id = str(result.get("run_id") or "")
        child_id = str(child.get("child_run_id") or "unknown_child")
        if not run_id:
            continue
        try:
            runtime_root = v5_real_cluster_runner.run_dir(run_id)
        except ValueError:
            continue
        if not runtime_root.is_dir():
            continue
        for path in sorted(runtime_root.rglob("*")):
            if not path.is_file() or path.suffix.lower() not in _DIAGNOSTIC_EXTENSIONS:
                continue
            size = path.stat().st_size
            if size > _MAX_DIAGNOSTIC_FILE_BYTES or total + size > _MAX_DIAGNOSTIC_TOTAL_BYTES:
                continue
            archive_name = (Path("runtime_diagnostics") / child_id / path.relative_to(runtime_root)).as_posix()
            entries.append((path, archive_name, "failed_child_runtime"))
            total += size
    return entries
