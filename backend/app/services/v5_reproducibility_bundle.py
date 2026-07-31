from __future__ import annotations

import json
import zipfile
from pathlib import Path

from backend.app.services.v5_artifact_contract import file_sha256


def build(group_dir: Path, group: dict) -> Path:
    files = [path for path in group_dir.rglob("*") if path.is_file() and path.name != "artifacts.zip"]
    manifest = {
        "run_group_id": group["run_group_id"],
        "file_count": len(files),
        "files": [
            {
                "name": path.relative_to(group_dir).as_posix(),
                "size_bytes": path.stat().st_size,
                "sha256": file_sha256(path),
            }
            for path in files
        ],
    }
    (group_dir / "reproducibility_manifest.json").write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")
    (group_dir / "artifact_manifest.json").write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")
    output = group_dir / "artifacts.zip"
    with zipfile.ZipFile(output, "w", zipfile.ZIP_DEFLATED) as archive:
        for path in files + [group_dir / "reproducibility_manifest.json", group_dir / "artifact_manifest.json"]:
            if path.is_file():
                archive.write(path, path.relative_to(group_dir).as_posix())
    return output
