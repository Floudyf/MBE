from __future__ import annotations

import json
from pathlib import Path


def list_artifacts(run_dir: Path, run_id: str) -> list[dict]:
    root = run_dir.resolve()
    items: list[dict] = []
    for path in sorted(root.rglob("*")):
        if path.is_file():
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
    if not candidate.is_file():
        raise FileNotFoundError(filename)
    return candidate


def read_summary(run_dir: Path) -> dict:
    path = run_dir / "real_cluster_summary.json"
    return json.loads(path.read_text(encoding="utf-8")) if path.is_file() else {}
