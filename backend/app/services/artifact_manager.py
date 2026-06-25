from __future__ import annotations

import shutil
from pathlib import Path

ARTIFACT_ALLOWLIST = frozenset({
    "config.yaml",
    "used_config.yaml",
    "used_config.json",
    "trace_meta.json",
    "summary.csv",
    "latency.csv",
    "runtime.log",
    "report.md",
    "dual_chain_summary.csv",
    "dual_chain_summary.json",
    "stage_metrics.csv",
    "protocol_summary.csv",
    "protocol_summary.json",
    "protocol_results.csv",
    "protocol_events.csv",
    "sweep_summary.csv",
    "sweep_summary.json",
    "sweep_report.md",
    "case_artifacts_index.json",
})


class ArtifactError(ValueError):
    """Raised when an artifact request is malformed."""


class ArtifactForbidden(PermissionError):
    """Raised when an artifact is not downloadable."""


class ArtifactMissing(FileNotFoundError):
    """Raised when a valid artifact does not exist."""


def validate_filename(filename: str) -> None:
    if "/" in filename or "\\" in filename or filename in {".", ".."}:
        raise ArtifactError("invalid artifact filename")
    if filename not in ARTIFACT_ALLOWLIST:
        raise ArtifactForbidden("artifact is not downloadable")


def list_artifacts(run_dir: Path, run_id: str) -> list[dict[str, object]]:
    artifacts = []
    for filename in sorted(ARTIFACT_ALLOWLIST):
        path = run_dir / filename
        if path.is_file():
            artifacts.append({
                "name": filename,
                "download_url": f"/api/v2/runs/{run_id}/artifacts/{filename}",
                "size_bytes": path.stat().st_size,
            })
    return artifacts


def get_artifact_path(run_dir: Path, filename: str) -> Path:
    validate_filename(filename)
    root = run_dir.resolve()
    path = (run_dir / filename).resolve()
    try:
        path.relative_to(root)
    except ValueError as exc:
        raise ArtifactError("artifact path must stay inside the run directory") from exc
    if not path.is_file():
        raise ArtifactMissing("artifact not found")
    return path


def mirror_run_to_latest(run_dir: Path, latest_dir: Path) -> None:
    if latest_dir.exists():
        shutil.rmtree(latest_dir)
    latest_dir.parent.mkdir(parents=True, exist_ok=True)
    shutil.copytree(run_dir, latest_dir)
