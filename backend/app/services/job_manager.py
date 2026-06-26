from __future__ import annotations

import json
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from backend.app.services.artifact_manager import list_artifacts
from backend.app.services.run_id import new_run_id

ROOT = Path(__file__).resolve().parents[3]
DEFAULT_JOBS_ROOT = ROOT / ".cache/v2_jobs"
VALID_STATUSES = {"created", "running", "completed", "failed", "cancelled"}


class JobNotFound(FileNotFoundError):
    """Raised when a run_id has no metadata."""


def utc_now_text() -> str:
    return datetime.now(UTC).replace(tzinfo=None).isoformat(timespec="microseconds")


class JobManager:
    def __init__(self, root: Path = DEFAULT_JOBS_ROOT):
        self.root = root

    def create_run(
        self,
        source: str,
        experiment_name: str,
        data_truth_label: str = "",
        stage: str = "V2.2",
        extra_metadata: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        self.root.mkdir(parents=True, exist_ok=True)
        run_id = self._unique_run_id()
        run_dir = self.run_dir(run_id)
        run_dir.mkdir(parents=True, exist_ok=False)
        now = utc_now_text()
        metadata = {
            "run_id": run_id,
            "created_at": now,
            "created_sequence": time.time_ns(),
            "updated_at": now,
            "stage": stage,
            "source": source,
            "experiment_name": experiment_name,
            "status": "created",
            "status_message": "created",
            "output_dir": str(run_dir),
            "data_truth_label": data_truth_label,
            "summary_available": False,
            "report_available": False,
            "artifact_count": 0,
        }
        if extra_metadata:
            metadata.update(extra_metadata)
        self.write_metadata(metadata)
        return metadata

    def mark_running(self, run_id: str) -> dict[str, Any]:
        return self.update_run(run_id, status="running", status_message="running")

    def mark_completed(self, run_id: str, data_truth_label: str | None = None) -> dict[str, Any]:
        updates: dict[str, Any] = {"status": "completed", "status_message": "completed"}
        if data_truth_label is not None:
            updates["data_truth_label"] = data_truth_label
        return self.update_run(run_id, **updates)

    def mark_failed(self, run_id: str, message: str) -> dict[str, Any]:
        return self.update_run(run_id, status="failed", status_message=message)

    def update_run(self, run_id: str, **updates: Any) -> dict[str, Any]:
        metadata = self.get_run(run_id)
        status = updates.get("status")
        if status is not None and status not in VALID_STATUSES:
            raise ValueError(f"invalid run status {status}")
        metadata.update(updates)
        metadata["updated_at"] = utc_now_text()
        metadata["summary_available"] = (
            (self.run_dir(run_id) / "summary.csv").is_file()
            or (self.run_dir(run_id) / "dual_chain_summary.csv").is_file()
            or (self.run_dir(run_id) / "protocol_summary.csv").is_file()
            or (self.run_dir(run_id) / "sweep_summary.csv").is_file()
            or (self.run_dir(run_id) / "calibration_summary.csv").is_file()
        )
        metadata["report_available"] = (self.run_dir(run_id) / "report.md").is_file() or (self.run_dir(run_id) / "sweep_report.md").is_file() or (self.run_dir(run_id) / "calibration_report.md").is_file()
        metadata["artifact_count"] = len(list_artifacts(self.run_dir(run_id), run_id))
        self.write_metadata(metadata)
        return metadata

    def get_run(self, run_id: str) -> dict[str, Any]:
        path = self.metadata_path(run_id)
        if not path.is_file():
            raise JobNotFound(f"unknown run_id {run_id}")
        return json.loads(path.read_text(encoding="utf-8"))

    def list_runs(self, limit: int = 50) -> list[dict[str, Any]]:
        if not self.root.exists():
            return []
        runs = []
        for metadata_path in self.root.glob("*/metadata.json"):
            metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
            metadata["_sort_mtime_ns"] = metadata_path.stat().st_mtime_ns
            runs.append(metadata)
        runs.sort(key=lambda item: (item.get("created_sequence", 0), item.get("created_at", ""), item.get("updated_at", ""), item.get("_sort_mtime_ns", 0)), reverse=True)
        for item in runs:
            item.pop("_sort_mtime_ns", None)
        return runs[: max(1, min(limit, 200))]

    def get_latest_run(self) -> dict[str, Any]:
        runs = self.list_runs(limit=1)
        if not runs:
            raise JobNotFound("no V2 runs recorded")
        return runs[0]

    def write_metadata(self, metadata: dict[str, Any]) -> None:
        self.run_dir(metadata["run_id"]).mkdir(parents=True, exist_ok=True)
        self.metadata_path(metadata["run_id"]).write_text(json.dumps(metadata, indent=2) + "\n", encoding="utf-8")

    def run_dir(self, run_id: str) -> Path:
        if "/" in run_id or "\\" in run_id or run_id in {".", ".."}:
            raise JobNotFound(f"invalid run_id {run_id}")
        return self.root / run_id

    def metadata_path(self, run_id: str) -> Path:
        return self.run_dir(run_id) / "metadata.json"

    def _unique_run_id(self) -> str:
        for _ in range(10):
            run_id = new_run_id()
            if not self.run_dir(run_id).exists():
                return run_id
        raise RuntimeError("unable to generate unique run_id")
