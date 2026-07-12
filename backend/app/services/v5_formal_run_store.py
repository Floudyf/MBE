from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path
from uuid import uuid4

from backend.app.core.paths import ROOT

ROOT_DIR = ROOT / ".cache" / "v5_formal_runs"


def now() -> str:
    return datetime.now(UTC).isoformat()


def group_dir(group_id: str) -> Path:
    if not group_id.startswith("v5grp_") or "/" in group_id or "\\" in group_id:
        raise ValueError("invalid V5 formal run group id")
    return ROOT_DIR / group_id


def create_group(payload: dict) -> dict:
    group_id = "v5grp_" + datetime.now(UTC).strftime("%Y%m%d_%H%M%S_") + uuid4().hex[:8]
    record = {"run_group_id": group_id, "status": "queued", "created_at": now(), "updated_at": now(), "cancel_requested": False, **payload}
    write_group(record)
    return record


def write_group(record: dict) -> None:
    directory = group_dir(record["run_group_id"])
    directory.mkdir(parents=True, exist_ok=True)
    record["updated_at"] = now()
    (directory / "run_group.json").write_text(json.dumps(record, indent=2) + "\n", encoding="utf-8")


def read_group(group_id: str) -> dict:
    path = group_dir(group_id) / "run_group.json"
    if not path.is_file():
        raise FileNotFoundError(group_id)
    return json.loads(path.read_text(encoding="utf-8"))


def write_child(group_id: str, child: dict) -> None:
    directory = group_dir(group_id) / "children"
    directory.mkdir(parents=True, exist_ok=True)
    (directory / f"{child['child_run_id']}.json").write_text(json.dumps(child, indent=2) + "\n", encoding="utf-8")


def write_attempt(group_id: str, child_id: str, attempt: dict) -> None:
    directory = group_dir(group_id) / "children" / child_id
    directory.mkdir(parents=True, exist_ok=True)
    number = int(attempt.get("attempt_number", 1))
    (directory / f"attempt_{number}.json").write_text(json.dumps(attempt, indent=2) + "\n", encoding="utf-8")


def children(group_id: str) -> list[dict]:
    directory = group_dir(group_id) / "children"
    if not directory.is_dir():
        return []
    return [json.loads(path.read_text(encoding="utf-8")) for path in sorted(directory.glob("v5child_*.json"))]


def read_child(group_id: str, child_id: str) -> dict:
    if not child_id.startswith("v5child_") or "/" in child_id or "\\" in child_id:
        raise ValueError("invalid V5 child id")
    path = group_dir(group_id) / "children" / f"{child_id}.json"
    if not path.is_file(): raise FileNotFoundError(child_id)
    return json.loads(path.read_text(encoding="utf-8"))
