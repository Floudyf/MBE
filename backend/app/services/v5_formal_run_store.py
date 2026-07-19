from __future__ import annotations

import json
import os
import threading
import time
from datetime import UTC, datetime
from pathlib import Path
from uuid import uuid4

from backend.app.core.paths import ROOT

ROOT_DIR = ROOT / ".cache" / "v5_formal_runs"
_LOCKS_GUARD = threading.Lock()
_PATH_LOCKS: dict[str, threading.Lock] = {}


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
    _atomic_write_json(directory / "run_group.json", record)


def read_group(group_id: str) -> dict:
    path = group_dir(group_id) / "run_group.json"
    if not path.is_file():
        raise FileNotFoundError(group_id)
    return _read_json(path)


def write_child(group_id: str, child: dict) -> None:
    directory = group_dir(group_id) / "children"
    directory.mkdir(parents=True, exist_ok=True)
    _atomic_write_json(directory / f"{child['child_run_id']}.json", child)


def write_attempt(group_id: str, child_id: str, attempt: dict) -> None:
    directory = group_dir(group_id) / "children" / child_id
    directory.mkdir(parents=True, exist_ok=True)
    number = int(attempt.get("attempt_number", 1))
    _atomic_write_json(directory / f"attempt_{number}.json", attempt)


def children(group_id: str) -> list[dict]:
    directory = group_dir(group_id) / "children"
    if not directory.is_dir():
        return []
    return [_read_json(path) for path in sorted(directory.glob("v5child_*.json"))]


def read_child(group_id: str, child_id: str) -> dict:
    if not child_id.startswith("v5child_") or "/" in child_id or "\\" in child_id:
        raise ValueError("invalid V5 child id")
    path = group_dir(group_id) / "children" / f"{child_id}.json"
    if not path.is_file(): raise FileNotFoundError(child_id)
    return _read_json(path)


def _atomic_write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path.with_name(f".{path.name}.{uuid4().hex}.tmp")
    with _path_lock(path):
        try:
            with tmp_path.open("w", encoding="utf-8") as handle:
                json.dump(payload, handle, indent=2)
                handle.write("\n")
                handle.flush()
                try:
                    os.fsync(handle.fileno())
                except OSError:
                    pass
            _replace_atomic(tmp_path, path)
        finally:
            try:
                tmp_path.unlink()
            except FileNotFoundError:
                pass


def _read_json(path: Path) -> dict:
    with _path_lock(path):
        for attempt in range(200):
            try:
                return json.loads(path.read_text(encoding="utf-8"))
            except PermissionError:
                if attempt == 199:
                    raise
                time.sleep(0.005)
        raise RuntimeError("unreachable")


def _path_lock(path: Path) -> threading.Lock:
    key = str(path.resolve())
    with _LOCKS_GUARD:
        lock = _PATH_LOCKS.get(key)
        if lock is None:
            lock = threading.Lock()
            _PATH_LOCKS[key] = lock
        return lock


def _replace_atomic(tmp_path: Path, path: Path) -> None:
    # Windows can reject replacing a file that a concurrent reader has open for
    # a few milliseconds.  The target remains the previous complete JSON until
    # this bounded retry succeeds, so readers never observe partial content.
    for attempt in range(200):
        try:
            os.replace(tmp_path, path)
            return
        except PermissionError:
            if attempt == 199:
                raise
            time.sleep(0.005)
