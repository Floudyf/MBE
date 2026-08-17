from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path
import os
import threading
import time
from uuid import uuid4

from backend.app.services import v5_artifact_storage, v5_real_cluster_runner
from backend.app.services.v5_formal_run_store import children, read_child, read_group, write_child, write_group

TERMINAL_STATUSES = {"completed", "completed_with_failures", "failed", "cancelled", "interrupted"}
_STORAGE_ELIGIBLE_CHILD_STATUSES = {"completed", "failed", "timed_out", "cancelled", "interrupted"}
_AUTO_ARCHIVE_CHILD_STATUSES = {"completed", "failed", "timed_out"}
_ARCHIVE_STATES = {
    "archived",
    "archive_online",
    "archive_verified_pending_raw_cleanup",
    "archive_verified_raw_cleanup_incomplete",
}
_RESTORABLE_STATES = {
    "archived",
    "archive_verified_pending_raw_cleanup",
    "archive_verified_raw_cleanup_incomplete",
}


class FormalArtifactStorageError(RuntimeError):
    pass


_JOB_LOCK = threading.Lock()
_JOB_THREADS: dict[str, threading.Thread] = {}
_JOB_PERSIST_INTERVAL_SECONDS = 1.5
_JOB_STALE_SECONDS = 15.0
_ACTIVE_JOB_STATES = {"queued", "running"}


def _now() -> str:
    return datetime.now(UTC).isoformat()


def _job_active(group_id: str) -> bool:
    with _JOB_LOCK:
        worker = _JOB_THREADS.get(group_id)
        return bool(worker and worker.is_alive())


def _set_job_thread(group_id: str, worker: threading.Thread | None) -> None:
    with _JOB_LOCK:
        if worker is None:
            _JOB_THREADS.pop(group_id, None)
        else:
            _JOB_THREADS[group_id] = worker


def _job_stale(job: dict) -> bool:
    text = str(job.get("heartbeat_at") or "")
    if not text:
        return True
    try:
        heartbeat = datetime.fromisoformat(text.replace("Z", "+00:00"))
        if heartbeat.tzinfo is None:
            heartbeat = heartbeat.replace(tzinfo=UTC)
        return (datetime.now(UTC) - heartbeat.astimezone(UTC)).total_seconds() >= _JOB_STALE_SECONDS
    except ValueError:
        return True


def _reconcile_storage_job(group_id: str, group: dict | None = None) -> dict:
    current = dict(group or read_group(group_id))
    job = current.get("artifact_storage_job") if isinstance(current.get("artifact_storage_job"), dict) else {}
    if job.get("status") in _ACTIVE_JOB_STATES and not _job_active(group_id) and _job_stale(job):
        job = {**job, "status": "interrupted", "phase": "interrupted", "interrupted_at": _now(), "heartbeat_at": _now()}
        current["artifact_storage_job"] = job
        write_group(current)
    return current

def _job_child_fields(child: dict) -> dict:
    workload = child.get("workload_point") if isinstance(child.get("workload_point"), dict) else {}
    return {
        "current_child_run_id": child.get("child_run_id"),
        "current_method_id": child.get("method_config_id") or child.get("method_id"),
        "current_method_name": _method_name(child),
        "current_repeat_index": child.get("repeat_index"),
        "current_theta": workload.get("target_theta"),
        "current_alpha": workload.get("target_alpha"),
        "current_run_id": _run_id(child),
    }

def _write_job(group_id: str, job_id: str, updates: dict) -> dict:
    group = read_group(group_id)
    current = group.get("artifact_storage_job") if isinstance(group.get("artifact_storage_job"), dict) else {}
    if str(current.get("job_id") or "") != job_id:
        return current
    next_job = {**current, **updates, "heartbeat_at": _now()}
    group["artifact_storage_job"] = next_job
    write_group(group)
    return next_job


def _progress_writer(group_id: str, job_id: str):
    state = {"last_write": 0.0}
    def update(payload: dict) -> None:
        now = time.monotonic()
        phase = str(payload.get("phase") or "")
        if now - state["last_write"] < _JOB_PERSIST_INTERVAL_SECONDS and phase not in {"completed", "native_tar_fallback"}:
            return
        state["last_write"] = now
        allowed = {
            "phase", "archive_engine", "native_tar_executable", "native_tar_error", "source_file_count",
            "processed_file_count", "source_logical_bytes", "processed_source_bytes", "processed_tar_bytes",
            "archive_bytes_written", "archive_bytes", "verified_file_count", "released_file_count",
            "raw_deleted_after_verification",
        }
        _write_job(group_id, job_id, {key: value for key, value in payload.items() if key in allowed})
    return update


def _terminal_group(group: dict) -> None:
    if group.get("status") not in TERMINAL_STATUSES:
        raise FormalArtifactStorageError("artifact storage operations require a terminal RunGroup")


def _run_id(child: dict) -> str:
    result = child.get("result") if isinstance(child.get("result"), dict) else {}
    run_id = str(result.get("run_id") or "")
    return run_id if run_id.startswith("v5_") else ""


def _method_name(child: dict) -> str:
    method = child.get("method") if isinstance(child.get("method"), dict) else {}
    return str(method.get("display_name") or child.get("method_name") or child.get("method_config_id") or "")


def _logical_bytes_from_child(child: dict) -> int:
    result = child.get("result") if isinstance(child.get("result"), dict) else {}
    artifacts = result.get("artifacts") if isinstance(result.get("artifacts"), list) else []
    return sum(int(item.get("size_bytes") or 0) for item in artifacts if isinstance(item, dict))



def _artifact_count_from_child(child: dict) -> int:
    result = child.get("result") if isinstance(child.get("result"), dict) else {}
    artifacts = result.get("artifacts") if isinstance(result.get("artifacts"), list) else []
    return sum(1 for item in artifacts if isinstance(item, dict))


def _persist_child_storage(group_id: str, child: dict, storage: dict) -> dict:
    child = dict(child)
    child["artifact_storage"] = storage
    result = dict(child.get("result") or {})
    summary = dict(result.get("summary") or {})
    summary["artifact_storage"] = storage
    result["summary"] = summary
    child["result"] = result
    write_child(group_id, child)
    return child


def _eligible_child(child: dict) -> bool:
    return str(child.get("status") or "") in _STORAGE_ELIGIBLE_CHILD_STATUSES


def _measure_unmanaged_run(run_dir: Path, run_id: str) -> dict:
    measured = v5_artifact_storage.measure_online_tree(run_dir)
    return {
        "run_id": run_id,
        "storage_state": "online_unmanaged",
        "original_file_count": int(measured["file_count"]),
        "original_logical_bytes": int(measured["logical_bytes"]),
        "online_logical_bytes": int(measured["logical_bytes"]),
        "online_physical_bytes": int(measured["physical_bytes"]),
        "current_effective_bytes": int(measured["physical_bytes"]),
        "saved_bytes": max(0, int(measured["logical_bytes"]) - int(measured["physical_bytes"])),
        "saving_ratio": (1.0 - int(measured["physical_bytes"]) / int(measured["logical_bytes"])) if measured["logical_bytes"] else 0.0,
        "formal_eligibility_affected": False,
    }


def auto_archive_terminal_child(group_id: str, child_id: str, *, compression_level: int = 3) -> dict:
    """Cold-archive a measured terminal child before the next Formal child starts.

    This is failure-open by design. Formal execution status, metrics and correctness
    were already frozen in the child record before this function is called.
    """
    child = read_child(group_id, child_id)
    status_value = str(child.get("status") or "")
    execution_status = str(child.get("execution_status") or "")
    if status_value not in _AUTO_ARCHIVE_CHILD_STATUSES:
        return {"attempted": False, "reason": f"child_status:{status_value}"}
    if execution_status == "blocked_resource_disk_pressure":
        return {"attempted": False, "reason": "disk_pressure_child"}
    run_id = _run_id(child)
    if not run_id:
        return {"attempted": False, "reason": "no_real_cluster_run_id"}
    run_dir = v5_real_cluster_runner.run_dir(run_id)
    if not run_dir.is_dir():
        return {"attempted": False, "reason": "real_cluster_run_dir_missing", "run_id": run_id}
    try:
        storage = v5_artifact_storage.archive_run(
            run_dir,
            run_id=run_id,
            delete_raw=True,
            compression_level=compression_level,
        )
        _persist_child_storage(group_id, child, storage)
        return {
            "attempted": True,
            "succeeded": bool(storage.get("archive_verified")) and bool(storage.get("raw_deleted_after_verification")),
            "run_id": run_id,
            "storage_state": storage.get("storage_state"),
            "archive_bytes": storage.get("archive_bytes"),
            "raw_deleted_after_verification": storage.get("raw_deleted_after_verification"),
            "error": storage.get("raw_cleanup_error") or "",
        }
    except Exception as exc:
        child = dict(child)
        child["artifact_storage_auto_archive"] = {
            "attempted": True,
            "succeeded": False,
            "error": str(exc),
            "formal_eligibility_affected": False,
        }
        write_child(group_id, child)
        return {"attempted": True, "succeeded": False, "run_id": run_id, "error": str(exc)}


def _child_status(child: dict) -> dict:
    run_id = _run_id(child)
    storage: dict = {}
    run_dir: Path | None = None
    if run_id:
        run_dir = v5_real_cluster_runner.run_dir(run_id)
        if run_dir.is_dir():
            storage = v5_artifact_storage.read_storage_summary(run_dir)
    frozen = child.get("artifact_storage") if isinstance(child.get("artifact_storage"), dict) else {}
    if not storage:
        result = child.get("result") if isinstance(child.get("result"), dict) else {}
        summary = result.get("summary") if isinstance(result.get("summary"), dict) else {}
        from_summary = summary.get("artifact_storage") if isinstance(summary.get("artifact_storage"), dict) else {}
        storage = dict(from_summary or frozen)
    if not storage and run_dir is not None and run_dir.is_dir():
        try:
            storage = _measure_unmanaged_run(run_dir, run_id)
        except Exception:
            storage = {}
    logical = int(storage.get("original_logical_bytes") or _logical_bytes_from_child(child))
    state = str(storage.get("storage_state") or ("online_unmanaged" if run_dir and run_dir.is_dir() else "unavailable"))
    effective = storage.get("current_effective_bytes")
    if effective is None:
        effective = storage.get("online_physical_bytes")
    return {
        "child_run_id": child.get("child_run_id"),
        "run_id": run_id,
        "method_id": child.get("method_config_id") or child.get("method_id"),
        "method_name": _method_name(child),
        "seed": child.get("seed"),
        "repeat_index": child.get("repeat_index"),
        "child_status": child.get("status"),
        "formal_eligibility": child.get("formal_eligibility"),
        "storage_state": state,
        "original_file_count": int(storage.get("original_file_count") or child.get("runtime_artifact_count") or _artifact_count_from_child(child)),
        "original_logical_bytes": logical,
        "online_physical_bytes": storage.get("online_physical_bytes"),
        "current_effective_bytes": effective,
        "saved_bytes": storage.get("saved_bytes"),
        "saving_ratio": storage.get("saving_ratio"),
        "ntfs_compression_succeeded": storage.get("ntfs_compression_succeeded"),
        "ntfs_compression_error": storage.get("ntfs_compression_error"),
        "archive_format": storage.get("archive_format"),
        "archive_bytes": storage.get("archive_bytes"),
        "archive_sha256": storage.get("archive_sha256"),
        "archive_verified": storage.get("archive_verified"),
        "archived_file_count": storage.get("archived_file_count"),
        "raw_deleted_after_verification": storage.get("raw_deleted_after_verification"),
        "raw_cleanup_error": storage.get("raw_cleanup_error"),
        "archive_download_url": f"/api/v5/formal/run-groups/{child.get('run_group_id')}/storage/children/{child.get('child_run_id')}/archive" if storage.get("archive_relative_path") else None,
        "formal_eligibility_affected": False,
    }


def status(group_id: str) -> dict:
    group = _reconcile_storage_job(group_id)
    items = [_child_status(child) for child in children(group_id)]
    original = sum(int(item.get("original_logical_bytes") or 0) for item in items)
    known = [item for item in items if item.get("current_effective_bytes") is not None]
    unknown = [item for item in items if item.get("current_effective_bytes") is None]
    known_original = sum(int(item.get("original_logical_bytes") or 0) for item in known)
    known_effective = sum(int(item.get("current_effective_bytes") or 0) for item in known)
    effective = known_effective if not unknown else None
    archive_bytes = sum(int(item.get("archive_bytes") or 0) for item in items)
    known_saved = max(0, known_original - known_effective)
    saved = max(0, original - effective) if effective is not None else None
    persistent_errors = [
        {"child_run_id": item.get("child_run_id"), "error": str(item.get("raw_cleanup_error"))}
        for item in items
        if item.get("raw_cleanup_error")
    ]
    return {
        "schema_version": "mbe_v5_formal_artifact_storage_v2",
        "run_group_id": group_id,
        "group_status": group.get("status"),
        "operation_ready": group.get("status") in TERMINAL_STATUSES,
        "child_count": len(items),
        "run_dir_child_count": sum(bool(item.get("run_id")) for item in items),
        "unavailable_child_count": sum(item.get("storage_state") == "unavailable" for item in items),
        "managed_child_count": sum(item.get("storage_state") not in {"online_unmanaged", "unavailable"} for item in items),
        "ntfs_compressed_child_count": sum(bool(item.get("ntfs_compression_succeeded")) for item in items),
        "archived_child_count": sum(item.get("storage_state") == "archived" for item in items),
        "cold_archive_child_count": sum(bool(item.get("archive_download_url")) for item in items),
        "archive_online_child_count": sum(item.get("storage_state") in {"archive_online", "archive_verified_pending_raw_cleanup", "archive_verified_raw_cleanup_incomplete"} for item in items),
        "restorable_child_count": sum(item.get("storage_state") in _RESTORABLE_STATES for item in items),
        "restored_child_count": sum(str(item.get("storage_state") or "").startswith("online_restored") for item in items),
        "known_effective_child_count": len(known),
        "unmeasured_effective_child_count": len(unknown),
        "original_logical_bytes": original,
        "known_original_logical_bytes": known_original,
        "current_effective_bytes": effective,
        "current_known_effective_bytes": known_effective,
        "archive_bytes": archive_bytes,
        "saved_bytes": saved,
        "saving_ratio": (saved / original) if saved is not None and original else None,
        "known_saved_bytes": known_saved,
        "known_saving_ratio": (known_saved / known_original) if known_original else None,
        "artifact_storage_job": dict(group.get("artifact_storage_job") or {}) if isinstance(group.get("artifact_storage_job"), dict) else None,
        "children": items,
        "operation_errors": persistent_errors,
        "formal_eligibility_affected": False,
    }


def _update_group_storage(group_id: str) -> dict:
    group = read_group(group_id)
    current = status(group_id)
    group["artifact_storage"] = {
        key: current.get(key)
        for key in (
            "schema_version", "run_dir_child_count", "unavailable_child_count", "managed_child_count",
            "ntfs_compressed_child_count", "archived_child_count", "cold_archive_child_count", "archive_online_child_count",
            "restorable_child_count", "restored_child_count", "known_effective_child_count", "unmeasured_effective_child_count",
            "original_logical_bytes", "known_original_logical_bytes", "current_effective_bytes", "current_known_effective_bytes",
            "archive_bytes", "saved_bytes", "saving_ratio", "known_saved_bytes", "known_saving_ratio", "formal_eligibility_affected",
        )
    }
    write_group(group)
    return current


def _storage_candidates(group_id: str) -> list[dict]:
    return [child for child in children(group_id) if _eligible_child(child)]


def _job_worker(group_id: str, job_id: str, *, delete_raw: bool, compression_level: int) -> None:
    progress = _progress_writer(group_id, job_id)
    errors: list[dict] = []
    skipped: list[dict] = []
    processed = 0
    archived = 0
    try:
        _write_job(group_id, job_id, {"status": "running", "phase": "preparing", "started_at": _now()})
        candidates = _storage_candidates(group_id)
        for child in candidates:
            fields = _job_child_fields(child)
            _write_job(group_id, job_id, {**fields, "phase": "preparing_child", "processed_children": processed, "archived_children": archived, "error_count": len(errors), "skipped_count": len(skipped)})
            run_id = _run_id(child)
            if not run_id:
                skipped.append({"child_run_id": child.get("child_run_id"), "reason": "no_real_cluster_run_id"})
                processed += 1
                continue
            run_dir = v5_real_cluster_runner.run_dir(run_id)
            if not run_dir.is_dir():
                errors.append({"child_run_id": child.get("child_run_id"), "error": "real-cluster run directory is missing"})
                processed += 1
                continue
            existing = v5_artifact_storage.read_storage_summary(run_dir)
            if existing.get("storage_state") == "archived" and existing.get("raw_deleted_after_verification") is True:
                _persist_child_storage(group_id, child, existing)
                archived += 1
                processed += 1
                _write_job(group_id, job_id, {"phase": "already_archived", "processed_children": processed, "archived_children": archived, "error_count": len(errors), "skipped_count": len(skipped)})
                continue
            try:
                storage = v5_artifact_storage.archive_run(
                    run_dir,
                    run_id=run_id,
                    delete_raw=delete_raw,
                    compression_level=compression_level,
                    progress=progress,
                )
                _persist_child_storage(group_id, child, storage)
                if storage.get("archive_verified"):
                    archived += 1
                if storage.get("raw_cleanup_error"):
                    errors.append({"child_run_id": child.get("child_run_id"), "error": str(storage.get("raw_cleanup_error"))})
            except Exception as exc:
                errors.append({"child_run_id": child.get("child_run_id"), "error": str(exc)})
            processed += 1
            _write_job(group_id, job_id, {"phase": "child_finished", "processed_children": processed, "archived_children": archived, "error_count": len(errors), "skipped_count": len(skipped), "recent_errors": errors[-10:], "recent_skipped": skipped[-10:]})
        final_status = "completed_with_errors" if errors else "completed"
        _write_job(group_id, job_id, {"status": final_status, "phase": "completed", "processed_children": processed, "archived_children": archived, "error_count": len(errors), "skipped_count": len(skipped), "recent_errors": errors[-20:], "recent_skipped": skipped[-20:], "finished_at": _now(), "current_child_run_id": None, "current_run_id": None})
        _update_group_storage(group_id)
    except Exception as exc:
        _write_job(group_id, job_id, {"status": "failed", "phase": "failed", "fatal_error": str(exc), "finished_at": _now()})
    finally:
        _set_job_thread(group_id, None)


def start_archive_job(group_id: str, *, delete_raw: bool = True, compression_level: int = 3) -> dict:
    group = _reconcile_storage_job(group_id)
    _terminal_group(group)
    existing_job = group.get("artifact_storage_job") if isinstance(group.get("artifact_storage_job"), dict) else {}
    if _job_active(group_id) or (existing_job.get("status") in _ACTIVE_JOB_STATES and not _job_stale(existing_job)):
        return status(group_id)
    level = max(1, min(19, int(compression_level)))
    candidates = _storage_candidates(group_id)
    previous = group.get("artifact_storage_job") if isinstance(group.get("artifact_storage_job"), dict) else {}
    job_id = f"v5storage_{uuid4().hex[:16]}"
    job = {
        "schema_version": "mbe_v5_artifact_storage_job_v1",
        "job_id": job_id,
        "action": "archive",
        "status": "queued",
        "phase": "queued",
        "delete_raw": bool(delete_raw),
        "compression_level": level,
        "total_children": len(candidates),
        "processed_children": 0,
        "archived_children": 0,
        "error_count": 0,
        "skipped_count": 0,
        "created_at": _now(),
        "heartbeat_at": _now(),
        "worker_pid": os.getpid(),
        "resumed_from_job_id": previous.get("job_id") if previous.get("status") == "interrupted" else None,
        "archive_engine_preference": "windows_tar_zstd_multithread",
        "formal_eligibility_affected": False,
    }
    group["artifact_storage_job"] = job
    write_group(group)
    worker = threading.Thread(target=_job_worker, args=(group_id, job_id), kwargs={"delete_raw": bool(delete_raw), "compression_level": level}, name=f"v5-storage-archive-{group_id}", daemon=True)
    _set_job_thread(group_id, worker)
    worker.start()
    return status(group_id)


def storage_job_active(group_id: str) -> bool:
    return _job_active(group_id)


def compact(group_id: str) -> dict:
    group = _reconcile_storage_job(group_id)
    _terminal_group(group)
    if _job_active(group_id):
        raise FormalArtifactStorageError("artifact storage archive job is running")
    errors: list[dict] = []
    skipped: list[dict] = []
    for child in children(group_id):
        if not _eligible_child(child):
            continue
        run_id = _run_id(child)
        if not run_id:
            skipped.append({"child_run_id": child.get("child_run_id"), "reason": "no_real_cluster_run_id"})
            continue
        run_dir = v5_real_cluster_runner.run_dir(run_id)
        if not run_dir.is_dir():
            errors.append({"child_run_id": child.get("child_run_id"), "error": "real-cluster run directory is missing"})
            continue
        try:
            existing_storage = v5_artifact_storage.read_storage_summary(run_dir)
            if existing_storage.get("storage_state") in _ARCHIVE_STATES:
                # The verified archive already owns cold storage. Do not recursively
                # recompress the tar.zst or change its storage state.
                _persist_child_storage(group_id, child, existing_storage)
                continue
            storage = v5_artifact_storage.compact_online_run(run_dir, run_id=run_id, force=True)
            _persist_child_storage(group_id, child, storage)
        except Exception as exc:
            errors.append({"child_run_id": child.get("child_run_id"), "error": str(exc)})
    current = _update_group_storage(group_id)
    current["operation"] = "compact"
    current["operation_errors"] = errors
    current["operation_skipped"] = skipped
    return current


def archive(group_id: str, *, delete_raw: bool = True, compression_level: int = 3) -> dict:
    group = _reconcile_storage_job(group_id)
    _terminal_group(group)
    if _job_active(group_id):
        raise FormalArtifactStorageError("artifact storage archive job is running")
    errors: list[dict] = []
    skipped: list[dict] = []
    for child in children(group_id):
        if not _eligible_child(child):
            continue
        run_id = _run_id(child)
        if not run_id:
            skipped.append({"child_run_id": child.get("child_run_id"), "reason": "no_real_cluster_run_id"})
            continue
        run_dir = v5_real_cluster_runner.run_dir(run_id)
        if not run_dir.is_dir():
            errors.append({"child_run_id": child.get("child_run_id"), "error": "real-cluster run directory is missing"})
            continue
        try:
            storage = v5_artifact_storage.archive_run(
                run_dir,
                run_id=run_id,
                delete_raw=delete_raw,
                compression_level=compression_level,
            )
            _persist_child_storage(group_id, child, storage)
            if storage.get("raw_cleanup_error"):
                errors.append({"child_run_id": child.get("child_run_id"), "error": str(storage.get("raw_cleanup_error"))})
        except Exception as exc:
            errors.append({"child_run_id": child.get("child_run_id"), "error": str(exc)})
    current = _update_group_storage(group_id)
    current["operation"] = "archive"
    current["operation_errors"] = errors
    current["operation_skipped"] = skipped
    current["delete_raw_requested"] = bool(delete_raw)
    current["compression_level"] = max(1, min(19, int(compression_level)))
    return current


def restore(group_id: str) -> dict:
    group = _reconcile_storage_job(group_id)
    _terminal_group(group)
    if _job_active(group_id):
        raise FormalArtifactStorageError("artifact storage archive job is running")
    errors: list[dict] = []
    for child in children(group_id):
        run_id = _run_id(child)
        if not run_id:
            continue
        run_dir = v5_real_cluster_runner.run_dir(run_id)
        storage = v5_artifact_storage.read_storage_summary(run_dir) if run_dir.is_dir() else {}
        if storage.get("storage_state") not in _RESTORABLE_STATES:
            continue
        try:
            updated = v5_artifact_storage.restore_run(run_dir, run_id=run_id, reapply_ntfs=True)
            _persist_child_storage(group_id, child, updated)
        except Exception as exc:
            errors.append({"child_run_id": child.get("child_run_id"), "error": str(exc)})
    current = _update_group_storage(group_id)
    current["operation"] = "restore"
    current["operation_errors"] = errors
    return current


def child_archive_path(group_id: str, child_id: str) -> Path:
    read_group(group_id)
    child = read_child(group_id, child_id)
    run_id = _run_id(child)
    if not run_id:
        raise FileNotFoundError(child_id)
    run_dir = v5_real_cluster_runner.run_dir(run_id)
    try:
        return v5_artifact_storage.archive_path_for_download(run_dir)
    except v5_artifact_storage.ArtifactStorageError as exc:
        raise FormalArtifactStorageError(str(exc)) from exc
