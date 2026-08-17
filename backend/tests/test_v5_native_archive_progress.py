from __future__ import annotations

import os
import shutil
import time
from pathlib import Path

import pytest


def test_native_archive_source_contract() -> None:
    root = Path(__file__).resolve().parents[2]
    storage = (root / "backend/app/services/v5_artifact_storage.py").read_text(encoding="utf-8")
    formal = (root / "backend/app/services/v5_formal_artifact_storage.py").read_text(encoding="utf-8")
    api = (root / "backend/app/api/v5_formal_experiments.py").read_text(encoding="utf-8")
    frontend_api = (root / "frontend/src/api.ts").read_text(encoding="utf-8")
    results = (root / "frontend/src/pages/V5ResultsPage.tsx").read_text(encoding="utf-8")
    evidence = (root / "frontend/src/components/v5/V5EvidencePanel.tsx").read_text(encoding="utf-8")

    assert 'shutil.which("tar.exe")' in storage
    assert 'threads=-1' in storage
    assert 'write_checksum=True' in storage
    assert 'windows_tar_zstd_multithread' in storage
    assert 'python_tarfile_zstd_multithread' in storage
    assert 'progress: ProgressCallback | None = None' in storage

    assert 'artifact_storage_job' in formal
    assert 'start_archive_job' in formal
    assert 'status": "interrupted"' in formal
    assert 'archive_engine_preference' in formal
    assert 'progress=progress' in formal

    assert 'start_archive_job(group_id' in api
    assert 'artifact_storage_job?: V5ArtifactStorageJob' in frontend_api
    assert 'window.setInterval' in results
    assert 'fetchV5ArtifactStorage(selectedGroupId)' in results
    assert '后台归档' in evidence
    assert 'Windows tar.exe + Zstandard 多线程' in evidence
    assert '最后心跳' in evidence




@pytest.mark.skipif(os.name != "nt" or shutil.which("tar.exe") is None, reason="Windows tar.exe required")
def test_windows_native_tar_engine_is_actually_used(tmp_path: Path) -> None:
    pytest.importorskip("zstandard")
    from backend.app.services import v5_artifact_storage as storage

    run_dir = tmp_path / "v5_native_tar_probe"
    run_dir.mkdir()
    (run_dir / "probe.txt").write_text("native tar probe\n" * 100, encoding="utf-8")
    result = storage.archive_run(run_dir, run_id="v5_native_tar_probe", delete_raw=False, compression_level=3)
    assert result.get("native_tar_used") is True
    assert result.get("archive_engine") == "windows_tar_zstd_multithread"
    assert result.get("zstd_threads") == -1
    assert result.get("archive_verified") is True

def test_native_tar_compatibility_guard() -> None:
    from backend.app.services import v5_artifact_storage as storage

    assert storage._native_tar_compatible({"files": [{"name": "a/b.json"}]}) is True
    assert storage._native_tar_compatible({"files": [{"name": "-option-like"}]}) is False
    assert storage._native_tar_compatible({"files": [{"name": "bad\nname"}]}) is False


def test_archive_job_persists_progress_and_finishes(monkeypatch, tmp_path: Path) -> None:
    from backend.app.services import v5_formal_artifact_storage as formal

    run_dir = tmp_path / "v5_run_1"
    run_dir.mkdir()
    group_state = {
        "run_group_id": "v5grp_test_native_archive",
        "status": "cancelled",
    }
    child_state = {
        "child_run_id": "v5child_1",
        "run_group_id": group_state["run_group_id"],
        "status": "completed",
        "execution_status": "completed",
        "method_config_id": "hash_serial",
        "method": {"display_name": "Serial"},
        "repeat_index": 0,
        "workload_point": {"target_theta": 0.6},
        "result": {"run_id": "v5_run_1", "summary": {}, "artifacts": []},
    }

    def read_group(_group_id: str):
        return dict(group_state)

    def write_group(value: dict):
        group_state.clear()
        group_state.update(value)

    def children(_group_id: str):
        return [dict(child_state)]

    def write_child(_group_id: str, value: dict):
        child_state.clear()
        child_state.update(value)

    def fake_archive_run(_run_dir: Path, *, run_id: str, delete_raw: bool, compression_level: int, progress=None):
        assert run_id == "v5_run_1"
        assert delete_raw is True
        assert compression_level == 3
        if progress:
            progress({"phase": "packing", "archive_engine": "windows_tar_zstd_multithread", "source_logical_bytes": 1000})
            progress({"phase": "compressing", "archive_engine": "windows_tar_zstd_multithread", "source_logical_bytes": 1000, "archive_bytes_written": 250})
            progress({"phase": "verifying", "source_file_count": 2, "verified_file_count": 2, "archive_bytes": 250})
            progress({"phase": "completed", "archive_engine": "windows_tar_zstd_multithread", "archive_bytes": 250, "raw_deleted_after_verification": True})
        return {
            "run_id": run_id,
            "storage_state": "archived",
            "archive_verified": True,
            "raw_deleted_after_verification": True,
            "archive_bytes": 250,
            "archive_engine": "windows_tar_zstd_multithread",
            "current_effective_bytes": 250,
            "original_logical_bytes": 1000,
            "saved_bytes": 750,
            "saving_ratio": 0.75,
            "formal_eligibility_affected": False,
        }

    monkeypatch.setattr(formal, "read_group", read_group)
    monkeypatch.setattr(formal, "write_group", write_group)
    monkeypatch.setattr(formal, "children", children)
    monkeypatch.setattr(formal, "write_child", write_child)
    monkeypatch.setattr(formal.v5_real_cluster_runner, "run_dir", lambda _run_id: run_dir)
    monkeypatch.setattr(formal.v5_artifact_storage, "read_storage_summary", lambda _run_dir: {})
    monkeypatch.setattr(formal.v5_artifact_storage, "archive_run", fake_archive_run)
    monkeypatch.setattr(formal, "_measure_unmanaged_run", lambda _run_dir, run_id: {
        "run_id": run_id,
        "storage_state": "online_unmanaged",
        "original_file_count": 0,
        "original_logical_bytes": 0,
        "online_logical_bytes": 0,
        "online_physical_bytes": 0,
        "current_effective_bytes": 0,
        "saved_bytes": 0,
        "saving_ratio": 0.0,
        "formal_eligibility_affected": False,
    })

    formal.start_archive_job(group_state["run_group_id"], delete_raw=True, compression_level=3)
    deadline = time.time() + 3
    while time.time() < deadline:
        status = str((group_state.get("artifact_storage_job") or {}).get("status") or "")
        if status in {"completed", "completed_with_errors", "failed"}:
            break
        time.sleep(0.02)

    job = group_state.get("artifact_storage_job") or {}
    assert job.get("status") == "completed"
    assert job.get("processed_children") == 1
    assert job.get("archived_children") == 1
    assert child_state.get("artifact_storage", {}).get("storage_state") == "archived"
    assert child_state.get("artifact_storage", {}).get("archive_engine") == "windows_tar_zstd_multithread"


def test_persisted_running_job_becomes_interrupted_without_local_worker(monkeypatch) -> None:
    from backend.app.services import v5_formal_artifact_storage as formal

    group = {
        "run_group_id": "v5grp_stale_storage_job",
        "status": "cancelled",
        "artifact_storage_job": {
            "job_id": "v5storage_dead",
            "status": "running",
            "phase": "compressing",
            "heartbeat_at": "2026-08-15T00:00:00+00:00",
        },
    }
    saved: dict = {}
    monkeypatch.setattr(formal, "read_group", lambda _group_id: dict(group))
    monkeypatch.setattr(formal, "write_group", lambda value: saved.update(value))
    monkeypatch.setattr(formal, "_job_active", lambda _group_id: False)

    result = formal._reconcile_storage_job(group["run_group_id"])
    assert result["artifact_storage_job"]["status"] == "interrupted"
    assert result["artifact_storage_job"]["phase"] == "interrupted"
    assert saved["artifact_storage_job"]["status"] == "interrupted"
