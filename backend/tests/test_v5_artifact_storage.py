from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from backend.app.services import v5_artifact_storage as storage


def _require_zstd():
    return pytest.importorskip("zstandard")


def _seed_run(tmp_path: Path) -> Path:
    run_dir = tmp_path / "v5_test_storage"
    (run_dir / "nodes" / "node_0").mkdir(parents=True)
    (run_dir / "client").mkdir(parents=True)
    (run_dir / "nodes" / "node_0" / "network_log.csv").write_text("kind,value\nPBFT,1\n" * 100, encoding="utf-8")
    (run_dir / "nodes" / "node_0" / "node_runtime_status.json").write_text(json.dumps({"committed_height": 3}) + "\n", encoding="utf-8")
    (run_dir / "nodes" / "node_0" / "commit_log.csv").write_text("height,status\n1,committed\n", encoding="utf-8")
    (run_dir / "nodes" / "node_0" / "committed_chain.csv").write_text("height,hash\n1,abc\n", encoding="utf-8")
    (run_dir / "client" / "transaction_lifecycle.jsonl").write_text('{"tx":"a","state":"final"}\n' * 100, encoding="utf-8")
    (run_dir / "real_cluster_summary.json").write_text(json.dumps({"execution_status": "completed", "formal_eligibility": True}) + "\n", encoding="utf-8")
    (run_dir / "drain_status.json").write_text(json.dumps({"completion_reason": "drain_quiescent"}) + "\n", encoding="utf-8")
    (run_dir / "resource_usage_summary.json").write_text(json.dumps({"average_cluster_cpu_cores": 3.5}) + "\n", encoding="utf-8")
    (run_dir / "network_metrics_summary.json").write_text(json.dumps({"delivered_network_bytes": 1234}) + "\n", encoding="utf-8")
    (run_dir / "aggregate").mkdir(parents=True)
    (run_dir / "aggregate" / "block_production_summary.json").write_text(json.dumps({"actual_committed_block_count": 3}) + "\n", encoding="utf-8")
    catalog = {
        "schema_version": "mbe_v5_artifact_catalog_v1",
        "run_id": "v5_test_storage",
        "files": [
            {"name": "nodes/node_0/network_log.csv", "size_bytes": (run_dir / "nodes" / "node_0" / "network_log.csv").stat().st_size},
            {"name": "client/transaction_lifecycle.jsonl", "size_bytes": (run_dir / "client" / "transaction_lifecycle.jsonl").stat().st_size},
        ],
    }
    (run_dir / "artifact_catalog.json").write_text(json.dumps(catalog) + "\n", encoding="utf-8")
    return run_dir


def test_tar_zst_archive_delete_stream_and_restore_roundtrip(tmp_path: Path):
    _require_zstd()
    run_dir = _seed_run(tmp_path)
    source = (run_dir / "client" / "transaction_lifecycle.jsonl").read_bytes()

    result = storage.archive_run(run_dir, run_id="v5_test_storage", delete_raw=True, compression_level=3)

    assert result["storage_state"] == "archived"
    assert result["archive_format"] == "tar.zst"
    assert result["archive_verified"] is True
    assert result["raw_deleted_after_verification"] is True
    assert result["formal_eligibility_affected"] is False
    assert not (run_dir / "client" / "transaction_lifecycle.jsonl").exists()
    assert (run_dir / "real_cluster_summary.json").is_file()
    assert (run_dir / "artifact_catalog.json").is_file()
    # Small research diagnostics remain online after raw cleanup so Formal
    # bundles can explain timeout/failed/completed-invalid children directly.
    assert (run_dir / "drain_status.json").is_file()
    assert (run_dir / "resource_usage_summary.json").is_file()
    assert (run_dir / "network_metrics_summary.json").is_file()
    assert (run_dir / "aggregate" / "block_production_summary.json").is_file()
    assert (run_dir / "nodes" / "node_0" / "node_runtime_status.json").is_file()
    assert (run_dir / "nodes" / "node_0" / "commit_log.csv").is_file()
    assert (run_dir / "nodes" / "node_0" / "committed_chain.csv").is_file()
    assert b"".join(storage.stream_archived_artifact(run_dir, "client/transaction_lifecycle.jsonl")) == source

    restored = storage.restore_run(run_dir, run_id="v5_test_storage", reapply_ntfs=False)
    assert restored["storage_state"] == "online_restored"
    assert (run_dir / "client" / "transaction_lifecycle.jsonl").read_bytes() == source
    assert storage.archive_path_for_download(run_dir).is_file()


def test_archive_verification_failure_never_deletes_raw(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    _require_zstd()
    run_dir = _seed_run(tmp_path)
    raw = run_dir / "nodes" / "node_0" / "network_log.csv"

    def fail_verify(*args, **kwargs):
        raise storage.ArtifactStorageError("forced verification failure")

    monkeypatch.setattr(storage, "verify_archive", fail_verify)
    with pytest.raises(storage.ArtifactStorageError, match="forced verification failure"):
        storage.archive_run(run_dir, run_id="v5_test_storage", delete_raw=True, compression_level=3)
    assert raw.is_file()


def test_ntfs_compression_failure_is_failure_open(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    run_dir = _seed_run(tmp_path)
    monkeypatch.setattr(storage, "_is_windows", lambda: True)
    monkeypatch.setattr(storage, "_run_compact", lambda _run_dir: SimpleNamespace(returncode=1, stdout="", stderr="forced compact failure"))

    result = storage.compact_online_run(run_dir, run_id="v5_test_storage", force=True)

    assert result["storage_state"] == "online_uncompressed"
    assert result["ntfs_compression_attempted"] is True
    assert result["ntfs_compression_succeeded"] is False
    assert "forced compact failure" in result["ntfs_compression_error"]
    assert result["formal_eligibility_affected"] is False
    assert (run_dir / "client" / "transaction_lifecycle.jsonl").is_file()



def test_verified_archive_survives_partial_raw_cleanup_and_resumes(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    _require_zstd()
    run_dir = _seed_run(tmp_path)
    victim = run_dir / "client" / "transaction_lifecycle.jsonl"
    original_delete = storage._delete_archived_raw

    def partial_delete(_run_dir, _manifest):
        victim.unlink()
        raise storage.ArtifactStorageError("forced raw cleanup interruption")

    monkeypatch.setattr(storage, "_delete_archived_raw", partial_delete)
    first = storage.archive_run(run_dir, run_id="v5_test_storage", delete_raw=True, compression_level=3)
    assert first["storage_state"] == "archive_verified_raw_cleanup_incomplete"
    assert first["archive_verified"] is True
    assert not victim.exists()
    assert storage.archive_path_for_download(run_dir).is_file()
    assert b"".join(storage.stream_archived_artifact(run_dir, "client/transaction_lifecycle.jsonl"))

    monkeypatch.setattr(storage, "_delete_archived_raw", original_delete)
    resumed = storage.archive_run(run_dir, run_id="v5_test_storage", delete_raw=True, compression_level=3)
    assert resumed["storage_state"] == "archived"
    assert resumed["raw_deleted_after_verification"] is True
    assert resumed.get("raw_cleanup_error") == ""

def test_archive_member_path_rejects_traversal():
    for value in ("../secret", "/absolute", "C:/escape", "a/../../b", "a//b"):
        with pytest.raises(storage.ArtifactStorageError):
            storage._safe_relative_name(value)


def test_archived_storage_state_survives_manual_ntfs_compaction(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    _require_zstd()
    run_dir = _seed_run(tmp_path)
    archived = storage.archive_run(run_dir, run_id="v5_test_storage", delete_raw=True, compression_level=3)
    assert archived["storage_state"] == "archived"
    monkeypatch.setattr(storage, "_is_windows", lambda: True)
    monkeypatch.setattr(storage, "_run_compact", lambda _run_dir: SimpleNamespace(returncode=0, stdout="", stderr=""))

    updated = storage.compact_online_run(run_dir, run_id="v5_test_storage", force=True)

    assert updated["storage_state"] == "archived"
    assert updated["archive_verified"] is True
    assert storage.archive_path_for_download(run_dir).is_file()


def test_corrupted_archive_is_rejected_before_restore(tmp_path: Path):
    _require_zstd()
    run_dir = _seed_run(tmp_path)
    storage.archive_run(run_dir, run_id="v5_test_storage", delete_raw=True, compression_level=3)
    archive = storage.archive_path_for_download(run_dir)
    payload = bytearray(archive.read_bytes())
    payload[len(payload) // 2] ^= 0x01
    archive.write_bytes(payload)

    with pytest.raises(storage.ArtifactStorageError, match="archive SHA-256 mismatch"):
        storage.restore_run(run_dir, run_id="v5_test_storage", reapply_ntfs=False)

    assert not (run_dir / "client" / "transaction_lifecycle.jsonl").exists()


def test_frozen_artifact_catalog_survives_cold_archive(tmp_path: Path):
    _require_zstd()
    from backend.app.services import v5_real_cluster_artifacts

    run_dir = _seed_run(tmp_path)
    storage.archive_run(run_dir, run_id="v5_test_storage", delete_raw=True, compression_level=3)

    names = {item["name"] for item in v5_real_cluster_artifacts.list_artifacts(run_dir, "v5_test_storage")}
    assert "client/transaction_lifecycle.jsonl" in names
    assert "nodes/node_0/network_log.csv" in names


def test_raw_cleanup_preserves_compact_diagnostic_shell_without_archive_dependency(tmp_path: Path):
    run_dir = _seed_run(tmp_path)
    heavy = run_dir / "client" / "transaction_lifecycle.jsonl"
    manifest = {
        "files": [
            {"name": path.relative_to(run_dir).as_posix()}
            for path in sorted(run_dir.rglob("*"))
            if path.is_file()
        ]
    }

    storage._delete_archived_raw(run_dir, manifest)

    assert not heavy.exists()
    assert not (run_dir / "nodes" / "node_0" / "network_log.csv").exists()
    assert (run_dir / "real_cluster_summary.json").is_file()
    assert (run_dir / "artifact_catalog.json").is_file()
    assert (run_dir / "drain_status.json").is_file()
    assert (run_dir / "resource_usage_summary.json").is_file()
    assert (run_dir / "network_metrics_summary.json").is_file()
    assert (run_dir / "aggregate" / "block_production_summary.json").is_file()
    assert (run_dir / "nodes" / "node_0" / "node_runtime_status.json").is_file()
    assert (run_dir / "nodes" / "node_0" / "commit_log.csv").is_file()
    assert (run_dir / "nodes" / "node_0" / "committed_chain.csv").is_file()
