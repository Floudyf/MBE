from __future__ import annotations

from pathlib import Path
from backend.app.services import v5_artifact_storage as storage

def _make_legacy(run_dir: Path, run_id: str) -> None:
    manifest = storage._snapshot(run_dir, run_id)
    archive, manifest_path, checksum_path = storage._archive_paths(run_dir, run_id)
    engine = storage._create_archive_python(run_dir, archive, manifest, 10)
    verified = storage.verify_archive(archive, manifest)
    assert verified["archive_layout"] == "legacy_paths_v1"
    archive_sha = storage._sha256_file(archive)
    storage._atomic_json(manifest_path, manifest)
    checksum_path.write_text(f"{archive_sha}  {archive.name}\n", encoding="ascii")
    storage._write_storage_summary(run_dir, {
        "run_id": run_id,
        "storage_state": "archive_online",
        "archive_format": "tar.zst",
        "archive_layout": "legacy_paths_v1",
        "archive_compression": "zstd",
        "archive_compression_level": 10,
        "archive_engine": engine["archive_engine"],
        "archive_relative_path": f"{storage.ARCHIVE_DIR_NAME}/{archive.name}",
        "archive_manifest_relative_path": f"{storage.ARCHIVE_DIR_NAME}/{manifest_path.name}",
        "archive_checksum_relative_path": f"{storage.ARCHIVE_DIR_NAME}/{checksum_path.name}",
        "archive_bytes": archive.stat().st_size,
        "archive_sha256": archive_sha,
        "archive_verified": True,
        "archive_verified_at": verified["verified_at"],
        "archived_file_count": manifest["file_count"],
        "original_file_count": manifest["file_count"],
        "original_logical_bytes": manifest["logical_bytes"],
        "online_logical_bytes": manifest["logical_bytes"],
        "online_physical_bytes": manifest["logical_bytes"],
        "current_effective_bytes": manifest["logical_bytes"] + archive.stat().st_size,
        "raw_deleted_after_verification": False,
    })

def test_exact_dedup_new_stream_restore_and_legacy_migration(tmp_path: Path, monkeypatch):
    run_id = "v5_20000101_000000_deadbeef"
    run = tmp_path / run_id
    run.mkdir()
    duplicate = b'{"block":1}\n' * 20000
    unique = b'{"unique":true}\n' * 2000
    for node in range(4):
        p = run / "nodes" / f"n{node}" / "blocks.jsonl"
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_bytes(duplicate)
    (run / "summary.json").write_bytes(unique)
    with storage.exact_dedup_archive_mode():
        result = storage.archive_run(run, run_id=run_id, delete_raw=True, compression_level=10)
    assert result["archive_layout"] == storage.DEDUP_ARCHIVE_LAYOUT
    assert result["archive_verified"] is True
    assert result["archive_unique_object_count"] == 2
    assert result["archive_duplicate_logical_bytes"] == 3 * len(duplicate)
    assert b"".join(storage.stream_archived_artifact(run, "nodes/n2/blocks.jsonl")) == duplicate
    storage.restore_run(run, run_id=run_id, reapply_ntfs=False)
    assert (run / "nodes" / "n3" / "blocks.jsonl").read_bytes() == duplicate

    generic_id = "v5_20000101_000002_cafebabe"
    generic = tmp_path / generic_id
    generic.mkdir()
    (generic / "probe.txt").write_text("generic archive contract\n" * 100, encoding="utf-8")
    generic_result = storage.archive_run(generic, run_id=generic_id, delete_raw=False, compression_level=10)
    assert generic_result["archive_layout"] == "legacy_paths_v1"

    legacy_id = "v5_20000101_000001_feedface"
    legacy = tmp_path / legacy_id
    legacy.mkdir()
    payload = b"legacy\n" * 30000
    for node in range(4):
        p = legacy / "nodes" / f"n{node}" / "blocks.jsonl"
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_bytes(payload)
    _make_legacy(legacy, legacy_id)
    assert b"".join(storage.stream_archived_artifact(legacy, "nodes/n1/blocks.jsonl")) == payload
    migrated = storage.repack_archive_exact_dedup(legacy, run_id=legacy_id, compression_level=10)
    assert migrated["migration_succeeded"] is True
    assert migrated["archive_layout"] == storage.DEDUP_ARCHIVE_LAYOUT
    probe = legacy / "nodes" / "n3" / "blocks.jsonl"
    probe.unlink()
    storage.restore_run(legacy, run_id=legacy_id, reapply_ntfs=False)
    assert probe.read_bytes() == payload
