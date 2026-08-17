from __future__ import annotations

from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]


def test_storage_v2_source_contract():
    runner = (ROOT / "backend" / "app" / "services" / "v5_real_cluster_runner.py").read_text(encoding="utf-8")
    scheduler = (ROOT / "backend" / "app" / "services" / "v5_formal_scheduler.py").read_text(encoding="utf-8")
    formal_storage = (ROOT / "backend" / "app" / "services" / "v5_formal_artifact_storage.py").read_text(encoding="utf-8")
    formal_api = (ROOT / "backend" / "app" / "api" / "v5_formal_experiments.py").read_text(encoding="utf-8")
    real_api = (ROOT / "backend" / "app" / "api" / "v5_real_cluster.py").read_text(encoding="utf-8")
    storage = (ROOT / "backend" / "app" / "services" / "v5_artifact_storage.py").read_text(encoding="utf-8")
    evidence = (ROOT / "frontend" / "src" / "components" / "v5" / "V5EvidencePanel.tsx").read_text(encoding="utf-8")
    api = (ROOT / "frontend" / "src" / "api.ts").read_text(encoding="utf-8")

    assert "finalize_online_storage(run_dir, run_id=run_id)" in runner
    assert "formal_eligibility_affected" in storage
    assert "verify_archive(archive, manifest" in storage
    assert "_delete_archived_raw(run_dir, manifest)" in storage
    assert "_run_storage_lock" in storage
    assert "restore_prefix" in storage
    assert "archive_verified_raw_cleanup_incomplete" in storage
    assert storage.index("verify_archive(archive, manifest") < storage.index("_delete_archived_raw(run_dir, manifest)")
    assert "stream_archived_artifact" in real_api

    # V2 intentionally hooks Formal scheduling only after result/metric freezing.
    assert "v5_formal_artifact_storage.auto_archive_terminal_child" in scheduler
    assert scheduler.index("extract_metrics(result_dir") < scheduler.index("auto_archive_terminal_child")
    assert '"auto_cold_archive_terminal_children": True' in scheduler
    assert '"auto_cold_archive_delete_raw": True' in scheduler
    assert '"completed", "failed", "timed_out"' in formal_storage
    assert '"completed", "failed", "timed_out", "cancelled", "interrupted"' in formal_storage
    assert "current_known_effective_bytes" in formal_storage
    assert "unmeasured_effective_child_count" in formal_storage

    for route in ("/storage", "/storage/compact", "/storage/archive", "/storage/restore", "/storage/children/{child_id}/archive"):
        assert route in formal_api
    for token in ("fetchV5ArtifactStorage", "compactV5ArtifactStorage", "archiveV5ArtifactStorage", "restoreV5ArtifactStorage"):
        assert token in api
    for label in ("产物存储管理", "自动冷归档", "冷归档并释放 Raw", "恢复原始产物", "下载 TAR.ZST"):
        assert label in evidence
