from __future__ import annotations

import hashlib
import json
import zipfile
from pathlib import Path

from backend.app.services import v5_reproducibility_bundle as bundle


def test_v38_0_4_stateful_evidence_name_scope_is_exact() -> None:
    assert bundle._is_stateful_oracle_evidence_name("nodes/n0/committed_chain.csv")
    assert bundle._is_stateful_oracle_evidence_name("nodes/n0/block_execution_summary.json")
    assert bundle._is_stateful_oracle_evidence_name("nodes/n0/transaction_execution_trace.csv")
    assert bundle._is_stateful_oracle_evidence_name("nodes/n0/observed_state_access.csv")
    assert bundle._is_stateful_oracle_evidence_name("nodes/n0/state_delta.1.wal")
    assert bundle._is_stateful_oracle_evidence_name("nodes/n0/state_snapshot.json")
    assert bundle._is_stateful_oracle_evidence_name("nodes/n0/state_snapshot_metadata.json")
    assert not bundle._is_stateful_oracle_evidence_name("nodes/n0/network_log.csv")
    assert not bundle._is_stateful_oracle_evidence_name("client/transaction_execution_trace.csv")


def test_v38_0_4_bundle_streams_stateful_oracle_evidence_from_cold_archive(tmp_path, monkeypatch) -> None:
    monkeypatch.setenv("MBE_ARTIFACTS_FULL_ORACLE_EVIDENCE", "1")  # MBE_MV_FIX_V2
    group_dir = tmp_path / "group"
    group_dir.mkdir()
    (group_dir / "run_group.json").write_text("{}\n", encoding="utf-8")
    runtime_root = tmp_path / "runtime"
    archive_dir = runtime_root / "_cold_archive"
    archive_dir.mkdir(parents=True)
    evidence_name = "nodes/n0/transaction_execution_trace.csv"
    evidence_bytes = b"tx_id,success\nt0,true\n"
    evidence_sha = hashlib.sha256(evidence_bytes).hexdigest()
    manifest = {
        "files": [{"name": evidence_name, "size_bytes": len(evidence_bytes), "sha256": evidence_sha}]
    }
    (archive_dir / "run.manifest.json").write_text(json.dumps(manifest), encoding="utf-8")

    monkeypatch.setattr(bundle, "children", lambda _group_id: [{
        "status": "completed",
        "comparison_semantics_class": "stateful_local_legacy_v1",
        "child_run_id": "child0",
        "result": {"run_id": "run0"},
    }])
    monkeypatch.setattr(bundle.v5_real_cluster_runner, "run_dir", lambda _run_id: runtime_root)
    monkeypatch.setattr(bundle.v5_artifact_storage, "read_storage_summary", lambda _root: {
        "archive_manifest_relative_path": "_cold_archive/run.manifest.json"
    })
    monkeypatch.setattr(bundle.v5_artifact_storage, "stream_archived_artifact", lambda _root, name: iter([evidence_bytes]) if name == evidence_name else iter([]))

    out = bundle.build(group_dir, {"run_group_id": "g0", "execution_backend": "real_cluster", "plan": {}})
    with zipfile.ZipFile(out) as archive:
        member = "stateful_oracle_evidence/child0/nodes/n0/transaction_execution_trace.csv"
        assert archive.read(member) == evidence_bytes
        manifest_out = json.loads(archive.read("artifact_manifest.json"))
        item = next(value for value in manifest_out["files"] if value["name"] == member)
        assert item["sha256"] == evidence_sha
        assert item["source"] == "stateful_oracle_cold_archive_evidence"
