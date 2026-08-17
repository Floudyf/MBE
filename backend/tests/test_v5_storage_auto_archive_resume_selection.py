from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace


def test_resume_candidate_modes_and_theta_major_order(monkeypatch):
    import backend.app.services.v5_formal_scheduler as scheduler

    group = {
        "run_group_id": "g1",
        "status": "cancelled",
        "execution_backend": "real_cluster",
        "plan": {"suites": ["workload_sensitivity"], "methods": [
            {"method_id": "serial", "display_name": "Serial"},
            {"method_id": "aria", "display_name": "Aria"},
        ]},
        "matrix": [
            {"child_run_id": "a11", "method_config_id": "aria", "method": {"method_id": "aria", "display_name": "Aria"}, "repeat_index": 1, "seed": 11, "workload_point": {"target_theta": 1.2, "tx_count": 10000}, "estimated_transactions": 10000, "estimated_processes": 8, "scan_variable": "target_theta", "scan_value": "1.2"},
            {"child_run_id": "s00", "method_config_id": "serial", "method": {"method_id": "serial", "display_name": "Serial"}, "repeat_index": 0, "seed": 11, "workload_point": {"target_theta": 0.0, "tx_count": 10000}, "estimated_transactions": 10000, "estimated_processes": 8, "scan_variable": "target_theta", "scan_value": "0.0"},
            {"child_run_id": "a00", "method_config_id": "aria", "method": {"method_id": "aria", "display_name": "Aria"}, "repeat_index": 0, "seed": 11, "workload_point": {"target_theta": 0.0, "tx_count": 10000}, "estimated_transactions": 10000, "estimated_processes": 8, "scan_variable": "target_theta", "scan_value": "0.0"},
            {"child_run_id": "s11", "method_config_id": "serial", "method": {"method_id": "serial", "display_name": "Serial"}, "repeat_index": 1, "seed": 11, "workload_point": {"target_theta": 1.2, "tx_count": 10000}, "estimated_transactions": 10000, "estimated_processes": 8, "scan_variable": "target_theta", "scan_value": "1.2"},
        ],
    }
    items = [
        {"child_run_id": "s00", "status": "completed", "attempt": 1},
        {"child_run_id": "a00", "status": "failed", "execution_status": "failed", "attempt": 1, "error": "boom"},
        {"child_run_id": "s11", "status": "interrupted", "attempt": 1},
    ]
    plan = SimpleNamespace(suites=["workload_sensitivity"], methods=[SimpleNamespace(method_id="serial"), SimpleNamespace(method_id="aria")])
    monkeypatch.setattr(scheduler, "reconcile_stale_group", lambda group_id: group)
    monkeypatch.setattr(scheduler, "worker_active", lambda group_id: False)
    monkeypatch.setattr(scheduler, "children", lambda group_id: items)
    monkeypatch.setattr(scheduler.V5FormalExperimentPlan, "model_validate", lambda payload: plan)

    result = scheduler.list_resume_candidates("g1")
    assert [item["child_run_id"] for item in result["candidates"]] == ["a00", "s11", "a11"]
    assert [item["mode"] for item in result["candidates"]] == ["retry_failed", "resume_unfinished", "resume_unfinished"]
    assert result["resume_unfinished_count"] == 2
    assert result["retry_failed_count"] == 1
    assert result["selection_allowed"] is True


def test_resume_selected_persists_exact_selection_and_invalidates_old_bundle(monkeypatch, tmp_path: Path):
    import backend.app.services.v5_formal_scheduler as scheduler

    group = {
        "run_group_id": "g2",
        "status": "cancelled",
        "cancel_requested": True,
        "matrix": [],
        "bundle_status": "ready",
        "aggregate": {"old": True},
    }
    candidates = {
        "run_group_id": "g2",
        "selection_allowed": True,
        "candidates": [
            {"child_run_id": "c1", "mode": "resume_unfinished", "estimated_transactions": 10000, "estimated_processes": 8},
            {"child_run_id": "c2", "mode": "resume_unfinished", "estimated_transactions": 10000, "estimated_processes": 8},
            {"child_run_id": "f1", "mode": "retry_failed", "estimated_transactions": 10000, "estimated_processes": 8},
        ],
    }
    bundle = tmp_path / "artifacts.zip"
    bundle.write_bytes(b"stale")
    writes = []
    monkeypatch.setattr(scheduler, "list_resume_candidates", lambda group_id: candidates)
    monkeypatch.setattr(scheduler, "reconcile_stale_group", lambda group_id: group)
    monkeypatch.setattr(scheduler, "worker_active", lambda group_id: False)
    monkeypatch.setattr(scheduler, "group_dir", lambda group_id: tmp_path)
    monkeypatch.setattr(scheduler, "write_group", lambda value: writes.append(dict(value)))
    monkeypatch.setattr(scheduler, "start", lambda group_id: None)
    monkeypatch.setattr(scheduler, "read_group", lambda group_id: group)

    result = scheduler.resume_selected("g2", ["c2"], mode="resume_unfinished")
    assert group["resume_requested_child_ids"] == ["c2"]
    assert group["resume_selection"]["child_run_ids"] == ["c2"]
    assert group["resume_selection"]["estimated_transactions"] == 10000
    assert group["cancel_requested"] is False
    assert group["status"] == "queued"
    assert "aggregate" not in group
    assert not bundle.exists()
    assert result["status"] == "queued"


def test_resume_selected_rejects_wrong_mode_child(monkeypatch):
    import backend.app.services.v5_formal_scheduler as scheduler
    monkeypatch.setattr(scheduler, "list_resume_candidates", lambda group_id: {
        "selection_allowed": True,
        "candidates": [{"child_run_id": "f1", "mode": "retry_failed", "estimated_transactions": 1, "estimated_processes": 1}],
    })
    try:
        scheduler.resume_selected("g", ["f1"], mode="resume_unfinished")
    except ValueError as exc:
        assert "not eligible" in str(exc)
    else:
        raise AssertionError("wrong-mode child must be rejected")


def test_partial_storage_accounting_keeps_known_bytes(monkeypatch):
    import backend.app.services.v5_formal_artifact_storage as formal_storage

    monkeypatch.setattr(formal_storage, "read_group", lambda group_id: {"status": "cancelled"})
    monkeypatch.setattr(formal_storage, "children", lambda group_id: [{"child_run_id": "a"}, {"child_run_id": "b"}])
    states = iter([
        {"child_run_id": "a", "run_id": "v5_a", "storage_state": "archived", "original_logical_bytes": 1000, "current_effective_bytes": 200, "archive_bytes": 150, "archive_download_url": "/x", "ntfs_compression_succeeded": True},
        {"child_run_id": "b", "run_id": "", "storage_state": "unavailable", "original_logical_bytes": 0, "current_effective_bytes": None, "archive_bytes": None, "archive_download_url": None, "ntfs_compression_succeeded": False},
    ])
    monkeypatch.setattr(formal_storage, "_child_status", lambda child: next(states))
    result = formal_storage.status("g")
    assert result["current_effective_bytes"] is None
    assert result["current_known_effective_bytes"] == 200
    assert result["known_effective_child_count"] == 1
    assert result["unmeasured_effective_child_count"] == 1
    assert result["known_saved_bytes"] == 800


def test_manual_archive_accepts_failed_and_timed_out_children(monkeypatch, tmp_path: Path):
    import backend.app.services.v5_formal_artifact_storage as formal_storage

    children = [
        {"child_run_id": "ok", "status": "completed", "result": {"run_id": "v5_ok"}},
        {"child_run_id": "bad", "status": "failed", "result": {"run_id": "v5_bad"}},
        {"child_run_id": "slow", "status": "timed_out", "result": {"run_id": "v5_slow"}},
        {"child_run_id": "none", "status": "failed", "result": {}},
    ]
    paths = {name: tmp_path / name for name in ("v5_ok", "v5_bad", "v5_slow")}
    for path in paths.values():
        path.mkdir()
    archived = []
    monkeypatch.setattr(formal_storage, "read_group", lambda group_id: {"status": "cancelled"})
    monkeypatch.setattr(formal_storage, "children", lambda group_id: children)
    monkeypatch.setattr(formal_storage.v5_real_cluster_runner, "run_dir", lambda run_id: paths[run_id])
    monkeypatch.setattr(formal_storage.v5_artifact_storage, "archive_run", lambda run_dir, run_id, delete_raw, compression_level: archived.append(run_id) or {"storage_state": "archived", "archive_verified": True, "raw_deleted_after_verification": True})
    monkeypatch.setattr(formal_storage, "_persist_child_storage", lambda group_id, child, storage: child)
    monkeypatch.setattr(formal_storage, "_update_group_storage", lambda group_id: {"children": []})

    result = formal_storage.archive("g")
    assert archived == ["v5_ok", "v5_bad", "v5_slow"]
    assert result["operation_skipped"] == [{"child_run_id": "none", "reason": "no_real_cluster_run_id"}]
