from __future__ import annotations

import json
import os
from datetime import UTC, datetime
from pathlib import Path

from backend.app.services import v5_cleanup_service as cleanup


def test_running_group_is_not_deleted(tmp_path: Path) -> None:
    formal_root = tmp_path / "formal"
    real_root = tmp_path / "runtime" / "v5_real_cluster_runs"
    _write_group(formal_root, "v5grp_running", {"status": "running"})

    report = cleanup.delete_run_group(
        "v5grp_running",
        formal_root=formal_root,
        real_cluster_root=real_root,
    )

    assert report["deleted_run_group_ids"] == []
    assert report["skipped_active_runs"] == ["v5grp_running"]
    assert (formal_root / "v5grp_running").is_dir()


def test_pinned_paper_candidate_and_latest_successful_are_preserved(tmp_path: Path) -> None:
    formal_root = tmp_path / "formal"
    real_root = tmp_path / "runtime" / "v5_real_cluster_runs"
    _write_group(formal_root, "v5grp_pinned", {"status": "failed", "pinned": True})
    _write_group(formal_root, "v5grp_paper", {"status": "completed"}, children=[{"paper_candidate": True}])
    _write_four_method_group(formal_root, "v5grp_latest", "2026-07-28T09:00:00+00:00")

    report = cleanup.delete_selected_run_groups(
        ["v5grp_pinned", "v5grp_paper", "v5grp_latest"],
        formal_root=formal_root,
        real_cluster_root=real_root,
    )

    assert report["deleted_run_group_ids"] == []
    assert set(report["preserved_run_group_ids"]) == {"v5grp_pinned", "v5grp_paper", "v5grp_latest"}
    assert any("v5grp_pinned:pinned" == item for item in report["errors"])
    assert any("v5grp_paper:paper_candidate" == item for item in report["errors"])
    assert any("v5grp_latest:latest_successful_four_method" == item for item in report["errors"])


def test_completed_paper_result_analysis_file_is_preserved(tmp_path: Path) -> None:
    formal_root = tmp_path / "formal"
    real_root = tmp_path / "runtime" / "v5_real_cluster_runs"
    _write_group(formal_root, "v5grp_paper_file", {"status": "completed"})
    aggregate = formal_root / "v5grp_paper_file" / "aggregate"
    aggregate.mkdir()
    (aggregate / "paper_result_analysis.json").write_text(json.dumps({"analysis_status": "complete"}), encoding="utf-8")

    report = cleanup.delete_run_group(
        "v5grp_paper_file",
        formal_root=formal_root,
        real_cluster_root=real_root,
    )

    assert report["deleted_run_group_ids"] == []
    assert report["preserved_run_group_ids"] == ["v5grp_paper_file"]
    assert "v5grp_paper_file:paper_candidate" in report["errors"]
    assert (formal_root / "v5grp_paper_file").is_dir()


def test_completed_group_deletes_only_exclusive_real_cluster_output(tmp_path: Path) -> None:
    formal_root = tmp_path / "formal"
    real_root = tmp_path / "runtime" / "v5_real_cluster_runs"
    exclusive = real_root / "v5_exclusive"
    shared = real_root / "v5_shared"
    _touch_file(exclusive / "summary.json", "exclusive")
    _touch_file(shared / "summary.json", "shared")
    _write_group(
        formal_root,
        "v5grp_delete",
        {"status": "failed"},
        children=[{"result": {"output_dir": str(exclusive)}}, {"result": {"output_dir": str(shared)}}],
    )
    _write_group(
        formal_root,
        "v5grp_other",
        {"status": "failed"},
        children=[{"result": {"output_dir": str(shared)}}],
    )

    report = cleanup.delete_run_group(
        "v5grp_delete",
        dry_run=False,
        formal_root=formal_root,
        real_cluster_root=real_root,
    )

    assert report["deleted_run_group_ids"] == ["v5grp_delete"]
    assert report["deleted_output_dirs"] == [str(exclusive.resolve())]
    assert not (formal_root / "v5grp_delete").exists()
    assert not exclusive.exists()
    assert shared.exists()


def test_orphan_scan_requires_controlled_old_unreferenced_dirs(tmp_path: Path) -> None:
    formal_root = tmp_path / "formal"
    real_root = tmp_path / "runtime" / "v5_real_cluster_runs"
    old_orphan = real_root / "v5_old_orphan"
    referenced = real_root / "v5_referenced"
    recent = real_root / "v5_recent"
    uncontrolled = real_root / "manual_dir"
    active = real_root / "v5_active"
    for path in (old_orphan, referenced, recent, uncontrolled, active):
        _touch_file(path / "summary.json", path.name)
    _touch_file(active / "RUNNING", "1")
    _write_group(formal_root, "v5grp_ref", {"status": "failed"}, children=[{"result": {"output_dir": str(referenced)}}])

    old_time = datetime(2026, 7, 26, tzinfo=UTC).timestamp()
    os.utime(old_orphan, (old_time, old_time))
    os.utime(referenced, (old_time, old_time))
    os.utime(uncontrolled, (old_time, old_time))
    os.utime(active, (old_time, old_time))

    scan = cleanup.scan_orphan_real_cluster_dirs(
        now_time=datetime(2026, 7, 28, tzinfo=UTC),
        formal_root=formal_root,
        real_cluster_root=real_root,
    )

    assert [item["path"] for item in scan["orphan_dirs"]] == [str(old_orphan)]


def test_cleanup_report_writes_json_and_csv(tmp_path: Path) -> None:
    report = {"deleted_run_group_ids": ["v5grp_a"], "released_bytes": 12, "errors": []}

    cleanup.write_cleanup_report(report, tmp_path)

    assert json.loads((tmp_path / "cleanup_report.json").read_text(encoding="utf-8")) == report
    assert "deleted_run_group_ids,v5grp_a" in (tmp_path / "cleanup_report.csv").read_text(encoding="utf-8")


def test_legacy_saved_config_cleanup_preserves_current_runnable_v5_profiles(tmp_path: Path) -> None:
    saved_root = tmp_path / "saved"
    _write_saved_config(saved_root, "v3cfg_formal", {"config_kind": "formal_plan", "name": "Old Formal Plan", "payload": {"schema_version": "v3_formal_plan"}})
    _write_saved_config(saved_root, "v3cfg_legacy_method", {"config_kind": "method", "name": "Old Method", "payload": {"schema_version": "legacy_method"}, "validation_status": "valid"})
    _write_saved_config(saved_root, "v3cfg_invalid_v5", {"config_kind": "method", "name": "Invalid V5 Method", "payload": {"schema_version": "v5_plugin_profile_v1", "plugin_selections": [], "compatibility_snapshot": {"valid": False}}, "validation_status": "runnable"})
    _write_saved_config(saved_root, "v3cfg_current_v5", {"config_kind": "method", "name": "Current V5 Method", "payload": {"schema_version": "v5_plugin_profile_v1", "plugin_selections": [{"category": "routing", "plugin_id": "hash_routing_baseline"}], "compatibility_snapshot": {"valid": True}}, "validation_status": "runnable"})
    _write_saved_config(saved_root, "v3cfg_workload", {"config_kind": "workload", "name": "Workload", "payload": {}})

    scan = cleanup.scan_legacy_saved_configs(saved_config_root=saved_root)
    dry_run = cleanup.cleanup_legacy_saved_configs(saved_config_root=saved_root)

    assert {item["config_id"] for item in scan["candidate_configs"]} == {"v3cfg_invalid_v5", "v3cfg_legacy_method", "v3cfg_formal"}
    assert set(item["config_id"] for item in scan["preserved_configs"]) == {"v3cfg_current_v5", "v3cfg_workload"}
    assert set(dry_run["deleted_saved_config_ids"]) == {"v3cfg_invalid_v5", "v3cfg_legacy_method", "v3cfg_formal"}
    assert (saved_root / "v3cfg_formal.json").is_file()
    actual = cleanup.cleanup_legacy_saved_configs(dry_run=False, saved_config_root=saved_root)

    assert set(actual["deleted_saved_config_ids"]) == {"v3cfg_invalid_v5", "v3cfg_legacy_method", "v3cfg_formal"}
    assert not (saved_root / "v3cfg_formal.json").exists()
    assert not (saved_root / "v3cfg_legacy_method.json").exists()
    assert not (saved_root / "v3cfg_invalid_v5.json").exists()
    assert (saved_root / "v3cfg_current_v5.json").is_file()
    assert (saved_root / "v3cfg_workload.json").is_file()


def _write_four_method_group(formal_root: Path, group_id: str, finished_at: str) -> None:
    children = [{"status": "completed", "method_config_id": method_id} for method_id in cleanup.FOUR_METHOD_IDS]
    _write_group(formal_root, group_id, {"status": "completed", "finished_at": finished_at}, children=children)


def _write_group(formal_root: Path, group_id: str, data: dict, children: list[dict] | None = None) -> None:
    group_dir = formal_root / group_id
    group_dir.mkdir(parents=True)
    payload = {"run_group_id": group_id, "created_at": "2026-07-27T00:00:00+00:00", **data}
    (group_dir / "run_group.json").write_text(json.dumps(payload), encoding="utf-8")
    if children:
        child_dir = group_dir / "children"
        child_dir.mkdir()
        for index, child in enumerate(children):
            child_id = f"v5child_{index:016x}"
            child_payload = {"child_run_id": child_id, "run_group_id": group_id, **child}
            (child_dir / f"{child_id}.json").write_text(json.dumps(child_payload), encoding="utf-8")


def _touch_file(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


def _write_saved_config(root: Path, config_id: str, data: dict) -> None:
    root.mkdir(parents=True, exist_ok=True)
    payload = {
        "config_id": config_id,
        "config_kind": "method",
        "name": config_id,
        "description": "",
        "owner_label": "local_user",
        "tags": [],
        "created_at": "2026-07-28T00:00:00",
        "updated_at": "2026-07-28T00:00:00",
        "payload": {},
        "validation_status": "valid",
        "last_validation": None,
        "last_smoke_run_id": None,
        "source": "user_saved",
        "truth_boundary": "local_emulator_config_not_production_chain",
        "version": 1,
        **data,
    }
    (root / f"{config_id}.json").write_text(json.dumps(payload), encoding="utf-8")
