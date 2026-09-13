from __future__ import annotations

import csv
import json
import zipfile
from pathlib import Path

import pytest
from fastapi import HTTPException

from backend.app.api import v5_formal_experiments as api
from backend.app.services import v5_timeout_extrapolation_backfill as backfill


def _write_stable_diagnostics(path: Path, *, recovering: bool = False) -> None:
    path.mkdir(parents=True, exist_ok=True)
    first = 1_800_000_000_000
    (path / "client").mkdir(parents=True, exist_ok=True)
    (path / "client" / "client_submission_complete.json").write_text(
        json.dumps({"first_submitted_at": str(first)}), encoding="utf-8"
    )
    terminal = 0
    with (path / "drain_progress.csv").open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=["timestamp", "phase", "submitted", "terminal"])
        writer.writeheader()
        writer.writerow({"timestamp": first + 5_000, "phase": "DRAINING", "submitted": 10_000, "terminal": 0})
        for minute in range(60):
            if recovering and minute >= 40:
                rate = [40, 60, 90, 130][min(3, (minute - 40) // 5)]
            else:
                rate = 100 if minute >= 40 else 110
            terminal += rate
            writer.writerow({
                "timestamp": first + 5_000 + (minute + 1) * 60_000,
                "phase": "DRAINING",
                "submitted": 10_000,
                "terminal": terminal,
            })
    (path / "drain_status.json").write_text(json.dumps({
        "submitted": 10_000,
        "terminal": terminal,
        "incomplete": 10_000 - terminal,
        "phase": "FAILED",
        "completion_reason": "hard_timeout",
        "fresh_status_complete": True,
    }), encoding="utf-8")
    (path / "stalled_runtime_report.json").write_text(json.dumps({
        "reason": "drain hard timeout",
        "last_snapshot": {"min_height": 120, "max_height": 120},
    }), encoding="utf-8")


def _child() -> dict:
    return {
        "child_run_id": "v5child_timeout",
        "status": "failed",
        "execution_status": "failed",
        "formal_eligibility": False,
        "paper_candidate": False,
        "error": "drain hard timeout",
        "suite_type": "workload_sensitivity",
        "method_config_id": "hash_cg",
        "method": {"display_name": "CG/Nezha"},
        "seed": 11,
        "repeat_index": 0,
        "scan_variable": "target_theta",
        "scan_value": 1.1,
        "workload_point": {"target_theta": 1.1, "tx_count": 10_000},
        "result": {"status": "failed", "summary": {"execution_status": "failed", "formal_eligibility": False}},
        "metrics": {},
    }


def _install_store(monkeypatch: pytest.MonkeyPatch, tmp_path: Path, child: dict) -> tuple[Path, dict, list[dict]]:
    gid = "v5grp_timeout_test"
    gdir = tmp_path / gid
    (gdir / "children").mkdir(parents=True)
    (gdir / "children" / f"{child['child_run_id']}.json").write_text(json.dumps(child), encoding="utf-8")
    group = {"run_group_id": gid, "status": "completed_with_failures", "aggregate": {}}
    state = [child]

    monkeypatch.setattr(backfill, "group_dir", lambda _: gdir)
    monkeypatch.setattr(backfill, "read_group", lambda _: dict(group))
    monkeypatch.setattr(backfill, "children", lambda _: [dict(item) for item in state])

    def write_child(_gid: str, value: dict) -> None:
        state[:] = [dict(value)]
        (gdir / "children" / f"{value['child_run_id']}.json").write_text(json.dumps(value), encoding="utf-8")

    monkeypatch.setattr(backfill, "write_child", write_child)
    monkeypatch.setattr(backfill, "write_group", lambda value: group.update(value))
    monkeypatch.setattr(backfill, "export_paper", lambda directory, current_group, items: {"count": 0, "children": len(items)})
    return gdir, group, state


def test_backfill_attaches_stable_estimate_without_changing_execution_truth(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    child = _child()
    gdir, group, state = _install_store(monkeypatch, tmp_path, child)
    _write_stable_diagnostics(gdir / "runtime_diagnostics" / child["child_run_id"])

    result = backfill.analyze_group_timeout_extrapolation("v5grp_timeout_test")

    assert result["candidate_count"] == 1
    assert result["qualified_count"] == 1
    assert result["unstable_count"] == 0
    assert result["original_execution_truth_preserved"] is True
    updated = state[0]
    assert updated["status"] == "failed"
    assert updated["execution_status"] == "failed"
    assert updated["formal_eligibility"] is False
    assert updated["paper_candidate"] is False
    assert updated["error"] == "drain hard timeout"
    evidence = updated["result"]["summary"]["timeout_extrapolation"]
    assert evidence["qualified_for_tps_estimate"] is True
    assert evidence["estimated_end_to_end_tps"] > 0
    assert updated["throughput_measurement_mode"] == "timeout_extrapolated"
    assert group["timeout_extrapolation_analysis"]["qualified_count"] == 1
    backups = list((gdir / ".timeout_extrapolation_backups").glob("*/v5child_timeout.json"))
    assert len(backups) == 1


def test_backfill_records_unstable_timeout_but_does_not_publish_tps(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    child = _child()
    gdir, _group, state = _install_store(monkeypatch, tmp_path, child)
    _write_stable_diagnostics(gdir / "runtime_diagnostics" / child["child_run_id"], recovering=True)

    result = backfill.analyze_group_timeout_extrapolation("v5grp_timeout_test")

    assert result["candidate_count"] == 1
    assert result["qualified_count"] == 0
    assert result["unstable_count"] == 1
    summary = state[0]["result"]["summary"]
    assert summary["throughput_measurement_mode"] == "timeout_unstable"
    assert summary["throughput_estimate_eligible"] is False
    assert summary["estimated_end_to_end_tps"] is None


def test_backfill_reads_historical_diagnostics_from_group_bundle(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    child = _child()
    gdir, _group, state = _install_store(monkeypatch, tmp_path, child)
    source = tmp_path / "source_diagnostics"
    _write_stable_diagnostics(source)
    prefix = f"runtime_diagnostics/{child['child_run_id']}"
    with zipfile.ZipFile(gdir / "artifacts.zip", "w", zipfile.ZIP_DEFLATED) as archive:
        for relative in (
            "drain_progress.csv",
            "drain_status.json",
            "stalled_runtime_report.json",
            "client/client_submission_complete.json",
        ):
            archive.write(source / relative, f"{prefix}/{relative}")

    result = backfill.analyze_group_timeout_extrapolation("v5grp_timeout_test")

    assert result["qualified_count"] == 1
    assert result["qualified"][0]["evidence_source"] == "run_group_artifacts_zip"
    evidence = state[0]["result"]["summary"]["timeout_extrapolation"]
    assert evidence["historical_evidence_source"] == "run_group_artifacts_zip"
    assert (gdir / "timeout_extrapolation_evidence" / "v5child_timeout.json").is_file()


def test_backfill_skips_missing_runtime_diagnostics(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    child = _child()
    _gdir, _group, state = _install_store(monkeypatch, tmp_path, child)
    result = backfill.analyze_group_timeout_extrapolation("v5grp_timeout_test")
    assert result["candidate_count"] == 1
    assert result["analyzed_count"] == 0
    assert result["skipped_count"] == 1
    assert state[0] == child


def test_api_rejects_timeout_analysis_for_active_group(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(api, "read_group", lambda _: {"run_group_id": "v5grp_active", "status": "running"})
    monkeypatch.setattr(api, "worker_active", lambda _: True)
    with pytest.raises(HTTPException) as exc:
        api.analyze_timeout_extrapolation_run_group("v5grp_active")
    assert exc.value.status_code == 409


def test_api_dispatches_terminal_group_to_backfill(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(api, "read_group", lambda _: {"run_group_id": "v5grp_done", "status": "completed_with_failures"})
    monkeypatch.setattr(api, "worker_active", lambda _: False)
    monkeypatch.setattr(
        api.v5_timeout_extrapolation_backfill,
        "analyze_group_timeout_extrapolation",
        lambda gid: {"run_group_id": gid, "qualified_count": 4},
    )
    result = api.analyze_timeout_extrapolation_run_group("v5grp_done")
    assert result == {"run_group_id": "v5grp_done", "qualified_count": 4}
