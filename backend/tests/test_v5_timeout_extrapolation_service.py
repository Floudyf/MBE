from __future__ import annotations

import csv
import json
from pathlib import Path

from backend.app.services.v5_timeout_extrapolation import (
    analyze_run_timeout_extrapolation,
    write_timeout_tps_exports,
)


def _write_timeout_fixture(root: Path, per_minute: list[int], *, fresh: bool = True, aligned: bool = True) -> Path:
    root.mkdir(parents=True, exist_ok=True)
    (root / "client").mkdir(parents=True, exist_ok=True)
    first_submitted = 1_700_000_000_000
    (root / "client" / "client_submission_complete.json").write_text(
        json.dumps({"first_submitted_at": str(first_submitted)}), encoding="utf-8"
    )
    terminal = 0
    progress = root / "drain_progress.csv"
    with progress.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=["timestamp", "phase", "submitted", "terminal"])
        writer.writeheader()
        writer.writerow({"timestamp": first_submitted + 5000, "phase": "DRAINING", "submitted": 10000, "terminal": 0})
        for minute, delta in enumerate(per_minute, start=1):
            terminal += delta
            writer.writerow({
                "timestamp": first_submitted + 5000 + minute * 60_000,
                "phase": "DRAINING",
                "submitted": 10000,
                "terminal": terminal,
            })
    (root / "drain_status.json").write_text(
        json.dumps({
            "submitted": 10000,
            "terminal": terminal,
            "incomplete": 10000 - terminal,
            "phase": "FAILED",
            "completion_reason": "hard_timeout",
            "fresh_status_complete": fresh,
        }),
        encoding="utf-8",
    )
    (root / "stalled_runtime_report.json").write_text(
        json.dumps({
            "reason": "drain hard timeout",
            "last_snapshot": {"min_height": 123 if aligned else 122, "max_height": 123},
        }),
        encoding="utf-8",
    )
    return root


def test_stable_tail_qualifies(tmp_path: Path) -> None:
    per_minute = [100] * 60
    run_dir = _write_timeout_fixture(tmp_path / "stable", per_minute)
    evidence = analyze_run_timeout_extrapolation(run_dir)
    assert evidence["qualified_for_tps_estimate"] is True
    assert evidence["measurement_type"] == "timeout_extrapolated"
    assert evidence["estimated_end_to_end_tps"] > 0
    assert evidence["tail_terminal_r2"] >= 0.995


def test_recovering_tail_is_rejected(tmp_path: Path) -> None:
    per_minute = [80] * 40 + [40] * 5 + [60] * 5 + [90] * 5 + [130] * 5
    run_dir = _write_timeout_fixture(tmp_path / "recovering", per_minute)
    evidence = analyze_run_timeout_extrapolation(run_dir)
    assert evidence["qualified_for_tps_estimate"] is False
    assert evidence["measurement_type"] == "timeout_unstable"
    assert evidence["estimated_end_to_end_tps"] is None


def test_incomplete_status_is_rejected(tmp_path: Path) -> None:
    run_dir = _write_timeout_fixture(tmp_path / "incomplete", [100] * 60, fresh=False)
    evidence = analyze_run_timeout_extrapolation(run_dir)
    assert evidence["qualified_for_tps_estimate"] is False
    assert "status_snapshot_incomplete" in evidence["reasons"]


def test_non_hard_timeout_is_ignored(tmp_path: Path) -> None:
    run_dir = _write_timeout_fixture(tmp_path / "no_progress", [100] * 60)
    payload = json.loads((run_dir / "drain_status.json").read_text(encoding="utf-8"))
    payload["completion_reason"] = "no_progress_timeout"
    (run_dir / "drain_status.json").write_text(json.dumps(payload), encoding="utf-8")
    (run_dir / "stalled_runtime_report.json").write_text(
        json.dumps({"reason": "drain no-progress timeout", "last_snapshot": {"min_height": 1, "max_height": 1}}),
        encoding="utf-8",
    )
    assert analyze_run_timeout_extrapolation(run_dir) == {}


def test_additive_export_keeps_observed_and_marks_estimate(tmp_path: Path) -> None:
    timeout_evidence = {
        "schema_version": "mbe_v5_timeout_extrapolation_v1",
        "measurement_type": "timeout_extrapolated",
        "qualified_for_tps_estimate": True,
        "estimated_end_to_end_tps": 2.4,
        "observed_duration_ms": 3_600_000,
        "terminal_at_timeout": 9000,
        "incomplete_at_timeout": 1000,
        "tail_regression_tps": 1.7,
        "tail_cv": 0.05,
        "tail_relative_drift": -0.02,
        "tail_terminal_r2": 0.999,
        "estimated_remaining_seconds": 588.0,
        "estimated_total_duration_seconds": 4188.0,
        "empirical_estimated_tps_low": 2.35,
        "empirical_estimated_tps_high": 2.45,
    }
    common = {
        "suite_type": "workload_sensitivity",
        "method_config_id": "hash_cg",
        "method": {"display_name": "CG"},
        "seed": 1,
        "scan_variable": "target_theta",
        "scan_value": 1.1,
    }
    completed = {
        **common,
        "child_run_id": "complete",
        "repeat_index": 0,
        "status": "completed",
        "execution_status": "completed",
        "formal_eligibility": True,
        "metrics": {"end_to_end_tps": 2.5},
    }
    timed = {
        **common,
        "child_run_id": "timeout",
        "repeat_index": 1,
        "status": "failed",
        "execution_status": "failed",
        "formal_eligibility": False,
        "metrics": {},
        "result": {"summary": {"timeout_extrapolation": timeout_evidence}},
    }
    report = write_timeout_tps_exports(tmp_path, {"run_group_id": "g"}, [completed, timed])
    assert report["timeout_extrapolated_count"] == 1
    with (tmp_path / "paper_figure_tps_with_timeout_extrapolation.csv").open(newline="", encoding="utf-8") as handle:
        rows = list(csv.DictReader(handle))
    assert len(rows) == 1
    assert rows[0]["marker"] == "*"
    assert rows[0]["measurement_modes"] == "observed_complete,timeout_extrapolated"
    assert int(rows[0]["sample_count"]) == 2
