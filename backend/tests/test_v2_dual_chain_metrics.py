from pathlib import Path

import csv

from backend.app.services.dual_chain_replay import run_dual_chain_replay

ROOT = Path(__file__).resolve().parents[2]


def test_dual_chain_summary_reports_waits_and_imbalance(tmp_path: Path) -> None:
    result = run_dual_chain_replay(ROOT / "configs/experiments/v2_dual_chain_sample.yaml", tmp_path)
    summary = result["summary"]

    assert summary["finality_wait_time_ms"] > 0
    assert summary["target_wait_time_ms"] > summary["source_wait_time_ms"]
    assert summary["chain_speed_imbalance"] == 5.0
    assert summary["source_block_interval_ms"] == 100
    assert summary["target_block_interval_ms"] == 300


def test_stage_metrics_include_expected_commit_and_finality(tmp_path: Path) -> None:
    run_dual_chain_replay(ROOT / "configs/experiments/v2_dual_chain_sample.yaml", tmp_path)
    with (tmp_path / "stage_metrics.csv").open(encoding="utf-8", newline="") as stream:
        rows = list(csv.DictReader(stream))

    assert rows
    assert {"expected_commit_time_ms", "expected_finality_time_ms", "finality_wait_time_ms", "backend_type"}.issubset(rows[0])
    assert all(row["backend_type"] == "local_virtual" for row in rows)
