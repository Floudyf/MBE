from __future__ import annotations

import csv
from pathlib import Path

from backend.app.services.v5_metric_extractor import _read_scheduler_metrics


def test_batch_si_scheduler_deferral_events_are_not_execution_aborts(tmp_path: Path) -> None:
    path = tmp_path / "metatrack_scheduler_trace.csv"
    fieldnames = [
        "block_height",
        "tx_id",
        "decision_reason",
        "blocked",
        "wakeup",
        "stolen_work",
        "local_execution",
        "ready_queue_depth",
        "fast_queue_depth",
        "conservative_queue_depth",
        "dependency_wait_ms",
        "scheduler_idle_ms",
    ]
    rows = [
        {"block_height": "1", "tx_id": "T1", "decision_reason": "batch_si_ofas_cycle_deferred", "blocked": "true"},
        {"block_height": "2", "tx_id": "T1", "decision_reason": "batch_si_ofas_cycle_deferred", "blocked": "true"},
        {"block_height": "3", "tx_id": "T1", "decision_reason": "batch_si_accepted", "local_execution": "true"},
        {"block_height": "1", "tx_id": "T2", "decision_reason": "batch_si_accepted", "local_execution": "true"},
    ]
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow({name: row.get(name, "") for name in fieldnames})

    metrics = _read_scheduler_metrics(path)

    assert metrics["batch_si_deferred_event_count"] == 2
    assert metrics["batch_si_unique_deferred_tx_count"] == 1
    assert metrics["batch_si_accepted_transaction_count"] == 2
    assert metrics["batch_si_deferral_event_rate"] == 0.5
    assert metrics["batch_si_unique_deferral_rate"] == 0.5
    assert metrics["batch_si_mean_deferrals_per_finalized_tx"] == 1.0
    assert "abort_count" not in metrics
