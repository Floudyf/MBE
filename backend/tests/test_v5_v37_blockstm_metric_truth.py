from __future__ import annotations

import json
from pathlib import Path

from backend.app.services import v5_metric_extractor


def _write(path: Path, value: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value), encoding="utf-8")


def test_v37_blockstm_suspension_truth_is_replica_deduplicated_and_modes_survive(tmp_path: Path) -> None:
    _write(
        tmp_path / "aggregate" / "block_stm_aggregate_summary.json",
        {
            "status": "available",
            "worker_count": 4,
            "maximum_parallel_width": 8,
            "maximum_concurrent_executions": 8,
            "maximum_incarnation": 2,
            "serial_fallback_count": 0,
            "serial_equivalent": True,
            "scheduler_mode": "priority_heap_v1",
            "scheduler_mode_replica_consistent": True,
            "dependency_wait_mode": "suspend_same_incarnation_v1",
            "dependency_wait_mode_replica_consistent": True,
            "per_validator": [
                {"node_id": "n0", "shard_id": "s0", "dependency_suspend_count": 5, "dependency_wait_count": 5, "dependency_resume_count": 5},
                {"node_id": "n1", "shard_id": "s0", "dependency_suspend_count": 6, "dependency_wait_count": 6, "dependency_resume_count": 6},
                {"node_id": "n4", "shard_id": "s1", "dependency_suspend_count": 3, "dependency_wait_count": 3, "dependency_resume_count": 3},
            ],
        },
    )
    metrics = {"source_artifacts": []}
    v5_metric_extractor._apply_block_stm_metrics(metrics, tmp_path)
    assert metrics["dependency_suspend_count"] == 9
    assert metrics["dependency_wait_count"] == 9
    assert metrics["dependency_resume_count"] == 9
    assert metrics["block_stm_scheduler_mode"] == "priority_heap_v1"
    assert metrics["block_stm_scheduler_mode_replica_consistent"] is True
    assert metrics["block_stm_dependency_wait_mode"] == "suspend_same_incarnation_v1"
    assert metrics["block_stm_dependency_wait_mode_replica_consistent"] is True
