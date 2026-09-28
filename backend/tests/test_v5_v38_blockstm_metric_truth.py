from __future__ import annotations

import json
from pathlib import Path

from backend.app.services import v5_metric_extractor


def test_v38_blockstm_metric_aliases_preserve_suspend_and_modes(tmp_path: Path) -> None:
    aggregate = tmp_path / "aggregate"
    aggregate.mkdir()
    payload = {
        "status": "available",
        "worker_count": 8,
        "maximum_parallel_width": 8,
        "dependency_suspend_count": 24,
        "scheduler_mode": "priority_heap_v1",
        "scheduler_mode_replica_consistent": True,
        "dependency_wait_mode": "suspend_same_incarnation_v1",
        "dependency_wait_mode_replica_consistent": True,
        "maximum_incarnation": 2,
        "serial_fallback_count": 0,
        "serial_equivalent": True,
        "per_validator": [
            {"node_id": "n0", "shard_id": "s0", "dependency_wait_count": 12, "dependency_resume_count": 12, "dependency_suspend_count": 12},
            {"node_id": "n1", "shard_id": "s0", "dependency_wait_count": 12, "dependency_resume_count": 12, "dependency_suspend_count": 12},
            {"node_id": "n4", "shard_id": "s1", "dependency_wait_count": 7, "dependency_resume_count": 7, "dependency_suspend_count": 7},
            {"node_id": "n5", "shard_id": "s1", "dependency_wait_count": 7, "dependency_resume_count": 7, "dependency_suspend_count": 7},
        ],
    }
    (aggregate / "block_stm_aggregate_summary.json").write_text(json.dumps(payload), encoding="utf-8")
    metrics = {"source_artifacts": []}
    v5_metric_extractor._apply_block_stm_metrics(metrics, tmp_path)
    # Replica-deduplicated truth: max per shard, then sum across shards.
    assert metrics["block_stm_dependency_wait_count"] == 19
    assert metrics["block_stm_dependency_resume_count"] == 19
    assert metrics["block_stm_dependency_suspend_count"] == 19
    assert metrics["dependency_suspend_count"] == 19
    assert metrics["block_stm_scheduler_mode"] == "priority_heap_v1"
    assert metrics["block_stm_scheduler_mode_replica_consistent"] is True
    assert metrics["block_stm_dependency_wait_mode"] == "suspend_same_incarnation_v1"
    assert metrics["block_stm_dependency_wait_mode_replica_consistent"] is True
