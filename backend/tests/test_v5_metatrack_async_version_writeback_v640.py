from __future__ import annotations

import json
from pathlib import Path

from backend.app.services.v5_metric_extractor import _apply_metatrack_async_version_writeback_v640_metrics


def _write(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload), encoding="utf-8")


def test_v640_async_writeback_uses_leader_truth_only(tmp_path: Path) -> None:
    _write(tmp_path / "compiled_run_plan.json", {
        "node_configs": [
            {"node_id": "n0", "shard_id": "s0", "leader": True},
            {"node_id": "n1", "shard_id": "s0", "leader": False},
            {"node_id": "n4", "shard_id": "s1", "leader": True},
        ]
    })
    _write(tmp_path / "nodes/n0/node_summary.json", {"runtime_metric_counts": {
        "metatrack_async_version_writeback_v640_block_count": 4,
        "metatrack_block_remote_value_consumer_edge_count": 11,
        "metatrack_block_consumer_critical_final_publish_count": 3,
        "metatrack_background_final_enqueue_count": 19,
        "metatrack_critical_version_publish_wait_ms": 27,
        "metatrack_background_final_writeback_ms": 31,
        "metatrack_final_join_wait_ms": 5,
        "metatrack_async_version_writeback_batch_count": 8,
        "metatrack_async_version_writeback_item_count": 19,
        "metatrack_async_version_writeback_max_queue_depth": 13,
    }})
    _write(tmp_path / "nodes/n1/node_summary.json", {"runtime_metric_counts": {
        "metatrack_async_version_writeback_v640_block_count": 999,
        "metatrack_final_join_wait_ms": 9999,
        "metatrack_async_version_writeback_max_queue_depth": 999,
    }})
    _write(tmp_path / "nodes/n4/node_summary.json", {"runtime_metric_counts": {
        "metatrack_async_version_writeback_v640_block_count": 3,
        "metatrack_block_remote_value_consumer_edge_count": 5,
        "metatrack_block_consumer_critical_final_publish_count": 1,
        "metatrack_background_final_enqueue_count": 9,
        "metatrack_critical_version_publish_wait_ms": 10,
        "metatrack_background_final_writeback_ms": 12,
        "metatrack_final_join_wait_ms": 2,
        "metatrack_async_version_writeback_batch_count": 4,
        "metatrack_async_version_writeback_item_count": 9,
        "metatrack_async_version_writeback_max_queue_depth": 8,
    }})
    metrics = {"source_artifacts": []}
    _apply_metatrack_async_version_writeback_v640_metrics(metrics, tmp_path)
    assert metrics["metatrack_async_version_writeback_v640_block_count"] == 7
    assert metrics["metatrack_background_final_enqueue_count"] == 28
    assert metrics["metatrack_final_join_wait_ms"] == 7
    assert metrics["metatrack_async_version_writeback_max_queue_depth"] == 13
    assert metrics["metatrack_implementation_revision"] == "v6.4.0"
    assert metrics["metatrack_execution_scope_policy"] == "transaction_level_full_locality_shared_block_v640"
    assert "nodes/n1/node_summary.json" not in metrics["source_artifacts"]


def test_v640_restores_transaction_level_execution_and_only_backgrounds_noncritical_finals() -> None:
    root = Path(__file__).resolve().parents[2]
    registry = (root / "executor/v5/registry.go").read_text(encoding="utf-8")
    liveness = (root / "executor/v5/metatrack_version_liveness_v5.go").read_text(encoding="utf-8")
    async_source = (root / "executor/v5/metatrack_async_version_writeback_v640.go").read_text(encoding="utf-8")
    assert "executeMetaTrackProjectionPlanV630" not in registry
    assert "executeMetaTrackScheduleWithFullLocality" in registry
    assert "bindMetaTrackInBlockVersionHandoffs(input.Block.TxList, &classification)" in registry
    assert "enqueueMetaTrackAsyncFinalV640" in liveness
    assert "closureBackgroundAsync := buffer.asyncV640 != nil" in liveness
    assert "metatrack_block_consumer_critical_final_publish_count" in liveness
    assert "metatrack_version_remote_live_immediate_publish_count" in liveness
    assert "deferOrJoinMetaTrackAsyncVersionsV656" in liveness
    overlap = (root / "executor/v5/metatrack_final_join_overlap_v656.go").read_text(encoding="utf-8")
    runtime = (root / "executor/v5/runtime.go").read_text(encoding="utf-8")
    assert "joinMetaTrackAsyncVersionsV640" in overlap
    assert "joinDeferredMetaTrackAsyncVersionsV656(ctx, block)" in runtime
    assert runtime.index("joinDeferredMetaTrackAsyncVersionsV656(ctx, block)") < runtime.index('r.setCommitPhase("durable_commit", block)')
    assert "critical []" not in async_source
    assert "time.Sleep(" not in async_source
    assert "time.NewTicker(" not in async_source
    assert "50" not in async_source
    assert "100" not in async_source
    assert "r.applyRemoteStateDeltaBatch(publisherState.ctx, block, homeShard, items, unqualified, deltas)" in async_source
