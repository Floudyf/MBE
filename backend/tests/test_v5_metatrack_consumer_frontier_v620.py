from __future__ import annotations

import json
from pathlib import Path

from backend.app.services.v5_metric_extractor import _apply_metatrack_liveness_safe_boundary_batch_v621_metrics


def _write(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload), encoding="utf-8")


def test_v621_historical_metric_extractor_remains_backward_compatible(tmp_path: Path) -> None:
    _write(tmp_path / "compiled_run_plan.json", {
        "node_configs": [
            {"node_id": "n0", "shard_id": "s0", "leader": True},
            {"node_id": "n1", "shard_id": "s0", "leader": False},
            {"node_id": "n4", "shard_id": "s1", "leader": True},
        ]
    })
    _write(tmp_path / "nodes/n0/node_summary.json", {"runtime_metric_counts": {
        "metatrack_closure_boundary_batch_request_group_count": 7,
        "metatrack_closure_boundary_batch_item_count": 19,
        "metatrack_closure_boundary_immediate_publish_count": 23,
        "metatrack_version_remote_live_immediate_publish_count": 11,
        "metatrack_version_final_immediate_publish_count": 12,
    }})
    _write(tmp_path / "nodes/n1/node_summary.json", {"runtime_metric_counts": {
        "metatrack_closure_boundary_batch_item_count": 9999,
    }})
    _write(tmp_path / "nodes/n4/node_summary.json", {"runtime_metric_counts": {
        "metatrack_closure_boundary_batch_request_group_count": 5,
        "metatrack_closure_boundary_batch_item_count": 14,
        "metatrack_closure_boundary_immediate_publish_count": 17,
        "metatrack_version_remote_live_immediate_publish_count": 8,
        "metatrack_version_final_immediate_publish_count": 9,
    }})
    metrics = {"source_artifacts": []}
    _apply_metatrack_liveness_safe_boundary_batch_v621_metrics(metrics, tmp_path)
    assert metrics["metatrack_closure_boundary_batch_request_group_count"] == 12
    assert metrics["metatrack_closure_boundary_batch_item_count"] == 33
    assert metrics["metatrack_closure_boundary_immediate_publish_count"] == 40
    assert metrics["metatrack_version_remote_live_immediate_publish_count"] == 19
    assert metrics["metatrack_version_final_immediate_publish_count"] == 21
    assert metrics["metatrack_closure_boundary_batch_policy"] == "producer_completion_home_batch_v621"
    assert "nodes/n1/node_summary.json" not in metrics["source_artifacts"]


def test_v640_preserves_v621_liveness_fix_without_forcing_every_final_immediate() -> None:
    root = Path(__file__).resolve().parents[2]
    registry = (root / "executor/v5/registry.go").read_text(encoding="utf-8")
    liveness = (root / "executor/v5/metatrack_version_liveness_v5.go").read_text(encoding="utf-8")
    async_helper = (root / "executor/v5/metatrack_async_version_writeback_v640.go").read_text(encoding="utf-8")

    # v6.2.0 JIT StateReady remains removed; v6.3 projection barriers remain removed.
    assert "activateConsumerFrontierState := func" not in registry
    assert "producer_dependency_jit_state_ready_v620" not in registry
    assert "executeMetaTrackProjectionPreservingV630" not in registry
    assert "executeMetaTrackProjectionPlanV630" not in registry
    assert 'bindMetaTrackInBlockVersionHandoffs(input.Block.TxList, &classification)' in registry
    assert "executeMetaTrackScheduleWithFullLocality" in registry

    # v6.4 replaces the v6.2.1 unconditional closure-final publication rule with
    # an aggregate-PBFT-block exact-value consumer guard.
    assert 'closureImmediateBatch := r.metaTrackBlockExecutorFlag("dependency_closed_consensus")' not in liveness
    assert 'boundaryRequired := r.metaTrackBlockExecutorFlag("dependency_closed_consensus")' not in liveness
    assert "closureBackgroundAsync := buffer.asyncV640 != nil" in liveness
    assert "buffer.blockRemoteValueSuccessorCount[identity] > 0" in liveness
    assert "enqueueMetaTrackAsyncFinalV640" in liveness
    assert "deferOrJoinMetaTrackAsyncVersionsV656" in liveness
    overlap = (root / "executor/v5/metatrack_final_join_overlap_v656.go").read_text(encoding="utf-8")
    runtime = (root / "executor/v5/runtime.go").read_text(encoding="utf-8")
    assert "joinMetaTrackAsyncVersionsV640" in async_helper
    assert "joinMetaTrackAsyncVersionsV640" in overlap
    assert "joinDeferredMetaTrackAsyncVersionsV656(ctx, block)" in runtime
    assert runtime.index("joinDeferredMetaTrackAsyncVersionsV656(ctx, block)") < runtime.index('r.setCommitPhase("durable_commit", block)')
    assert "metaTrackBlockRemoteValueConsumerIndexV640" in async_helper
    assert "transactionRequiresExactStateValue" in async_helper
    assert "sourceShard != consumerShard" in async_helper

    # Same-block remote exact-value consumers and remote_live versions stay on
    # the immediate publication path; only non-critical closure finals go async.
    assert "metatrack_block_consumer_critical_final_publish_count" in liveness
    assert "metatrack_version_remote_live_immediate_publish_count" in liveness
    assert "metatrack_closure_boundary_immediate_publish_count" in liveness
    assert "metatrack_background_final_writeback_ms" in async_helper
    assert "metatrack_final_join_wait_ms" in async_helper


def test_v640_batch_transport_still_wakes_each_exact_version() -> None:
    root = Path(__file__).resolve().parents[2]
    full_locality = (root / "executor/v5/metatrack_full_locality_v4.go").read_text(encoding="utf-8")
    runtime = (root / "executor/v5/runtime.go").read_text(encoding="utf-8")
    assert "func (r *NodeRuntime) handleStateDeltaApplyBatch" in full_locality
    assert "acks = append(acks, r.handleStateDeltaApplyRequest(item))" in full_locality
    handler = runtime[
        runtime.index("func (r *NodeRuntime) handleStateDeltaApplyRequest"):
        runtime.index("func (r *NodeRuntime) applyQueuedStateDeltas")
    ]
    assert 'if request.ProducedVersion > 0 && request.UpdateSemantics != "commutative_delta" {' in handler
    assert "r.publishStateVersion(request.Key, request.ProducedVersion, request.Value)" in handler
