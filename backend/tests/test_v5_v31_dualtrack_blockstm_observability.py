import json
from pathlib import Path

from backend.app.services.v5_metric_extractor import (
    _apply_block_stm_metrics,
    _apply_metatrack_track_observability,
)


def _write(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload), encoding="utf-8")


def test_v31_metatrack_nanosecond_sojourn_and_business_truth(tmp_path: Path) -> None:
    _write(tmp_path / "compiled_run_plan.json", {
        "node_configs": [
            {"node_id": "n0", "shard_id": "s0", "leader": True, "role": "leader"},
            {"node_id": "n4", "shard_id": "s1", "leader": True, "role": "leader"},
        ]
    })
    for node_id, shard_id in (("n0", "s0"), ("n4", "s1")):
        _write(tmp_path / "nodes" / node_id / "block_execution_summary.json", {
            "node_id": node_id,
            "shard_id": shard_id,
            "block_executor_id": "metatrack_block_executor",
            "blocks": [{
                "metatrack_fast_initial_tx_count": 1,
                "metatrack_conservative_initial_tx_count": 1,
                "metatrack_fast_final_tx_count": 1,
                "metatrack_conservative_final_tx_count": 1,
                "metatrack_fast_business_execution_attempt_count": 1,
                "metatrack_conservative_business_execution_attempt_count": 1,
                "metatrack_fast_business_execution_sum_ms": 0.0005,
                "metatrack_conservative_business_execution_sum_ms": 0.0015,
                "metatrack_fast_track_sojourn_sum_ms": 10.0,
                "metatrack_conservative_track_sojourn_sum_ms": 20.0,
                "metatrack_fast_state_wait_sum_ms": 3.0,
                "metatrack_conservative_state_wait_sum_ms": 4.0,
                "metatrack_fast_dependency_wait_sum_ms": 2.0,
                "metatrack_conservative_dependency_wait_sum_ms": 5.0,
                "metatrack_fast_queue_wait_sum_ms": 1.0,
                "metatrack_conservative_queue_wait_sum_ms": 2.0,
                "metatrack_fast_fallback_count": 0,
                "metatrack_fast_discarded_tentative_count": 0,
                "metatrack_fast_discarded_execution_ms": 0.0,
                "metatrack_conservative_reexecution_count": 0,
            }],
        })
        (tmp_path / "nodes" / node_id / "business_execute_invocation_count_by_node.csv").write_text(
            "node_id,shard_id,block_height,block_hash,tx_id,track,attempt,reason,success,final_completion,duration_us,duration_ns,sojourn_ns,state_wait_ns,dependency_wait_ns,queue_wait_ns,attempt_start_offset_ns,attempt_end_offset_ns\n"
            f"{node_id},{shard_id},1,b1,f1,fast,1,done,true,true,0,500,10000000,3000000,2000000,1000000,1000000,1000500\n"
            f"{node_id},{shard_id},1,b1,c1,conservative,1,done,true,true,1,1500,20000000,4000000,5000000,2000000,2000000,2001500\n",
            encoding="utf-8",
        )
    metrics = {
        "source_artifacts": [],
        "submitted_unique_tx_count": 4,
        "business_execution_cpu_sum_ms": 999.0,
        "business_execution_critical_path_ms": 888.0,
    }
    _apply_metatrack_track_observability(metrics, tmp_path)
    assert metrics["metatrack_fast_business_execution_sum_ms"] == 0.001
    assert metrics["metatrack_conservative_business_execution_sum_ms"] == 0.003
    assert metrics["metatrack_fast_track_sojourn_mean_ms"] == 10.0
    assert metrics["metatrack_conservative_track_sojourn_mean_ms"] == 20.0
    assert metrics["metatrack_fast_state_wait_sum_ms"] == 6.0
    assert metrics["metatrack_conservative_dependency_wait_sum_ms"] == 10.0
    assert metrics["metatrack_fast_business_execution_p95_ms"] == 0.0005
    assert metrics["metatrack_conservative_business_execution_p99_ms"] == 0.0015
    assert metrics["business_execution_cpu_sum_ms"] == 0.004
    assert metrics["metatrack_business_execution_cpu_sum_ms"] == 0.004
    assert metrics["metatrack_legacy_execution_envelope_business_cpu_sum_ms"] == 999.0
    assert metrics["metatrack_legacy_execution_envelope_business_critical_path_ms"] == 888.0
    assert metrics["business_execution_critical_path_ms"] == 0.002
    assert metrics["metatrack_business_execution_critical_path_ms"] == 0.002
    assert metrics["metatrack_attempt_timing_precision"] == "nanosecond_monotonic"


def test_v31_blockstm_unique_transaction_rates_from_leader_block_evidence(tmp_path: Path) -> None:
    _write(tmp_path / "compiled_run_plan.json", {
        "node_configs": [
            {"node_id": "n0", "shard_id": "s0", "leader": True, "role": "leader"},
            {"node_id": "n4", "shard_id": "s1", "leader": True, "role": "leader"},
        ]
    })
    _write(tmp_path / "aggregate" / "block_stm_aggregate_summary.json", {
        "status": "available",
        "worker_count": 8,
        "maximum_parallel_width": 8,
        "maximum_concurrent_executions": 8,
        "abort_count": 12,
        "reexecution_count": 10,
        "validation_failure_count": 8,
        "serial_equivalent": True,
        "per_validator": [
            {"node_id": "n0", "shard_id": "s0", "abort_count": 6, "reexecution_count": 5, "validation_failure_count": 4},
            {"node_id": "n4", "shard_id": "s1", "abort_count": 6, "reexecution_count": 5, "validation_failure_count": 4},
        ],
    })
    for node_id, shard_id, unique_abort, unique_reexec in (("n0", "s0", 3, 2), ("n4", "s1", 4, 3)):
        _write(tmp_path / "nodes" / node_id / "block_execution_summary.json", {
            "node_id": node_id,
            "shard_id": shard_id,
            "block_executor_id": "block_stm_block_executor",
            "blocks": [{
                "block_stm_metrics": {
                    "unique_aborted_transaction_count": unique_abort,
                    "unique_reexecuted_transaction_count": unique_reexec,
                }
            }],
        })
    metrics = {"source_artifacts": [], "submitted_unique_tx_count": 20}
    _apply_block_stm_metrics(metrics, tmp_path)
    assert metrics["block_stm_unique_aborted_tx_count"] == 7
    assert metrics["block_stm_unique_reexecuted_tx_count"] == 5
    assert metrics["block_stm_unique_aborted_tx_rate"] == 0.35
    assert metrics["block_stm_unique_reexecuted_tx_rate"] == 0.25
    assert metrics["block_stm_unique_tx_truth_scope"] == "sum_of_per_execution_shard_leader_block_unique_transaction_counts"
