import json
from pathlib import Path

from backend.app.services.v5_metric_extractor import (
    _apply_metatrack_track_observability,
    _apply_stateless_version_frontier_metrics,
)


def _write(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload), encoding="utf-8")


def test_v30_stateless_frontier_metrics_are_leader_scoped_and_derived(tmp_path: Path) -> None:
    _write(tmp_path / "compiled_run_plan.json", {
        "node_configs": [
            {"node_id": "n0", "shard_id": "s0", "leader": True, "role": "leader"},
            {"node_id": "n1", "shard_id": "s0", "leader": False, "role": "validator"},
            {"node_id": "n4", "shard_id": "s1", "leader": True, "role": "leader"},
        ]
    })
    _write(tmp_path / "nodes" / "n0" / "node_summary.json", {
        "runtime_metric_counts": {
            "stateless_version_admission_candidate_event_count": 2,
            "stateless_version_admission_nonempty_frontier_event_count": 1,
            "stateless_version_admission_zero_frontier_event_count": 1,
            "stateless_version_admission_candidate_tx_count": 200,
            "stateless_version_admission_admitted_tx_count": 20,
            "stateless_version_admission_deferred_event_count": 180,
            "stateless_version_admission_external_exact_dependency_tx_count": 150,
            "stateless_version_admission_external_exact_dependency_edge_count": 170,
            "stateless_version_admission_external_exact_dependency_token_count": 100,
            "stateless_version_admission_external_version_ready_token_count": 10,
            "stateless_version_admission_external_version_not_ready_token_count": 90,
            "stateless_version_admission_internal_candidate_dependency_edge_count": 30,
            "stateless_version_admission_deferred_direct_external_not_ready_count": 140,
            "stateless_version_admission_deferred_internal_propagation_count": 40,
        }
    })
    _write(tmp_path / "nodes" / "n1" / "node_summary.json", {"runtime_metric_counts": {"stateless_version_admission_candidate_tx_count": 9999}})
    _write(tmp_path / "nodes" / "n4" / "node_summary.json", {
        "runtime_metric_counts": {
            "stateless_version_admission_candidate_event_count": 1,
            "stateless_version_admission_nonempty_frontier_event_count": 1,
            "stateless_version_admission_zero_frontier_event_count": 0,
            "stateless_version_admission_candidate_tx_count": 100,
            "stateless_version_admission_admitted_tx_count": 10,
            "stateless_version_admission_deferred_event_count": 90,
            "stateless_version_admission_external_exact_dependency_tx_count": 80,
            "stateless_version_admission_external_exact_dependency_edge_count": 90,
            "stateless_version_admission_external_exact_dependency_token_count": 50,
            "stateless_version_admission_external_version_ready_token_count": 5,
            "stateless_version_admission_external_version_not_ready_token_count": 45,
            "stateless_version_admission_internal_candidate_dependency_edge_count": 20,
            "stateless_version_admission_deferred_direct_external_not_ready_count": 70,
            "stateless_version_admission_deferred_internal_propagation_count": 20,
        }
    })
    metrics = {"source_artifacts": []}
    _apply_stateless_version_frontier_metrics(metrics, tmp_path)
    assert metrics["stateless_version_admission_candidate_event_count"] == 3
    assert metrics["stateless_version_admission_candidate_tx_count"] == 300
    assert metrics["stateless_version_admission_admitted_tx_count"] == 30
    assert metrics["stateless_version_admission_candidate_mean_tx_count"] == 100.0
    assert metrics["stateless_version_admission_mean_frontier_width"] == 10.0
    assert metrics["stateless_version_admission_mean_nonempty_frontier_width"] == 15.0
    assert metrics["stateless_version_admission_zero_frontier_rate"] == 1 / 3
    assert metrics["stateless_version_admission_admission_ratio"] == 0.1
    assert metrics["stateless_version_admission_external_not_ready_ratio"] == 0.9
    assert metrics["stateless_version_admission_deferred_direct_external_not_ready_count"] == 210
    assert "nodes/n0/node_summary.json" in metrics["source_artifacts"]
    assert "nodes/n4/node_summary.json" in metrics["source_artifacts"]


def test_v30_metatrack_track_metrics_aggregate_leader_block_evidence(tmp_path: Path) -> None:
    _write(tmp_path / "compiled_run_plan.json", {"node_configs": [{"node_id": "n0", "shard_id": "s0", "leader": True, "role": "leader"}]})
    _write(tmp_path / "nodes" / "n0" / "block_execution_summary.json", {
        "node_id": "n0", "shard_id": "s0", "block_executor_id": "metatrack_block_executor",
        "blocks": [
            {
                "metatrack_fast_initial_tx_count": 9,
                "metatrack_conservative_initial_tx_count": 1,
                "metatrack_fast_final_tx_count": 8,
                "metatrack_conservative_final_tx_count": 2,
                "metatrack_fast_business_execution_attempt_count": 9,
                "metatrack_conservative_business_execution_attempt_count": 2,
                "metatrack_fast_business_execution_sum_ms": 18.0,
                "metatrack_conservative_business_execution_sum_ms": 10.0,
                "metatrack_fast_fallback_count": 1,
                "metatrack_fast_discarded_tentative_count": 1,
                "metatrack_fast_discarded_execution_ms": 2.0,
                "metatrack_conservative_reexecution_count": 1,
            },
            {
                "metatrack_fast_initial_tx_count": 10,
                "metatrack_conservative_initial_tx_count": 0,
                "metatrack_fast_final_tx_count": 10,
                "metatrack_conservative_final_tx_count": 0,
                "metatrack_fast_business_execution_attempt_count": 10,
                "metatrack_conservative_business_execution_attempt_count": 0,
                "metatrack_fast_business_execution_sum_ms": 20.0,
                "metatrack_conservative_business_execution_sum_ms": 0.0,
                "metatrack_fast_fallback_count": 0,
                "metatrack_fast_discarded_tentative_count": 0,
                "metatrack_fast_discarded_execution_ms": 0.0,
                "metatrack_conservative_reexecution_count": 0,
            },
        ],
    })
    business = tmp_path / "nodes" / "n0" / "business_execute_invocation_count_by_node.csv"
    business.write_text(
        "node_id,shard_id,block_height,block_hash,tx_id,track,attempt,reason,success,final_completion,duration_us\n"
        "n0,s0,1,b1,f1,fast,1,done,true,true,1000\n"
        "n0,s0,1,b1,f2,fast,1,done,true,true,3000\n"
        "n0,s0,1,b1,c1,conservative,1,done,true,true,4000\n"
        "n0,s0,1,b1,c2,conservative,2,done,true,true,6000\n",
        encoding="utf-8",
    )
    metrics = {"source_artifacts": []}
    _apply_metatrack_track_observability(metrics, tmp_path)
    assert metrics["metatrack_fast_initial_tx_count"] == 19
    assert metrics["metatrack_fast_fallback_count"] == 1
    assert metrics["metatrack_fast_fallback_rate"] == 1 / 19
    assert metrics["metatrack_fast_business_execution_sum_ms"] == 38.0
    assert metrics["metatrack_fast_business_execution_mean_ms"] == 2.0
    assert metrics["metatrack_conservative_business_execution_sum_ms"] == 10.0
    assert metrics["metatrack_conservative_business_execution_mean_ms"] == 5.0
    assert metrics["metatrack_conservative_reexecution_share"] == 0.5
    assert abs(metrics["metatrack_fast_business_execution_p95_ms"] - 2.9) < 1e-9
    assert abs(metrics["metatrack_fast_business_execution_p99_ms"] - 2.98) < 1e-9
    assert abs(metrics["metatrack_conservative_business_execution_p95_ms"] - 5.9) < 1e-9
    assert abs(metrics["metatrack_conservative_business_execution_p99_ms"] - 5.98) < 1e-9
    assert metrics["metatrack_track_duration_trace_available"] is True
