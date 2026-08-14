import json
from pathlib import Path

from backend.app.services.v5_metric_extractor import _apply_batch_si_metrics


def test_batch_si_runtime_performance_evidence_is_aggregated_from_leader_blocks(tmp_path: Path) -> None:
    (tmp_path / "compiled_run_plan.json").write_text(
        json.dumps({"node_configs": [{"node_id": "n0", "shard_id": "s0", "leader": True, "role": "leader"}]}),
        encoding="utf-8",
    )
    node_dir = tmp_path / "nodes" / "n0"
    node_dir.mkdir(parents=True)
    (node_dir / "block_execution_summary.json").write_text(
        json.dumps({
            "node_id": "n0",
            "shard_id": "s0",
            "block_executor_id": "batch_si_block_executor",
            "blocks": [
                {
                    "configured_worker_count": 8,
                    "maximum_parallel_width": 8,
                    "batch_count": 2,
                    "batch_si_executor_plan_parse_ms": 3,
                    "batch_si_executor_plan_verify_ms": 4,
                    "batch_si_executor_plan_verify_mode": "preverified_projection",
                    "batch_si_plan_payload_bytes": 12000,
                    "batch_si_worker_pool_setup_ms": 2,
                    "batch_si_worker_pool_wait_ms": 5,
                    "batch_si_execution_plan_preverified": True,
                    "batch_si_executor_full_verify_count": 0,
                    "batch_si_executor_full_verify_skip_count": 1,
                },
                {
                    "configured_worker_count": 8,
                    "maximum_parallel_width": 8,
                    "batch_count": 1,
                    "batch_si_executor_plan_parse_ms": 2,
                    "batch_si_executor_plan_verify_ms": 1,
                    "batch_si_executor_plan_verify_mode": "preverified_projection",
                    "batch_si_plan_payload_bytes": 6000,
                    "batch_si_worker_pool_setup_ms": 1,
                    "batch_si_worker_pool_wait_ms": 2,
                    "batch_si_execution_plan_preverified": True,
                    "batch_si_executor_full_verify_count": 0,
                    "batch_si_executor_full_verify_skip_count": 1,
                },
            ],
        }),
        encoding="utf-8",
    )
    metrics = {"source_artifacts": []}
    _apply_batch_si_metrics(metrics, tmp_path)
    assert metrics["batch_si_executor_plan_parse_ms"] == 5
    assert metrics["batch_si_executor_plan_verify_ms"] == 5
    assert metrics["batch_si_plan_payload_bytes"] == 18000
    assert metrics["batch_si_worker_pool_setup_ms"] == 3
    assert metrics["batch_si_worker_pool_wait_ms"] == 7
    assert metrics["batch_si_executor_full_verify_count"] == 0
    assert metrics["batch_si_executor_full_verify_skip_count"] == 2
    assert metrics["batch_si_executor_plan_verify_mode"] == "preverified_projection"
    assert metrics["batch_si_execution_plan_preverified"] is True
