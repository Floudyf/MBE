from __future__ import annotations

import json

from backend.app.services import v5_metric_extractor, v5_paper_exporter, v5_reproducibility_bundle


def test_raw_export_schema_is_union_and_order_independent() -> None:
    left = [
        {"child_run_id": "a", "status": "completed", "batch_count": 3},
        {"child_run_id": "b", "status": "completed", "groundhog_reservation_count": 7},
    ]
    right = list(reversed(left))
    expected = v5_paper_exporter._stable_raw_fields(left)
    assert expected == v5_paper_exporter._stable_raw_fields(right)
    assert "batch_count" in expected
    assert "groundhog_reservation_count" in expected


def test_reproducibility_conditions_capture_offered_load() -> None:
    group = {
        "execution_backend": "real_cluster",
        "plan": {
            "worker_count": 8,
            "base_spec": {
                "tx_count": 10000,
                "topology": {"nodes": 8, "shards": 1, "validators_per_shard": 8},
                "workload_source": {
                    "source_type": "dataset",
                    "dataset_id": "axie",
                    "replay_mode": "fixed_rate",
                    "target_submission_tps": 500,
                    "seed": 11,
                },
            },
        },
    }
    conditions = v5_reproducibility_bundle._experiment_conditions(group)
    assert conditions["workload_source"]["replay_mode"] == "fixed_rate"
    assert conditions["workload_source"]["target_submission_tps"] == 500
    assert conditions["worker_count"] == 8


def test_common_timing_and_replay_metrics_are_method_neutral(tmp_path) -> None:
    (tmp_path / "compiled_run_plan.json").write_text(
        json.dumps({"node_configs": [{"node_id": "n0", "shard_id": "s0", "leader": True}]}),
        encoding="utf-8",
    )
    node_dir = tmp_path / "nodes" / "n0"
    node_dir.mkdir(parents=True)
    (node_dir / "block_execution_summary.json").write_text(
        json.dumps(
            {
                "node_id": "n0",
                "shard_id": "s0",
                "block_executor_id": "aria_block_executor",
                "blocks": [
                    {
                        "block_execution_ms": 13,
                        "transaction_execution_ms": 5,
                        "deterministic_apply_ms": 2,
                        "state_commitment_ms": 3,
                        "state_root_version": "mbe_state_merkle_treap_v2",
                    },
                    {
                        "block_execution_ms": 17,
                        "transaction_execution_ms": 7,
                        "deterministic_materialization_ms": 4,
                        "state_commitment_ms": 6,
                        "state_root_version": "mbe_state_merkle_treap_v2",
                    },
                ],
            }
        ),
        encoding="utf-8",
    )
    (node_dir / "transaction_lifecycle.jsonl").write_text(
        "\n".join(
            json.dumps({"timestamp_ms": ts, "tx_id": f"t{i}", "logical_tx_id": f"t{i}", "stage": "admitted", "success": True})
            for i, ts in enumerate((1000, 1002, 1004))
        ) + "\n",
        encoding="utf-8",
    )
    (tmp_path / "workload_replay_summary.json").write_text(
        json.dumps(
            {
                "replay_mode": "fixed_rate",
                "target_submission_tps": 500,
                "observed_submission_tps": 499.2,
                "submission_duration_ms": 20032,
                "pacing_schedule": "absolute_release_timeline_v1",
                "pacing_late_release_count": 4,
                "pacing_max_schedule_lag_ms": 1,
            }
        ),
        encoding="utf-8",
    )
    metrics = {"source_artifacts": []}
    v5_metric_extractor._apply_workload_replay_metrics(metrics, tmp_path)
    v5_metric_extractor._apply_mempool_admission_metrics(metrics, tmp_path)
    v5_metric_extractor._apply_common_block_execution_timing(metrics, tmp_path)
    assert metrics["replay_mode"] == "fixed_rate"
    assert metrics["target_submission_tps"] == 500
    assert metrics["mempool_admitted_unique_tx_count"] == 3
    assert metrics["mempool_admission_duration_ms"] == 4
    assert metrics["observed_mempool_admission_tps"] == 500.0
    assert metrics["mempool_admission_target_ratio"] == 1.0
    assert metrics["block_execution_ms"] == 30
    assert metrics["transaction_execution_ms"] == 12
    assert metrics["deterministic_materialization_ms"] == 6
    assert metrics["state_commitment_ms"] == 9
    assert metrics["common_timing_block_count"] == 2
    assert metrics["state_root_version"] == "mbe_state_merkle_treap_v2"
