from __future__ import annotations

import json
from pathlib import Path

from backend.app.services import v5_metric_extractor
from backend.app.services.v5_plugin_manifest_store import STORE


def test_metatrack_routing_exposes_optional_micro_batch_without_picking_optimum() -> None:
    manifest = STORE.get("metatrack_coaccess_routing")
    # Preserve the existing paper-default manifest contract exactly.
    # micro_batch_size is optional: absence from default_config means runtime
    # inheritance (0 -> block_size), while the schema documents the optional knob.
    assert manifest.default_config == {"routing_epoch": 0}
    properties = manifest.config_schema.get("properties", {})
    assert properties["micro_batch_size"]["default"] == 0
    assert properties["micro_batch_size"]["maximum"] == 5000


def test_common_block_execution_timing_exports_effective_worker_truth(tmp_path: Path) -> None:
    run_dir = tmp_path
    (run_dir / "nodes" / "n0").mkdir(parents=True)
    (run_dir / "compiled_run_plan.json").write_text(
        json.dumps({"node_configs": [{"node_id": "n0", "shard_id": "s0", "leader": True, "role": "leader"}]}),
        encoding="utf-8",
    )
    (run_dir / "nodes" / "n0" / "block_execution_summary.json").write_text(
        json.dumps({
            "node_id": "n0",
            "shard_id": "s0",
            "block_executor_id": "metatrack_block_executor",
            "blocks": [{
                "block_execution_ms": 10,
                "transaction_execution_ms": 7,
                "deterministic_materialization_ms": 2,
                "state_commitment_ms": 1,
                "worker_count": 8,
            }],
        }),
        encoding="utf-8",
    )
    metrics = {"source_artifacts": []}
    v5_metric_extractor._apply_common_block_execution_timing(metrics, run_dir)
    assert metrics["configured_worker_count"] == 8
    assert metrics["worker_count"] == 8


def test_all_four_mechanism_timing_uses_microsecond_truth_and_blockstm_dedup(tmp_path: Path) -> None:
    from backend.app.services import v5_metric_extractor

    nodes = tmp_path / "nodes" / "n0"
    nodes.mkdir(parents=True)
    (nodes / "block_execution_summary.json").write_text(
        """{
          "shard_id":"s0",
          "block_executor_id":"serial_block_executor",
          "blocks":[{
            "block_execution_ms":20,
            "transaction_execution_ms":0,
            "transaction_execution_us":1250,
            "deterministic_materialization_ms":0,
            "deterministic_materialization_us":750,
            "state_commitment_ms":2,
            "versioned_state_ready_execution_ms":18,
            "worker_count":1
          }]
        }""",
        encoding="utf-8",
    )
    metrics = {"source_artifacts": []}
    v5_metric_extractor._apply_common_block_execution_timing(metrics, tmp_path)
    assert metrics["transaction_execution_ms"] == 1.25
    assert metrics["deterministic_materialization_ms"] == 0.75
    assert metrics["versioned_state_ready_execution_ms"] == 18
    assert metrics["maximum_parallel_width"] == 1
    assert metrics["execution_phase_timing_precision"] == "microsecond_accumulated_then_reported_ms"

    aggregate = tmp_path / "aggregate"
    aggregate.mkdir()
    (aggregate / "block_stm_aggregate_summary.json").write_text(
        """{
          "status":"available",
          "worker_count":8,
          "maximum_parallel_width":5,
          "maximum_concurrent_executions":5,
          "maximum_incarnation":4,
          "abort_count":20,
          "reexecution_count":12,
          "dependency_wait_count":28,
          "dependency_resume_count":24,
          "validation_failure_count":8,
          "serial_fallback_count":0,
          "serial_equivalent":true,
          "per_validator":[
            {"node_id":"n0","shard_id":"s0","abort_count":2,"reexecution_count":3,"dependency_wait_count":7,"dependency_resume_count":6,"validation_failure_count":1},
            {"node_id":"n1","shard_id":"s0","abort_count":2,"reexecution_count":3,"dependency_wait_count":7,"dependency_resume_count":6,"validation_failure_count":1},
            {"node_id":"n4","shard_id":"s1","abort_count":1,"reexecution_count":1,"dependency_wait_count":4,"dependency_resume_count":4,"validation_failure_count":0},
            {"node_id":"n5","shard_id":"s1","abort_count":1,"reexecution_count":1,"dependency_wait_count":4,"dependency_resume_count":4,"validation_failure_count":0}
          ]
        }""",
        encoding="utf-8",
    )
    v5_metric_extractor._apply_block_stm_metrics(metrics, tmp_path)
    assert metrics["abort_count"] == 3
    assert metrics["reexecution_count"] == 4
    assert metrics["dependency_wait_count"] == 11
    assert metrics["dependency_resume_count"] == 10
    assert metrics["validation_failure_count"] == 1
    assert metrics["maximum_incarnation_observed"] == 4
    assert metrics["block_stm_metric_truth_scope"] == "sum_of_per_shard_replica_maxima_from_per_validator_evidence"
