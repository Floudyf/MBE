from __future__ import annotations

import json
from pathlib import Path

from backend.app.services import v5_metric_extractor


def _write(path: Path, value: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value), encoding="utf-8")


def test_blockstm_physical_mechanism_totals_do_not_overwrite_formal_replica_deduplicated_truth(tmp_path: Path) -> None:
    rows = [
        # shard s0: maxima abort=335, reexec=341, validation=262, wait=76
        {"node_id": "n0", "shard_id": "s0", "abort_count": 335, "reexecution_count": 341, "validation_failure_count": 262, "dependency_wait_count": 76, "dependency_resume_count": 75},
        {"node_id": "n1", "shard_id": "s0", "abort_count": 330, "reexecution_count": 337, "validation_failure_count": 255, "dependency_wait_count": 72, "dependency_resume_count": 71},
        {"node_id": "n2", "shard_id": "s0", "abort_count": 330, "reexecution_count": 334, "validation_failure_count": 250, "dependency_wait_count": 70, "dependency_resume_count": 69},
        {"node_id": "n3", "shard_id": "s0", "abort_count": 325, "reexecution_count": 328, "validation_failure_count": 243, "dependency_wait_count": 67, "dependency_resume_count": 66},
        # shard s1: maxima abort=268, reexec=274, validation=224, wait=49
        {"node_id": "n4", "shard_id": "s1", "abort_count": 268, "reexecution_count": 274, "validation_failure_count": 224, "dependency_wait_count": 49, "dependency_resume_count": 48},
        {"node_id": "n5", "shard_id": "s1", "abort_count": 255, "reexecution_count": 263, "validation_failure_count": 215, "dependency_wait_count": 46, "dependency_resume_count": 45},
        {"node_id": "n6", "shard_id": "s1", "abort_count": 250, "reexecution_count": 255, "validation_failure_count": 212, "dependency_wait_count": 44, "dependency_resume_count": 43},
        {"node_id": "n7", "shard_id": "s1", "abort_count": 243, "reexecution_count": 252, "validation_failure_count": 208, "dependency_wait_count": 43, "dependency_resume_count": 42},
    ]
    _write(
        tmp_path / "aggregate" / "block_stm_aggregate_summary.json",
        {
            "status": "available",
            "metric_truth_scope": "physical_replica_totals_with_explicit_per_validator_and_replica_deduplicated_counts",
            "worker_count": 8,
            "maximum_parallel_width": 8,
            "maximum_concurrent_executions": 8,
            "maximum_incarnation": 5,
            # These are deliberately the physical-replica totals seen in artifacts (95).
            "abort_count": 2336,
            "reexecution_count": 2384,
            "validation_failure_count": 1869,
            "dependency_wait_count": 467,
            "dependency_resume_count": 459,
            "serial_fallback_count": 0,
            "serial_equivalent": True,
            "per_validator": rows,
        },
    )
    _write(
        tmp_path / "aggregate" / "mechanism_metrics_summary.json",
        {
            "block_stm": {
                "status": "available",
                "metric_truth_scope": "physical_replica_totals_with_explicit_per_validator_and_replica_deduplicated_counts",
                "worker_count": 8,
                "maximum_parallel_width": 8,
                "abort_count": 2336,
                "reexecution_count": 2384,
                "validation_failure_count": 1869,
                "dependency_wait_count": 467,
                "dependency_resume_count": 459,
                "serial_equivalent": True,
            }
        },
    )

    _write(
        tmp_path / "compiled_run_plan.json",
        {
            "node_configs": [
                {"node_id": "n0", "shard_id": "s0", "leader": True, "role": "leader"},
                {"node_id": "n4", "shard_id": "s1", "leader": True, "role": "leader"},
            ]
        },
    )
    _write(
        tmp_path / "nodes" / "n0" / "block_execution_summary.json",
        {
            "node_id": "n0",
            "shard_id": "s0",
            "block_executor_id": "block_stm_block_executor",
            "blocks": [
                {"block_stm_internal_version_dependency_delegated_count": 11},
                {"block_stm_internal_version_dependency_delegated_count": 19},
            ],
        },
    )
    _write(
        tmp_path / "nodes" / "n4" / "block_execution_summary.json",
        {
            "node_id": "n4",
            "shard_id": "s1",
            "block_executor_id": "block_stm_block_executor",
            "blocks": [
                {"block_stm_internal_version_dependency_delegated_count": 7},
            ],
        },
    )

    metrics = {
        "source_artifacts": [],
        "submitted_unique_tx_count": 1000,
        "block_executor_id": "block_stm_block_executor",
    }

    v5_metric_extractor._apply_block_stm_metrics(metrics, tmp_path)

    # Formal truth = max replica per shard, then cross-shard sum.
    assert metrics["abort_count"] == 603
    assert metrics["reexecution_count"] == 615
    assert metrics["validation_failure_count"] == 486
    assert metrics["dependency_wait_count"] == 125
    assert metrics["block_stm_metric_truth_scope"] == "sum_of_per_shard_replica_maxima_from_per_validator_evidence"

    # New executor evidence is promoted without multiplying PBFT replicas.
    assert metrics["block_stm_internal_version_dependency_delegated_count"] == 37
    assert metrics["block_stm_internal_version_dependency_delegated_truth_scope"] == "sum_of_per_shard_leader_maxima_from_block_execution_evidence"

    # This call used to overwrite 603/615/486 with 2336/2384/1869.
    v5_metric_extractor._apply_mechanism_metrics(metrics, tmp_path)

    assert metrics["abort_count"] == 603
    assert metrics["reexecution_count"] == 615
    assert metrics["validation_failure_count"] == 486
    assert metrics["dependency_wait_count"] == 125

    # Physical replica totals remain available as explicitly named diagnostics.
    assert metrics["block_stm_physical_replica_abort_count"] == 2336
    assert metrics["block_stm_physical_replica_reexecution_count"] == 2384
    assert metrics["block_stm_physical_replica_validation_failure_count"] == 1869
    assert metrics["block_stm_physical_replica_dependency_wait_count"] == 467
    assert metrics["block_stm_mechanism_metric_truth_scope"] == "physical_replica_totals_with_explicit_per_validator_and_replica_deduplicated_counts"

    v5_metric_extractor._derive_research_metrics(metrics)
    assert metrics["block_stm_abort_events_per_tx"] == 0.603
    assert metrics["reexecution_events_per_tx"] == 0.615
    assert metrics["validation_failures_per_tx"] == 0.486
    assert metrics["dependency_waits_per_tx"] == 0.125
    assert metrics["block_stm_internal_version_dependencies_delegated_per_tx"] == 0.037
