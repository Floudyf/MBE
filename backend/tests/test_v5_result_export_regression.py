import csv
import json
from pathlib import Path

from backend.app.services.v5_formal_scheduler import _physical_result_dir
from backend.app.services.v5_paper_exporter import _effective_metrics, export


def _embedded_child(child_id: str, method_id: str, name: str, *, tps: float) -> dict:
    uses_block_stm = "block_stm" in method_id
    uses_metatrack = "metatrack" in method_id
    mechanism = {
        "schema_version": "mbe_mechanism_metrics_summary_v1",
        "block_stm": {
            "status": "available" if uses_block_stm else "not_applicable",
            "worker_count": 4,
            "maximum_parallel_width": 4,
            "abort_count": 3,
            "reexecution_count": 3,
            "validation_failure_count": 1,
            "serial_equivalent": True,
        },
        "metatrack": {
            "status": "available" if uses_metatrack else "not_applicable",
            "fast_track_logical_tx_count": 10,
            "conservative_track_logical_tx_count": 90,
            "replica_deduplicated_remote_fetch_count": 20,
            "replica_deduplicated_remote_writeback_count": 10,
            "aggregation_group_count": 2,
            "pre_aggregation_physical_op_count": 120,
            "post_aggregation_physical_op_count": 100,
        },
        "remote_state": {},
    }
    return {
        "child_run_id": child_id,
        "status": "completed",
        "execution_status": "completed",
        "artifact_status": "complete",
        "formal_eligibility": True,
        "completion_gate": {"passed": True, "blockers": []},
        "suite_type": "comparison_experiment",
        "method_config_id": method_id,
        "method": {"display_name": name},
        "method_role": "main",
        "topology_point": {"nodes": 8, "shards": 2, "validators_per_shard": 4},
        "workload_point": {},
        "fault_point": {},
        "estimated_transactions": 100,
        "block_size": 100,
        "block_interval_ms": 75,
        "metrics": {"missing": ["real_cluster_summary.json", "finality_summary.json"]},
        "result": {
            "status": "completed",
            "summary": {
                "no_fallback": True,
                "state_root_consistent": True,
                "receipt_root_consistent": True,
                "plan_digest_consistent": True,
                "lifecycle_complete": True,
                "orphan_process_count": 0,
                "configured_block_size": 100,
                "configured_block_interval_ms": 75,
                "finality_evidence": {
                    "submitted_unique_tx_count": 100,
                    "terminal_unique_tx_count": 100,
                    "incomplete_unique_tx_count": 0,
                    "finalized_unique_logical_tx_count": 100,
                    "cross_shard_requested_unique_count": 25,
                    "cross_shard_target_committed_unique_count": 25,
                    "cross_shard_finalized_unique_count": 25,
                    "cross_shard_refunded_unique_count": 0,
                    "cross_shard_failed_unique_count": 0,
                    "throughput_tps": tps,
                    "end_to_end_tps": tps,
                    "logical_finality_tps": tps + 1,
                    "p50_finality_ms": 10,
                    "p95_finality_ms": 20,
                    "p99_finality_ms": 30,
                },
                "mechanism_metrics": mechanism,
                "workload_replay_summary": {
                    "actual_cross_shard_count": 25,
                    "actual_cross_shard_ratio": 0.25,
                    "materialized_sha256": "a" * 64,
                    "mapping_digest": "mapping",
                    "truth_label": "real_derived_resampled",
                    "variant_id": "key_zipf:count=100:seed=73:axis=contract:alpha=0.8",
                },
            },
        },
    }


def test_export_recovers_authoritative_embedded_metrics_from_logical_path_failure(tmp_path: Path) -> None:
    children = [
        _embedded_child("serial", "hash_serial", "Baseline", tps=12.5),
        _embedded_child("block", "hash_block_stm", "Block-STM", tps=10.0),
        _embedded_child("meta", "metatrack_serial", "MetaTrack", tps=11.0),
        _embedded_child("both", "metatrack_block_stm", "MetaTrack + Block-STM", tps=9.0),
    ]

    overall = export(tmp_path, {"run_group_id": "v5grp_embedded"}, children)

    assert overall["count"] == 4
    assert overall["missing_count"] == 0
    comparison = list(csv.DictReader((tmp_path / "comparison_summary.csv").open(encoding="utf-8")))
    assert {row["method_config_id"] for row in comparison} == {
        "hash_serial",
        "hash_block_stm",
        "metatrack_serial",
        "metatrack_block_stm",
    }
    assert all(row["sample_count"] == "1" and row["missing_count"] == "0" for row in comparison)
    assert all(row["cross_shard_ratio"] == "0.25" for row in comparison)
    assert (tmp_path / "missing_metrics.csv").read_text(encoding="utf-8") == "child_run_id,missing\n"

    paper = json.loads((tmp_path / "aggregate" / "paper_result_analysis.json").read_text(encoding="utf-8"))
    assert paper["analysis_status"] == "complete"
    assert paper["excluded_samples"] == []
    assert all(row["valid_sample_count"] == 1 for row in paper["metrics"]["end_to_end_tps"])

    block_metrics = _effective_metrics(children[1])
    assert block_metrics["serial_equivalent"] is True
    assert block_metrics["metric_completeness"] == "complete"
    meta_metrics = _effective_metrics(children[2])
    assert meta_metrics["fast_track_logical_tx_count"] == 10
    assert meta_metrics["metric_completeness"] == "complete"


def test_physical_result_dir_uses_authoritative_run_id(monkeypatch, tmp_path: Path) -> None:
    expected = tmp_path / "v5_real_cluster_runs" / "v5_test"
    monkeypatch.setattr(
        "backend.app.services.v5_formal_scheduler.v5_real_cluster_runner.run_dir",
        lambda run_id: expected if run_id == "v5_test" else (_ for _ in ()).throw(AssertionError(run_id)),
    )

    resolved = _physical_result_dir(
        {
            "run_id": "v5_test",
            "output_dir": "$MBE_RUNTIME_ROOT\\v5_real_cluster_runs\\v5_test",
        }
    )

    assert resolved == expected
