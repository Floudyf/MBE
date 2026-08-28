from __future__ import annotations

import csv
import json

from backend.app.services.v5_formal_scheduler import DEFAULT_FORMAL_EXECUTION_POLICY
from backend.app.services.v5_paper_exporter import export
from backend.app.services.v5_real_cluster_runner import _canonical_digest, _initial_state_digest
from backend.app.services.v5_serial_order_oracle import EMPTY_STATE_ROOT


def _child(child_id: str, method_id: str) -> dict:
    return {
        "child_run_id": child_id,
        "status": "completed",
        "execution_status": "completed",
        "suite_type": "workload_sensitivity",
        "method_config_id": method_id,
        "method": {"display_name": method_id},
        "method_role": "main",
        "seed": 11,
        "repeat_index": 0,
        "scan_variable": "target_theta",
        "scan_value": "0.6",
        "topology_point": {"nodes": 8, "shards": 1, "validators_per_shard": 8, "worker_count": 8},
        "workload_point": {"tx_count": 10000, "cross_shard_ratio": 0, "timeout_every": 0},
        "fault_point": {"mode": "disabled"},
        "estimated_transactions": 10000,
        "block_size": 1000,
        "block_interval_ms": 100,
        "individual_result_valid": True,
        "paper_candidate": True,
        "formal_eligibility": True,
        "metrics": {
            "end_to_end_tps": 100.0,
            "logical_finality_tps": 100.0,
            "p50_latency_ms": 1.0,
            "p95_latency_ms": 2.0,
            "p99_latency_ms": 3.0,
            "p95_finality_ms": 2.0,
            "p99_finality_ms": 3.0,
            "lifecycle_complete": True,
            "no_fallback": True,
            "state_root_consistent": True,
            "receipt_root_consistent": True,
            "plan_digest_consistent": True,
            "metric_completeness": "complete",
            "submitted_unique_tx_count": 10000,
            "terminal_unique_tx_count": 10000,
            "finalized_unique_logical_tx_count": 10000,
            "incomplete_unique_tx_count": 0,
            "cross_shard_failed_unique_count": 0,
        },
        "result": {"summary": {
            "no_fallback": True,
            "state_root_consistent": True,
            "receipt_root_consistent": True,
            "plan_digest_consistent": True,
            "finality_evidence": {
                "submitted_unique_tx_count": 10000,
                "terminal_unique_tx_count": 10000,
                "finalized_unique_logical_tx_count": 10000,
                "incomplete_unique_tx_count": 0,
                "cross_shard_failed_unique_count": 0,
            },
        }},
    }


def test_formal_child_wall_timeout_is_two_hours_for_high_conflict_faithful_baselines() -> None:
    assert DEFAULT_FORMAL_EXECUTION_POLICY["child_wall_timeout_seconds"] == 7200


def test_initial_state_digest_uses_actual_execution_root_when_legacy_chain_contains_empty_placeholder(tmp_path) -> None:
    for node_id in ("n0", "n1"):
        node = tmp_path / "nodes" / node_id
        node.mkdir(parents=True)
        with (node / "committed_chain.csv").open("w", newline="", encoding="utf-8") as handle:
            writer = csv.DictWriter(handle, fieldnames=["node_id", "shard_id", "height", "block_hash", "previous_hash", "tx_count", "state_root_before"])
            writer.writeheader()
            writer.writerow({"node_id": node_id, "shard_id": "s0", "height": 1, "block_hash": "b1", "previous_hash": "genesis", "tx_count": 1, "state_root_before": "empty"})
        (node / "block_execution_summary.json").write_text(
            json.dumps({"blocks": [{"block_hash": "b1", "height": 1, "state_root_before": EMPTY_STATE_ROOT}]}),
            encoding="utf-8",
        )
    assert _initial_state_digest(tmp_path) == _canonical_digest({"s0": EMPTY_STATE_ROOT})


def test_multi_method_paper_figure_and_table_exports_fail_closed_when_state_gate_fails(tmp_path) -> None:
    children = [_child("cg", "hash_cg"), _child("acg", "hash_acg")]
    group = {
        "run_group_id": "v5grp_gate_failed",
        "performance_comparison_valid": False,
        "cross_method_serial_order_oracle_valid": False,
    }
    export(tmp_path, group, children)
    figure_rows = list(csv.DictReader((tmp_path / "paper_figure_data.csv").open(encoding="utf-8")))
    table_rows = list(csv.DictReader((tmp_path / "paper_table_data.csv").open(encoding="utf-8")))
    assert figure_rows == []
    assert table_rows == []
    observed_rows = list(csv.DictReader((tmp_path / "observed_results.csv").open(encoding="utf-8")))
    assert len(observed_rows) == 2


def test_single_method_paper_export_remains_available_without_cross_method_gate(tmp_path) -> None:
    export(tmp_path, {"run_group_id": "single", "performance_comparison_valid": False}, [_child("cg", "hash_cg")])
    assert list(csv.DictReader((tmp_path / "paper_figure_data.csv").open(encoding="utf-8")))
    assert list(csv.DictReader((tmp_path / "paper_table_data.csv").open(encoding="utf-8")))
