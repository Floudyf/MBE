from __future__ import annotations

import csv
from pathlib import Path

from backend.app.services.v5_optme_txallo_method_specific_v18 import (
    apply_group_fidelity_gate,
    compute_method_specific_correctness_v18,
    compute_stateless_txallo_relay_guard_v18,
    compute_writeback_fanout_v18,
    compute_metric_coherence_v18,
)


def _csv(path: Path, rows: list[dict[str, object]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0]))
        writer.writeheader(); writer.writerows(rows)


def test_stateful_partition_oracle_is_method_specific_pass() -> None:
    m = {
        "method_correctness_oracle_status": "passed",
        "stateful_serializability_actual_partition_business_digests": {"s0": "a", "s1": "b"},
        "stateful_serializability_replay_partition_business_digests": {"s0": "a", "s1": "b"},
        "stateful_serializability_actual_partition_effect_digests": {"s0": "x", "s1": "y"},
        "stateful_serializability_replay_partition_effect_digests": {"s0": "x", "s1": "y"},
    }
    got = compute_method_specific_correctness_v18("stateful_txallo", m)
    assert got["status"] == "passed" and got["valid"] is True


def test_stateless_generic_replica_determinism_is_not_correctness_proof() -> None:
    m = {
        "method_correctness_oracle_status": "passed",
        "method_correctness_oracle_kind": "multishard_replica_determinism_completion_v1",
        "serial_order_oracle_status": "not_applicable",
        "serial_order_replay_equivalent": None,
        "serial_order_replay_supported_scope": "single_shard_empty_initial_direct_access_committed_txid_v2",
        "serial_order_replay_not_applicable_reason": "multi_shard_execution_outside_single_shard_serial_replay_scope",
    }
    got = compute_method_specific_correctness_v18("stateless_optme", m)
    assert got["status"] == "unproven"
    assert "stateless_method_specific_multishard_serial_replay_missing" in got["blockers"]


def test_stateless_multishard_serial_replay_can_pass() -> None:
    m = {
        "serial_order_oracle_status": "passed",
        "serial_order_replay_equivalent": True,
        "serial_order_replay_supported_scope": "multi_shard_method_specific_logical_replay_v1",
    }
    got = compute_method_specific_correctness_v18("stateless_txallo", m)
    assert got["status"] == "passed" and got["valid"] is True


def test_cross_method_stateless_final_digest_mismatch_is_not_v18_correctness_blocker() -> None:
    base = {
        "v16_logical_transaction_identity_digest": "same-workload",
        "v18_method_correctness": {"status": "passed", "valid": True, "blockers": []},
        "v18_method_correctness_oracle_status": "passed",
        "v18_metric_coherence_passed": True,
        "v18_writeback_evidence": {"status": "available_exact_logical_dedup"},
        "v17_optme_transaction_fidelity": {"status": "available"},
    }
    items = [
        {"method": {"method_id": "stateless_optme"}, "metrics": {**base, "v17_logical_business_state_digest": "A", "v18_optme_transaction_fidelity": {"status": "available"}}},
        {"method": {"method_id": "stateless_txallo"}, "metrics": {**base, "v17_logical_business_state_digest": "B",
            "v18_txallo_mapping_to_placement": {"passed": True},
            "v18_txallo_independent_account_coverage": {"passed": True},
            "v18_stateless_txallo_legacy_relay_guard": {"passed": True}}},
    ]
    _, report = apply_group_fidelity_gate(items, {})
    assert report["v18_cross_method_final_state_equality_required"] is False
    assert "stateless_variants_logical_business_state_not_equivalent" in report["v18_retired_v17_group_blockers"]
    assert all("stateless_variants_logical_business_state_not_equivalent" not in x for x in report["v18_group_fidelity_blockers"])


def test_exact_version_writeback_excludes_non_version_rows(tmp_path: Path) -> None:
    _csv(tmp_path / "physical_remote_state_operations.csv", [
        {"kind": "STATE_DELTA_APPLY", "logical_tx_id": "t1", "state_key": "k", "produced_version": "7", "home_shard": "s0", "update_semantics": "set", "replica": "n0"},
        {"kind": "STATE_DELTA_APPLY", "logical_tx_id": "t1", "state_key": "k", "produced_version": "7", "home_shard": "s0", "update_semantics": "set", "replica": "n1"},
        {"kind": "STATE_DELTA_APPLY", "logical_tx_id": "legacy", "state_key": "k2", "produced_version": "0", "home_shard": "s0", "update_semantics": "set", "replica": "n0"},
    ])
    got = compute_writeback_fanout_v18(tmp_path, {})
    assert got["physical_message_count"] == 2
    assert got["unique_logical_delta_count"] == 1
    assert got["replica_fanout_ratio"] == 2
    assert got["excluded_non_version_write_apply_count"] == 1


def test_stateless_txallo_relay_guard_detects_multi_shard_business_execution(tmp_path: Path) -> None:
    _csv(tmp_path / "nodes/n0/transaction_execution_trace.csv", [
        {"tx_id": "t1", "shard_id": "s0"}, {"tx_id": "t2", "shard_id": "s0"},
    ])
    _csv(tmp_path / "nodes/n4/transaction_execution_trace.csv", [
        {"tx_id": "t1", "shard_id": "s1"},
    ])
    got = compute_stateless_txallo_relay_guard_v18(tmp_path)
    assert got["passed"] is False
    assert got["multi_shard_execution_transaction_count"] == 1


def test_v18_writeback_search_continues_from_legacy_summary_to_node_exact_version(tmp_path: Path) -> None:
    (tmp_path / "physical_remote_state_operations.csv").write_text(
        "access_kind,produced_version,tx_id,state_key,home_shard,update_semantics\n"
        "write_apply,0,t0,k0,s0,set\n",
        encoding="utf-8",
    )
    node = tmp_path / "nodes" / "n0"
    node.mkdir(parents=True)
    (node / "remote_state_access.csv").write_text(
        "access_kind,tx_id,state_key,home_shard,update_semantics,logical_tx_ids,previous_version,produced_version,ordering_noop,apply_origin,delta_kind\n"
        "write_apply,t1,k1,s0,,t1,4,5,false,versioned_remote_home,\n"
        "write_apply,t1,k1,s0,,t1,4,5,false,versioned_remote_home,\n",
        encoding="utf-8",
    )
    result = compute_writeback_fanout_v18(tmp_path, {})
    assert result["status"] == "available_exact_logical_dedup"
    assert result["source"] == "nodes/n0/remote_state_access.csv"
    assert result["physical_message_count"] == 2
    assert result["unique_logical_delta_count"] == 1
    assert result["replica_fanout_ratio"] == 2
    assert result["canonicalized_default_set_semantics_count"] == 2


def test_v18_metric_coherence_uses_physical_truth_over_legacy_zero_counter() -> None:
    result = compute_metric_coherence_v18("stateless_optme", {
        "physical_remote_operation_count": 12,
        "physical_remote_fetch_count": 8,
        "physical_remote_writeback_count": 4,
        "remote_state_access_count": 0,
    })
    assert result["passed"] is True
    assert result["failures"] == []
    assert "legacy_remote_state_access_count_zero_is_narrower_than_physical_transport_truth" in result["diagnostics"]
    assert result["canonical_physical_remote_operation_count"] == 12
