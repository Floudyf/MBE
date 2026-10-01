from __future__ import annotations

from pathlib import Path

from backend.app.services.v5_fairness_validator import _performance_contract_class, validate
from backend.app.services.v5_formal_scheduler import _execution_semantics, _state_equivalence_individual_reasons
from backend.app.services.v5_porygon_correctness_oracle import _owner_identity, _state_home
from backend.app.services.v5_paper_exporter import _individual_result_reasons


def _row(method: str, semantic: str, *, state_home: str, remote_fetch: str, remote_write: str, proof: str) -> dict:
    return {
        "comparison_group_id": "g", "method_config_id": method, "seed": 1, "repeat_index": 0,
        "execution_backend": "real_cluster", "estimated_transactions": 1000,
        "workload_snapshot_digest": "w", "topology_snapshot_digest": "t", "fault_snapshot_digest": "f",
        "fairness_key": "k", "block_size": 1000, "block_interval_ms": 100,
        "topology_point": {"shards": 2}, "comparison_semantics_class": semantic,
        "state_access_semantics": "different-by-design", "state_home_mapping_policy": state_home,
        "remote_fetch_policy": remote_fetch, "remote_writeback_policy": remote_write,
        "version_plan_policy": None, "proof_policy": proof, "legacy_cross_shard_protocol": False,
        "measurement_boundary": "client_submit_to_method_terminal", "runnable": True, "blockers": [],
    }


def test_porygon_owner_groups_related_alien_worlds_account_state() -> None:
    assert _owner_identity("m.federation/miners/alice") == "alice"
    assert _owner_identity("m.federation/bags/alice") == "alice"
    assert _owner_identity("userpoints.worlds/userpoints/alice") == "alice"
    assert _state_home("m.federation/miners/alice", 4) == _state_home("m.federation/bags/alice", 4)


def test_porygon_formal_truth_reports_final_mechanisms() -> None:
    semantics = _execution_semantics({"block_executor": "porygon_block_executor"}, "stateless_porygon")
    assert semantics["comparison_semantics_class"] == "porygon_3d_global_ordering_paper_fidelity_v5"
    assert semantics["state_home_mapping_policy"] == "porygon_account_object_owner_partition"
    assert semantics["remote_fetch_policy"] == "signed_access_projection_with_physical_state_fetch_and_verified_merkle_treap_proof"
    assert semantics["remote_writeback_policy"] == "oc_multi_shard_update_majority_ack_then_partition_materialization"
    assert "ec_witness_certificate" in semantics["proof_policy"]


def test_porygon_and_metatrack_share_external_multishard_performance_contract() -> None:
    porygon = _row(
        "p", "porygon_3d_global_ordering_paper_fidelity_v5",
        state_home="porygon_account_object_owner_partition",
        remote_fetch="signed_access_projection_with_physical_state_fetch_and_verified_merkle_treap_proof",
        remote_write="oc_multi_shard_update_majority_ack_then_partition_materialization",
        proof="ec_witness_certificate_plus_state_merkle_proof_plus_esc_batch_certificate_plus_multishard_root_certificate",
    )
    metatrack = _row(
        "m", "stateless_remote_home_v1",
        state_home="deterministic_state_home",
        remote_fetch="exact_statehome_remote_fetch",
        remote_write="exact_statehome_writeback",
        proof="method_correctness_oracle",
    )
    assert _performance_contract_class(porygon) == "multi_shard_stateless_eventual_completion_v1"
    assert _performance_contract_class(metatrack) == "multi_shard_stateless_eventual_completion_v1"
    checked, result = validate([porygon, metatrack])
    assert result["performance_comparison_valid"] is True
    assert all(row["performance_comparison_valid"] is True for row in checked)
    assert all(row["comparison_internal_semantics_differ"] is True for row in checked)


def test_porygon_manifest_truth_no_longer_claims_metadata_only_witness() -> None:
    root = Path(__file__).resolve().parents[2]
    text = (root / "backend/app/services/v5_plugin_manifest_store.py").read_text(encoding="utf-8")
    assert "witness_threshold_is_metadata_not_independent_quorum" not in text[text.index('"block_producer", "porygon_transaction_block_producer"'):]
    assert "porygon_real_ec_witness_certificate_v4" in text
    assert "verified_merkle_treap_proof" in text
    assert "porygon_explicit_multi_shard_update_state_machine_v5" in text


def _porygon_completed_item(*, finalized: int, abandoned: int, cross_failed: int = 267, protocol_abandoned: int | None = None) -> dict:
    if protocol_abandoned is None:
        protocol_abandoned = abandoned
    return {
        "status": "completed",
        "execution_status": "completed",
        "comparison_semantics_class": "porygon_3d_global_ordering_paper_fidelity_v5",
        "metrics": {
            "submitted_unique_tx_count": 1000,
            "terminal_unique_tx_count": 1000,
            "finalized_unique_logical_tx_count": finalized,
            "incomplete_unique_tx_count": 0,
            # Workload source/target cross-shard failures are a diagnostic axis,
            # not Porygon's execution-ESC CTx classification.
            "cross_shard_failed_unique_count": cross_failed,
            "abort_count": abandoned,
            "porygon_protocol_abandoned_unique_tx_count": protocol_abandoned,
            "lifecycle_complete": True,
            "method_correctness_oracle_valid": True,
            "serial_order_replay_applicable": True,
            "serial_order_replay_equivalent": True,
            "no_fallback": True,
            "state_root_consistent": True,
            "receipt_root_consistent": True,
            "plan_digest_consistent": True,
            "metric_completeness": "complete",
            "end_to_end_tps": 10.0,
            "logical_finality_tps": 10.0,
            "p95_finality_ms": 100.0,
            "p99_finality_ms": 120.0,
        },
        "result": {"summary": {}},
    }


def test_porygon_protocol_abandoned_is_valid_terminal_semantics_but_runtime_failure_is_not() -> None:
    valid = _porygon_completed_item(finalized=475, abandoned=525, cross_failed=267)
    assert _state_equivalence_individual_reasons(valid) == []
    assert _individual_result_reasons(valid) == []

    # The workload-level source/target cross-shard failure count is not the
    # Porygon protocol-abandon count and must not invalidate the sample.
    workload_axis_changed = _porygon_completed_item(finalized=475, abandoned=525, cross_failed=999)
    assert _state_equivalence_individual_reasons(workload_axis_changed) == []
    assert _individual_result_reasons(workload_axis_changed) == []

    mismatched_terminal = _porygon_completed_item(finalized=474, abandoned=525)
    assert "finalized_plus_abort_not_equal_terminal" in _state_equivalence_individual_reasons(mismatched_terminal)
    assert "finalized_plus_abort_not_equal_terminal" in _individual_result_reasons(mismatched_terminal)

    wrong_protocol_count = _porygon_completed_item(finalized=475, abandoned=525, protocol_abandoned=524)
    assert "finalized_plus_abort_not_equal_terminal" in _state_equivalence_individual_reasons(wrong_protocol_count)
    assert "finalized_plus_abort_not_equal_terminal" in _individual_result_reasons(wrong_protocol_count)

    oracle_failed = _porygon_completed_item(finalized=475, abandoned=525)
    oracle_failed["metrics"]["method_correctness_oracle_valid"] = False
    assert "method_correctness_oracle_not_true" in _state_equivalence_individual_reasons(oracle_failed)
    assert "method_correctness_oracle_not_true" in _individual_result_reasons(oracle_failed)
