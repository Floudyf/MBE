from __future__ import annotations

from backend.app.services.v5_formal_scheduler import (
    _apply_state_equivalence_gate,
    _build_external_performance_contract_reports,
    _execution_semantics,
    _worker_truth_reasons,
)


def _item(child_id: str, method_id: str, semantic: str, *, serial_applicable: bool, serial_ok, correctness_ok: bool) -> dict:
    return {
        "child_run_id": child_id,
        "status": "completed",
        "individual_result_valid": True,
        "comparison_group_id": "g",
        "comparison_semantics_class": semantic,
        "performance_contract_class": "single_shard_stateful_eventual_completion_v1",
        "method_config_id": method_id,
        "initial_state_digest": "initial",
        "serial_order_replay_input_digest": "workload",
        "serial_order_replay_applicable": serial_applicable,
        "serial_order_replay_equivalent": serial_ok,
        "serial_order_replay_blockers": [] if serial_ok is not False else ["serial_fail"],
        "method_correctness_oracle_valid": correctness_ok,
        "method_correctness_oracle_blockers": [] if correctness_ok else ["correctness_fail"],
        "global_final_state_digest": method_id + "-state",
        "serial_order_actual_global_business_state_digest": method_id + "-business",
    }


def test_external_contract_accepts_groundhog_method_oracle_without_fake_serial_equivalence() -> None:
    items = [
        _item("direct", "hash_cg", "nezha_cg_johnson_retryable_v4", serial_applicable=True, serial_ok=True, correctness_ok=True),
        _item("groundhog", "hash_groundhog", "groundhog_typed_commutative_snapshot_v1", serial_applicable=False, serial_ok=None, correctness_ok=True),
    ]
    reports, valid = _build_external_performance_contract_reports(items)
    assert valid is True
    assert len(reports) == 1
    report = reports[0]
    assert report["status"] == "passed"
    assert report["serial_order_oracle_failures"] == {}
    assert report["method_correctness_oracle_failures"] == {}
    assert report["cross_method_correctness_oracle_valid"] is True
    assert report["cross_method_serial_order_oracle_valid"] is True


def test_external_contract_fails_when_groundhog_method_oracle_fails() -> None:
    items = [
        _item("direct", "hash_cg", "nezha_cg_johnson_retryable_v4", serial_applicable=True, serial_ok=True, correctness_ok=True),
        _item("groundhog", "hash_groundhog", "groundhog_typed_commutative_snapshot_v1", serial_applicable=False, serial_ok=None, correctness_ok=False),
    ]
    reports, valid = _build_external_performance_contract_reports(items)
    assert valid is False
    report = reports[0]
    assert report["status"] == "failed"
    assert report["serial_order_oracle_failures"] == {}
    assert report["method_correctness_oracle_failures"] == {"groundhog": ["correctness_fail"]}


def _legacy_direct_item(child_id: str, method_id: str, semantic: str, *, serial_ok: bool = True) -> dict:
    item = _item(
        child_id,
        method_id,
        semantic,
        serial_applicable=True,
        serial_ok=serial_ok,
        correctness_ok=True,
    )
    # StateGate v3.3 artifacts/tests predate the additive generic correctness
    # fields. Their established oracle is serial_order_replay_equivalent.
    item.pop("serial_order_replay_applicable", None)
    item.pop("method_correctness_oracle_valid", None)
    item.pop("method_correctness_oracle_blockers", None)
    return item


def test_external_contract_preserves_stategate_v3_3_direct_state_oracle_without_new_fields() -> None:
    items = [
        _legacy_direct_item("cg", "hash_cg", "nezha_cg_johnson_retryable_v4"),
        _legacy_direct_item("acg", "hash_acg", "nezha_acg_hs_retryable_v2"),
        _legacy_direct_item("batch", "hash_batch_si", "batch_si_common_batch_snapshot_v1"),
    ]
    reports, valid = _build_external_performance_contract_reports(items)
    assert valid is True
    assert reports[0]["status"] == "passed"
    assert reports[0]["serial_order_oracle_failures"] == {}
    assert reports[0]["method_correctness_oracle_failures"] == {}


def test_external_contract_still_fails_closed_for_legacy_direct_state_serial_oracle_failure() -> None:
    items = [
        _legacy_direct_item("cg", "hash_cg", "nezha_cg_johnson_retryable_v4"),
        _legacy_direct_item("batch", "hash_batch_si", "batch_si_common_batch_snapshot_v1", serial_ok=False),
    ]
    reports, valid = _build_external_performance_contract_reports(items)
    assert valid is False
    assert reports[0]["status"] == "failed"
    assert reports[0]["serial_order_oracle_failures"] == {"batch": ["serial_fail"]}


def test_external_contract_fails_closed_on_explicit_direct_method_oracle_contradiction() -> None:
    items = [
        _item("cg", "hash_cg", "nezha_cg_johnson_retryable_v4", serial_applicable=True, serial_ok=True, correctness_ok=False),
        _legacy_direct_item("batch", "hash_batch_si", "batch_si_common_batch_snapshot_v1"),
    ]
    reports, valid = _build_external_performance_contract_reports(items)
    assert valid is False
    assert reports[0]["method_correctness_oracle_failures"] == {"cg": ["correctness_fail"]}




def test_serial_fixed_single_worker_is_valid_under_global_worker_sweep() -> None:
    item = {
        "method_config_id": "hash_serial",
        "method": {"plugin_overrides": {"block_executor": "serial_block_executor"}},
        "topology_point": {"worker_count": 8},
        "metrics": {"block_executor_id": "serial_block_executor"},
    }
    assert _worker_truth_reasons(item) == []


def test_fabricpp_fixed_single_worker_is_valid_under_global_worker_sweep() -> None:
    item = {
        "method_config_id": "hash_fabricpp_cg",
        "method": {"plugin_overrides": {"block_executor": "fabricpp_cg_block_executor"}},
        "topology_point": {"worker_count": 8},
        "metrics": {"block_executor_id": "fabricpp_cg_block_executor"},
    }
    assert _worker_truth_reasons(item) == []


def test_parallel_method_worker_mismatch_still_fails_closed() -> None:
    item = {
        "method_config_id": "hash_block_stm",
        "method": {"plugin_overrides": {"block_executor": "block_stm_block_executor"}},
        "topology_point": {"worker_count": 8},
        "metrics": {"worker_count": 4},
    }
    assert "effective_worker_not_equal_requested" in _worker_truth_reasons(item)


def test_aria_has_own_retryable_reordering_semantics_class() -> None:
    semantics = _execution_semantics(
        {"block_executor": "aria_block_executor"},
        "hash_aria",
    )
    assert semantics["comparison_semantics_class"] == "aria_reordered_retryable_v1"
    assert semantics["state_access_semantics"] == "stateful_local_aria_deterministic_reordering_retryable"
    assert semantics["proof_policy"] == "consensus_bound_aria_candidate_selection_v2"
    assert semantics["measurement_boundary"] == "client_submit_to_aria_eventual_finality"


def test_rebuild_recovers_serial_paper_candidate_after_fixed_worker_gate_correction() -> None:
    item = {
        "child_run_id": "serial",
        "status": "completed",
        "method_config_id": "hash_serial",
        "method": {"plugin_overrides": {"block_executor": "serial_block_executor"}},
        "comparison_group_id": "g",
        "comparison_semantics_class": "stateful_local_legacy_v1",
        "topology_point": {"worker_count": 8},
        # Simulate artifacts (55): stale worker-only invalidation, while the
        # Serial runtime itself completed correctly and exposes executor identity
        # but no generic worker_count metric.
        "paper_candidate": False,
        "individual_result_valid": False,
        "individual_result_validity_reasons": ["effective_worker_not_equal_requested"],
        "comparison_eligibility_status": "individual_result_invalid",
        "metrics": {
            "metric_completeness": "complete",
            "missing": [],
            "block_executor_id": "serial_block_executor",
            "submitted_unique_tx_count": 10_000,
            "terminal_unique_tx_count": 10_000,
            "finalized_unique_logical_tx_count": 10_000,
            "incomplete_unique_tx_count": 0,
            "cross_shard_failed_unique_count": 0,
            "lifecycle_complete": True,
            "serial_order_oracle_status": "passed",
            "serial_order_replay_applicable": True,
            "serial_order_replay_equivalent": True,
            "serial_order_replay_input_digest": "workload",
            "serial_order_replay_commit_order_digest": "order",
            "serial_order_replay_transaction_count": 10_000,
            "serial_order_replay_business_state_digest": "business",
            "serial_order_actual_business_state_digest": "business",
            "serial_order_replay_global_business_state_digest": "business",
            "serial_order_actual_global_business_state_digest": "business",
            "serial_order_replay_replica_order_consistent": True,
            "method_correctness_oracle_valid": True,
        },
        "result": {
            "status": "completed",
            "summary": {
                "ready_to_commit": True,
                "no_fallback": True,
                "initial_state_digest": "initial",
                "global_final_state_digest": "final",
                "finality_evidence": {
                    "submitted_unique_tx_count": 10_000,
                    "terminal_unique_tx_count": 10_000,
                    "finalized_unique_logical_tx_count": 10_000,
                    "incomplete_unique_tx_count": 0,
                    "cross_shard_failed_unique_count": 0,
                    "lifecycle_complete": True,
                },
            },
        },
    }
    enriched, report = _apply_state_equivalence_gate([item])
    assert enriched[0]["individual_result_valid"] is True
    assert enriched[0]["paper_candidate"] is True
    assert enriched[0]["comparison_eligibility_status"] == "passed"
    assert report["within_semantic_cohort_state_equivalence_valid"] is True


def test_state_gate_preserves_existing_candidate_for_minimal_historical_fixture() -> None:
    item = {
        "child_run_id": "legacy-minimal",
        "status": "completed",
        "method_config_id": "hash_bsx",
        "comparison_group_id": "legacy-g",
        "comparison_semantics_class": "bsx_deterministic_coloring_serializable_v1",
        "paper_candidate": True,
        "metrics": {
            "submitted_unique_tx_count": 100,
            "terminal_unique_tx_count": 100,
            "finalized_unique_logical_tx_count": 100,
            "incomplete_unique_tx_count": 0,
            "cross_shard_failed_unique_count": 0,
            "lifecycle_complete": True,
        },
        "result": {
            "summary": {
                "initial_state_digest": "initial",
                "global_final_state_digest": "final",
                "finality_evidence": {
                    "submitted_unique_tx_count": 100,
                    "terminal_unique_tx_count": 100,
                    "finalized_unique_logical_tx_count": 100,
                    "incomplete_unique_tx_count": 0,
                    "cross_shard_failed_unique_count": 0,
                    "lifecycle_complete": True,
                },
            }
        },
    }
    enriched, report = _apply_state_equivalence_gate([item])
    assert enriched[0]["individual_result_valid"] is True
    assert enriched[0]["paper_candidate"] is True
    assert report["within_semantic_cohort_state_equivalence_valid"] is True
