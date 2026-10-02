from __future__ import annotations

from pathlib import Path
import csv
import gzip
import hashlib
import json

from backend.app.services.v5_fairness_validator import _performance_contract_class, validate
from backend.app.services.v5_formal_scheduler import _build_external_performance_contract_reports, _execution_semantics, _state_equivalence_individual_reasons
from backend.app.services.v5_porygon_correctness_oracle import _load_access, _owner_identity, _state_home
from backend.app.services.v5_serial_order_oracle import _load_access_entries
from backend.app.services.v5_paper_exporter import _individual_result_reasons
from backend.app.services.v5_real_cluster_runner import _logical_initial_state_digest


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




def test_porygon_workload_digest_matches_common_logical_access_projection(tmp_path: Path) -> None:
    path = tmp_path / "resolved_access_lists.jsonl.gz"
    rows = [
        {
            "index": 1, "tx_id": "physical-b", "logical_id": "logical-b",
            "access_list_schema": "mbe", "access_list_source": "dataset",
            "access_list": [{"key": "state/b", "mode": "write", "update_semantics": "replace", "delta": 0}],
        },
        {
            "index": 0, "tx_id": "physical-a", "logical_id": "logical-a",
            "access_list_schema": "mbe", "access_list_source": "dataset",
            "access_list": [{"key": "state/a", "mode": "read_write", "update_semantics": "replace", "delta": 0}],
        },
    ]
    with gzip.open(path, "wt", encoding="utf-8") as handle:
        for row in rows:
            handle.write(json.dumps(row) + "\n")

    _, porygon_digest, porygon_blockers = _load_access(path)
    _, common_digest, common_blockers = _load_access_entries(path)
    assert porygon_blockers == []
    assert common_blockers == []
    assert porygon_digest == common_digest

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


def _write_empty_chain(root: Path, shard_ids: list[str]) -> None:
    empty_root = hashlib.sha256(b"mbe-state-merkle-treap-v2:empty").hexdigest()
    for index, shard_id in enumerate(shard_ids):
        node = root / "nodes" / f"n{index}"
        node.mkdir(parents=True, exist_ok=True)
        with (node / "committed_chain.csv").open("w", newline="", encoding="utf-8") as handle:
            writer = csv.DictWriter(handle, fieldnames=["height", "shard_id", "block_hash", "state_root_before"])
            writer.writeheader()
            writer.writerow({"height": 1, "shard_id": shard_id, "block_hash": f"b{index}", "state_root_before": empty_root})


def test_logical_initial_state_digest_ignores_physical_partition_names_for_proven_empty_state(tmp_path: Path) -> None:
    metatrack = tmp_path / "metatrack"
    porygon = tmp_path / "porygon"
    _write_empty_chain(metatrack, ["s0", "s1"])
    _write_empty_chain(porygon, ["porygon-global"])
    left = _logical_initial_state_digest(metatrack)
    right = _logical_initial_state_digest(porygon)
    assert left
    assert left == right


def test_external_multishard_contract_uses_logical_not_physical_initial_state_digest() -> None:
    base = {
        "status": "completed",
        "individual_result_valid": True,
        "comparison_group_id": "g",
        "performance_contract_class": "multi_shard_stateless_eventual_completion_v1",
        "logical_initial_state_digest": "same-logical-empty",
        "serial_order_replay_input_digest": "same-logical-access",
        "serial_order_replay_applicable": True,
        "serial_order_replay_equivalent": True,
        "method_correctness_oracle_valid": True,
        "global_final_state_digest": "diagnostic-only",
    }
    porygon = {**base, "method_config_id": "stateless_porygon", "comparison_semantics_class": "porygon_3d_global_ordering_paper_fidelity_v5", "initial_state_digest": "physical-porygon-global"}
    metatrack = {**base, "method_config_id": "metatrack_latest", "comparison_semantics_class": "stateless_remote_home_v1", "initial_state_digest": "physical-s0-s1"}
    reports, valid = _build_external_performance_contract_reports([porygon, metatrack])
    assert valid is True
    assert len(reports) == 1
    assert reports[0]["status"] == "passed"
    assert reports[0]["mismatched_evidence"] == {}
    assert reports[0]["required_evidence"][:2] == ["logical_initial_state_digest", "serial_order_replay_input_digest"]


def test_porygon_formal_worker_truth_uses_requested_runtime_resource_not_registry_default() -> None:
    porygon = _row(
        "stateless_porygon", "porygon_3d_global_ordering_paper_fidelity_v5",
        state_home="porygon_account_object_owner_partition",
        remote_fetch="signed_access_projection_with_physical_state_fetch_and_verified_merkle_treap_proof",
        remote_write="oc_multi_shard_update_majority_ack_then_partition_materialization",
        proof="ec_witness_certificate_plus_state_merkle_proof_plus_esc_batch_certificate_plus_multishard_root_certificate",
    )
    porygon["topology_point"]["worker_count"] = 8
    porygon["method"] = {"method_id": "stateless_porygon", "plugin_config_overrides": {"block_executor": {"worker_count": 4}}}
    checked, _ = validate([porygon])
    assert checked[0]["requested_worker_count"] == 8
    assert checked[0]["method_config_worker_count"] == 4
    assert checked[0]["effective_worker_count"] == 8
