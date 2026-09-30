from __future__ import annotations

import csv
from pathlib import Path

from backend.app.services.v5_optme_txallo_fidelity_closure import (
    CLOSURE_VERSION,
    apply_fairness_fidelity_gate,
    apply_group_fidelity_gate,
    compute_logical_business_state_evidence,
    compute_txallo_evidence,
    compute_writeback_fanout,
    enrich_metrics,
)


def _csv(path: Path, fields: list[str], rows: list[dict[str, object]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        w = csv.DictWriter(handle, fieldnames=fields); w.writeheader(); w.writerows(rows)


def _business_commitment(path: Path, shard: str, node: str, rows: list[tuple[str, str]]) -> None:
    import json
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "schema_version": "mbe_business_state_commitments_v16",
        "node_id": node,
        "shard_id": shard,
        "commitments": [{"key_digest": key, "value_digest": value} for key, value in rows],
    }
    path.write_text(json.dumps(payload, sort_keys=True), encoding="utf-8")


def test_business_state_evidence_is_placement_independent_and_conflict_sensitive(tmp_path: Path) -> None:
    left = tmp_path / "left"
    right = tmp_path / "right"
    # Same logical key/value pairs, opposite physical shard placement.
    for node, shard, rows in (
        ("n0", "s0", [("kA", "v1")]), ("n1", "s0", [("kA", "v1")]),
        ("n4", "s1", [("kB", "v2")]), ("n5", "s1", [("kB", "v2")]),
    ):
        _business_commitment(left / "nodes" / node / "business_state_commitments_v16.json", shard, node, rows)
    for node, shard, rows in (
        ("n0", "s0", [("kB", "v2")]), ("n1", "s0", [("kB", "v2")]),
        ("n4", "s1", [("kA", "v1")]), ("n5", "s1", [("kA", "v1")]),
    ):
        _business_commitment(right / "nodes" / node / "business_state_commitments_v16.json", shard, node, rows)
    a = compute_logical_business_state_evidence(left)
    b = compute_logical_business_state_evidence(right)
    assert a["status"] == b["status"] == "available"
    assert a["digest"] == b["digest"]
    assert a["conflict_key_count"] == b["conflict_key_count"] == 0

    conflict = tmp_path / "conflict"
    _business_commitment(conflict / "nodes" / "n0" / "business_state_commitments_v16.json", "s0", "n0", [("kA", "v1")])
    _business_commitment(conflict / "nodes" / "n4" / "business_state_commitments_v16.json", "s1", "n4", [("kA", "v2")])
    c = compute_logical_business_state_evidence(conflict)
    assert c["status"] == "available"
    assert c["conflict_key_count"] == 1


def test_business_state_evidence_fails_closed_on_replica_disagreement(tmp_path: Path) -> None:
    _business_commitment(tmp_path / "nodes" / "n0" / "business_state_commitments_v16.json", "s0", "n0", [("kA", "v1")])
    _business_commitment(tmp_path / "nodes" / "n1" / "business_state_commitments_v16.json", "s0", "n1", [("kA", "v2")])
    evidence = compute_logical_business_state_evidence(tmp_path)
    assert evidence["status"] == "replica_inconsistent"
    assert evidence["digest"] is None
    assert evidence["replica_consistent"] is False


def test_business_state_evidence_requires_every_node_commitment(tmp_path: Path) -> None:
    _business_commitment(tmp_path / "nodes" / "n0" / "business_state_commitments_v16.json", "s0", "n0", [("kA", "v1")])
    (tmp_path / "nodes" / "n1").mkdir(parents=True)
    evidence = compute_logical_business_state_evidence(tmp_path)
    assert evidence["status"] == "incomplete_node_evidence"
    assert evidence["digest"] is None
    assert evidence["missing_nodes"] == ["n1"]


def test_group_gate_requires_business_evidence_for_all_configured_shards() -> None:
    item = {
        "method_id": "stateful_optme",
        "topology_point": {"shards": 2, "worker_count": 8},
        "initial_state_digest": "i",
        "global_final_state_digest": "f",
        "state_home_mapping_digest": "h",
        "metrics": {
            "v16_logical_business_state_digest": "b",
            "v16_logical_business_state_evidence_shard_count": 1,
            "logical_transaction_identity_digest": "workload",
            "v16_metric_coherence_passed": True,
            "v16_business_execution_timer_complete": True,
            "v16_effective_worker_count": 8,
        },
    }
    enriched, _ = apply_group_fidelity_gate([item], {})
    assert "logical_business_state_commitment_shard_coverage_mismatch" in enriched[0]["v16_fidelity_blockers"]


def test_txallo_four_evidence_dimensions_are_not_conflated(tmp_path: Path) -> None:
    _csv(tmp_path / "client" / "transaction_placement.csv", ["logical_id", "execution_shard"], [
        {"logical_id": "t1", "execution_shard": "s1"},
    ])
    _csv(tmp_path / "client" / "routing_decision_log.csv", ["logical_id", "route"], [
        {"logical_id": "t1", "route": "s0"},
    ])
    _csv(tmp_path / "state_home_mapping.csv", ["state_key", "home_shard"], [
        {"state_key": "a", "home_shard": "s0"},
    ])
    evidence = compute_txallo_evidence(tmp_path, {"txallo_mapping_digest": "account-map"})
    assert evidence["account_allocation"]["digest"] == "account-map"
    assert evidence["transaction_placement"]["digest"]
    assert evidence["routing_decision"]["digest"]
    assert evidence["state_home_mapping"]["digest"]
    assert len({
        evidence["account_allocation"]["digest"], evidence["transaction_placement"]["digest"],
        evidence["routing_decision"]["digest"], evidence["state_home_mapping"]["digest"],
    }) == 4


def test_txallo_current_3366_pattern_uses_paper_parameter_semantics(tmp_path: Path) -> None:
    metrics = enrich_metrics(tmp_path, "stateful_txallo", {
        "txallo_mapping_digest": "map",
        "txallo_history_transaction_count": 3366,
        "txallo_mapped_account_count": 3367,
        "txallo_graph_edge_count": 3366,
        "txallo_lambda": 183,
        "txallo_eta": 2,
        "txallo_epsilon": 0.00366,
        "txallo_adaptive_chunk_records": 500,
        "txallo_a_txallo_run_count": 6,
        "txallo_g_txallo_run_count": 1,
        "txallo_modeled_cross_shard_ratio": 0,
        "txallo_modeled_workload_stddev": 1683,
        "txallo_modeled_throughput": 183,
        "finalized_tx_count": 1000,
        "transaction_execution_ms": 10,
    })
    # Per-run extraction must preserve the paper meanings: eta is cross-shard
    # workload multiplier, lambda is per-shard capacity, epsilon is convergence
    # threshold.  The 3366/183/0.00366 values alone are not a window mismatch.
    assert metrics["v16_txallo_parameter_semantics_valid"] is True
    assert metrics["v16_txallo_account_graph_tree_like_suspected"] is True
    assert metrics["paper_model_Lambda_is_measured_tps"] is False

    item = {
        "method_id": "stateful_txallo",
        "topology_point": {"shards": 2, "worker_count": 8},
        "method": {"plugin_config_overrides": {"sharding": {"lambda": 0, "epsilon": 0}}},
        "initial_state_digest": "i",
        "global_final_state_digest": "f",
        "state_home_mapping_digest": "h",
        "metrics": {
            **metrics,
            "v16_logical_business_state_digest": "b",
            "serial_order_replay_input_digest": "same",
            "v16_effective_worker_count": 1,
        },
    }
    enriched, _ = apply_group_fidelity_gate([item], {})
    diag = enriched[0]["v16_txallo_topology_diagnostics"]
    assert diag["initial_g_history_count_implied_by_lambda_k"] == 366
    assert diag["initial_g_history_count_implied_by_epsilon"] == 366
    assert diag["reconstructed_history_from_g_plus_a_chunks"] == 3366
    assert diag["adaptive_history_accounting_consistent"] is True
    assert diag["extreme_workload_imbalance_suspected"] is True


def test_missing_business_truth_is_unknown_not_equal() -> None:
    items = [
        {"method_id": "stateful_optme", "metrics": {"serial_order_replay_input_digest": "x"}},
        {"method_id": "stateful_txallo", "metrics": {"serial_order_replay_input_digest": "x", "v16_txallo_account_mapping_digest": "m"}},
    ]
    _, report = apply_group_fidelity_gate(items, {"performance_comparison_valid": True})
    assert report["pairwise_logical_business_state_equivalent"] is None
    assert report["performance_comparison_valid"] is False
    assert "logical_business_state_not_proven_equal" in report["v16_group_fidelity_blockers"]


def test_group_gate_reads_business_digest_from_metrics_not_empty_top_level() -> None:
    base = {"initial_state_digest": "i", "global_final_state_digest": "f", "state_home_mapping_digest": "h"}
    items = [
        {**base, "method_id": "stateful_optme", "metrics": {
            "v16_logical_business_state_digest": "b1", "serial_order_replay_input_digest": "same",
            "v16_metric_coherence_passed": True, "v16_business_execution_timer_complete": True,
            "v16_effective_worker_count": 8, "block_executor_id": "optme_block_executor",
        }},
        {**base, "method_id": "stateful_txallo", "metrics": {
            "v16_logical_business_state_digest": "b2", "serial_order_replay_input_digest": "same",
            "v16_txallo_account_mapping_digest": "map", "v16_metric_coherence_passed": True,
            "v16_business_execution_timer_complete": True, "v16_effective_worker_count": 1,
            "block_executor_id": "serial_block_executor",
        }},
    ]
    enriched, report = apply_group_fidelity_gate(items, {})
    assert report["pairwise_logical_business_state_equivalent"] is False
    assert report["runtime_execution_stack_equivalent"] is False
    assert report["direct_single_variable_performance_comparison_valid"] is False
    assert all(x["paper_candidate"] is False for x in enriched)


def test_tx_identity_mismatch_blocks_paper_candidate() -> None:
    items = [
        {"initial_state_digest": "i", "global_final_state_digest": "f", "state_home_mapping_digest": "h",
         "method_id": "stateful_optme", "metrics": {"v16_logical_business_state_digest": "b", "logical_transaction_identity_digest": "A",
         "v16_metric_coherence_passed": True, "v16_business_execution_timer_complete": True, "v16_effective_worker_count": 8}},
        {"initial_state_digest": "i", "global_final_state_digest": "f", "state_home_mapping_digest": "h",
         "method_id": "stateful_txallo", "metrics": {"v16_logical_business_state_digest": "b", "logical_transaction_identity_digest": "B",
         "v16_txallo_account_mapping_digest": "map", "v16_metric_coherence_passed": True,
         "v16_business_execution_timer_complete": True, "v16_effective_worker_count": 1}},
    ]
    enriched, report = apply_group_fidelity_gate(items, {})
    assert report["pairwise_logical_transaction_identity_equivalent"] is False
    assert report["performance_comparison_valid"] is False
    assert all(x["paper_candidate"] is False for x in enriched)


def test_logical_identity_prefers_materialized_workload_over_physical_replay_digest() -> None:
    common_summary = {
        "workload_replay_summary": {
            "materialized_sha256": "same-materialized-workload",
            "identity_mapping_version": "mbe_dataset_identity_v1",
            "submitted_count": 1000,
        }
    }
    items = [
        {
            "method_id": "stateful_optme",
            "initial_state_digest": "i", "global_final_state_digest": "f1", "state_home_mapping_digest": "h1",
            "result": {"summary": common_summary},
            "metrics": {
                "v16_logical_business_state_digest": "b",
                "serial_order_replay_input_digest": "optme-replay",
                "serial_order_replay_identity_basis": "logical_id_access_list_digest_for_workload;tx_id_for_durable_trace",
                "serial_order_replay_transaction_count": 1000,
                "serial_order_replay_unique_transaction_count": 1000,
                "v16_metric_coherence_passed": True, "v16_business_execution_timer_complete": True,
                "v16_effective_worker_count": 8,
            },
        },
        {
            "method_id": "stateful_txallo",
            "initial_state_digest": "i", "global_final_state_digest": "f2", "state_home_mapping_digest": "h2",
            "result": {"summary": common_summary},
            "metrics": {
                "v16_logical_business_state_digest": "b",
                "serial_order_replay_input_digest": "txallo-physical-replay",
                "serial_order_replay_identity_basis": "tx_id",
                "serial_order_replay_transaction_count": 1800,
                "serial_order_replay_unique_transaction_count": 1000,
                "v16_txallo_account_mapping_digest": "map",
                "v16_metric_coherence_passed": True, "v16_business_execution_timer_complete": True,
                "v16_effective_worker_count": 1,
            },
        },
    ]
    enriched, report = apply_group_fidelity_gate(items, {})
    assert report["pairwise_logical_transaction_identity_equivalent"] is True
    assert {x["v16_logical_transaction_identity_source"] for x in enriched} == {"workload_replay_summary.materialized_sha256"}


def test_effective_worker_resolves_to_topology_when_runtime_matches_requested_topology() -> None:
    item = {
        "method_id": "stateful_optme",
        "method": {"plugin_config_overrides": {"block_executor": {"worker_count": 4}}},
        "topology_point": {"worker_count": 8},
        "initial_state_digest": "i", "global_final_state_digest": "f", "state_home_mapping_digest": "h",
        "metrics": {
            "v16_logical_business_state_digest": "b", "logical_transaction_identity_digest": "workload",
            "v16_metric_coherence_passed": True, "v16_business_execution_timer_complete": True,
            "v16_effective_worker_count": 8,
        },
    }
    enriched, _ = apply_group_fidelity_gate([item], {})
    row = enriched[0]
    assert "effective_worker_count_matches_neither_method_profile_nor_topology_request" not in row["v16_fidelity_blockers"]
    assert row["v16_resolved_worker_contract_source"] == "topology_request"
    assert "stale_method_profile_worker_shadowed_by_topology_request" in row["v16_fidelity_warnings"]


def test_effective_worker_unresolved_mismatch_blocks_candidate() -> None:
    item = {
        "method_id": "stateful_optme",
        "method": {"plugin_config_overrides": {"block_executor": {"worker_count": 4}}},
        "topology_point": {"worker_count": 8},
        "initial_state_digest": "i", "global_final_state_digest": "f", "state_home_mapping_digest": "h",
        "metrics": {
            "v16_logical_business_state_digest": "b", "logical_transaction_identity_digest": "workload",
            "v16_metric_coherence_passed": True, "v16_business_execution_timer_complete": True,
            "v16_effective_worker_count": 6,
        },
    }
    enriched, _ = apply_group_fidelity_gate([item], {})
    assert "effective_worker_count_matches_neither_method_profile_nor_topology_request" in enriched[0]["v16_fidelity_blockers"]



def test_runtime_stack_digest_does_not_include_method_label() -> None:
    common_method = {
        "plugin_overrides": {
            "routing": "same_routing",
            "execution": "same_execution",
            "scheduler": "same_scheduler",
            "block_executor": "same_executor",
        },
        "plugin_config_overrides": {"block_executor": {"worker_count": 4, "mode": "paper"}},
    }
    base_metrics = {
        "v16_logical_business_state_digest": "b",
        "logical_transaction_identity_digest": "workload",
        "v16_metric_coherence_passed": True,
        "v16_business_execution_timer_complete": True,
        "v16_effective_worker_count": 4,
        "block_executor_id": "same_executor",
    }
    left = {
        "method_id": "stateful_optme",
        "method": {"method_id": "stateful_optme", **common_method},
        "topology_point": {"worker_count": 4},
        "initial_state_digest": "i", "global_final_state_digest": "f", "state_home_mapping_digest": "h",
        "metrics": dict(base_metrics),
    }
    right = {
        "method_id": "stateless_optme",
        "method": {"method_id": "stateless_optme", **common_method},
        "topology_point": {"worker_count": 4},
        "initial_state_digest": "i", "global_final_state_digest": "f", "state_home_mapping_digest": "h",
        "metrics": dict(base_metrics),
    }
    enriched, _ = apply_group_fidelity_gate([left, right], {})
    assert enriched[0]["v16_execution_stack_digest"] == enriched[1]["v16_execution_stack_digest"]


def test_txallo_proven_single_nonempty_shard_blocks_paper_candidate() -> None:
    item = {
        "method_id": "stateful_txallo",
        "topology_point": {"shards": 2, "worker_count": 8},
        "initial_state_digest": "i", "global_final_state_digest": "f", "state_home_mapping_digest": "h",
        "metrics": {
            "v16_logical_business_state_digest": "b", "logical_transaction_identity_digest": "workload",
            "v16_txallo_account_mapping_digest": "map",
            "v16_txallo_account_mapping_nonempty_shard_count": 1,
            "v16_txallo_account_mapping_uniqueness_passed": True,
            "v16_txallo_account_mapping_completeness_passed": True,
            "txallo_lambda": 10, "txallo_eta": 2, "txallo_epsilon": 0.001,
            "v16_metric_coherence_passed": True, "v16_business_execution_timer_complete": True,
            "v16_effective_worker_count": 1,
        },
    }
    enriched, _ = apply_group_fidelity_gate([item], {})
    assert "txallo_account_mapping_uses_fewer_nonempty_shards_than_configured" in enriched[0]["v16_fidelity_blockers"]
    assert enriched[0]["paper_candidate"] is False


def test_fairness_separates_complete_system_from_controlled_single_variable() -> None:
    rows = [
        {"comparison_group_id": "g", "method_id": "optme", "method": {"plugin_overrides": {"block_executor": "optme_block_executor"}}, "topology_point": {"worker_count": 8}},
        {"comparison_group_id": "g", "method_id": "txallo", "method": {"plugin_overrides": {"block_executor": "serial_block_executor"}}, "topology_point": {"worker_count": 8}},
    ]
    checked, report = apply_fairness_fidelity_gate(rows, {"passed": True})
    assert report["v16_legacy_structural_fairness_passed"] is True
    assert report["v16_direct_single_variable_plan_fairness_valid"] is False
    assert {x["v16_comparison_mode"] for x in checked} == {"complete_system"}


def test_optme_replica_counts_are_not_silently_divided(tmp_path: Path) -> None:
    out = enrich_metrics(tmp_path, "stateful_optme", {
        "optme_early_abort_count": 3904, "optme_reexecution_count": 3904,
        "optme_reordered_transaction_count": 0, "worker_count": 8,
    })
    assert out["v16_optme_replica_physical_early_abort_observation_count"] == 3904
    assert out["v16_optme_logical_early_abort_count"] is None
    assert out["v16_optme_logical_count_status"] == "missing_tx_level_dedup_evidence"
    assert out["v16_optme_reorder_path_warning"] is True


def test_writeback_dedup_requires_logical_identity(tmp_path: Path) -> None:
    path = tmp_path / "physical_remote_state_operations.csv"
    _csv(path, ["message_type", "state_key", "target_node"], [
        {"message_type": "STATE_DELTA_APPLY", "state_key": "a", "target_node": "n0"},
        {"message_type": "STATE_DELTA_APPLY", "state_key": "a", "target_node": "n1"},
    ])
    out = compute_writeback_fanout(tmp_path)
    assert out["physical_message_count"] == 2
    assert out["unique_logical_delta_count"] is None
    assert out["replica_fanout_ratio"] is None


def test_version_constant() -> None:
    assert CLOSURE_VERSION.endswith("v16")
