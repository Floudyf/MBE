from __future__ import annotations

import csv
import json
from pathlib import Path

from backend.app.services.v5_optme_txallo_paperfaithful_v17 import (
    apply_group_fidelity_gate,
    compute_business_state_evidence_v17,
    compute_optme_transaction_evidence_v17,
    compute_txallo_paper_audit_v17,
    compute_txallo_physical_execution_coherence_v17,
    compute_txallo_routing_coherence_v17,
    compute_writeback_fanout_v17,
    enrich_metrics,
)


def _write_json(path: Path, value) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, sort_keys=True), encoding="utf-8")


def _write_csv(path: Path, rows: list[dict[str, object]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fields = list(rows[0]) if rows else []
    with path.open("w", newline="", encoding="utf-8") as h:
        w = csv.DictWriter(h, fieldnames=fields); w.writeheader(); w.writerows(rows)


def _commit(node: Path, shard: str, rows: list[tuple[str,str,str]]) -> None:
    _write_json(node / "business_state_commitments_v17.json", {
        "schema_version":"mbe_business_state_commitments_v17", "shard_id":shard,
        "commitments":[{"partition_key_digest":p,"logical_key_digest":l,"value_digest":v} for p,l,v in rows],
    })


def test_stateful_keeps_partition_identity(tmp_path: Path) -> None:
    for n in ("n0","n1"): _commit(tmp_path/"nodes"/n, "s0", [("p0","k","v0")])
    for n in ("n2","n3"): _commit(tmp_path/"nodes"/n, "s1", [("p1","k","v1")])
    got = compute_business_state_evidence_v17(tmp_path, "stateful_txallo", {})
    assert got["status"] == "available"
    assert got["storage_partition_digest"]
    assert got["global_logical_business_digest"] is None
    assert got["global_logical_business_projection_status"] == "not_applicable_without_explicit_stateful_global_projection"
    assert got["global_logical_conflict_key_count"] == 1


def test_stateless_requires_conflict_free_global_projection(tmp_path: Path) -> None:
    for n in ("n0","n1"): _commit(tmp_path/"nodes"/n, "s0", [("p0","k","v")])
    for n in ("n2","n3"): _commit(tmp_path/"nodes"/n, "s1", [("p1","k","v")])
    got = compute_business_state_evidence_v17(tmp_path, "stateless_optme", {})
    assert got["global_logical_business_projection_status"] == "available"
    assert got["global_logical_business_digest"]
    assert got["global_logical_conflict_key_count"] == 0


def test_txallo_empty_community_is_diagnostic_not_paper_failure(tmp_path: Path) -> None:
    _write_csv(tmp_path/"client/txallo_account_mapping.csv", [
        {"account":"a","shard":"s0"}, {"account":"b","shard":"s0"},
    ])
    _write_json(tmp_path/"compiled_run_plan.json", {"topology":{"shards":2}})
    m = {"txallo_mapping_digest":"d", "txallo_mapped_account_count":2, "txallo_eta":2,
         "txallo_lambda":10, "txallo_epsilon":0.1, "txallo_g_txallo_run_count":1,
         "txallo_a_txallo_run_count":1}
    got = compute_txallo_paper_audit_v17(tmp_path, m)
    assert got["account_uniqueness_passed"] is True
    assert got["account_completeness_passed"] is True
    assert got["single_nonempty_shard_diagnostic"] is True
    assert got["empty_community_is_not_intrinsically_invalid"] is True


def test_txallo_routing_must_be_induced_by_mapping(tmp_path: Path) -> None:
    _write_csv(tmp_path/"client/txallo_account_mapping.csv", [
        {"account":"a","shard":"s0"}, {"account":"b","shard":"s1"},
    ])
    _write_csv(tmp_path/"client/txallo_transaction_placement.csv", [
        {"logical_id":"t1","sender":"a","receiver":"b","involved_shards":"s0|s1","execution_shard":"s0"},
        {"logical_id":"t2","sender":"a","receiver":"a","involved_shards":"s1","execution_shard":"s1"},
    ])
    got = compute_txallo_routing_coherence_v17(tmp_path)
    assert got["checked_transaction_count"] == 2
    assert got["mismatch_count"] == 1
    assert got["passed"] is False


def test_writeback_requires_exact_version_identity_tuple(tmp_path: Path) -> None:
    _write_csv(tmp_path/"physical_remote_state_operations.csv", [
        {"kind":"STATE_DELTA_APPLY","logical_tx_id":"t","state_key":"k","produced_version":"2","home_shard":"s0","update_semantics":"set","replica":"n0"},
        {"kind":"STATE_DELTA_APPLY","logical_tx_id":"t","state_key":"k","produced_version":"2","home_shard":"s0","update_semantics":"set","replica":"n1"},
    ])
    got = compute_writeback_fanout_v17(tmp_path, {})
    assert got["status"] == "available_exact_logical_dedup"
    assert got["unique_logical_delta_count"] == 1
    assert got["replica_fanout_ratio"] == 2


def test_writeback_does_not_guess_when_version_missing(tmp_path: Path) -> None:
    _write_csv(tmp_path/"physical_remote_state_operations.csv", [
        {"kind":"STATE_DELTA_APPLY","logical_tx_id":"t","state_key":"k","produced_version":"","home_shard":"s0","update_semantics":"set"},
    ])
    got = compute_writeback_fanout_v17(tmp_path, {})
    assert got["status"] == "missing_exact_version_identity"
    assert "produced_version" in got["missing_identity_fields"]


def test_optme_zero_reorder_is_not_automatically_invalid(tmp_path: Path) -> None:
    got = compute_optme_transaction_evidence_v17(tmp_path, {"optme_early_abort_count":20,"optme_reexecution_count":20,"optme_reordered_transaction_count":0})
    assert got["status"] == "missing_tx_level_fidelity_trace"
    assert got["aggregate_reordered_observation_count"] == 0


def test_optme_transaction_trace_deduplicates_by_logical_tx(tmp_path: Path) -> None:
    _write_csv(tmp_path/"aggregate/optme_transaction_fidelity.csv", [
        {"logical_tx_id":"t1","early_abort":"true","reexecution":"true","reordered":"false"},
        {"logical_tx_id":"t1","early_abort":"true","reexecution":"true","reordered":"false"},
        {"logical_tx_id":"t2","early_abort":"false","reexecution":"false","reordered":"true"},
    ])
    got = compute_optme_transaction_evidence_v17(tmp_path, {})
    assert got["logical_transaction_count"] == 2
    assert got["logical_early_abort_count"] == 1
    assert got["logical_reexecution_count"] == 1
    assert got["logical_reordered_count"] == 1


def test_stateful_partition_oracle_match_overrides_legacy_digest_contradiction(tmp_path: Path) -> None:
    base = {
        "method_id":"stateful_txallo", "txallo_mapping_digest":"d", "txallo_mapped_account_count":1,
        "txallo_eta":2,"txallo_lambda":1,"txallo_epsilon":0.01,
        "stateful_serializability_actual_partition_business_digests":{"s0":"a"},
        "stateful_serializability_replay_partition_business_digests":{"s0":"a"},
        "stateful_serializability_actual_partition_effect_digests":{"s0":"b"},
        "stateful_serializability_replay_partition_effect_digests":{"s0":"b"},
        "global_business_state_digest":"legacy",
    }
    got = enrich_metrics(tmp_path, "stateful_txallo", base)
    assert got["v17_stateful_serial_partition_equivalent"] is True


def test_group_does_not_compare_stateful_partition_digest_as_global_business_state() -> None:
    items = [
        {"method":{"method_id":"stateful_optme"}, "metrics":{"v16_logical_transaction_identity_digest":"w", "v17_storage_partition_state_digest":"p1", "v17_fidelity_blockers":[]}},
        {"method":{"method_id":"stateful_txallo"}, "metrics":{"v16_logical_transaction_identity_digest":"w", "v17_storage_partition_state_digest":"p2", "v17_fidelity_blockers":[]}},
    ]
    # Canonical child consumes metrics produced by the extractor. Different partition placement is diagnostic, not cross-method logical-state failure.
    _, report = apply_group_fidelity_gate(items, {})
    assert report["cross_semantic_business_state_equivalence"] == "not_asserted_without_explicit_canonical_global_projection"


def test_txallo_stateful_physical_execution_matches_explicit_paper_placement(tmp_path: Path) -> None:
    _write_csv(tmp_path/"client/txallo_transaction_placement.csv", [
        {"logical_tx_id":"t1","involved_shards":"s0|s1"},
        {"logical_tx_id":"t2","involved_shards":"s0"},
    ])
    _write_csv(tmp_path/"nodes/n0/transaction_execution_trace.csv", [
        {"tx_id":"t1","shard_id":"s0"}, {"tx_id":"t2","shard_id":"s0"},
    ])
    _write_csv(tmp_path/"nodes/n4/transaction_execution_trace.csv", [
        {"tx_id":"t1","shard_id":"s1"},
    ])
    got = compute_txallo_physical_execution_coherence_v17(tmp_path)
    assert got["status"] == "available"
    assert got["checked_transaction_count"] == 2
    assert got["mismatch_count"] == 0
    assert got["passed"] is True
    assert got["legacy_source_shard_cross_shard_flags_used"] is False


def test_txallo_stateful_physical_execution_detects_legacy_routing_override(tmp_path: Path) -> None:
    _write_csv(tmp_path/"client/txallo_transaction_placement.csv", [
        {"logical_tx_id":"t1","involved_shards":"s0"},
    ])
    _write_csv(tmp_path/"nodes/n0/transaction_execution_trace.csv", [
        {"tx_id":"t1","shard_id":"s0"},
    ])
    _write_csv(tmp_path/"nodes/n4/transaction_execution_trace.csv", [
        {"tx_id":"t1","shard_id":"s1"},
    ])
    got = compute_txallo_physical_execution_coherence_v17(tmp_path)
    assert got["passed"] is False
    assert got["mismatch_count"] == 1
