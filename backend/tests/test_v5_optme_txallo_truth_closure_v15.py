from __future__ import annotations

import csv
from pathlib import Path

from backend.app.services.v5_optme_txallo_truth_closure import (
    WRITEBACK_REPLICATION_CONTRACT,
    apply_truth_closure_to_state_equivalence,
    compute_txallo_placement_evidence,
    compute_writeback_fanout,
    enrich_extracted_metrics,
)


def _write_csv(path: Path, fields: list[str], rows: list[dict[str, object]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader(); writer.writerows(rows)


def test_v15_compatibility_surface_keeps_transaction_placement_evidence_separate(tmp_path: Path) -> None:
    placement = tmp_path / "client" / "transaction_placement.csv"
    _write_csv(placement, ["logical_id", "home_shard", "execution_shard"], [
        {"logical_id": "t1", "home_shard": "s0", "execution_shard": "s1"},
    ])
    evidence = compute_txallo_placement_evidence(tmp_path)
    assert evidence["status"] == "available"
    out = enrich_extracted_metrics(tmp_path, "stateless_txallo", {
        "txallo_mapping_digest": "account-map",
        "state_home_mapping_digest": "home-map",
    })
    assert out["v16_txallo_account_mapping_digest"] == "account-map"
    assert out["v16_txallo_transaction_placement_digest"] == evidence["digest"]
    # v16 fixes the old semantic bug: transaction placement must not overwrite Home mapping.
    assert out.get("state_home_mapping_digest") == "home-map"


def test_v15_compatibility_missing_truth_never_becomes_equal() -> None:
    items = [
        {"method_id": "stateless_optme", "metrics": {"serial_order_replay_input_digest": "same"}},
        {"method_id": "stateless_txallo", "metrics": {"serial_order_replay_input_digest": "same", "v16_txallo_account_mapping_digest": "m"}},
    ]
    _, report = apply_truth_closure_to_state_equivalence(items, {})
    assert report["pairwise_logical_business_state_equivalent"] is None
    assert report["performance_comparison_valid"] is False


def test_v15_compatibility_writeback_exact_dedup(tmp_path: Path) -> None:
    path = tmp_path / "physical_remote_state_operations.csv"
    fields = ["message_type", "logical_tx_id", "state_key", "produced_version", "home_shard", "target_node"]
    rows = []
    for logical in ("t1", "t2"):
        for node in ("n0", "n1", "n2", "n3"):
            rows.append({
                "message_type": "STATE_DELTA_APPLY", "logical_tx_id": logical,
                "state_key": "asset:" + logical, "produced_version": "1", "home_shard": "s0", "target_node": node,
            })
    _write_csv(path, fields, rows)
    result = compute_writeback_fanout(tmp_path)
    assert result["physical_message_count"] == 8
    assert result["unique_logical_delta_count"] == 2
    assert result["replica_fanout_ratio"] == 4
    assert result["status"] == "available_exact_logical_dedup"
    assert WRITEBACK_REPLICATION_CONTRACT == "home_all_replicas_failover_preserving_v1"
