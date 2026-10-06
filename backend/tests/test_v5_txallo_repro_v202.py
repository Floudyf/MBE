import csv
from pathlib import Path

from backend.app.services.v5_formal_plan_validator import ALL_BUILTIN_METHODS
from backend.app.services.v5_plugin_manifest_store import STORE
from backend.app.services.v5_optme_txallo_paperfaithful_v17 import compute_txallo_routing_coherence_v17
from backend.app.services.v5_optme_txallo_method_specific_v18 import compute_txallo_independent_account_coverage_v18


def _write_csv(path: Path, rows: list[dict[str, str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)


def test_stateless_txallo_explicitly_composes_no_legacy_relay() -> None:
    method = ALL_BUILTIN_METHODS["stateless_txallo"]
    assert method.plugin_overrides["cross_shard"] == "txallo_no_relay"
    manifest = STORE.get("txallo_no_relay")
    assert "no_legacy_business_relay" in manifest.capabilities
    assert "single_business_execution_owner" in manifest.capabilities


def test_txallo_fallback_is_causal_evidence_not_missing_history(tmp_path: Path) -> None:
    _write_csv(tmp_path / "client/txallo_account_mapping.csv", [
        {"account": "known", "shard": "s0"},
        {"account": "m.federation", "shard": "s0"},
    ])
    _write_csv(tmp_path / "client/txallo_transaction_placement.csv", [
        {
            "batch_index": "0", "logical_id": "t1", "tx_index": "0",
            "sender_account": "known", "receiver_account": "m.federation",
            "sender_mapping_source": "history_mapping", "receiver_mapping_source": "history_mapping",
            "sender_shard": "s0", "receiver_shard": "s0", "involved_shards": "s0",
            "home_shard": "s0", "execution_shard": "s0", "target_shard": "s0",
            "txallo_cross_shard": "false", "cross_shard_edge_count": "0", "mapping_digest": "d", "reason": "x", "plan_digest": "p",
        },
        {
            "batch_index": "0", "logical_id": "t2", "tx_index": "1",
            "sender_account": "new-user", "receiver_account": "m.federation",
            "sender_mapping_source": "fallback_hash", "receiver_mapping_source": "history_mapping",
            "sender_shard": "s1", "receiver_shard": "s0", "involved_shards": "s0|s1",
            "home_shard": "s1", "execution_shard": "s1", "target_shard": "s0",
            "txallo_cross_shard": "true", "cross_shard_edge_count": "1", "mapping_digest": "d", "reason": "x", "plan_digest": "p",
        },
    ])
    route = compute_txallo_routing_coherence_v17(tmp_path)
    coverage = compute_txallo_independent_account_coverage_v18(tmp_path)
    assert route["passed"] is True
    assert route["fallback_account_count"] == 1
    assert coverage["passed"] is True
    assert coverage["fallback_account_count"] == 1
    assert coverage["history_mapped_account_count"] == 2


def test_txallo_legacy_v17_placement_columns_remain_readable(tmp_path: Path) -> None:
    _write_csv(tmp_path / "client/txallo_account_mapping.csv", [
        {"account": "a", "shard": "s0"},
        {"account": "b", "shard": "s1"},
    ])
    _write_csv(tmp_path / "client/txallo_transaction_placement.csv", [
        {"logical_id": "t1", "sender": "a", "receiver": "b", "involved_shards": "s0|s1", "execution_shard": "s0"},
        {"logical_id": "t2", "sender": "a", "receiver": "a", "involved_shards": "s1", "execution_shard": "s1"},
    ])
    got = compute_txallo_routing_coherence_v17(tmp_path)
    assert got["status"] == "available"
    assert got["checked_transaction_count"] == 2
    assert got["mismatch_count"] == 1
    assert got["passed"] is False
