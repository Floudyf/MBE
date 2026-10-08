from __future__ import annotations

import csv
import gzip
import hashlib
import json
from pathlib import Path

from backend.app.services.v5_txallo_mapping_epoch_v229 import compute_account_coverage, compute_routing_coherence
from backend.app.services.v5_txallo_stateful_oracle_v229 import evaluate, _proven_null_noop_wal_blockers
from backend.app.services import v5_txallo_stateful_oracle_v229 as oracle


def _digest(value: str) -> str:
    return hashlib.sha256(value.encode()).hexdigest()


def test_epoch_audit_does_not_compare_fallback_against_future_mapping(tmp_path: Path) -> None:
    (tmp_path / "client").mkdir()
    (tmp_path / "workload").mkdir()
    def epoch_row(epoch: int, mapping: dict[str, str]) -> dict[str, object]:
        aliases: dict[str, str] = {}
        md = hashlib.sha256(json.dumps(mapping, sort_keys=True, separators=(",", ":")).encode()).hexdigest()
        ad = hashlib.sha256(json.dumps(aliases, sort_keys=True, separators=(",", ":")).encode()).hexdigest()
        sd = hashlib.sha256(json.dumps({"mapping": mapping, "aliases": aliases}, sort_keys=True, separators=(",", ":")).encode()).hexdigest()
        return {"schema_version": "mbe_txallo_mapping_snapshot_v2", "epoch": epoch, "mapping": mapping, "aliases": aliases, "mapping_digest": md, "aliases_digest": ad, "state_digest": sd}
    epoch0 = epoch_row(0, {"bob": "s0"})
    epoch1 = epoch_row(1, {"bob": "s0", "alice": "s0"})
    (tmp_path / "workload" / "txallo_mapping_epochs.jsonl").write_text(json.dumps(epoch0)+"\n"+json.dumps(epoch1)+"\n", encoding="utf-8")
    with (tmp_path / "client" / "txallo_transaction_placement.csv").open("w", newline="", encoding="utf-8") as handle:
        fieldnames = ["batch_index","logical_id","tx_index","sender_account","receiver_account","sender_mapping_source","receiver_mapping_source","sender_shard","receiver_shard","involved_shards","home_shard","execution_shard","target_shard","txallo_cross_shard","cross_shard_edge_count","routing_epoch","mapping_digest","mapping_state_digest","reason","plan_digest"]
        writer = csv.DictWriter(handle, fieldnames=fieldnames); writer.writeheader()
        writer.writerow({"batch_index":0,"logical_id":"l0","tx_index":0,"sender_account":"alice","receiver_account":"bob","sender_mapping_source":"fallback_hash","receiver_mapping_source":"history_mapping","sender_shard":"s1","receiver_shard":"s0","involved_shards":"s0|s1","home_shard":"s1","execution_shard":"s1","target_shard":"s0","txallo_cross_shard":"true","cross_shard_edge_count":1,"routing_epoch":0,"mapping_digest":epoch0["mapping_digest"],"mapping_state_digest":epoch0["state_digest"],"reason":"dynamic","plan_digest":"p"})
    routing = compute_routing_coherence(tmp_path)
    coverage = compute_account_coverage(tmp_path)
    assert routing and routing["passed"] is True
    assert coverage and coverage["passed"] is True
    assert routing["fallback_account_count"] == 1


def _write_node(root: Path, node: str, shard: str, role: str, value: str) -> None:
    nd = root / "nodes" / node; nd.mkdir(parents=True)
    (nd / "node_summary.json").write_text(json.dumps({"shard_id": shard}), encoding="utf-8")
    with (nd / "committed_chain.csv").open("w", newline="", encoding="utf-8") as handle:
        w=csv.DictWriter(handle, fieldnames=["node_id","shard_id","height","view","block_hash","parent_hash","tx_count","tx_root","state_root_before","state_root_after","receipt_root","committed_at_ms","timestamp_ms"]); w.writeheader(); w.writerow({"node_id":node,"shard_id":shard,"height":1,"view":0,"block_hash":f"b-{node}","parent_hash":"genesis","tx_count":1 if role=="source_local" else 0,"tx_root":"r","state_root_before":"x","state_root_after":"y","receipt_root":"z","committed_at_ms":1,"timestamp_ms":1})
    (nd / "state_snapshot.json").write_text(json.dumps({f"{shard}::asset": value}), encoding="utf-8")
    (nd / "block_execution_summary.json").write_text(json.dumps({"block_executor_id":"serial_block_executor","blocks":[{"block_hash":f"b-{node}","height":1}]}), encoding="utf-8")
    row={"schema_version":"mbe_txallo_replica_commit_v229","token":"ignored-by-oracle","key":"asset","routing_ordinal":1,"value":value,"value_digest":_digest(value),"source_tx_id":"tx1","source_shard":"s0","materialized_shard":shard,"source_block_hash":"b-n0","source_height":1,"materialized_block_hash":f"b-{node}","materialized_height":1,"role":role,"commutative":False,"timestamp_ms":1}
    (nd / "txallo_replica_commit.jsonl").write_text(json.dumps(row)+"\n",encoding="utf-8")


def _write_network_summary(root: Path, *, fetch_requests: int = 0, fetch_responses: int = 0) -> None:
    with (root / "network_message_summary.csv").open("w", newline="", encoding="utf-8") as handle:
        fields = ["scope","category","message_type","message_count","bytes","message_share_percent","byte_share_percent"]
        w = csv.DictWriter(handle, fieldnames=fields); w.writeheader()
        w.writerow({"scope":"message_type","category":"remote_state","message_type":"V5_STATE_FETCH_REQUEST","message_count":fetch_requests,"bytes":0})
        w.writerow({"scope":"message_type","category":"remote_state","message_type":"V5_STATE_FETCH_RESPONSE","message_count":fetch_responses,"bytes":0})
        w.writerow({"scope":"message_type","category":"remote_state","message_type":"V5_STATE_DELTA_APPLY","message_count":8,"bytes":1})
        w.writerow({"scope":"message_type","category":"remote_state","message_type":"V5_STATE_DELTA_APPLY_ACK","message_count":8,"bytes":1})


def test_stateful_replica_oracle_requires_same_durable_logical_state(tmp_path: Path) -> None:
    (tmp_path / "client").mkdir()
    access={"index":0,"logical_id":"l1","tx_id":"tx1","execution_shard":"s0","access_list":[{"key":"asset","mode":"write","update_semantics":"set","delta":0}],"state_versions":[{"key":"asset","required_version":0,"produced_version":1}]}
    with gzip.open(tmp_path / "client" / "resolved_access_lists.jsonl.gz","wt",encoding="utf-8") as handle: handle.write(json.dumps(access)+"\n")
    value="value-1"
    _write_node(tmp_path,"n0","s0","source_local",value)
    _write_node(tmp_path,"n1","s1","replica_system",value)
    _write_network_summary(tmp_path)
    result=evaluate(tmp_path,{"method_config_id":"stateful_txallo","finality_evidence":{"submitted_unique_tx_count":1}})
    assert result["method_correctness_oracle_status"] == "passed", result
    assert result["txallo_stateful_replica_expected_write_count"] == 1
    assert result["txallo_stateful_physical_zero_fetch_verified"] is True
    assert result["txallo_stateful_physical_generic_versioned_wave_count"] == 0


def test_stateful_replica_oracle_fails_on_replica_value_divergence(tmp_path: Path) -> None:
    (tmp_path / "client").mkdir()
    access={"index":0,"logical_id":"l1","tx_id":"tx1","execution_shard":"s0","access_list":[{"key":"asset","mode":"write","update_semantics":"set","delta":0}],"state_versions":[{"key":"asset","required_version":0,"produced_version":1}]}
    with gzip.open(tmp_path / "client" / "resolved_access_lists.jsonl.gz","wt",encoding="utf-8") as handle: handle.write(json.dumps(access)+"\n")
    _write_node(tmp_path,"n0","s0","source_local","v1")
    _write_node(tmp_path,"n1","s1","replica_system","v2")
    _write_network_summary(tmp_path)
    result=evaluate(tmp_path,{"method_config_id":"stateful_txallo"})
    assert result["method_correctness_oracle_status"] == "failed"
    assert any("exact_value_disagreement" in item or "final_logical_state_not_globally_equal" in item for item in result["method_correctness_oracle_blockers"])


def _write_noop_wal_fixture(root: Path, *, after_root: str = "root-a", include_updates: bool = True) -> tuple[Path, set[str]]:
    node = root / "nodes" / "n0"
    node.mkdir(parents=True)
    block_hash = "noop-block"
    with (node / "committed_chain.csv").open("w", newline="", encoding="utf-8") as handle:
        fields = ["node_id","shard_id","height","view","block_hash","parent_hash","tx_count","tx_root","state_root_before","state_root_after","receipt_root","committed_at_ms","timestamp_ms"]
        w = csv.DictWriter(handle, fieldnames=fields); w.writeheader()
        w.writerow({"node_id":"n0","shard_id":"s0","height":1,"view":0,"block_hash":block_hash,"parent_hash":"genesis","tx_count":0,"tx_root":"r","state_root_before":"root-a","state_root_after":after_root,"receipt_root":"z","committed_at_ms":1,"timestamp_ms":1})
    record = {"version":"state_delta_wal_v1","namespace":"s0","block_height":1,"block_hash":block_hash,"parent_hash":"genesis","delta_id":"d1","state_root_before":"root-a","state_root_after":after_root,"state_root_version":"smt_v1","checksum":"ignored"}
    if include_updates:
        record["state_updates"] = None
    (node / "state_delta.1.wal").write_text(json.dumps(record)+"\n", encoding="utf-8")
    return node, {block_hash}


def test_stateful_replica_oracle_accepts_only_proven_null_noop_wal(tmp_path: Path) -> None:
    node, committed = _write_noop_wal_fixture(tmp_path)
    safe = _proven_null_noop_wal_blockers(node, "s0", committed)
    assert safe == {"stateful_serializability_wal_updates_missing:n0:1"}


def test_stateful_replica_oracle_rejects_null_wal_when_root_changes(tmp_path: Path) -> None:
    node, committed = _write_noop_wal_fixture(tmp_path, after_root="root-b")
    assert _proven_null_noop_wal_blockers(node, "s0", committed) == set()


def test_stateful_replica_oracle_rejects_missing_state_updates_field(tmp_path: Path) -> None:
    node, committed = _write_noop_wal_fixture(tmp_path, include_updates=False)
    assert _proven_null_noop_wal_blockers(node, "s0", committed) == set()


def test_replica_feed_complete_is_independent_from_non_feed_correctness_blocker(tmp_path: Path, monkeypatch) -> None:
    (tmp_path / "client").mkdir()
    access={"index":0,"logical_id":"l1","tx_id":"tx1","execution_shard":"s0","access_list":[{"key":"asset","mode":"write","update_semantics":"set","delta":0}],"state_versions":[{"key":"asset","required_version":0,"produced_version":1}]}
    with gzip.open(tmp_path / "client" / "resolved_access_lists.jsonl.gz","wt",encoding="utf-8") as handle: handle.write(json.dumps(access)+"\n")
    value="value-1"
    _write_node(tmp_path,"n0","s0","source_local",value)
    _write_node(tmp_path,"n1","s1","replica_system",value)
    _write_network_summary(tmp_path)
    monkeypatch.setattr(oracle, "_logical_business_state", lambda node_dir, shard_id: ({"asset": value}, ["synthetic_non_feed_correctness_blocker"], 0))
    result=oracle.evaluate(tmp_path,{"method_config_id":"stateful_txallo","finality_evidence":{"submitted_unique_tx_count":1}})
    assert result["method_correctness_oracle_status"] == "failed"
    assert result["txallo_stateful_replica_feed_complete"] is True
    assert result["txallo_stateful_replica_feed_blockers"] == []


def test_stateful_replica_oracle_rejects_physical_state_fetch_rpc(tmp_path: Path) -> None:
    (tmp_path / "client").mkdir()
    access={"index":0,"logical_id":"l1","tx_id":"tx1","execution_shard":"s0","access_list":[{"key":"asset","mode":"write","update_semantics":"set","delta":0}],"state_versions":[{"key":"asset","required_version":0,"produced_version":1}]}
    with gzip.open(tmp_path / "client" / "resolved_access_lists.jsonl.gz","wt",encoding="utf-8") as handle: handle.write(json.dumps(access)+"\n")
    _write_node(tmp_path,"n0","s0","source_local","value-1")
    _write_node(tmp_path,"n1","s1","replica_system","value-1")
    _write_network_summary(tmp_path, fetch_requests=1, fetch_responses=1)
    result=evaluate(tmp_path,{"method_config_id":"stateful_txallo","finality_evidence":{"submitted_unique_tx_count":1}})
    assert result["method_correctness_oracle_status"] == "failed"
    assert result["txallo_stateful_physical_zero_fetch_verified"] is False
    assert any("physical_state_fetch_request_nonzero" in item for item in result["method_correctness_oracle_blockers"])


def test_stateful_replica_oracle_rejects_generic_versioned_wave(tmp_path: Path) -> None:
    (tmp_path / "client").mkdir()
    access={"index":0,"logical_id":"l1","tx_id":"tx1","execution_shard":"s0","access_list":[{"key":"asset","mode":"write","update_semantics":"set","delta":0}],"state_versions":[{"key":"asset","required_version":0,"produced_version":1}]}
    with gzip.open(tmp_path / "client" / "resolved_access_lists.jsonl.gz","wt",encoding="utf-8") as handle: handle.write(json.dumps(access)+"\n")
    _write_node(tmp_path,"n0","s0","source_local","value-1")
    _write_node(tmp_path,"n1","s1","replica_system","value-1")
    _write_network_summary(tmp_path)
    p = tmp_path / "nodes" / "n0" / "block_execution_summary.json"
    payload=json.loads(p.read_text(encoding="utf-8"))
    payload["blocks"][0].update({"versioned_wave_execution_policy":"delta_only_v1","versioned_state_ready_wave_count":1})
    p.write_text(json.dumps(payload),encoding="utf-8")
    result=evaluate(tmp_path,{"method_config_id":"stateful_txallo","finality_evidence":{"submitted_unique_tx_count":1}})
    assert result["method_correctness_oracle_status"] == "failed"
    assert result["txallo_stateful_physical_generic_versioned_wave_count"] >= 1
    assert any("physical_generic_versioned_wave_nonzero" in item for item in result["method_correctness_oracle_blockers"])
