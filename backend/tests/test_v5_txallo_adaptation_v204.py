from __future__ import annotations

import csv
import gzip
import json
from pathlib import Path

from backend.app.services.v5_optme_txallo_method_specific_v18 import compute_writeback_fanout_v18
from backend.app.services.v5_serial_order_oracle import _business_state_digest, _canonical_digest, _stable_direct_access_value
from backend.app.services.v5_txallo_stateless_oracle import INITIAL_STATE_FILE, evaluate


def _csv(path: Path, fields: list[str], rows: list[dict[str, object]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def test_stateless_txallo_replay_uses_real_nonempty_initial_business_state(tmp_path: Path) -> None:
    client = tmp_path / "client"
    client.mkdir(parents=True)
    logical_id = "logical-1"
    tx_id = "tx-1"
    key = "contract:k"
    initial_value = "seed"
    final_value = _stable_direct_access_value(logical_tx_id=logical_id, key=key, semantics="set", previous=initial_value)
    with gzip.open(client / "resolved_access_lists.jsonl.gz", "wt", encoding="utf-8") as handle:
        handle.write(json.dumps({
            "index": 0,
            "logical_id": logical_id,
            "tx_id": tx_id,
            "execution_shard": "s0",
            "access_list": [{"key": key, "mode": "read_write", "update_semantics": "set"}],
            "state_versions": [{"key": key, "required_version": 0, "produced_version": 1}],
        }) + "\n")
    _csv(client / "placement_plan.csv", ["state_key", "home_shard"], [{"state_key": key, "home_shard": "s0"}])

    initial_state = {"s0::contract:k": initial_value}
    final_state = {"s0::contract:k": final_value}
    for node_id in ("n0", "n1"):
        node = tmp_path / "nodes" / node_id
        node.mkdir(parents=True)
        (node / "node_summary.json").write_text(json.dumps({
            "node_id": node_id,
            "shard_id": "s0",
            "business_state_digest": _business_state_digest(final_state),
        }), encoding="utf-8")
        (node / INITIAL_STATE_FILE).write_text(json.dumps({
            "schema_version": "mbe_txallo_stateless_initial_business_state_v204",
            "node_id": node_id,
            "shard_id": "s0",
            "business_state_digest": _business_state_digest(initial_state),
            "business_state": initial_state,
        }), encoding="utf-8")
        _csv(node / "committed_chain.csv",
             ["height", "shard_id", "block_hash", "parent_hash", "tx_count", "state_root_before"],
             [{"height": 1, "shard_id": "s0", "block_hash": "b1", "parent_hash": "genesis", "tx_count": 1, "state_root_before": "physical-root-is-allowed-to-be-nonempty"}])
        _csv(node / "transaction_execution_trace.csv",
             ["block_hash", "height", "tx_id", "success", "original_index"],
             [{"block_hash": "b1", "height": 1, "tx_id": tx_id, "success": "true", "original_index": 0}])

    global_digest = _canonical_digest({"s0": _business_state_digest(final_state)})
    result = evaluate(tmp_path, {"global_business_state_digest": global_digest})
    assert result["serial_order_oracle_status"] == "passed", result
    assert result["serial_order_replay_equivalent"] is True
    assert result["serial_order_replay_initial_state_empty"] is False
    assert result["serial_order_replay_initial_business_state_digests"]["s0"] == _business_state_digest(initial_state)


def test_exact_version_writeback_five_tuple_closes_from_txallo_node_evidence(tmp_path: Path) -> None:
    node = tmp_path / "nodes" / "n0"
    node.mkdir(parents=True)
    fields = [
        "access_kind", "tx_id", "state_key", "home_shard", "update_semantics",
        "logical_tx_ids", "previous_version", "produced_version", "ordering_noop",
        "apply_origin", "delta_kind",
    ]
    rows = [
        {
            "access_kind": "write_apply",
            "tx_id": "tx-1",
            "state_key": "contract:k",
            "home_shard": "s0",
            "update_semantics": "",
            "logical_tx_ids": "logical-1",
            "previous_version": "0",
            "produced_version": "1",
            "ordering_noop": "false",
            "apply_origin": "optme_txallo_versioned_remote_home_v10",
            "delta_kind": "",
        },
        {
            "access_kind": "write_apply",
            "tx_id": "tx-1",
            "state_key": "contract:k",
            "home_shard": "s0",
            "update_semantics": "",
            "logical_tx_ids": "logical-1",
            "previous_version": "0",
            "produced_version": "1",
            "ordering_noop": "false",
            "apply_origin": "optme_txallo_versioned_remote_home_v10",
            "delta_kind": "",
        },
    ]
    _csv(node / "remote_state_access.csv", fields, rows)
    got = compute_writeback_fanout_v18(tmp_path, {})
    assert got["status"] == "available_exact_logical_dedup", got
    assert got["physical_message_count"] == 2
    assert got["unique_logical_delta_count"] == 1
    assert got["replica_fanout_ratio"] == 2.0
    assert got["canonicalized_default_set_semantics_count"] == 2


def test_v204_runtime_source_exports_txallo_stateless_evidence() -> None:
    source = Path("executor/v5/runtime.go").read_text(encoding="utf-8")
    assert "MBE_TXALLO_ADAPTATION_V204" in source
    assert "txallo_stateless_initial_business_state_v204.json" in source
    assert "writeTxAlloStatelessEvidenceV204" in source
    assert 'metrics.WriteCSV(filepath.Join(r.node.DataDir, "remote_state_access.csv")' in source


def test_v2041_legacy_empty_root_fixture_compatibility_is_narrow(tmp_path: Path) -> None:
    client = tmp_path / "client"
    client.mkdir(parents=True)
    logical_id = "logical-empty"
    tx_id = "tx-empty"
    key = "contract:k"
    final_value = _stable_direct_access_value(logical_tx_id=logical_id, key=key, semantics="set", previous="")
    with gzip.open(client / "resolved_access_lists.jsonl.gz", "wt", encoding="utf-8") as handle:
        handle.write(json.dumps({
            "index": 0,
            "logical_id": logical_id,
            "tx_id": tx_id,
            "execution_shard": "s0",
            "access_list": [{"key": key, "mode": "read_write", "update_semantics": "set"}],
            "state_versions": [{"key": key, "required_version": 0, "produced_version": 1}],
        }) + "\n")
    _csv(client / "placement_plan.csv", ["state_key", "home_shard"], [{"state_key": key, "home_shard": "s0"}])
    final_state = {"s0::contract:k": final_value}
    node = tmp_path / "nodes" / "n0"
    node.mkdir(parents=True)
    (node / "node_summary.json").write_text(json.dumps({
        "node_id": "n0", "shard_id": "s0", "business_state_digest": _business_state_digest(final_state),
    }), encoding="utf-8")
    from backend.app.services.v5_serial_order_oracle import EMPTY_STATE_ROOT
    _csv(node / "committed_chain.csv", ["height", "shard_id", "block_hash", "parent_hash", "tx_count", "state_root_before"], [{
        "height": 1, "shard_id": "s0", "block_hash": "b1", "parent_hash": "genesis", "tx_count": 1, "state_root_before": EMPTY_STATE_ROOT,
    }])
    _csv(node / "transaction_execution_trace.csv", ["block_hash", "height", "tx_id", "success", "original_index"], [{
        "block_hash": "b1", "height": 1, "tx_id": tx_id, "success": "true", "original_index": 0,
    }])
    result = evaluate(tmp_path, {"global_business_state_digest": _canonical_digest({"s0": _business_state_digest(final_state)})})
    assert result["serial_order_replay_equivalent"] is True, result
    assert result["serial_order_replay_initial_state_sources"]["s0"] == "legacy_committed_chain_empty_root_compat_v2041"


def test_v2041_missing_initial_artifact_with_nonempty_root_stays_fail_closed(tmp_path: Path) -> None:
    client = tmp_path / "client"
    client.mkdir(parents=True)
    logical_id = "logical-nonempty"
    tx_id = "tx-nonempty"
    key = "contract:k"
    final_value = _stable_direct_access_value(logical_tx_id=logical_id, key=key, semantics="set", previous="")
    with gzip.open(client / "resolved_access_lists.jsonl.gz", "wt", encoding="utf-8") as handle:
        handle.write(json.dumps({
            "index": 0,
            "logical_id": logical_id,
            "tx_id": tx_id,
            "execution_shard": "s0",
            "access_list": [{"key": key, "mode": "read_write", "update_semantics": "set"}],
            "state_versions": [{"key": key, "required_version": 0, "produced_version": 1}],
        }) + "\n")
    _csv(client / "placement_plan.csv", ["state_key", "home_shard"], [{"state_key": key, "home_shard": "s0"}])
    node = tmp_path / "nodes" / "n0"
    node.mkdir(parents=True)
    (node / "node_summary.json").write_text(json.dumps({
        "node_id": "n0", "shard_id": "s0", "business_state_digest": _business_state_digest({"s0::contract:k": final_value}),
    }), encoding="utf-8")
    _csv(node / "committed_chain.csv", ["height", "shard_id", "block_hash", "parent_hash", "tx_count", "state_root_before"], [{
        "height": 1, "shard_id": "s0", "block_hash": "b1", "parent_hash": "genesis", "tx_count": 1, "state_root_before": "nonempty-physical-root",
    }])
    _csv(node / "transaction_execution_trace.csv", ["block_hash", "height", "tx_id", "success", "original_index"], [{
        "block_hash": "b1", "height": 1, "tx_id": tx_id, "success": "true", "original_index": 0,
    }])
    result = evaluate(tmp_path, {})
    assert result["serial_order_replay_equivalent"] is False
    assert any("initial_business_state" in blocker for blocker in result["serial_order_replay_blockers"]), result
