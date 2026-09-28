from __future__ import annotations

import csv
import hashlib
import json
from pathlib import Path

from backend.app.services.v5_serial_order_oracle import evaluate
from backend.app.services.v5_stateful_serializability_oracle import EMPTY_STATE_ROOT


def _canonical(value: object) -> str:
    raw = json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def _digest(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def _business(state: dict[str, str]) -> str:
    rows = []
    for key in sorted(state):
        logical = key.split("::", 1)[1] if "::" in key else key
        if logical.startswith("relay_commit:") or logical.startswith("protocol:"):
            continue
        rows.append(f"{key}={state[key]}")
    return hashlib.sha256("\n".join(rows).encode()).hexdigest()


def _write_csv(path: Path, fields: list[str], rows: list[dict[str, object]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)


def _write_node(
    root: Path,
    node: str,
    shard: str,
    block_hash: str,
    txs: list[dict[str, object]],
    observed: list[dict[str, object]],
    final_state: dict[str, str],
) -> None:
    node_dir = root / "nodes" / node
    node_dir.mkdir(parents=True, exist_ok=True)
    _write_csv(
        node_dir / "committed_chain.csv",
        ["node_id", "shard_id", "height", "block_hash", "parent_hash", "tx_count", "state_root_before"],
        [{
            "node_id": node,
            "shard_id": shard,
            "height": 1,
            "block_hash": block_hash,
            "parent_hash": "genesis",
            "tx_count": len(txs),
            "state_root_before": EMPTY_STATE_ROOT,
        }],
    )
    _write_csv(
        node_dir / "transaction_execution_trace.csv",
        ["node_id", "shard_id", "block_hash", "height", "tx_id", "original_index", "success", "error"],
        [
            {
                "node_id": node,
                "shard_id": shard,
                "block_hash": block_hash,
                "height": 1,
                "tx_id": tx["tx_id"],
                "original_index": index,
                "success": tx.get("success", True),
                "error": tx.get("error", ""),
            }
            for index, tx in enumerate(txs)
        ],
    )
    _write_csv(
        node_dir / "observed_state_access.csv",
        ["node_id", "shard_id", "block_hash", "height", "tx_id", "original_index", "access_type", "state_key", "value_digest", "source"],
        [
            {
                "node_id": node,
                "shard_id": shard,
                "block_hash": block_hash,
                "height": 1,
                **row,
            }
            for row in observed
        ],
    )
    wal = {
        "version": "state_delta_wal_v1",
        "namespace": shard,
        "block_height": 1,
        "block_hash": block_hash,
        "parent_hash": "genesis",
        "delta_id": f"{node}-d1",
        "state_updates": [
            {"key": key, "value": value}
            for key, value in sorted(final_state.items())
        ],
        "state_root_before": EMPTY_STATE_ROOT,
        "state_root_after": "test-root",
        "checksum": "not-used-by-postrun-truth-oracle",
    }
    (node_dir / "state_delta.1.wal").write_text(json.dumps(wal) + "\n", encoding="utf-8")
    (node_dir / "node_summary.json").write_text(
        json.dumps({"node_id": node, "shard_id": shard, "business_state_digest": _business(final_state)}),
        encoding="utf-8",
    )


def _summary(global_business: str, executor: str = "block_stm_block_executor") -> dict:
    return {
        "block_executor_id": executor,
        "comparison_semantics_class": "stateful_local_legacy_v1",
        "state_home_mapping_policy": "execution_shard_local_namespace",
        "remote_fetch_policy": "none",
        "remote_writeback_policy": "none",
        "block_executor_consistent": True,
        "state_root_consistent": True,
        "receipt_root_consistent": True,
        "plan_digest_consistent": True,
        "no_fallback": True,
        "ready_to_commit": True,
        "initial_state_digest": "initial-evidence",
        "global_final_state_digest": "final-evidence",
        "global_business_state_digest": global_business,
        "executed_logical_transaction_count": 2,
        "executed_logical_tx_global_dedup_available": True,
        "finality_evidence": {
            "submitted_unique_tx_count": 2,
            "terminal_unique_tx_count": 2,
            "finalized_unique_logical_tx_count": 2,
            "incomplete_unique_tx_count": 0,
            "cross_shard_failed_unique_count": 0,
        },
    }


def test_v38_stateful_blockstm_multishard_uses_existing_effect_evidence_without_statehome(tmp_path: Path) -> None:
    s0 = {"s0::k": "v1"}
    s1 = {"s1::x": "v2", "s1::relay_commit:tx-cross": "1"}
    s0_observed = [
        {"tx_id":"tx-0","original_index":0,"access_type":"read","state_key":"k","value_digest":_digest(""),"source":"stm_mvmemory_base"},
        {"tx_id":"tx-0","original_index":0,"access_type":"write","state_key":"k","value_digest":_digest("v1"),"source":"write_set"},
    ]
    s1_observed = [
        {"tx_id":"tx-1","original_index":0,"access_type":"write","state_key":"x","value_digest":_digest("v2"),"source":"write_set"},
        {"tx_id":"tx-1","original_index":0,"access_type":"write","state_key":"relay_commit:tx-cross","value_digest":_digest("1"),"source":"write_set"},
    ]
    for node in ("n0", "n1"):
        _write_node(tmp_path, node, "s0", "b0", [{"tx_id":"tx-0"}], s0_observed, s0)
    for node in ("n4", "n5"):
        _write_node(tmp_path, node, "s1", "b1", [{"tx_id":"tx-1"}], s1_observed, s1)
    global_business = _canonical({"s0": _business(s0), "s1": _business(s1)})
    result = evaluate(tmp_path, result_summary=_summary(global_business))
    assert result["method_correctness_oracle_kind"] == "stateful_local_legacy_partition_serializability_v2"
    assert result["method_correctness_oracle_valid"] is True, result["method_correctness_oracle_blockers"]
    assert result["serial_order_replay_equivalent"] is True
    assert result["state_home_mapping_required"] is False
    assert result["stateful_serializability_validated_external_read_count"] == 1
    assert "observed_state_access.csv" in result["stateful_serializability_evidence_sources"]


def test_v38_stateful_blockstm_fails_closed_on_future_value_read(tmp_path: Path) -> None:
    s0 = {"s0::k": "v1"}
    s1 = {"s1::x": "v2"}
    bad = [
        {"tx_id":"tx-0","original_index":0,"access_type":"read","state_key":"k","value_digest":_digest("future"),"source":"stm_mvmemory_tx_9_inc_0"},
        {"tx_id":"tx-0","original_index":0,"access_type":"write","state_key":"k","value_digest":_digest("v1"),"source":"write_set"},
    ]
    good = [{"tx_id":"tx-1","original_index":0,"access_type":"write","state_key":"x","value_digest":_digest("v2"),"source":"write_set"}]
    for node in ("n0", "n1"):
        _write_node(tmp_path, node, "s0", "b0", [{"tx_id":"tx-0"}], bad, s0)
    for node in ("n4", "n5"):
        _write_node(tmp_path, node, "s1", "b1", [{"tx_id":"tx-1"}], good, s1)
    global_business = _canonical({"s0": _business(s0), "s1": _business(s1)})
    result = evaluate(tmp_path, result_summary=_summary(global_business))
    assert result["method_correctness_oracle_valid"] is False
    assert any("read_digest_mismatch" in item for item in result["method_correctness_oracle_blockers"])


def test_v38_stateful_serial_does_not_misclassify_same_tx_local_account_reads(tmp_path: Path) -> None:
    # SerialExecutor labels base reads and same-transaction overlay reads with the
    # same source. Do not invent a false inter-transaction dependency from those
    # internal reads; the serial method is instead checked by order + final effects.
    s0 = {"s0::balance:alice": "999999", "s0::nonce:alice": "1"}
    s1 = {"s1::x": "v2"}
    serial_access = [
        {"tx_id":"tx-0","original_index":0,"access_type":"read","state_key":"balance:alice","value_digest":_digest(""),"source":"state_snapshot_overlay"},
        {"tx_id":"tx-0","original_index":0,"access_type":"read","state_key":"balance:alice","value_digest":_digest("1000000"),"source":"state_snapshot_overlay"},
        {"tx_id":"tx-0","original_index":0,"access_type":"write","state_key":"balance:alice","value_digest":_digest("999999"),"source":"write_set"},
        {"tx_id":"tx-0","original_index":0,"access_type":"write","state_key":"nonce:alice","value_digest":_digest("1"),"source":"write_set"},
    ]
    good = [{"tx_id":"tx-1","original_index":0,"access_type":"write","state_key":"x","value_digest":_digest("v2"),"source":"write_set"}]
    for node in ("n0", "n1"):
        _write_node(tmp_path, node, "s0", "b0", [{"tx_id":"tx-0"}], serial_access, s0)
    for node in ("n4", "n5"):
        _write_node(tmp_path, node, "s1", "b1", [{"tx_id":"tx-1"}], good, s1)
    global_business = _canonical({"s0": _business(s0), "s1": _business(s1)})
    result = evaluate(tmp_path, result_summary=_summary(global_business, executor="serial_block_executor"))
    assert result["method_correctness_oracle_valid"] is True, result["method_correctness_oracle_blockers"]
    assert result["stateful_serializability_validated_external_read_count"] == 0


def test_v38_stateful_fails_closed_when_observed_final_write_disagrees_with_persistence(tmp_path: Path) -> None:
    s0 = {"s0::k": "persisted"}
    s1 = {"s1::x": "v2"}
    observed = [{"tx_id":"tx-0","original_index":0,"access_type":"write","state_key":"k","value_digest":_digest("different"),"source":"write_set"}]
    good = [{"tx_id":"tx-1","original_index":0,"access_type":"write","state_key":"x","value_digest":_digest("v2"),"source":"write_set"}]
    for node in ("n0", "n1"):
        _write_node(tmp_path, node, "s0", "b0", [{"tx_id":"tx-0"}], observed, s0)
    for node in ("n4", "n5"):
        _write_node(tmp_path, node, "s1", "b1", [{"tx_id":"tx-1"}], good, s1)
    global_business = _canonical({"s0": _business(s0), "s1": _business(s1)})
    result = evaluate(tmp_path, result_summary=_summary(global_business))
    assert result["method_correctness_oracle_valid"] is False
    assert any("partition_effect_digest_mismatch:s0" in item for item in result["method_correctness_oracle_blockers"])
