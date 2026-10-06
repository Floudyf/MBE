from __future__ import annotations

import gzip
import json
from pathlib import Path

from backend.app.services.v5_optme_correctness_oracle_v23 import (
    _business_state_digest,
    _canonical_digest,
    _stable_direct_access_value,
    evaluate_optme,
)


def _write_access(root: Path, tx_id: str, logical_id: str, key: str, mode: str = "write") -> None:
    client = root / "client"
    client.mkdir(parents=True, exist_ok=True)
    with gzip.open(client / "resolved_access_lists.jsonl.gz", "wt", encoding="utf-8") as handle:
        handle.write(json.dumps({
            "tx_id": tx_id,
            "logical_id": logical_id,
            "access_list": [{"key": key, "mode": mode, "update_semantics": "set", "delta": 0}],
        }) + "\n")


def _write_block(root: Path, node: str, executor: str, tx_id: str, *, main_sequence: int = 1, epoch: int = 0) -> None:
    d = root / "nodes" / node
    d.mkdir(parents=True, exist_ok=True)
    payload = {
        "block_executor_id": executor,
        "blocks": [{
            "height": 1,
            "block_hash": "b1",
            "optme_transaction_evidence": [{
                "tx_id": tx_id,
                "logical_tx_id": "l1",
                "original_index": 0,
                "simulation_success": True,
                "early_detected": epoch > 0,
                "hierarchical_aborted": False,
                "reordered": False,
                "main_sequence": main_sequence,
                "reschedule_epoch": epoch,
                "reexecuted": epoch > 0,
                "reexecution_success": epoch > 0,
                "second_pass_invalidated": False,
                "terminal_success": True,
            }],
        }],
    }
    (d / "block_execution_summary.json").write_text(json.dumps(payload), encoding="utf-8")


def test_stateful_optme_v23_reuses_existing_transaction_evidence(tmp_path: Path) -> None:
    _write_access(tmp_path, "t1", "l1", "asset")
    for node in ("n0", "n1"):
        _write_block(tmp_path, node, "optme_block_executor", "t1")
    value = _stable_direct_access_value(logical_tx_id="l1", key="asset", semantics="set", previous="")
    business = _business_state_digest({"optme-global::asset": value})
    summary = {"block_executor_id": "optme_block_executor", "global_business_state_digest": _canonical_digest({"optme-global": business})}
    got = evaluate_optme(tmp_path, summary)
    assert got["method_correctness_oracle_status"] == "passed"
    assert got["optme_v23_apply_order_source"] == "existing_optme_transaction_evidence"


def test_stateless_optme_v23_uses_partition_business_truth(tmp_path: Path) -> None:
    _write_access(tmp_path, "t1", "l1", "asset")
    for node in ("n0", "n1"):
        _write_block(tmp_path, node, "stateless_optme_block_executor", "t1", main_sequence=0, epoch=1)
    client = tmp_path / "client"
    (client / "placement_plan.csv").write_text("state_key,home_shard\nasset,s1\n", encoding="utf-8")
    (tmp_path / "compiled_run_plan.json").write_text(json.dumps({"node_configs": [
        {"node_id": "n0", "execution_shard_id": "s0"},
        {"node_id": "n1", "execution_shard_id": "s1"},
    ]}), encoding="utf-8")
    value = _stable_direct_access_value(logical_tx_id="l1", key="asset", semantics="set", previous="")
    s0 = _business_state_digest({})
    s1 = _business_state_digest({"s1::asset": value})
    summary = {"block_executor_id": "stateless_optme_block_executor", "global_business_state_digest": _canonical_digest({"s0": s0, "s1": s1})}
    got = evaluate_optme(tmp_path, summary)
    assert got["method_correctness_oracle_status"] == "passed"


def test_optme_v23_replica_evidence_mismatch_fails(tmp_path: Path) -> None:
    _write_access(tmp_path, "t1", "l1", "asset")
    _write_block(tmp_path, "n0", "optme_block_executor", "t1")
    _write_block(tmp_path, "n1", "optme_block_executor", "t1", main_sequence=0, epoch=1)
    got = evaluate_optme(tmp_path, {"block_executor_id": "optme_block_executor", "global_business_state_digest": "x"})
    assert got["method_correctness_oracle_status"] == "failed"
    assert any("replica_apply_order_mismatch" in x for x in got["method_correctness_oracle_blockers"])
