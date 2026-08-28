from __future__ import annotations

import csv
import gzip
import json
from pathlib import Path

from backend.app.services.v5_serial_order_oracle import (
    EMPTY_STATE_ROOT,
    _business_state_digest,
    _canonical_digest,
    _stable_direct_access_value,
    evaluate,
)


def _write_run(root: Path, order: list[int]) -> tuple[str, str]:
    client = root / "client"
    node = root / "nodes" / "n0"
    client.mkdir(parents=True)
    node.mkdir(parents=True)

    rows = [
        {
            "index": 0,
            "logical_id": "logical-A",
            "tx_id": "tx-A",
            "access_list_schema": "mbe_logical_access_list_v1",
            "access_list_source": "controlled_workload",
            "access_list": [
                {"key": "account:hot", "mode": "read_write", "update_semantics": "rmw_hash_chain"}
            ],
        },
        {
            "index": 1,
            "logical_id": "logical-B",
            "tx_id": "tx-B",
            "access_list_schema": "mbe_logical_access_list_v1",
            "access_list_source": "controlled_workload",
            "access_list": [
                {"key": "account:hot", "mode": "read_write", "update_semantics": "rmw_hash_chain"}
            ],
        },
    ]
    with gzip.open(client / "resolved_access_lists.jsonl.gz", "wt", encoding="utf-8") as handle:
        for row in rows:
            handle.write(json.dumps(row, separators=(",", ":")) + "\n")

    with (node / "committed_chain.csv").open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=["shard_id", "state_root_before"])
        writer.writeheader()
        writer.writerow({"shard_id": "s0", "state_root_before": EMPTY_STATE_ROOT})

    with (node / "transaction_execution_trace.csv").open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=["tx_id", "original_index", "success"])
        writer.writeheader()
        for index in order:
            writer.writerow({"tx_id": rows[index]["tx_id"], "original_index": index, "success": "true"})

    state: dict[str, str] = {}
    for index in order:
        row = rows[index]
        key = "account:hot"
        qualified = "s0::account:hot"
        previous = state.get(qualified, "")
        state[qualified] = _stable_direct_access_value(
            logical_tx_id=row["logical_id"],
            key=key,
            semantics="rmw_hash_chain",
            previous=previous,
        )
    business = _business_state_digest(state)
    global_business = _canonical_digest({"s0": business})
    (node / "node_summary.json").write_text(
        json.dumps({"node_id": "n0", "shard_id": "s0", "business_state_digest": business}),
        encoding="utf-8",
    )
    return business, global_business


def test_serial_order_oracle_accepts_each_observed_legal_order_without_cross_method_digest_equality(tmp_path: Path) -> None:
    left = tmp_path / "left"
    right = tmp_path / "right"
    left_business, left_global = _write_run(left, [0, 1])
    right_business, right_global = _write_run(right, [1, 0])
    assert left_business != right_business

    left_result = evaluate(left, result_summary={"global_business_state_digest": left_global})
    right_result = evaluate(right, result_summary={"global_business_state_digest": right_global})

    assert left_result["serial_order_replay_equivalent"] is True
    assert right_result["serial_order_replay_equivalent"] is True
    assert left_result["serial_order_replay_business_state_digest"] == left_business
    assert right_result["serial_order_replay_business_state_digest"] == right_business
    assert left_result["serial_order_replay_input_digest"] == right_result["serial_order_replay_input_digest"]
    assert left_result["serial_order_replay_commit_order_digest"] != right_result["serial_order_replay_commit_order_digest"]


def test_serial_order_oracle_fails_closed_for_nonempty_initial_state(tmp_path: Path) -> None:
    business, global_business = _write_run(tmp_path, [0, 1])
    chain = tmp_path / "nodes" / "n0" / "committed_chain.csv"
    with chain.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=["shard_id", "state_root_before"])
        writer.writeheader()
        writer.writerow({"shard_id": "s0", "state_root_before": "not-empty"})

    result = evaluate(tmp_path, result_summary={"global_business_state_digest": global_business})
    assert result["serial_order_replay_equivalent"] is False
    assert "serial_oracle_nonempty_initial_state_unsupported" in result["serial_order_replay_blockers"]
