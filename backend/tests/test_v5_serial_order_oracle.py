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


def _rows() -> list[dict]:
    return [
        {
            "index": index,
            "logical_id": f"logical-{index}",
            "tx_id": f"tx-{index}",
            "access_list_schema": "mbe_logical_access_list_v1",
            "access_list_source": "controlled_workload",
            "access_list": [
                {"key": "account:hot", "mode": "read_write", "update_semantics": "rmw_hash_chain"}
            ],
        }
        for index in range(4)
    ]


def _write_run(root: Path, order: list[int], *, legacy_chain_placeholder: bool = False, initial_root: str = EMPTY_STATE_ROOT, replicas: int = 2) -> tuple[str, str]:
    client = root / "client"
    client.mkdir(parents=True)
    rows = _rows()
    with gzip.open(client / "resolved_access_lists.jsonl.gz", "wt", encoding="utf-8") as handle:
        for row in rows:
            handle.write(json.dumps(row, separators=(",", ":")) + "\n")

    state: dict[str, str] = {}
    for index in order:
        row = rows[index]
        key = "account:hot"
        qualified = "s0::account:hot"
        previous = state.get(qualified, "")
        state[qualified] = _stable_direct_access_value(
            logical_tx_id=row["logical_id"], key=key, semantics="rmw_hash_chain", previous=previous
        )
    business = _business_state_digest(state)
    global_business = _canonical_digest({"s0": business})

    blocks = [(1, "block-1", "genesis", order[:2]), (2, "block-2", "block-1", order[2:])]
    for replica in range(replicas):
        node = root / "nodes" / f"n{replica}"
        node.mkdir(parents=True)
        with (node / "committed_chain.csv").open("w", newline="", encoding="utf-8") as handle:
            fields = ["node_id", "shard_id", "height", "view", "block_hash", "parent_hash", "tx_count", "tx_root", "state_root_before", "state_root_after", "receipt_root", "committed_at", "finished_at"]
            writer = csv.DictWriter(handle, fieldnames=fields)
            writer.writeheader()
            for height, block_hash, parent, txs in blocks:
                writer.writerow({
                    "node_id": f"n{replica}", "shard_id": "s0", "height": height, "view": 0,
                    "block_hash": block_hash, "parent_hash": parent, "tx_count": len(txs), "tx_root": "t",
                    "state_root_before": "empty" if legacy_chain_placeholder and height == 1 else (initial_root if height == 1 else "after-block-1"),
                    "state_root_after": "after", "receipt_root": "r", "committed_at": 1, "finished_at": 1,
                })
        (node / "block_execution_summary.json").write_text(json.dumps({"blocks": [
            {"block_hash": "block-1", "height": 1, "state_root_before": initial_root},
            {"block_hash": "block-2", "height": 2, "state_root_before": "after-block-1"},
        ]}), encoding="utf-8")
        with (node / "transaction_execution_trace.csv").open("w", newline="", encoding="utf-8") as handle:
            fields = ["node_id", "shard_id", "block_hash", "height", "tx_id", "original_index", "success", "error"]
            writer = csv.DictWriter(handle, fieldnames=fields)
            writer.writeheader()
            # OriginalIndex deliberately resets for every block.  v1 wrongly
            # treated this as a workload-global identity and rejected the run.
            for height, block_hash, _parent, txs in blocks:
                for local_index, tx_index in enumerate(txs):
                    writer.writerow({
                        "node_id": f"n{replica}", "shard_id": "s0", "block_hash": block_hash,
                        "height": height, "tx_id": rows[tx_index]["tx_id"], "original_index": local_index,
                        "success": "true", "error": "",
                    })
        (node / "node_summary.json").write_text(
            json.dumps({"node_id": f"n{replica}", "shard_id": "s0", "business_state_digest": business}),
            encoding="utf-8",
        )
    return business, global_business


def test_serial_order_oracle_v2_uses_tx_id_across_multiple_blocks_with_block_local_original_indexes(tmp_path: Path) -> None:
    business, global_business = _write_run(tmp_path, [0, 1, 2, 3])
    result = evaluate(tmp_path, result_summary={"global_business_state_digest": global_business})
    assert result["serial_order_replay_equivalent"] is True
    assert result["serial_order_replay_identity_basis"] == "tx_id"
    assert result["serial_order_replay_original_index_semantics"] == "block_local_diagnostic_only"
    assert result["serial_order_replay_transaction_count"] == 4
    assert result["serial_order_replay_unique_transaction_count"] == 4
    assert result["serial_order_replay_committed_block_count"] == 2
    assert result["serial_order_replay_business_state_digest"] == business


def test_serial_order_oracle_ignores_uncommitted_trace_and_accepts_identical_committed_reexecution_chunks(tmp_path: Path) -> None:
    _business, global_business = _write_run(tmp_path, [0, 1, 2, 3], legacy_chain_placeholder=True)
    for node_id in ("n0", "n1"):
        trace = tmp_path / "nodes" / node_id / "transaction_execution_trace.csv"
        rows = list(csv.DictReader(trace.open(encoding="utf-8")))
        fields = list(rows[0])
        expanded: list[dict] = []
        for block_hash in ("block-1", "block-2"):
            chunk = [row for row in rows if row["block_hash"] == block_hash]
            expanded.extend(chunk)
            expanded.extend(dict(row) for row in chunk)
        ghost = dict(rows[0])
        ghost.update({"block_hash": "uncommitted-proposal", "height": "99", "tx_id": "ghost-tx"})
        expanded.append(ghost)
        with trace.open("w", newline="", encoding="utf-8") as handle:
            writer = csv.DictWriter(handle, fieldnames=fields)
            writer.writeheader(); writer.writerows(expanded)
    result = evaluate(tmp_path, result_summary={"global_business_state_digest": global_business})
    assert result["serial_order_replay_equivalent"] is True
    assert result["serial_order_replay_transaction_count"] == 4
    assert result["serial_order_replay_unique_transaction_count"] == 4
    assert result["serial_order_replay_trace_reexecution_count"] == 2


def test_serial_order_oracle_fails_closed_when_same_committed_block_reexecution_order_disagrees(tmp_path: Path) -> None:
    _business, global_business = _write_run(tmp_path, [0, 1, 2, 3])
    for node_id in ("n0", "n1"):
        trace = tmp_path / "nodes" / node_id / "transaction_execution_trace.csv"
        rows = list(csv.DictReader(trace.open(encoding="utf-8")))
        fields = list(rows[0])
        b1 = [row for row in rows if row["block_hash"] == "block-1"]
        b2 = [row for row in rows if row["block_hash"] == "block-2"]
        expanded = b1 + list(reversed([dict(row) for row in b1])) + b2
        with trace.open("w", newline="", encoding="utf-8") as handle:
            writer = csv.DictWriter(handle, fieldnames=fields)
            writer.writeheader(); writer.writerows(expanded)
    result = evaluate(tmp_path, result_summary={"global_business_state_digest": global_business})
    assert result["serial_order_replay_equivalent"] is False
    assert any("serial_oracle_reexecution_order_mismatch" in item for item in result["serial_order_replay_blockers"])


def test_serial_order_oracle_accepts_different_legal_committed_orders_without_cross_method_digest_equality(tmp_path: Path) -> None:
    left = tmp_path / "left"
    right = tmp_path / "right"
    left_business, left_global = _write_run(left, [0, 1, 2, 3])
    right_business, right_global = _write_run(right, [1, 0, 3, 2])
    assert left_business != right_business
    left_result = evaluate(left, result_summary={"global_business_state_digest": left_global})
    right_result = evaluate(right, result_summary={"global_business_state_digest": right_global})
    assert left_result["serial_order_replay_equivalent"] is True
    assert right_result["serial_order_replay_equivalent"] is True
    assert left_result["serial_order_replay_input_digest"] == right_result["serial_order_replay_input_digest"]
    assert left_result["serial_order_replay_commit_order_digest"] != right_result["serial_order_replay_commit_order_digest"]


def test_serial_order_oracle_recovers_legacy_empty_placeholder_from_execution_evidence(tmp_path: Path) -> None:
    _business, global_business = _write_run(tmp_path, [0, 1, 2, 3], legacy_chain_placeholder=True)
    result = evaluate(tmp_path, result_summary={"global_business_state_digest": global_business})
    assert result["serial_order_replay_equivalent"] is True
    assert result["serial_order_replay_initial_state_root"] == EMPTY_STATE_ROOT
    assert set(result["serial_order_replay_initial_state_sources"].values()) == {"block_execution_summary"}


def test_serial_order_oracle_still_fails_closed_for_real_nonempty_initial_state(tmp_path: Path) -> None:
    _business, global_business = _write_run(tmp_path, [0, 1, 2, 3], initial_root="not-empty-root")
    result = evaluate(tmp_path, result_summary={"global_business_state_digest": global_business})
    assert result["serial_order_replay_equivalent"] is False
    assert "serial_oracle_nonempty_initial_state_unsupported" in result["serial_order_replay_blockers"]


def test_serial_order_oracle_fails_when_replica_committed_tx_order_disagrees(tmp_path: Path) -> None:
    _business, global_business = _write_run(tmp_path, [0, 1, 2, 3])
    trace = tmp_path / "nodes" / "n1" / "transaction_execution_trace.csv"
    rows = list(csv.DictReader(trace.open(encoding="utf-8")))
    rows[0], rows[1] = rows[1], rows[0]
    with trace.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0]))
        writer.writeheader(); writer.writerows(rows)
    result = evaluate(tmp_path, result_summary={"global_business_state_digest": global_business})
    assert result["serial_order_replay_equivalent"] is False
    assert "serial_oracle_replica_commit_order_mismatch" in result["serial_order_replay_blockers"]

def test_serial_order_oracle_keeps_legacy_previous_hash_header_compatibility(tmp_path: Path) -> None:
    _business, global_business = _write_run(tmp_path, [0, 1, 2, 3])
    for node_id in ("n0", "n1"):
        chain = tmp_path / "nodes" / node_id / "committed_chain.csv"
        rows = list(csv.DictReader(chain.open(encoding="utf-8")))
        fields = ["previous_hash" if name == "parent_hash" else name for name in rows[0].keys()]
        with chain.open("w", newline="", encoding="utf-8") as handle:
            writer = csv.DictWriter(handle, fieldnames=fields)
            writer.writeheader()
            for row in rows:
                converted = dict(row)
                converted["previous_hash"] = converted.pop("parent_hash")
                writer.writerow(converted)
    result = evaluate(tmp_path, result_summary={"global_business_state_digest": global_business})
    assert result["serial_order_replay_equivalent"] is True
    assert result["serial_order_replay_committed_block_count"] == 2

def test_groundhog_uses_method_correctness_oracle_not_direct_state_serial_replay(tmp_path: Path) -> None:
    _business, global_business = _write_run(tmp_path, [0, 1, 2, 3])
    summary = {
        "block_executor_id": "groundhog_block_executor",
        "block_executor_consistent": True,
        "state_root_consistent": True,
        "receipt_root_consistent": True,
        "plan_digest_consistent": True,
        "no_fallback": True,
        "ready_to_commit": True,
        "executed_logical_transaction_count": 4,
        "global_business_state_digest": global_business,
        "finality_evidence": {
            "submitted_unique_tx_count": 4,
            "terminal_unique_tx_count": 4,
            "finalized_unique_logical_tx_count": 4,
            "incomplete_unique_tx_count": 0,
            "cross_shard_failed_unique_count": 0,
        },
    }
    result = evaluate(tmp_path, result_summary=summary)
    assert result["serial_order_oracle_status"] == "not_applicable"
    assert result["serial_order_replay_applicable"] is False
    assert result["serial_order_replay_equivalent"] is None
    assert result["serial_order_replay_blockers"] == []
    assert result["method_correctness_oracle_kind"] == "groundhog_replica_determinism_completion_v1"
    assert result["method_correctness_oracle_valid"] is True
    assert result["method_correctness_oracle_blockers"] == []
    assert result["serial_order_replay_input_digest"]
    assert result["serial_order_replay_transaction_count"] == 4


def test_groundhog_method_correctness_oracle_fails_closed_on_incomplete_finality(tmp_path: Path) -> None:
    _business, global_business = _write_run(tmp_path, [0, 1, 2, 3])
    summary = {
        "block_executor_id": "groundhog_block_executor",
        "block_executor_consistent": True,
        "state_root_consistent": True,
        "receipt_root_consistent": True,
        "plan_digest_consistent": True,
        "no_fallback": True,
        "ready_to_commit": True,
        "executed_logical_transaction_count": 3,
        "global_business_state_digest": global_business,
        "finality_evidence": {
            "submitted_unique_tx_count": 4,
            "terminal_unique_tx_count": 3,
            "finalized_unique_logical_tx_count": 3,
            "incomplete_unique_tx_count": 1,
            "cross_shard_failed_unique_count": 0,
        },
    }
    result = evaluate(tmp_path, result_summary=summary)
    assert result["serial_order_oracle_status"] == "not_applicable"
    assert result["method_correctness_oracle_valid"] is False
    assert "groundhog_oracle_terminal_not_equal_submitted" in result["method_correctness_oracle_blockers"]
    assert "groundhog_oracle_incomplete_not_zero" in result["method_correctness_oracle_blockers"]

