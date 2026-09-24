from __future__ import annotations

import csv
import gzip
import json
from pathlib import Path

from backend.app.services.v5_formal_plan_validator import ALL_BUILTIN_METHODS
from backend.app.services.v5_plugin_manifest_store import STORE
from backend.app.services.v5_serial_order_oracle import (
    EMPTY_STATE_ROOT,
    _business_state_digest,
    _calvin_state_home,
    _canonical_digest,
    _stable_direct_access_value,
    evaluate,
)


def test_v33_stateless_calvin_registry_uses_remote_home_plugins() -> None:
    method = ALL_BUILTIN_METHODS["stateless_calvin"]
    assert method.plugin_overrides["routing"] == "stateless_calvin_global_routing"
    assert method.plugin_overrides["block_executor"] == "stateless_calvin_block_executor"
    assert method.plugin_overrides["state_access"] == "stateless_calvin_state_access"
    state_access = STORE.get("stateless_calvin_state_access")
    executor = STORE.get("stateless_calvin_block_executor")
    assert {"remote_state_fetch", "remote_state_writeback", "exact_state_version_fetch"} <= set(state_access.capabilities)
    assert {"physical_remote_state_fetch", "physical_remote_state_writeback", "no_metatrack_state_ready"} <= set(executor.capabilities)


def test_v33_stateful_calvin_remains_partition_resident() -> None:
    method = ALL_BUILTIN_METHODS["stateful_calvin"]
    assert method.plugin_overrides["routing"] == "calvin_global_routing"
    assert method.plugin_overrides["state_access"] == "calvin_partition_state_access"
    assert "remote_state_fetch" not in set(STORE.get("calvin_partition_state_access").capabilities)


def test_calvin_state_home_matches_go_stable_key_and_prefix_contract() -> None:
    shards = ["s0", "s1", "s2", "s3"]
    for key in ("asset:7", "balance:alice", "nonce:bob", "land:42"):
        expected = shards[sum(ord(ch) for ch in key) % len(shards)]
        assert _calvin_state_home(key, shards) == expected
    assert _calvin_state_home("s2::asset:7", shards) == "s2"


def _write_csv(path: Path, fieldnames: list[str], rows: list[dict[str, object]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def test_calvin_partitioned_oracle_replays_global_order_to_state_homes(tmp_path: Path) -> None:
    run_dir = tmp_path / "run"
    (run_dir / "client").mkdir(parents=True)
    plan = {
        "node_configs": [
            {"node_id": "n0", "shard_id": "calvin-global", "execution_shard_id": "s0"},
            {"node_id": "n1", "shard_id": "calvin-global", "execution_shard_id": "s1"},
        ]
    }
    (run_dir / "compiled_run_plan.json").write_text(json.dumps(plan), encoding="utf-8")
    access = {"index": 0, "logical_id": "logical-1", "tx_id": "tx-1", "access_list_schema": "test", "access_list_source": "test", "access_list": [{"key": "asset", "mode": "write", "update_semantics": "set", "delta": 0}]}
    with gzip.open(run_dir / "client" / "resolved_access_lists.jsonl.gz", "wt", encoding="utf-8") as handle:
        handle.write(json.dumps(access) + "\n")

    home = _calvin_state_home("asset", ["s0", "s1"])
    value = _stable_direct_access_value(logical_tx_id="logical-1", key="asset", semantics="set", previous="")
    partition_states = {"s0": {}, "s1": {}}
    partition_states[home][f"{home}::asset"] = value
    partition_digests = {shard: _business_state_digest(state) for shard, state in partition_states.items()}

    for node, shard in (("n0", "s0"), ("n1", "s1")):
        node_dir = run_dir / "nodes" / node
        node_dir.mkdir(parents=True)
        (node_dir / "node_summary.json").write_text(json.dumps({"node_id": node, "shard_id": "calvin-global", "business_state_digest": partition_digests[shard]}), encoding="utf-8")
        _write_csv(node_dir / "committed_chain.csv", ["node_id", "shard_id", "height", "view", "block_hash", "parent_hash", "tx_count", "tx_root", "state_root_before", "state_root_after", "receipt_root"], [{"node_id": node, "shard_id": "calvin-global", "height": 1, "view": 0, "block_hash": "b1", "parent_hash": "genesis", "tx_count": 1, "tx_root": "t", "state_root_before": EMPTY_STATE_ROOT, "state_root_after": "after", "receipt_root": "r"}])
        _write_csv(node_dir / "transaction_execution_trace.csv", ["block_hash", "height", "tx_id", "success", "original_index"], [{"block_hash": "b1", "height": 1, "tx_id": "tx-1", "success": "true", "original_index": 0}])

    global_digest = _canonical_digest(dict(sorted(partition_digests.items())))
    result = evaluate(run_dir, result_summary={"block_executor_id": "calvin_block_executor", "global_business_state_digest": global_digest})
    assert result["method_correctness_oracle_kind"] == "calvin_partitioned_global_order_serial_replay_v1"
    assert result["method_correctness_oracle_valid"] is True, result["method_correctness_oracle_blockers"]
    assert result["serial_order_replay_equivalent"] is True
