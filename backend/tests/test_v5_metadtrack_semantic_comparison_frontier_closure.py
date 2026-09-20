from __future__ import annotations

import csv
import gzip
import json
from pathlib import Path

from backend.app.services.v5_plugin_manifest_store import STORE
from backend.app.services.v5_real_cluster_runner import _canonical_digest, _initial_state_digest
from backend.app.services.v5_serial_order_oracle import (
    _load_access_entries,
    _multishard_correctness_blockers,
)
from backend.app.services.v5_metric_truth import normalize_remote_operation_kind
from backend.app.services.v5_paper_exporter import _individual_result_reasons
from backend.app.services.v5_formal_scheduler import _state_equivalence_individual_reasons


def _write_first_block(node_dir: Path, *, node_id: str, shard_id: str, block_hash: str, execution_root: str, persistent_root: str) -> None:
    node_dir.mkdir(parents=True, exist_ok=True)
    with (node_dir / "committed_chain.csv").open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=[
            "node_id", "shard_id", "height", "view", "block_hash", "parent_hash",
            "tx_count", "tx_digest", "state_root_before", "state_root_after",
            "receipt_root", "commit_started_at", "commit_finished_at",
        ])
        writer.writeheader()
        writer.writerow({
            "node_id": node_id,
            "shard_id": shard_id,
            "height": 1,
            "view": 0,
            "block_hash": block_hash,
            "parent_hash": "genesis",
            "tx_count": 1,
            "tx_digest": "tx",
            # Deliberately method/execution-view specific: fairness must NOT use this
            # when durable pre-WAL evidence is available.
            "state_root_before": execution_root,
            "state_root_after": "after",
            "receipt_root": "receipt",
            "commit_started_at": 1,
            "commit_finished_at": 2,
        })
    (node_dir / "block_execution_summary.json").write_text(
        json.dumps({
            "blocks": [{
                "block_hash": block_hash,
                "state_root_before": execution_root,
                "state_root_before_wal": persistent_root,
            }]
        }),
        encoding="utf-8",
    )


def test_initial_state_digest_prefers_persistent_pre_wal_boundary(tmp_path: Path) -> None:
    for node_id in ("n0", "n1"):
        _write_first_block(
            tmp_path / "nodes" / node_id,
            node_id=node_id,
            shard_id="s0",
            block_hash="s0-b1",
            execution_root="metatrack-execution-overlay",
            persistent_root="shared-persistent-s0",
        )
    for node_id in ("n2", "n3"):
        _write_first_block(
            tmp_path / "nodes" / node_id,
            node_id=node_id,
            shard_id="s1",
            block_hash="s1-b1",
            execution_root="different-execution-overlay",
            persistent_root="shared-persistent-s1",
        )
    expected = _canonical_digest({"s0": "shared-persistent-s0", "s1": "shared-persistent-s1"})
    assert _initial_state_digest(tmp_path) == expected


def _write_access(path: Path, tx_id: str) -> None:
    row = {
        "index": 0,
        "logical_id": "logical-0",
        "tx_id": tx_id,
        "access_list_schema": "mbe_access_v1",
        "access_list_source": "dataset",
        "access_list": [{"key": "asset:k", "mode": "read_write", "update_semantics": "set", "delta": 0}],
    }
    with gzip.open(path, "wt", encoding="utf-8") as handle:
        handle.write(json.dumps(row) + "\n")


def test_serial_input_digest_is_logical_workload_not_method_specific_tx_id(tmp_path: Path) -> None:
    left = tmp_path / "left.jsonl.gz"
    right = tmp_path / "right.jsonl.gz"
    _write_access(left, "signed-tx-id-metatrack")
    _write_access(right, "signed-tx-id-hash")
    left_by_id, left_digest, left_blockers = _load_access_entries(left)
    right_by_id, right_digest, right_blockers = _load_access_entries(right)
    assert left_blockers == [] and right_blockers == []
    assert set(left_by_id) != set(right_by_id)  # durable identities may differ by method
    assert left_digest == right_digest          # logical workload identity must not


def test_multishard_method_correctness_contract_accepts_complete_deterministic_run() -> None:
    summary = {
        "block_executor_consistent": True,
        "state_root_consistent": True,
        "receipt_root_consistent": True,
        "plan_digest_consistent": True,
        "no_fallback": True,
        "ready_to_commit": True,
        "executed_logical_transaction_count": 1000,
        "initial_state_digest": "initial",
        "state_home_mapping_digest": "home",
        "global_final_state_digest": "final",
        "finality_evidence": {
            "submitted_unique_tx_count": 1000,
            "terminal_unique_tx_count": 1000,
            "finalized_unique_logical_tx_count": 1000,
            "incomplete_unique_tx_count": 0,
            "cross_shard_failed_unique_count": 0,
        },
    }
    assert _multishard_correctness_blockers(summary) == []
    summary["finality_evidence"]["terminal_unique_tx_count"] = 999
    assert "multishard_oracle_terminal_not_equal_submitted" in _multishard_correctness_blockers(summary)


def test_dual_track_manifest_has_no_historical_access_size_gate() -> None:
    manifest = STORE.get("dual_track_execution")
    assert manifest.default_config == {}
    assert "access_size_threshold" not in manifest.config_schema.get("properties", {})
    assert "access_size_threshold" not in manifest.capabilities
    assert "access_stability_admission" in manifest.capabilities
    assert "semantic_stability_admission" in manifest.capabilities
    assert manifest.truth_boundary == "metatrack_dual_track_dependency_frontier_v2"


def test_version_admission_probe_is_a_remote_fetch_not_unknown() -> None:
    assert normalize_remote_operation_kind("state_version_admission_probe") == "fetch"
    assert normalize_remote_operation_kind("write") == "fetch"


def _paper_gate_child(remote_unknown: int) -> dict:
    return {
        "status": "completed",
        "execution_status": "completed",
        "metrics": {
            "submitted_unique_tx_count": 10,
            "terminal_unique_tx_count": 10,
            "finalized_unique_logical_tx_count": 10,
            "incomplete_unique_tx_count": 0,
            "cross_shard_failed_unique_count": 0,
            "lifecycle_complete": True,
            "no_fallback": True,
            "state_root_consistent": True,
            "receipt_root_consistent": True,
            "plan_digest_consistent": True,
            "metric_completeness": "complete",
            "end_to_end_tps": 1.0,
            "p95_finality_ms": 1.0,
            "p99_finality_ms": 1.0,
            "remote_operation_unknown_kind_count": remote_unknown,
            "replica_deduplicated_remote_unknown_kind_count": remote_unknown,
        },
        "result": {"summary": {}},
    }


def test_unknown_remote_operation_kind_blocks_paper_truth() -> None:
    bad = _paper_gate_child(1)
    assert "remote_operation_unknown_kind_nonzero" in _individual_result_reasons(bad)
    assert "remote_operation_unknown_kind_nonzero" in _state_equivalence_individual_reasons(bad)
    good = _paper_gate_child(0)
    assert "remote_operation_unknown_kind_nonzero" not in _individual_result_reasons(good)
    assert "remote_operation_unknown_kind_nonzero" not in _state_equivalence_individual_reasons(good)
