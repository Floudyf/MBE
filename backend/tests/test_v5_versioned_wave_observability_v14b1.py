from __future__ import annotations

import json

from backend.app.services.v5_metric_extractor import extract
from backend.app.services.v5_paper_exporter import _raw_row, _stable_raw_fields
from backend.app.services.v5_real_cluster_runner import _metatrack_control_plane_evidence


def _node(node_id: str, shard_id: str, delta_count: int, fallback_count: int = 0) -> dict:
    return {
        "node_id": node_id,
        "shard_id": shard_id,
        "versioned_wave_execution_policy": "delta_only_v1",
        "versioned_wave_delta_only_count": delta_count,
        "versioned_wave_full_fallback_count": fallback_count,
        "versioned_state_ready_wave_count": delta_count + fallback_count,
    }


def test_v14b1_control_plane_deduplicates_wave_observability_by_shard(tmp_path) -> None:
    summary = {
        "node_summaries": [
            _node("n0", "s0", 11),
            _node("n1", "s0", 11),
            _node("n2", "s1", 13),
            _node("n3", "s1", 13),
        ]
    }
    evidence = _metatrack_control_plane_evidence(tmp_path, summary)
    assert evidence["versioned_wave_execution_policy"] == "delta_only_v1"
    assert evidence["versioned_wave_delta_only_count"] == 24
    assert evidence["versioned_wave_full_fallback_count"] == 0
    assert evidence["versioned_state_ready_wave_count"] == 24


def test_v14b1_metric_extractor_and_paper_row_keep_wave_observability(tmp_path) -> None:
    cluster = {
        "block_executor_id": "serial_block_executor",
        "block_executor_consistent": True,
        "plan_digest_consistent": True,
        "state_root_consistent": True,
        "receipt_root_consistent": True,
        "orphan_process_count": 0,
        "versioned_wave_execution_policy": "delta_only_v1",
        "versioned_wave_delta_only_count": 21,
        "versioned_wave_full_fallback_count": 0,
    }
    finality = {
        "finality_semantics_version": "v1",
        "submitted_unique_tx_count": 1,
        "terminal_unique_tx_count": 1,
        "finalized_unique_logical_tx_count": 1,
        "throughput_tps": 1.0,
        "logical_finality_tps": 1.0,
        "end_to_end_tps": 1.0,
        "p50_finality_ms": 1,
        "p95_finality_ms": 1,
        "p99_finality_ms": 1,
    }
    (tmp_path / "real_cluster_summary.json").write_text(json.dumps(cluster), encoding="utf-8")
    (tmp_path / "finality_summary.json").write_text(json.dumps(finality), encoding="utf-8")

    metrics = extract(tmp_path)
    assert metrics["versioned_wave_execution_policy"] == "delta_only_v1"
    assert metrics["versioned_wave_delta_only_count"] == 21
    assert metrics["versioned_wave_full_fallback_count"] == 0

    child = {
        "child_run_id": "v14b1",
        "status": "completed",
        "metrics": metrics,
        "method": {"display_name": "Stateless Hash + Serial"},
    }
    row = _raw_row(child)
    fields = _stable_raw_fields([row])
    assert row["versioned_wave_execution_policy"] == "delta_only_v1"
    assert row["versioned_wave_delta_only_count"] == 21
    assert row["versioned_wave_full_fallback_count"] == 0
    assert "versioned_wave_execution_policy" in fields
    assert "versioned_wave_delta_only_count" in fields
    assert "versioned_wave_full_fallback_count" in fields
