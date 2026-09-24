from __future__ import annotations

import json
from pathlib import Path

from backend.app.services.v5_metric_extractor import _apply_porygon_metrics


def _write(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload), encoding="utf-8")


def _block(height: int, block_hash: str, critical_us: int, business_us: int, exchange_us: int) -> dict:
    return {
        "height": height,
        "block_hash": block_hash,
        "porygon_execution_shard_count": 2,
        "porygon_execution_shard_histogram": {"esc_0": 1, "esc_1": 1},
        "porygon_local_business_execution_count": 1,
        "porygon_business_execution_us": business_us,
        "porygon_result_exchange_wait_us": exchange_us,
        "porygon_execution_critical_path_us": critical_us,
        "transaction_execution_us": business_us,
        "porygon_logical_state_cross_shard_transaction_count": 2,
        "porygon_witness_threshold_configured": 1,
        "porygon_witness_threshold_enforced": False,
        "porygon_witness_validation_mode": "full_validator_recompute_before_pbft_vote",
    }


def test_v26_porygon_critical_path_is_sum_of_per_block_replica_maxima(tmp_path: Path) -> None:
    nodes = [
        {"node_id": "n0", "shard_id": "porygon-global", "execution_shard_id": "s0", "consensus_domain_id": "porygon-global", "leader": True},
        {"node_id": "n1", "shard_id": "porygon-global", "execution_shard_id": "s1", "consensus_domain_id": "porygon-global", "leader": False},
    ]
    _write(tmp_path / "compiled_run_plan.json", {"node_configs": nodes})
    _write(tmp_path / "nodes/n0/block_execution_summary.json", {
        "node_id": "n0", "shard_id": "porygon-global", "block_executor_id": "porygon_block_executor",
        "blocks": [_block(1, "b1", 100_000, 20_000, 70_000), _block(2, "b2", 10_000, 4_000, 5_000)],
    })
    _write(tmp_path / "nodes/n1/block_execution_summary.json", {
        "node_id": "n1", "shard_id": "porygon-global", "block_executor_id": "porygon_block_executor",
        "blocks": [_block(1, "b1", 90_000, 80_000, 5_000), _block(2, "b2", 200_000, 80_000, 100_000)],
    })
    metrics: dict = {"source_artifacts": [], "transaction_execution_ms": 14.0}
    _apply_porygon_metrics(metrics, tmp_path)
    assert metrics["porygon_execution_critical_path_ms"] == 300.0
    # Independent per-phase maxima remain useful diagnostics but are not additive:
    # block 1's business maximum is n1, while total critical path is defined by n0.
    assert metrics["porygon_business_execution_critical_path_ms"] == 160.0
    assert metrics["porygon_result_exchange_wait_critical_path_ms"] == 170.0
    assert metrics["porygon_execution_critical_path_business_component_ms"] == 100.0
    assert metrics["porygon_execution_critical_path_exchange_component_ms"] == 170.0
    assert metrics["porygon_execution_critical_path_other_component_ms"] == 30.0
    assert metrics["porygon_leader_local_transaction_execution_ms"] == 14.0
    assert metrics["transaction_execution_ms"] == 300.0
    assert metrics["porygon_logical_state_cross_shard_ratio"] == 1.0
    assert metrics["porygon_witness_threshold_enforced"] is False


def test_v26_porygon_reproduction_docs_use_unified_frontend_shard_semantics() -> None:
    root = Path(__file__).resolve().parents[2]
    deviations = (root / "docs/reproductions/porygon/deviations.md").read_text(encoding="utf-8")
    acceptance = (root / "docs/reproductions/porygon/acceptance_matrix.md").read_text(encoding="utf-8")
    assert "topology.shards == 1" not in deviations
    assert "topology.shards" in deviations and "ESC" in deviations
    assert "maps exactly to Porygon ESC count" in acceptance
    assert "Ed25519" in acceptance


def test_v28_frontend_uses_aligned_porygon_breakdown_and_boolean_truth() -> None:
    root = Path(__file__).resolve().parents[2]
    frontend = (root / "frontend/src/components/v5/V5MechanismAnalysis.tsx").read_text(encoding="utf-8")
    assert "porygon_execution_critical_path_business_component_ms" in frontend
    assert "porygon_execution_critical_path_exchange_component_ms" in frontend
    assert "porygon_execution_critical_path_other_component_ms" in frontend
    assert 'typeof value === "boolean"' in frontend
    assert "booleans.every(Boolean)" in frontend


def test_v26_witness_truth_boundary_does_not_claim_independent_quorum() -> None:
    root = Path(__file__).resolve().parents[2]
    manifest = (root / "backend/app/services/v5_plugin_manifest_store.py").read_text(encoding="utf-8")
    assert "witness_threshold_is_metadata_not_independent_quorum" in manifest
    assert "full validator recomputation" in manifest
