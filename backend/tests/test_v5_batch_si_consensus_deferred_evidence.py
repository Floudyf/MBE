from __future__ import annotations

import json
from pathlib import Path

from backend.app.services.v5_metric_extractor import _apply_batch_si_metrics


def test_batch_si_unique_deferred_metrics_use_consensus_bound_block_evidence(tmp_path: Path) -> None:
    (tmp_path / "nodes" / "n0").mkdir(parents=True)
    (tmp_path / "compiled_run_plan.json").write_text(json.dumps({
        "node_configs": [{"node_id": "n0", "shard_id": "s0", "leader": True}],
    }), encoding="utf-8")
    (tmp_path / "nodes" / "n0" / "block_execution_summary.json").write_text(json.dumps({
        "block_executor_id": "batch_si_block_executor",
        "shard_id": "s0",
        "blocks": [
            {
                "batch_si_first_pass_candidate_count": 3,
                "batch_si_first_pass_accepted_count": 2,
                "batch_si_first_pass_ofas_abort_count": 1,
                "deferred_transaction_count": 1,
                "batch_si_deferred_tx_ids": ["T1"],
                "batch_si_accepted_tx_ids": ["T2", "T3"],
            },
            {
                "batch_si_first_pass_candidate_count": 3,
                "batch_si_first_pass_accepted_count": 1,
                "batch_si_first_pass_ofas_abort_count": 2,
                "deferred_transaction_count": 2,
                "batch_si_deferred_tx_ids": ["T1", "T2"],
                "batch_si_accepted_tx_ids": ["T1"],
            },
        ],
    }), encoding="utf-8")
    metrics = {"source_artifacts": []}
    _apply_batch_si_metrics(metrics, tmp_path)
    assert metrics["batch_si_deferred_identity_evidence_available"] is True
    assert metrics["batch_si_deferred_event_count"] == 3
    assert metrics["batch_si_unique_deferred_tx_count"] == 2
    assert metrics["batch_si_unique_deferral_rate"] == 2 / 3
    assert metrics["batch_si_mean_deferrals_per_finalized_tx"] == 1.0
    assert metrics["batch_si_first_pass_candidate_count"] == 6
    assert metrics["batch_si_first_pass_accepted_count"] == 3
    assert metrics["batch_si_first_pass_ofas_abort_count"] == 3
    assert metrics["abort_count"] == 0


def test_batch_si_unique_deferred_metrics_fail_closed_on_partial_identity_evidence(tmp_path: Path) -> None:
    (tmp_path / "nodes" / "n0").mkdir(parents=True)
    (tmp_path / "compiled_run_plan.json").write_text(json.dumps({
        "node_configs": [{"node_id": "n0", "shard_id": "s0", "leader": True}],
    }), encoding="utf-8")
    (tmp_path / "nodes" / "n0" / "block_execution_summary.json").write_text(json.dumps({
        "block_executor_id": "batch_si_block_executor",
        "shard_id": "s0",
        "blocks": [
            {
                "batch_si_first_pass_candidate_count": 2,
                "batch_si_first_pass_accepted_count": 1,
                "batch_si_first_pass_ofas_abort_count": 1,
                "deferred_transaction_count": 1,
                "batch_si_deferred_tx_ids": ["T1"],
                "batch_si_accepted_tx_ids": ["T2"],
            },
            {
                "batch_si_first_pass_candidate_count": 2,
                "batch_si_first_pass_accepted_count": 1,
                "batch_si_first_pass_ofas_abort_count": 1,
                "deferred_transaction_count": 1,
                "batch_si_deferred_tx_ids": ["T2"],
                # Deliberately emulate a truncated/old block summary.
            },
        ],
    }), encoding="utf-8")
    metrics = {"source_artifacts": []}
    _apply_batch_si_metrics(metrics, tmp_path)
    assert metrics["batch_si_deferred_event_count"] == 2
    assert metrics["batch_si_deferred_identity_evidence_available"] is False
    assert metrics["batch_si_unique_deferred_tx_count"] is None
    assert metrics["batch_si_unique_deferral_rate"] is None
    assert metrics["batch_si_mean_deferrals_per_finalized_tx"] is None
