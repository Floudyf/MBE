from __future__ import annotations

import json
from pathlib import Path

from backend.app.services.v5_metric_extractor import _apply_porygon_metrics
from backend.app.services.v5_formal_scheduler import _state_equivalence_individual_reasons


_PORYGON = "porygon_3d_global_ordering_paper_fidelity_v5"


def test_porygon_v54_post_drain_counts_are_not_overwritten(tmp_path: Path) -> None:
    node = tmp_path / "nodes" / "n0"
    node.mkdir(parents=True)
    (node / "block_execution_summary.json").write_text(json.dumps({
        "shard_id": "porygon-global",
        "block_executor_id": "porygon_block_executor",
        "blocks": [{
            "height": 1,
            "porygon_paper_itx_committed_count": 17,
            "porygon_paper_ctx_committed_count": 21,
            "porygon_protocol_abandoned_transaction_count": 961,
            "porygon_cross_shard_transaction_count": 945,
            "porygon_intra_shard_transaction_count": 55,
        }],
    }), encoding="utf-8")
    (tmp_path / "finality_summary.json").write_text(json.dumps({
        "submitted_unique_tx_count": 1000,
        "terminal_unique_tx_count": 1000,
        "finalized_unique_logical_tx_count": 39,
        "incomplete_unique_tx_count": 0,
    }), encoding="utf-8")
    metrics = {"submitted_unique_tx_count": 1000, "terminal_unique_tx_count": 976, "source_artifacts": [], "missing": []}
    _apply_porygon_metrics(metrics, tmp_path)
    assert (metrics["submitted_unique_tx_count"], metrics["terminal_unique_tx_count"],
            metrics["finalized_unique_logical_tx_count"], metrics["incomplete_unique_tx_count"]) == (1000, 1000, 39, 0)
    assert metrics["porygon_paper_committed_diagnostic_count"] == 38
    assert metrics["porygon_post_drain_count_identity_valid"] is True
    assert metrics["porygon_terminal_truth_scope"].startswith("post_drain_authoritative_")


def test_porygon_v54_validity_uses_finality_evidence_before_stale_metric_snapshot() -> None:
    item = {
        "status": "completed", "comparison_semantics_class": _PORYGON,
        "metrics": {"submitted_unique_tx_count": 1000, "terminal_unique_tx_count": 976,
                    "finalized_unique_logical_tx_count": 81, "incomplete_unique_tx_count": 24,
                    "porygon_protocol_abandoned_unique_tx_count": 961,
                    "lifecycle_complete": True},
        "result": {"summary": {"finality_evidence": {
            "submitted_unique_tx_count": 1000, "terminal_unique_tx_count": 1000,
            "finalized_unique_logical_tx_count": 39, "incomplete_unique_tx_count": 0,
        }}},
    }
    reasons = _state_equivalence_individual_reasons(item)
    for stale_reason in ("terminal_not_equal_submitted", "finalized_plus_abort_not_equal_terminal", "incomplete_not_zero"):
        assert stale_reason not in reasons
    item["result"]["summary"]["finality_evidence"]["terminal_unique_tx_count"] = 999
    assert "terminal_not_equal_submitted" in _state_equivalence_individual_reasons(item)


def test_porygon_v54_phase_trace_keeps_only_observed_events(tmp_path: Path) -> None:
    from backend.app.services.v5_porygon_phase_trace_v54 import export_porygon_phase_trace
    import csv

    node_dir = tmp_path / "nodes" / "n0"
    node_dir.mkdir(parents=True)
    path = node_dir / "consensus_message_log.csv"
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=("timestamp", "message_type", "height", "from_node", "block_hash"))
        writer.writeheader()
        writer.writerow({"timestamp": "100", "message_type": "PORYGON_PROPOSAL_PHASE_BUILD_CANDIDATE_BEGIN",
                         "height": "13", "from_node": "view=0;pool=91;reserved=91",
                         "block_hash": "digest13"})
        writer.writerow({"timestamp": "101", "message_type": "PBFT_PRE_PREPARE_LOCAL",
                         "height": "13"})
    result = export_porygon_phase_trace(tmp_path)
    assert result["porygon_proposal_phase_trace_available"] is True
    assert result["porygon_proposal_phase_trace_event_count"] == 1
    with (tmp_path / "porygon_proposal_phase_trace.csv").open("r", encoding="utf-8", newline="") as handle:
        rows = list(csv.DictReader(handle))
    assert len(rows) == 1
    assert rows[0]["phase"] == "BUILD_CANDIDATE_BEGIN"
    assert rows[0]["height"] == "13"
    assert rows[0]["time_ms"] == "100"
    assert rows[0]["view"] == "0"
    assert rows[0]["block_hash"] == "digest13"
