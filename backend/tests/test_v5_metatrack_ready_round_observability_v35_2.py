from __future__ import annotations

import json

from backend.app.services.v5_metric_extractor import (
    COMMON_REQUIRED_METRICS,
    METATRACK_READY_ROUND_REQUIRED_METRICS,
    METATRACK_REQUIRED_METRICS,
    _apply_mechanism_metrics,
    _apply_metric_completeness,
    _derive_research_metrics,
)


def _ready_round_payload() -> dict:
    return {
        "status": "available",
        "fast_track_logical_tx_count": 10,
        "conservative_track_logical_tx_count": 2,
        "aggregation_group_count": 0,
        "pre_aggregation_physical_op_count": 12,
        "post_aggregation_physical_op_count": 12,
        "physical_ops_saved_count": 0,
        "aggregation_reduction_ratio": 0.0,
        "metatrack_ready_round_metrics_available": True,
        "metatrack_ready_priority_policy": "dependency_influence_h_desc_d_desc_canonical_v1",
        "metatrack_dependency_influence_scheduler_enabled": True,
        "metatrack_ready_round_scheduler_enabled": True,
        "metatrack_ready_round_control_enabled": False,
        "arbitration_round_count": 10,
        "multi_candidate_ready_round_count": 6,
        "competition_arbitration_count": 4,
        "priority_candidate_set_max": 5,
        "priority_candidate_set_mean": 3.5,
        "influence_arbitration_decision_count": 8,
        "influence_changed_choice_count": 2,
        "ready_round_event_drain_skip_count": 3,
        "cross_round_bypass_count": 0,
        "ready_round_metric_truth_scope": "canonical_node_per_shard_from_block_execution_summary",
        "ready_round_observed_block_count": 3,
    }


def test_v352_ready_round_mechanism_metrics_are_exported(tmp_path):
    aggregate = tmp_path / "aggregate"
    aggregate.mkdir()
    (aggregate / "mechanism_metrics_summary.json").write_text(
        json.dumps({"metatrack": _ready_round_payload()}), encoding="utf-8"
    )
    metrics = {"source_artifacts": [], "missing": []}
    _apply_mechanism_metrics(metrics, tmp_path)
    _derive_research_metrics(metrics)
    assert metrics["ready_round_count"] == 10
    assert metrics["hd_arbitration_count"] == 8
    assert metrics["hd_order_changed_count"] == 2
    assert metrics["multi_candidate_ready_round_ratio"] == 0.6
    assert metrics["competition_arbitration_rate"] == 0.4
    assert metrics["hd_order_change_rate"] == 0.25
    assert metrics["cross_round_bypass_count"] == 0


def test_v352_ready_round_metrics_are_required_for_ab_methods():
    metrics = {"missing": []}
    for name in COMMON_REQUIRED_METRICS + METATRACK_REQUIRED_METRICS + METATRACK_READY_ROUND_REQUIRED_METRICS:
        metrics[name] = 0
    metrics["metatrack_ready_priority_policy"] = "dependency_influence_h_desc_d_desc_canonical_v1"
    metrics["ready_round_metric_truth_scope"] = "canonical_node_per_shard_from_block_execution_summary"
    _apply_metric_completeness(metrics, method_id="metatrack_influence")
    assert not [item for item in metrics["metric_missing"] if "ready_round" in item or "arbitration" in item or "priority_candidate" in item or "influence_changed" in item]

    broken = dict(metrics)
    broken["missing"] = []
    broken["competition_arbitration_count"] = None
    _apply_metric_completeness(broken, method_id="metatrack_influence")
    assert "metric:competition_arbitration_count" in broken["metric_missing"]
