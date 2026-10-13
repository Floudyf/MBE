from __future__ import annotations

import json
from backend.app.services.v5_metric_extractor import _apply_mechanism_metrics, _apply_metric_completeness

def test_candidate_dynamic_v24_metrics_are_exported(tmp_path):
    out = tmp_path / "aggregate"
    out.mkdir()
    payload = {
       "metatrack": {
          "status":"available", "metatrack_dynamic_resource_lane_enabled":True,
          "metatrack_dynamic_lane_runtime_version":"risk_resource_lane_v24",
          "metatrack_dynamic_lane_metric_complete":True,
          "metatrack_dynamic_lane_expected_block_count":2,
          "metatrack_dynamic_lane_present_block_count":2,
          "metatrack_dynamic_risk_signal_tx_count":0,
          "metatrack_dynamic_normal_dispatch_count":100,
          "metatrack_dynamic_risk_dispatch_count":0,
          "metatrack_dynamic_normal_protection_count":0,
          "metatrack_dynamic_uncontended_dispatch_count":100,
          "metatrack_dynamic_risk_contended_dispatch_count":0,
          "metatrack_unlock_arbitration_count":0,
          "metatrack_unlock_changed_choice_count":0,
       }
    }
    (out/"mechanism_metrics_summary.json").write_text(json.dumps(payload),encoding="utf-8")
    metrics={"source_artifacts":[],"missing":[]}
    _apply_mechanism_metrics(metrics,tmp_path)
    assert metrics["metatrack_dynamic_lane_runtime_version"] == "risk_resource_lane_v24"
    assert metrics["metatrack_dynamic_lane_metric_complete"] is True
    assert metrics["metatrack_dynamic_risk_signal_tx_count"] == 0
    assert metrics["metatrack_dynamic_normal_dispatch_count"] == 100
    assert metrics["metatrack_dynamic_risk_dispatch_count"] == 0
    assert metrics["metatrack_dynamic_uncontended_dispatch_count"] == 100


def test_candidate_missing_dynamic_evidence_is_not_silently_zero(tmp_path):
    out=tmp_path/"aggregate";out.mkdir()
    (out/"mechanism_metrics_summary.json").write_text(json.dumps({"metatrack":{"status":"available","metatrack_dynamic_resource_lane_enabled":True,"metatrack_dynamic_lane_metric_complete":False,"metatrack_dynamic_lane_expected_block_count":2,"metatrack_dynamic_lane_present_block_count":1,"metatrack_dynamic_normal_dispatch_count":None}}),encoding="utf-8")
    metrics={"source_artifacts":[],"missing":[]}
    _apply_mechanism_metrics(metrics,tmp_path)
    assert metrics["metatrack_dynamic_lane_metric_complete"] is False
    assert metrics["metatrack_dynamic_normal_dispatch_count"] is None


def test_candidate_missing_runtime_version_is_marked_incomplete():
    metrics={"missing":[],"metatrack_dynamic_resource_lane_enabled":True,
             "metatrack_dynamic_lane_metric_complete":False,
             "metatrack_dynamic_lane_runtime_version":"old_binary"}
    _apply_metric_completeness(metrics,method_id="metatrack_dyntrack")
    assert "evidence:dynamic_lane_incomplete" in metrics["metric_missing"]
    assert "evidence:dynamic_lane_wrong_runtime_version" in metrics["metric_missing"]
    assert metrics["metric_completeness"] == "incomplete"
