from __future__ import annotations

import json

from backend.app.services import v5_metric_extractor as extractor
from backend.app.services import v5_optme_txallo_method_specific_v18 as v18


def _write_run(tmp_path, *, include_runtime_fields: bool, projected_unknown: int = 3):
    (tmp_path / "compiled_run_plan.json").write_text(
        json.dumps({"node_configs": [
            {"node_id": "n0", "leader": True, "role": "leader"},
            {"node_id": "n1", "leader": False, "role": "validator"},
        ]}), encoding="utf-8"
    )
    for node_id, simulation_ms in (("n0", 10), ("n1", 12)):
        node = tmp_path / "nodes" / node_id
        node.mkdir(parents=True)
        block = {
            "optme_mode": "stateful",
            "optme_simulation_ms": simulation_ms,
            "optme_early_detection_count": 5,
            "optme_sequence_count": 2,
            "optme_rescheduled_epoch_count": 3,
            "optme_maximum_sequence_width": 4,
            "optme_source_commit": "4cac103bd98440670d71219dfa185b8516ea6512",
        }
        if include_runtime_fields:
            block.update({
                "optme_projected_unknown_access_count": projected_unknown,
                "optme_input_access_fidelity": (
                    "historical_static_unknown_promoted_to_rmw_projection"
                    if projected_unknown else "declared_runtime_rw_no_unknown_projection"
                ),
                "optme_execution_window_mapping": "one_mbe_pbft_block_to_one_optme_execution_window",
                "optme_commit_materialization_model": "mbe_deterministic_index_order_platform_adapter",
            })
        (node / "block_execution_summary.json").write_text(
            json.dumps({"block_executor_id": "optme_block_executor", "blocks": [block]}),
            encoding="utf-8",
        )


def test_optme_v241_exports_runtime_fidelity_from_reference_replica(tmp_path):
    _write_run(tmp_path, include_runtime_fields=True, projected_unknown=3)
    metrics = {"source_artifacts": []}
    extractor._apply_optme_metrics(metrics, tmp_path)
    assert metrics["optme_runtime_fidelity_evidence_status"] == "available"
    assert metrics["optme_runtime_fidelity_evidence_passed"] is True
    assert metrics["optme_projected_unknown_access_count"] == 3
    assert metrics["optme_replica_physical_projected_unknown_access_count_sum"] == 6
    assert metrics["optme_input_access_fidelity"] == "historical_static_unknown_promoted_to_rmw_projection"
    assert metrics["optme_input_access_truth_scope"] == "projected_static_declaration_runtime_semantics"
    assert metrics["optme_execution_window_mapping"] == "one_mbe_pbft_block_to_one_optme_execution_window"
    assert metrics["optme_commit_materialization_model"] == "mbe_deterministic_index_order_platform_adapter"
    assert metrics["optme_simulation_ms"] == 10
    assert metrics["optme_replica_physical_simulation_ms_sum"] == 22


def test_optme_v241_missing_runtime_fields_is_explicitly_unproven(tmp_path):
    _write_run(tmp_path, include_runtime_fields=False)
    metrics = {"source_artifacts": []}
    extractor._apply_optme_metrics(metrics, tmp_path)
    assert metrics["optme_runtime_fidelity_evidence_status"] == "missing_runtime_fidelity_fields"
    assert metrics["optme_runtime_fidelity_evidence_passed"] is False
    assert set(metrics["optme_runtime_fidelity_missing_fields"]) == {
        "optme_projected_unknown_access_count",
        "optme_input_access_fidelity",
        "optme_execution_window_mapping",
        "optme_commit_materialization_model",
    }


def test_optme_v241_enrich_promotes_runtime_fidelity(monkeypatch):
    monkeypatch.setattr(v18.v17, "enrich_metrics", lambda run_dir, method_id, result: dict(result))
    monkeypatch.setattr(v18, "compute_method_specific_correctness_v18", lambda method, out: {"status": "passed", "valid": True, "blockers": []})
    monkeypatch.setattr(v18, "compute_writeback_fanout_v18", lambda *args: {})
    monkeypatch.setattr(v18, "compute_metric_coherence_v18", lambda *args: {"passed": True})
    monkeypatch.setattr(v18.v17, "compute_optme_transaction_evidence_v17", lambda *args: {"status": "available", "logical_transaction_count": 1})
    got = v18.enrich_metrics(".", "stateful_optme", {
        "optme_runtime_fidelity_evidence_status": "available",
        "optme_runtime_fidelity_evidence_passed": True,
        "optme_input_access_fidelity": "historical_static_unknown_promoted_to_rmw_projection",
        "optme_projected_unknown_access_count": 7,
        "optme_execution_window_mapping": "one_mbe_pbft_block_to_one_optme_execution_window",
        "optme_commit_materialization_model": "mbe_deterministic_index_order_platform_adapter",
        "optme_input_access_truth_scope": "projected_static_declaration_runtime_semantics",
    })
    assert got["v18_optme_runtime_fidelity"]["status"] == "available"
    assert got["v18_optme_runtime_fidelity"]["projected_unknown_access_count"] == 7


def test_optme_v241_canonical_child_blocks_missing_runtime_fidelity(monkeypatch):
    monkeypatch.setattr(v18.v17, "_canonical_child", lambda item: {"paper_candidate": True, "paper_candidate_reasons": [], "v17_fidelity_blockers": []})
    item = {
        "method": {"method_id": "stateful_optme"},
        "metrics": {
            "v18_method_correctness": {"status": "passed", "valid": True, "blockers": []},
            "v18_metric_coherence_passed": True,
            "v18_optme_transaction_fidelity": {"status": "available"},
            "v18_optme_runtime_fidelity": {"status": "missing_runtime_fidelity_fields", "passed": False},
        },
    }
    got = v18._canonical_child(item)
    assert "optme_runtime_fidelity_evidence_unproven" in got["v18_fidelity_blockers"]
    assert got["paper_candidate"] is False


def test_optme_v241_projected_unknown_is_diagnostic_not_algorithm_failure(monkeypatch):
    monkeypatch.setattr(v18.v17, "_canonical_child", lambda item: {"paper_candidate": True, "paper_candidate_reasons": [], "v17_fidelity_blockers": []})
    item = {
        "method": {"method_id": "stateful_optme"},
        "metrics": {
            "v18_method_correctness": {"status": "passed", "valid": True, "blockers": []},
            "v18_metric_coherence_passed": True,
            "v18_optme_transaction_fidelity": {"status": "available"},
            "v18_optme_runtime_fidelity": {
                "status": "available",
                "passed": True,
                "input_access_fidelity": "historical_static_unknown_promoted_to_rmw_projection",
            },
        },
    }
    got = v18._canonical_child(item)
    assert "optme_runtime_fidelity_evidence_unproven" not in got["v18_fidelity_blockers"]
    assert "optme_historical_unknown_access_is_projected_rmw_not_native_runtime_rw_truth" in got["v18_diagnostics"]
