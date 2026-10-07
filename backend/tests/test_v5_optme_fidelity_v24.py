from __future__ import annotations

from backend.app.services import v5_metric_extractor as extractor
from backend.app.services import v5_optme_txallo_method_specific_v18 as v18


def test_optme_v24_logical_counts_preserve_physical_observations():
    out = {
        "optme_early_detection_count": 79328,
        "optme_hierarchical_abort_count": 0,
        "optme_rescheduled_transaction_count": 79328,
        "optme_reexecution_count": 79328,
        "optme_reordered_transaction_count": 0,
        "v17_optme_transaction_fidelity": {
            "status": "available",
            "logical_transaction_count": 10000,
            "logical_early_detection_count": 9916,
            "logical_hierarchical_abort_count": 0,
            "logical_rescheduled_count": 9916,
            "logical_reexecution_count": 9916,
            "logical_reordered_count": 0,
            "logical_simulation_failed_count": 0,
            "logical_reexecution_simulation_failed_count": 0,
            "logical_second_pass_invalidated_count": 0,
        },
    }
    got = v18._apply_optme_logical_counts_v24(out)
    assert got["optme_replica_physical_early_detection_observation_count"] == 79328
    assert got["optme_early_detection_count"] == 9916
    assert got["optme_logical_early_detection_count"] == 9916
    assert got["optme_logical_reexecution_count"] == 9916
    assert got["optme_logical_transaction_count"] == 10000
    assert got["optme_algorithm_event_count_scope"] == "logical_tx_deduplicated_from_optme_transaction_evidence"


def test_optme_v24_v23_truth_retires_only_stale_v17_blocker(monkeypatch):
    monkeypatch.setattr(
        v18.v17,
        "_canonical_child",
        lambda item: {
            "paper_candidate": False,
            "paper_candidate_reasons": ["stateless_global_logical_business_state_not_proven"],
            "v17_fidelity_blockers": ["stateless_global_logical_business_state_not_proven"],
        },
    )
    item = {
        "method": {"method_id": "stateless_optme"},
        "method_config_id": "stateless_optme",
        "metrics": {
            "v18_method_correctness": {"status": "passed", "valid": True, "blockers": []},
            "v18_method_correctness_oracle_status": "passed",
            "v18_method_correctness_oracle_valid": True,
            "v18_method_correctness_oracle_blockers": [],
            "v18_metric_coherence_passed": True,
            "v18_optme_transaction_fidelity": {"status": "available"},
            # v24.1+ requires proof that the active Go runtime emitted the
            # OptME fidelity fields. This test is about retiring the stale
            # v17 blocker, so provide the independent runtime-attestation
            # precondition instead of bypassing the fail-closed gate.
            "v18_optme_runtime_fidelity": {
                "status": "available",
                "passed": True,
                "input_access_fidelity": "declared_runtime_rw_no_unknown_projection",
                "execution_window_mapping": "one_mbe_pbft_block_to_one_optme_execution_window",
                "commit_materialization_model": "mbe_deterministic_index_order_platform_adapter",
            },
        },
    }
    got = v18._canonical_child(item)
    assert got["paper_candidate"] is True
    assert got["v18_retired_v17_blockers"] == ["stateless_global_logical_business_state_not_proven"]
    assert got["paper_candidate_reasons"] == []


def test_optme_v24_formal_worker_truth_uses_topology_request(monkeypatch):
    rows = [{
        "method": {
            "method_id": "stateful_optme",
            "plugin_config_overrides": {"block_executor": {"worker_count": 4}},
        },
        "method_config_id": "stateful_optme",
        "topology_point": {"worker_count": 8},
        "metrics": {},
    }]
    monkeypatch.setattr(v18.v17, "apply_fairness_fidelity_gate", lambda r, report: (r, {"passed": True}))
    got, _ = v18.apply_fairness_fidelity_gate(rows, {})
    assert got[0]["requested_worker_count"] == 8
    assert got[0]["method_config_worker_count"] == 4
    assert got[0]["effective_worker_count"] == 8


def test_optme_v24_metric_extractor_reference_replica_scope(tmp_path):
    (tmp_path / "compiled_run_plan.json").write_text(
        __import__("json").dumps({"node_configs": [
            {"node_id": "n0", "leader": True, "role": "leader"},
            {"node_id": "n1", "leader": False, "role": "validator"},
        ]}), encoding="utf-8"
    )
    for node_id, simulation_ms in (("n0", 10), ("n1", 12)):
        node = tmp_path / "nodes" / node_id
        node.mkdir(parents=True)
        (node / "block_execution_summary.json").write_text(
            __import__("json").dumps({
                "block_executor_id": "optme_block_executor",
                "blocks": [{
                    "optme_mode": "stateful",
                    "optme_simulation_ms": simulation_ms,
                    "optme_early_detection_count": 5,
                    "optme_sequence_count": 2,
                    "optme_rescheduled_epoch_count": 3,
                    "optme_maximum_sequence_width": 4,
                    "optme_source_commit": "4cac103bd98440670d71219dfa185b8516ea6512",
                }],
            }), encoding="utf-8"
        )
    metrics = {"source_artifacts": []}
    extractor._apply_optme_metrics(metrics, tmp_path)
    assert metrics["optme_replica_metric_reference_node"] == "n0"
    assert metrics["optme_replica_metric_replica_count"] == 2
    assert metrics["optme_simulation_ms"] == 10
    assert metrics["optme_replica_physical_simulation_ms_sum"] == 22
    assert metrics["optme_early_detection_count"] == 5
    assert metrics["optme_replica_physical_early_detection_count_sum"] == 10
    assert metrics["optme_sequence_count"] == 2
    assert metrics["optme_replica_physical_sequence_count_sum"] == 4


def test_optme_v24_group_retires_generic_v17_blocker_only_when_resolved(monkeypatch):
    stale = "stateless_global_logical_business_state_not_proven"
    v17_items = [{"method_config_id": "stateless_optme"}]
    v17_report = {
        "v17_group_fidelity_blockers": ["one_or_more_method_v17_fidelity_blockers"],
        "performance_comparison_valid": False,
        "paper_candidate": False,
    }
    monkeypatch.setattr(v18.v17, "apply_group_fidelity_gate", lambda items, report: (v17_items, dict(v17_report)))
    monkeypatch.setattr(v18, "_canonical_child", lambda item: {
        "v18_method_id": "stateless_optme",
        "v18_fidelity_blockers": [],
        "v17_fidelity_blockers": [stale],
        "v18_retired_v17_blockers": [stale],
    })
    _, report = v18.apply_group_fidelity_gate([], {"performance_comparison_valid": True, "paper_candidate": True})
    assert report["v18_group_fidelity_blockers"] == []
    assert report["v18_retired_v17_group_blockers"] == ["one_or_more_method_v17_fidelity_blockers"]
    assert report["performance_comparison_valid"] is True
    assert report["paper_candidate"] is True


def test_optme_v24_group_keeps_generic_v17_blocker_when_any_legacy_issue_remains(monkeypatch):
    remaining = "logical_transaction_identity_digest_missing"
    v17_items = [{"method_config_id": "stateless_optme"}]
    v17_report = {
        "v17_group_fidelity_blockers": ["one_or_more_method_v17_fidelity_blockers"],
        "performance_comparison_valid": False,
        "paper_candidate": False,
    }
    monkeypatch.setattr(v18.v17, "apply_group_fidelity_gate", lambda items, report: (v17_items, dict(v17_report)))
    monkeypatch.setattr(v18, "_canonical_child", lambda item: {
        "v18_method_id": "stateless_optme",
        "v18_fidelity_blockers": [],
        "v17_fidelity_blockers": [remaining],
        "v18_retired_v17_blockers": [],
    })
    _, report = v18.apply_group_fidelity_gate([], {"performance_comparison_valid": True, "paper_candidate": True})
    assert "one_or_more_method_v17_fidelity_blockers" in report["v18_group_fidelity_blockers"]
    assert "stateless_optme:" + remaining in report["v18_group_fidelity_blockers"]
    assert report["performance_comparison_valid"] is False
    assert report["paper_candidate"] is False
