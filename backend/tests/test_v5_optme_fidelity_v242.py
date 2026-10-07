from __future__ import annotations

from backend.app.services import v5_optme_txallo_method_specific_v18 as v18


def _base_metrics(runtime: dict) -> dict:
    return {
        "v18_method_correctness": {"status": "passed", "valid": True, "blockers": []},
        "v18_method_correctness_oracle_status": "passed",
        "v18_method_correctness_oracle_valid": True,
        "v18_method_correctness_oracle_blockers": [],
        "v18_metric_coherence_passed": True,
        "v18_optme_transaction_fidelity": {"status": "available"},
        "v18_optme_runtime_fidelity": runtime,
    }


def test_optme_v242_stale_v17_retirement_requires_runtime_attestation(monkeypatch):
    stale = "stateless_global_logical_business_state_not_proven"
    monkeypatch.setattr(
        v18.v17,
        "_canonical_child",
        lambda item: {
            "paper_candidate": False,
            "paper_candidate_reasons": [stale],
            "v17_fidelity_blockers": [stale],
        },
    )
    item = {
        "method": {"method_id": "stateless_optme"},
        "method_config_id": "stateless_optme",
        "metrics": _base_metrics({
            "status": "available",
            "passed": True,
            "input_access_fidelity": "declared_runtime_rw_no_unknown_projection",
            "execution_window_mapping": "one_mbe_pbft_block_to_one_optme_execution_window",
            "commit_materialization_model": "mbe_deterministic_index_order_platform_adapter",
        }),
    }
    got = v18._canonical_child(item)
    assert got["paper_candidate"] is True
    assert got["v18_retired_v17_blockers"] == [stale]
    assert "optme_runtime_fidelity_evidence_unproven" not in got["v18_fidelity_blockers"]


def test_optme_v242_stale_v17_retirement_does_not_override_missing_runtime(monkeypatch):
    stale = "stateless_global_logical_business_state_not_proven"
    monkeypatch.setattr(
        v18.v17,
        "_canonical_child",
        lambda item: {
            "paper_candidate": False,
            "paper_candidate_reasons": [stale],
            "v17_fidelity_blockers": [stale],
        },
    )
    item = {
        "method": {"method_id": "stateless_optme"},
        "method_config_id": "stateless_optme",
        "metrics": _base_metrics({"status": "missing_runtime_fidelity_fields", "passed": False}),
    }
    got = v18._canonical_child(item)
    assert got["paper_candidate"] is False
    assert got["v18_retired_v17_blockers"] == [stale]
    assert "optme_runtime_fidelity_evidence_unproven" in got["v18_fidelity_blockers"]
