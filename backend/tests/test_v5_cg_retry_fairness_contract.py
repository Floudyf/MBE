from backend.app.services.v5_formal_scheduler import _execution_semantics, _state_equivalence_individual_reasons
from backend.app.services.v5_metric_extractor import _literature_graph_required_metrics


def _cg_retry_item(finalized: int) -> dict:
    return {
        "status": "completed",
        "comparison_semantics_class": "nezha_cg_johnson_retryable_v4",
        "metrics": {
            "submitted_unique_tx_count": 1000,
            "terminal_unique_tx_count": 1000,
            "finalized_unique_logical_tx_count": finalized,
            "incomplete_unique_tx_count": 0,
            "cross_shard_failed_unique_count": 0,
            "abort_count": 400,
            "cg_cycle_abort_count": 400,
            "lifecycle_complete": True,
        },
        "result": {"summary": {}},
    }


def test_cg_retryable_semantics_require_eventual_logical_finality():
    semantics = _execution_semantics({"block_executor": "cg_block_executor"}, "hash_cg")
    assert semantics["comparison_semantics_class"] == "nezha_cg_johnson_retryable_v4"
    assert semantics["measurement_boundary"] == "client_submit_to_nezha_cg_eventual_finality"

    reasons = _state_equivalence_individual_reasons(_cg_retry_item(finalized=999))
    assert "finalized_not_equal_submitted" in reasons
    assert "finalized_plus_abort_not_equal_terminal" not in reasons

    assert _state_equivalence_individual_reasons(_cg_retry_item(finalized=1000)) == []


def test_cg_retry_evidence_is_required_for_paper_valid_results():
    required = _literature_graph_required_metrics("hash_cg")
    assert "cg_cycle_deferred_retry_count" in required
    assert "cg_cycle_retry_lifecycle" in required
