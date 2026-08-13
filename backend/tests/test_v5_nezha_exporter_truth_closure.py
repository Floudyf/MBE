from __future__ import annotations

from backend.app.services import v5_formal_scheduler, v5_paper_exporter


def _acg_child(*, submitted: int = 10000, finalized: int = 8718, aborts: int = 1282, hs_aborts: int | None = 1282) -> dict:
    metrics = {
        "end_to_end_tps": 100.0,
        "logical_finality_tps": 100.0,
        "p95_finality_ms": 10,
        "p99_finality_ms": 20,
        "submitted_unique_tx_count": submitted,
        "terminal_unique_tx_count": submitted,
        "finalized_unique_logical_tx_count": finalized,
        "incomplete_unique_tx_count": 0,
        "cross_shard_failed_unique_count": 0,
        "lifecycle_complete": True,
        "no_fallback": True,
        "state_root_consistent": True,
        "receipt_root_consistent": True,
        "plan_digest_consistent": True,
        "abort_count": aborts,
    }
    if hs_aborts is not None:
        metrics["nezha_hs_abort_count"] = hs_aborts
    return {
        "status": "completed",
        "execution_status": "completed",
        "artifact_status": "complete",
        "method_config_id": "hash_acg",
        "comparison_semantics_class": "nezha_acg_hs_abortable_v1",
        "formal_eligibility": True,
        "paper_candidate": True,
        "comparison_eligibility_status": "passed",
        "metrics": metrics,
        "result": {
            "summary": {
                "finality_evidence": {
                    "submitted_unique_tx_count": submitted,
                    "terminal_unique_tx_count": submitted,
                    "finalized_unique_logical_tx_count": finalized,
                    "incomplete_unique_tx_count": 0,
                    "cross_shard_failed_unique_count": 0,
                }
            }
        },
    }


def test_exporter_accepts_exact_nezha_terminal_abort_accounting() -> None:
    child = _acg_child()
    assert v5_paper_exporter._individual_result_reasons(child) == []
    assert v5_paper_exporter._sample_status_for_child(child) == "paper_eligible"


def test_exporter_rejects_nezha_terminal_abort_accounting_gap() -> None:
    child = _acg_child(finalized=8717, aborts=1282)
    reasons = v5_paper_exporter._individual_result_reasons(child)
    assert "finalized_plus_abort_not_equal_terminal" in reasons


def test_exporter_rejects_nezha_hs_abort_metric_mismatch() -> None:
    child = _acg_child(aborts=1282, hs_aborts=1281)
    reasons = v5_paper_exporter._individual_result_reasons(child)
    assert "nezha_hs_abort_count_mismatch" in reasons


def test_exporter_metric_completeness_requires_nezha_abort_fields() -> None:
    child = _acg_child(hs_aborts=None)
    metrics = v5_paper_exporter._effective_metrics(child)
    assert "metric:nezha_hs_abort_count" in metrics["missing"]
    assert metrics["metric_completeness"] == "incomplete"


def test_scheduler_rejects_nezha_terminal_abort_accounting_gap() -> None:
    child = _acg_child(finalized=9698, aborts=301, hs_aborts=301)
    reasons = v5_formal_scheduler._state_equivalence_individual_reasons(child)
    assert "finalized_plus_abort_not_equal_terminal" in reasons


def test_scheduler_rejects_nezha_hs_abort_metric_mismatch() -> None:
    child = _acg_child(finalized=9699, aborts=301, hs_aborts=300)
    reasons = v5_formal_scheduler._state_equivalence_individual_reasons(child)
    assert "nezha_hs_abort_count_mismatch" in reasons
