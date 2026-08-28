from __future__ import annotations

from backend.app.services.v5_metric_extractor import _derive_research_metrics
from backend.app.services.v5_formal_scheduler import _execution_semantics


def test_cg_retryable_semantics_disclose_multigraph_projection_and_eventual_finality() -> None:
    semantics = _execution_semantics(
        {"routing": "hash_routing_baseline", "block_executor": "cg_block_executor"},
        "hash_cg",
    )
    assert semantics["comparison_semantics_class"] == "nezha_cg_johnson_retryable_v4"
    assert semantics["proof_policy"] == "consensus_bound_nezha_cg_retry_projection_v4"
    assert semantics["measurement_boundary"] == "client_submit_to_nezha_cg_eventual_finality"


def test_block_stm_abort_alias_is_not_derived_for_cg_or_acg() -> None:
    for block_executor_id in ("cg_block_executor", "acg_block_executor"):
        metrics = {
            "block_executor_id": block_executor_id,
            "submitted_unique_tx_count": 10,
            "abort_count": 3,
        }
        _derive_research_metrics(metrics)
        assert "block_stm_abort_events_per_tx" not in metrics


def test_block_stm_abort_alias_is_still_derived_for_block_stm() -> None:
    metrics = {
        "block_executor_id": "block_stm_block_executor",
        "submitted_unique_tx_count": 10,
        "abort_count": 3,
    }
    _derive_research_metrics(metrics)
    assert metrics["block_stm_abort_events_per_tx"] == 0.3


def test_cg_worker_override_remains_execution_dimension() -> None:
    from backend.app.services.v5_formal_plan_validator import ALL_BUILTIN_METHODS
    method = ALL_BUILTIN_METHODS["hash_cg"]
    assert method.plugin_config_overrides["block_executor"]["worker_count"] == 4
