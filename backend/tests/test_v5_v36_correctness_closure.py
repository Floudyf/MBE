from __future__ import annotations

from backend.app.services.v5_formal_plan_validator import ALL_BUILTIN_METHODS
from backend.app.services.v5_formal_scheduler import _is_paper_candidate_result, _state_equivalence_individual_reasons
from backend.app.services.v5_paper_exporter import _individual_result_reasons
from backend.app.services.v5_plugin_manifest_store import STORE


def _failed_oracle_item() -> dict:
    metrics = {
        "submitted_unique_tx_count": 1,
        "terminal_unique_tx_count": 1,
        "finalized_unique_logical_tx_count": 1,
        "incomplete_unique_tx_count": 0,
        "cross_shard_failed_unique_count": 0,
        "lifecycle_complete": True,
        "method_correctness_oracle_valid": False,
        "serial_order_replay_applicable": True,
        "serial_order_replay_equivalent": False,
        "no_fallback": True,
        "state_root_consistent": True,
        "receipt_root_consistent": True,
        "plan_digest_consistent": True,
        "metric_completeness": "complete",
        "missing": [],
        "end_to_end_tps": 1.0,
        "p95_finality_ms": 1.0,
        "p99_finality_ms": 1.0,
    }
    return {"status": "completed", "execution_status": "completed", "metrics": metrics, "result": {"summary": {"ready_to_commit": True, "no_fallback": True}}, "comparison_semantics_class": "stateless_calvin_consensus_version_plan_adaptation_v4"}


def test_v36_scheduler_individual_gate_rejects_failed_method_oracle() -> None:
    reasons = _state_equivalence_individual_reasons(_failed_oracle_item())
    assert "method_correctness_oracle_not_true" in reasons
    assert "serial_order_replay_not_equivalent" in reasons


def test_v36_paper_exporter_rejects_failed_method_oracle() -> None:
    reasons = _individual_result_reasons(_failed_oracle_item())
    assert "method_correctness_oracle_not_true" in reasons
    assert "serial_order_replay_not_equivalent" in reasons


def test_v36_early_paper_candidate_gate_rejects_failed_method_oracle() -> None:
    item = _failed_oracle_item()
    assert _is_paper_candidate_result(item["result"] | {"status": "completed"}, item["metrics"]) is False


def test_v36_formal_porygon_requires_distributed_esc_but_generic_manifest_stays_test_compatible() -> None:
    method = ALL_BUILTIN_METHODS["stateless_porygon"]
    assert method.plugin_config_overrides["block_executor"]["require_distributed_esc"] is True
    manifest = STORE.get("porygon_block_executor")
    assert manifest.default_config["require_distributed_esc"] is False
