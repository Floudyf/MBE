from __future__ import annotations

from backend.app.services.v5_fairness_validator import _performance_contract_class, validate


def _row(child: str, method: str, semantic: str) -> dict:
    return {
        "child_run_id": child,
        "suite_type": "workload_sensitivity",
        "method_config_id": method,
        "comparison_group_id": "g",
        "seed": 11,
        "repeat_index": 0,
        "execution_backend": "real_cluster",
        "estimated_transactions": 10000,
        "workload_snapshot_digest": "w",
        "topology_snapshot_digest": "t",
        "fault_snapshot_digest": "f",
        "fairness_key": "fair",
        "block_size": 1000,
        "block_interval_ms": 100,
        "topology_point": {"nodes": 8, "shards": 1, "validators_per_shard": 8, "worker_count": 8},
        "comparison_semantics_class": semantic,
        "state_access_semantics": "method_specific_stateful",
        "state_home_mapping_policy": "execution_shard_local_namespace",
        "remote_fetch_policy": "none",
        "remote_writeback_policy": "none",
        "proof_policy": "method_specific",
        "legacy_cross_shard_protocol": False,
        "measurement_boundary": "client_submit_to_eventual_terminal",
        "runnable": True,
        "blockers": [],
    }


def test_aria_single_shard_uses_common_eventual_completion_contract() -> None:
    row = _row("a", "hash_aria", "aria_reordered_retryable_v1")
    assert _performance_contract_class(row) == "single_shard_stateful_eventual_completion_v1"


def test_aria_and_groundhog_are_directly_comparable_under_common_external_contract() -> None:
    aria = _row("a", "hash_aria", "aria_reordered_retryable_v1")
    groundhog = _row("g", "hash_groundhog", "groundhog_typed_commutative_snapshot_v1")
    checked, result = validate([aria, groundhog])
    assert result["passed"] is True
    assert result["performance_comparison_valid"] is True
    assert result["direct_cross_semantic_performance_comparison_valid"] is True
    assert {row["performance_contract_class"] for row in checked} == {"single_shard_stateful_eventual_completion_v1"}


def test_aria_multishard_is_not_accidentally_promoted_to_single_shard_contract() -> None:
    row = _row("a", "hash_aria", "aria_reordered_retryable_v1")
    row["topology_point"] = {"nodes": 8, "shards": 2, "validators_per_shard": 4, "worker_count": 8}
    assert _performance_contract_class(row) == "within_semantic:aria_reordered_retryable_v1"
