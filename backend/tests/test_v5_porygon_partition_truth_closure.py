from backend.app.services.v5_formal_scheduler import _execution_semantics
from backend.app.services.v5_real_cluster_runner import _global_business_state_digest, _global_final_state_digest


def test_porygon_partition_roots_aggregate_to_global_digest() -> None:
    summary = {
        "node_summaries": [
            {"node_id": "n0", "shard_id": "s0", "state_root": "root-s0", "business_state_digest": "business-s0"},
            {"node_id": "n1", "shard_id": "s0", "state_root": "root-s0", "business_state_digest": "business-s0"},
            {"node_id": "n4", "shard_id": "s1", "state_root": "root-s1", "business_state_digest": "business-s1"},
            {"node_id": "n5", "shard_id": "s1", "state_root": "root-s1", "business_state_digest": "business-s1"},
        ]
    }
    assert _global_final_state_digest(summary)
    assert _global_business_state_digest(summary)


def test_porygon_formal_truth_reports_physical_fetch_but_partition_local_materialization() -> None:
    semantics = _execution_semantics({"block_executor": "porygon_block_executor"}, "stateless_porygon")
    assert semantics["remote_fetch_policy"] == "signed_access_projection_with_physical_state_fetch"
    assert semantics["remote_writeback_policy"] == "partition_local_materialization_from_certified_esc_result"
    assert semantics["legacy_cross_shard_protocol"] is False
