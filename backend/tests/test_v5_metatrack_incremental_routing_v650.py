import json
from pathlib import Path

from backend.app.services.v5_metric_extractor import _apply_metatrack_incremental_routing_v650_metrics
from backend.app.services.v5_plugin_manifest_store import STORE


def test_v650_routing_manifest_accepts_incremental_continuity_flag():
    manifest = STORE.get("metatrack_coaccess_routing")
    prop = manifest.config_schema["properties"]["incremental_exact_continuity_routing_v65"]
    assert prop == {"type": "boolean", "default": False}
    assert "incremental_exact_version_continuity" in manifest.capabilities
    assert "route_batch_partition_invariant_routing" in manifest.capabilities


def test_v650_metric_extractor_aggregates_signed_route_batch_evidence(tmp_path: Path):
    rows = [
        {
            "incremental_routing_policy": "incremental_ready_balanced_coaccess_v651",
            "incremental_expected_transaction_count": 4,
            "incremental_execution_shard_capacity": 2,
            "incremental_history_transaction_count_before": 0,
            "incremental_history_transaction_count_after": 2,
            "incremental_exact_state_edge_count": 3,
            "incremental_exact_local_state_edge_count": 2,
            "incremental_exact_cross_shard_state_edge_count": 1,
            "incremental_exact_predecessor_edge_count": 2,
            "incremental_exact_local_predecessor_count": 1,
            "incremental_exact_cross_shard_predecessor_count": 1,
            "incremental_coaccess_pair_update_count": 8,
            "incremental_execution_shard_switch_count": 1,
            "incremental_capacity_forced_choice_count": 0,
            "incremental_exact_first_choice_count": 1,
            "incremental_coaccess_tiebreak_count": 1,
            "incremental_remote_tiebreak_count": 0,
            "incremental_load_tiebreak_count": 0,
            "incremental_history_digest_after": "d1",
            "incremental_batch_partition_invariant": True,
            "incremental_routing_plan_us": 11,
        },
        {
            "incremental_routing_policy": "incremental_ready_balanced_coaccess_v651",
            "incremental_expected_transaction_count": 4,
            "incremental_execution_shard_capacity": 2,
            "incremental_history_transaction_count_before": 2,
            "incremental_history_transaction_count_after": 4,
            "incremental_exact_state_edge_count": 2,
            "incremental_exact_local_state_edge_count": 2,
            "incremental_exact_cross_shard_state_edge_count": 0,
            "incremental_exact_predecessor_edge_count": 2,
            "incremental_exact_local_predecessor_count": 2,
            "incremental_exact_cross_shard_predecessor_count": 0,
            "incremental_coaccess_pair_update_count": 7,
            "incremental_execution_shard_switch_count": 0,
            "incremental_capacity_forced_choice_count": 1,
            "incremental_exact_first_choice_count": 1,
            "incremental_coaccess_tiebreak_count": 0,
            "incremental_remote_tiebreak_count": 1,
            "incremental_load_tiebreak_count": 0,
            "incremental_history_digest_after": "d2",
            "incremental_batch_partition_invariant": True,
            "incremental_routing_plan_us": 17,
        },
    ]
    path = tmp_path / "client" / "metatrack_batch_plan.jsonl"
    path.parent.mkdir(parents=True)
    path.write_text("".join(json.dumps(row) + "\n" for row in rows), encoding="utf-8")
    metrics = {"source_artifacts": []}
    _apply_metatrack_incremental_routing_v650_metrics(metrics, tmp_path)
    assert metrics["metatrack_incremental_routing_policy"] == "incremental_ready_balanced_coaccess_v651"
    assert metrics["metatrack_incremental_routing_batch_count"] == 2
    assert metrics["metatrack_incremental_history_transaction_count"] == 4
    assert metrics["metatrack_incremental_exact_state_edge_count"] == 5
    assert metrics["metatrack_incremental_exact_cross_shard_state_edge_count"] == 1
    assert metrics["metatrack_incremental_exact_cross_shard_state_edge_rate"] == 0.2
    assert metrics["metatrack_incremental_exact_cross_shard_predecessor_count"] == 1
    assert metrics["metatrack_incremental_coaccess_pair_update_count"] == 15
    assert metrics["metatrack_incremental_capacity_forced_choice_count"] == 1
    assert metrics["metatrack_incremental_batch_partition_invariant"] is True
    assert metrics["metatrack_incremental_history_digest"] == "d2"
    assert metrics["metatrack_incremental_routing_plan_total_us"] == 28
    assert metrics["metatrack_incremental_routing_plan_mean_us"] == 14.0
    assert metrics["metatrack_incremental_routing_plan_max_us"] == 17
    assert "client/metatrack_batch_plan.jsonl" in metrics["source_artifacts"]


def test_v650_only_latest_profile_enables_incremental_routing():
    root = Path(__file__).resolve().parents[2]
    frontend = (root / "frontend/src/v5MethodProfile.ts").read_text(encoding="utf-8")
    formal = (root / "backend/app/services/v5_formal_plan_validator.py").read_text(encoding="utf-8")

    front_current = frontend.split('method_id: "metatrack_full_locality"', 1)[1].split('method_id: "metatrack_latest"', 1)[0]
    front_latest = frontend.split('method_id: "metatrack_latest"', 1)[1]
    assert "incremental_exact_continuity_routing_v65" not in front_current
    assert "incremental_exact_continuity_routing_v65: true" in front_latest

    formal_current = formal.split('"metatrack_full_locality": V5FormalMethod(', 1)[1].split('"metatrack_latest": V5FormalMethod(', 1)[0]
    formal_latest = formal.split('"metatrack_latest": V5FormalMethod(', 1)[1].split('}\n\nLITERATURE_BUILTIN_METHODS', 1)[0]
    assert "incremental_exact_continuity_routing_v65" not in formal_current
    assert '"incremental_exact_continuity_routing_v65": True' in formal_latest


def test_v651_route_batch_size_remains_packaging_only_contract():
    root = Path(__file__).resolve().parents[2]
    helper = (root / "executor/v5/metatrack_incremental_routing_v650.go").read_text(encoding="utf-8")
    helper_test = (root / "executor/v5/metatrack_incremental_routing_v650_test.go").read_text(encoding="utf-8")
    registry = (root / "executor/v5/registry.go").read_text(encoding="utf-8")

    # Executable Go regressions are the behavior gates; prose/comment wrapping is not.
    assert "IncrementalBatchPartitionInvariant:       true" in helper
    assert "TestMetaTrackV650RoutingIsRouteBatchPartitionInvariant" in helper_test
    assert "TestMetaTrackV651ExactContinuityCannotOverrideThreeOtherCriteria" in helper_test
    assert "TestMetaTrackV651CoaccessMagnitudeCannotDominateOtherSignals" in helper_test
    assert "metaTrackIncrementalRankCandidatesV651" in helper
    assert "ready_remote_coaccess_load_minimax_rank_v651" in helper
    assert "reflect.DeepEqual(all, chunked)" in helper_test
    assert "allPlanner.incrementalV650.HistoryDigest != chunkedPlanner.incrementalV650.HistoryDigest" in helper_test
    assert 'boolFromAny(p.config["incremental_exact_continuity_routing_v65"])' in registry
    for forbidden in ("time.Sleep(", "time.NewTicker(", "hotspot_threshold", "chain_length_threshold", "fixed_window_size", "coaccess_weight", "exact_weight", "remote_weight", "load_weight"):
        assert forbidden not in helper
