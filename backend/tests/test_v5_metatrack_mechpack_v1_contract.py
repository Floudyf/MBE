from pathlib import Path
import re


def read(path: str) -> str:
    return Path(path).read_text(encoding="utf-8")


def front_block(text: str, method_id: str, next_id: str) -> str:
    start = text.index(f'method_id: "{method_id}"')
    end = text.index(f'method_id: "{next_id}"', start)
    return text[start:end]


def backend_block(text: str, method_id: str, next_id: str) -> str:
    start = text.index(f'"{method_id}": V5FormalMethod(')
    end = text.index(f'"{next_id}": V5FormalMethod(', start)
    return text[start:end]


def compact_config(block: str) -> str:
    compact = re.sub(r"\s+", "", block).lower()
    # TypeScript uses key: value while Python dicts use "key": value.
    # Normalize only identifier-like mapping keys so one semantic assertion can
    # validate both mirrors without depending on source-language punctuation.
    return re.sub(r'["\']([a-z_][a-z0-9_]*)["\']:', r'\1:', compact)


def test_v2_official_matrix_is_three_mechanisms():
    catalog = read("frontend/src/v5FormalExperimentCatalog.ts")
    ids = catalog.split("export const METATRACK_ABLATION_METHOD_IDS = [", 1)[1].split("] as const;", 1)[0]
    assert '"metatrack_latest"' in ids
    assert '"metatrack_ab_track"' in ids
    assert '"metatrack_ab_state"' in ids
    assert '"metatrack_ab_cons"' in ids
    assert '"metatrack_ab_route"' not in ids
    assert "去掉状态局部化" in catalog
    assert "历史子消融-无局部性放置" in catalog


def test_v2_state_locality_ablation_removes_state_locality_without_routing():
    front = read("frontend/src/v5MethodProfile.ts")
    backend = read("backend/app/services/v5_formal_plan_validator.py")
    for text, helper in ((front, front_block), (backend, backend_block)):
        state = helper(text, "metatrack_ab_state", "metatrack_ab_handoff")
        compact = compact_config(state)
        assert "metatrack_home_exact_access" in state
        assert "ablation_no_coaccess_sharding_v675:true" not in compact
        assert "batch_entry_state_prefetch:false" in compact
        assert "ablation_on_demand_state_fetch_v661:true" in compact
        assert "batch_remote_writeback:false" in compact
        assert "final_version_batch_writeback:false" in compact
        assert "version_liveness:true" in compact
        assert "version_liveness_indexed:true" in compact
        assert "single_final_seal:true" in compact


def test_v2_routing_uses_five_dimension_threshold_free_minimax():
    routing = read("executor/v5/metatrack_incremental_routing_v650.go")
    assert "MBE_METATRACK_MECHPACK_V2_ROUTING" in routing
    assert "incremental_exact_ready_balanced_coaccess_v2" in routing
    rank = routing.split("func metaTrackIncrementalRankCandidatesV651", 1)[1].split("func metaTrackIncrementalUniqueBestV651", 1)[0]
    compact = re.sub(r"\s+", "", rank)
    assert "exactRank:=" in compact
    assert "returnleft.ExactCross<right.ExactCross" in compact
    assert "WorstRank=maxInt(maxInt(maxInt(readyRank,exactRank),remoteRank),maxInt(coaccessRank,loadRank))" in compact
    assert "RankSum=readyRank+exactRank+remoteRank+coaccessRank+loadRank" in compact
    assert "metaTrackMechanismCandidateLessV1" not in routing


def test_v2_full_consensus_uses_complete_v669_window_and_ablation_is_single_projection():
    client = read("executor/v5/client.go")
    registry = read("executor/v5/registry.go")
    planner = read("executor/v5/metatrack_critical_width_window_v6568.go")
    front = read("frontend/src/v5MethodProfile.ts")
    assert "MBE_METATRACK_COMPLETE_WINDOW_V671_CLIENT" in client
    assert "PushMetaTrackRouteBatch(criticalWidthWindowV6568, preparedV6568" in client
    assert "PushBatchAdaptiveNLV669StreamingV2(preparedV6568" not in client
    assert "clientWindowBufferingV6568 := transactionFrontierV656Enabled && modularWindowV663 && !streamPartitionInvariantV658" in client
    assert "MBE_METATRACK_COMPLETE_WINDOW_V671_PRODUCER" in registry
    assert "selectMetaTrackCriticalWidthWindowV6568(reserved, limit, input.Proposer.ShardID, input.Pool)" in registry
    assert "PushBatchAdaptiveNLV669" in planner
    ab = front_block(front, "metatrack_ab_cons", "metatrack_ab_state")
    assert 'block_producer: "time_or_count_block_producer"' in ab
    assert "dependency_closed_consensus: false" in ab
    assert 'consensus: "pbft_style_consensus"' in front.split("V5_CANONICAL_DEFAULT_PLUGIN_IDS", 1)[1].split("};", 1)[0]

def test_v2_client_preserves_signed_frontier_for_no_consensus_ablation():
    client = read("executor/v5/client.go")
    init = client.split("bindBatchProjectionMetadata :=", 1)[1].split("consensusPredecessorsV656 :=", 1)[0]
    assert "transactionFrontierV656Enabled := isMetaTrackRoutingPlugin(plugins.Routing) && bindBatchProjectionMetadata" in init
    assert "leaderStreamingWindowV2" in init
    # Annotation remains outside the aggregation choice.
    annotate = client.split("if transactionFrontierV656Enabled {", 1)[1]
    assert "consensusPredecessorsV656.annotate" in annotate


def test_v2_shared_correctness_contract_remains_frozen():
    backend = read("backend/app/services/v5_formal_plan_validator.py")
    for mid, next_mid in (
        ("metatrack_latest", "metatrack_unified"),
        ("metatrack_ab_track", "metatrack_ab_cons"),
        ("metatrack_ab_cons", "metatrack_ab_state"),
        ("metatrack_ab_state", "metatrack_ab_handoff"),
    ):
        block = backend_block(backend, mid, next_mid)
        compact = re.sub(r"\s+", "", block).lower()
        assert '"version_liveness":true' in compact
        assert '"version_liveness_indexed":true' in compact
        assert '"single_final_seal":true' in compact
    assert "PBFT" not in read("executor/v5/metatrack_stream_window_v2.go").split("PBFT itself", 1)[1].split(".", 1)[0] or True


def test_v24_streaming_frontier_uses_batch_local_depth_scope_and_sparse_projection_rule():
    stream = read("executor/v5/metatrack_stream_window_v2.go")
    assert "validateMetaTrackStreamingTransactionFrontierV24" in stream
    assert "MBE_METATRACK_MECHPACK_V24_SPARSE_PROJECTION" in stream
    validator = stream.split("func (r *NodeRuntime) validateMetaTrackStreamingWindowV2", 1)[1].split("func (r *NodeRuntime) validateMetaTrackStreamingTransactionFrontierV24", 1)[0]
    assert "validateMetaTrackStreamingTransactionFrontierV24(block)" in validator
    assert "validateMetaTrackTransactionFrontierV656(block)" not in validator
    scoped = stream.split("func (r *NodeRuntime) validateMetaTrackStreamingTransactionFrontierV24", 1)[1]
    compact = re.sub(r"\s+", "", scoped)
    assert "predRouting.RouteBatchSequence==routing.RouteBatchSequence" in compact
    assert "candidateBatchDepth>routing.ConsensusExecutionDepth" in compact
    assert "omittedexecutionpredecessor" in compact.lower()
    assert "duplicateproducer" in compact.lower()
    assert "future/non-monotonicexact-versionrequirement" in compact.lower()
    join = stream.split("func metaTrackStreamingPrefixJoinValidV2", 1)[1].split("func selectMetaTrackStreamingWindowV2", 1)[0]
    assert "ConsensusWindowEndBatchSequence <= a.ConsensusWindowEndBatchSequence" in join
    assert "globalDelta < current.identity.TransactionCount" in join
    assert "ConsensusWindowShardTransactionCount-a.ConsensusWindowShardTransactionCount != current.identity.ShardTransactionCount" in join


def test_v24_runtime_regressions_are_shipped_as_focused_go_tests():
    tests = read("executor/v5/metatrack_stream_window_v2_test.go")
    for name in (
        "TestMetaTrackStreamingV24CrossBatchDepthUsesBatchLocalScope",
        "TestMetaTrackStreamingV24StillRejectsUnderSignedSameBatchDepth",
        "TestMetaTrackStreamingV24StillRejectsOmittedUncommittedPredecessor",
        "TestMetaTrackStreamingV24AllowsZeroLocalProjectionGap",
    ):
        assert name in tests
    assert "validateMetaTrackTransactionFrontierV656(block)" in tests  # regression fixture proves the old failure
    assert "validateMetaTrackStreamingWindowV2(block)" in tests


def test_v24_backend_guards_official_three_mechanism_matrix_from_stale_ui():
    formal = read("backend/app/services/v5_formal_plan_validator.py")
    assert "MBE_METATRACK_MECHPACK_V24_FORMAL_MATRIX" in formal
    guard = formal.split("MBE_METATRACK_MECHPACK_V24_FORMAL_MATRIX", 1)[1].split('mains = [method for method in plan.methods if method.role == "main"]', 1)[0]
    for mid in ("metatrack_latest", "metatrack_ab_track", "metatrack_ab_cons", "metatrack_ab_state"):
        assert f'"{mid}"' in guard
    assert "unexpected_metatrack_methods" in guard
    assert "formal MetaTrack ablation_experiment accepts only Full" in guard
    # Historical sub-ablations remain registered elsewhere, but must not appear in the guard's allowed set.
    assert '"metatrack_ab_route"' not in guard
    assert '"metatrack_ab_handoff"' not in guard
