from pathlib import Path


def read(path: str) -> str:
    return Path(path).read_text(encoding="utf-8")


def test_frontend_exposes_exactly_three_metatrack_profiles():
    catalog = read("frontend/src/v5FormalExperimentCatalog.ts")
    assert catalog.count('family: "metatrack"') == 3
    assert 'MetaTrack（初始版）' in catalog
    assert 'MetaTrack（当前版）' in catalog
    assert 'MetaTrack（新版）' in catalog


def test_current_profile_is_frozen_v5_and_new_profile_alone_enables_closure():
    backend = read("backend/app/services/v5_formal_plan_validator.py")
    current = backend.split('"metatrack_full_locality"', 1)[1].split('"metatrack_latest"', 1)[0]
    latest = backend.split('"metatrack_latest"', 1)[1].split('LITERATURE_BUILTIN_METHODS', 1)[0]
    assert '"version_liveness": True' in current
    assert '"final_version_batch_writeback": True' in current
    assert '"dependency_closed_consensus": True' not in current
    assert '"dependency_closed_consensus": True' in latest
    assert '"version_liveness_indexed": True' in latest
    assert '"single_final_seal": True' in latest


def test_proposer_and_validator_both_enforce_dependency_closure():
    registry = read("executor/v5/registry.go")
    runtime = read("executor/v5/runtime.go")
    closure = read("executor/v5/metatrack_dependency_closed_consensus_v6.go")
    pbft_safety = read("executor/v5/pbft_safety.go")
    frontier = read("executor/v5/metatrack_transaction_frontier_v656.go")
    assert 'selectMetaTrackTransactionFrontierV656' in registry
    assert 'validateMetaTrackDependencyClosedProjectionBlock' in runtime
    assert 'validateMetaTrackTransactionFrontierV656(block)' in pbft_safety
    assert 'ConsensusExecutionPredecessorOrdinals' in frontier
    assert 'ConsensusExecutionDepth' in frontier
    assert 'dependency.RequiredVersion >= ordinal' in closure
    assert 'dependency.ProducedVersion != ordinal' in closure
    assert 'duplicate producer' in closure


def test_new_profile_uses_single_final_seal_and_indexed_liveness():
    client = read("executor/v5/client.go")
    registry = read("executor/v5/registry.go")
    optimized = read("executor/v5/metatrack_version_liveness_optimized_v6.go")
    assert 'DeferFinalSeal: deferFinalSeal' in client
    assert 'finalizeMetaTrackSignedBatchPlan' in client
    assert 'version_liveness_indexed' in client
    assert 'if !input.DeferFinalSeal' in registry
    assert 'metaTrackBuildAccessIndex' in optimized


def test_closure_boundary_final_uses_aggregate_consumer_guard_and_async_join():
    source = read("executor/v5/metatrack_version_liveness_v5.go")
    helper = read("executor/v5/metatrack_async_version_writeback_v640.go")
    assert 'boundaryRequired := r.metaTrackBlockExecutorFlag("dependency_closed_consensus")' not in source
    assert 'closureImmediateBatch := r.metaTrackBlockExecutorFlag("dependency_closed_consensus")' not in source
    assert 'closureBackgroundAsync := buffer.asyncV640 != nil' in source
    assert 'buffer.blockRemoteValueSuccessorCount[identity] > 0' in source
    assert 'enqueueMetaTrackAsyncFinalV640' in source
    assert 'deferOrJoinMetaTrackAsyncVersionsV656' in source
    overlap = read("executor/v5/metatrack_final_join_overlap_v656.go")
    runtime = read("executor/v5/runtime.go")
    assert 'joinMetaTrackAsyncVersionsV640' in overlap
    assert 'joinDeferredMetaTrackAsyncVersionsV656(ctx, block)' in runtime
    assert runtime.index('joinDeferredMetaTrackAsyncVersionsV656(ctx, block)') < runtime.index('r.setCommitPhase("durable_commit", block)')
    assert 'metaTrackBlockRemoteValueConsumerIndexV640' in helper
    assert 'metatrack_block_consumer_critical_final_publish_count' in source
    assert 'metatrack_background_final_writeback_ms' in helper
    assert 'metatrack_final_join_wait_ms' in helper


def test_formal_metrics_export_liveness_and_closure_boundary_truth():
    extractor = read("backend/app/services/v5_metric_extractor.py")
    for key in [
        "metatrack_version_local_transient_count",
        "metatrack_version_remote_live_count",
        "metatrack_version_dead_intermediate_count",
        "metatrack_version_final_persistent_count",
        "metatrack_closure_boundary_immediate_publish_count",
        "metatrack_block_remote_value_consumer_edge_count",
        "metatrack_block_consumer_critical_final_publish_count",
        "metatrack_critical_version_publish_wait_ms",
        "metatrack_background_final_writeback_ms",
        "metatrack_final_join_wait_ms",
    ]:
        assert key in extractor


def test_three_profile_semantics_are_consistent_in_backend_and_analysis_ui():
    backend = read("backend/app/services/v5_formal_plan_validator.py")
    current = backend.split('"metatrack_full_locality"', 1)[1].split('"metatrack_latest"', 1)[0]
    latest = backend.split('"metatrack_latest"', 1)[1].split('LITERATURE_BUILTIN_METHODS', 1)[0]
    panel = read("frontend/src/components/v5/V5AnalysisPanel.tsx")
    mechanism = read("frontend/src/components/v5/V5MechanismAnalysis.tsx")
    assert 'display_name="MetaTrack（当前版）"' in current
    assert '"version_liveness": True' in current
    assert '"dependency_closed_consensus": True' not in current
    assert 'display_name="MetaTrack（新版）"' in latest
    assert '"dependency_closed_consensus": True' in latest
    assert '"metatrack_full_locality"' in panel
    assert 'metatrack_serial: ["MetaTrack", "初始版"]' in panel
    assert 'metatrack_full_locality: ["MetaTrack", "当前版"]' in panel
    assert 'metatrack_latest: ["MetaTrack", "新版"]' in panel
    assert 'id === "metatrack_serial") return "MetaTrack（初始版）"' in mechanism
    assert 'id === "metatrack_full_locality") return "MetaTrack（当前版）"' in mechanism
    assert 'id === "metatrack_latest") return "MetaTrack（新版）"' in mechanism


def test_legacy_contracts_are_migrated_to_frozen_current_v5_and_new_v6():
    paths = [
        "backend/tests/test_v5_metatrack_version_liveness_v5_contract.py",
        "backend/tests/test_v5_metatrack_full_locality_v4_contract.py",
        "backend/tests/test_v5_metatrack_latest_v3_contract.py",
        "backend/tests/test_v5_formal_plan_validator_closure.py",
        "backend/tests/test_v5_formal_row_compilation_propagation.py",
    ]
    combined = "\n".join(read(path) for path in paths)
    assert 'MetaTrack（当前版：状态本地化）' not in combined
    assert 'MetaTrack（最新版）' not in combined
    assert 'get("version_liveness") is None' not in combined
    assert '["version_liveness"] is False' not in combined
    assert 'dependency_closed_consensus' in combined

def test_block_producer_public_defaults_stay_legacy_and_closure_is_method_override_only():
    manifest = read("backend/app/services/v5_plugin_manifest_store.py")
    backend = read("backend/app/services/v5_formal_plan_validator.py")
    producer_line = next(
        line for line in manifest.splitlines()
        if '"block_producer", "time_or_count_block_producer"' in line
    )
    assert 'config={"block_size": 100, "interval_ms": 75}, schema=' in producer_line
    assert '"dependency_closed_consensus": False' not in producer_line
    assert '"dependency_closed_consensus": {"type": "boolean", "default": False}' in producer_line
    current = backend.split('"metatrack_full_locality"', 1)[1].split('"metatrack_latest"', 1)[0]
    latest = backend.split('"metatrack_latest"', 1)[1].split('LITERATURE_BUILTIN_METHODS', 1)[0]
    assert '"block_producer": {"dependency_closed_consensus": True}' not in current
    assert '"block_producer": {"dependency_closed_consensus": True}' in latest

