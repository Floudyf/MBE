from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]

def read(path: str) -> str:
    return (ROOT / path).read_text(encoding="utf-8")

def section(text: str, start: str, end: str | None = None) -> str:
    out = text.split(f'method_id: "{start}"', 1)[1]
    return out.split(f'method_id: "{end}"', 1)[0] if end else out[:5200]

def test_v661_four_main_ablations_match_research_mechanisms() -> None:
    front=read("frontend/src/v5MethodProfile.ts")
    full=section(front,"metatrack_latest","metatrack_ab_route")
    route=section(front,"metatrack_ab_route","metatrack_ab_track")
    track=section(front,"metatrack_ab_track","metatrack_ab_cons")
    cons=section(front,"metatrack_ab_cons","metatrack_ab_state")
    state=section(front,"metatrack_ab_state","metatrack_ab_handoff")
    handoff=section(front,"metatrack_ab_handoff")
    for token in ["incremental_exact_continuity_routing_v65: true","local_exact_version_handoff: true","batch_entry_state_prefetch: true","dependency_closed_consensus: true"]:
        assert token in full
    assert "ablation_ignore_coaccess_routing_v661: true" in route
    assert "ablation_ignore_exact_ready_routing_v660: true" not in route
    assert "ablation_single_ready_queue_v661: true" in track
    assert "ablation_force_all_conservative_v659: true" not in track
    assert "ablation_single_route_batch_consensus_v660: true" in cons
    assert "ablation_on_demand_state_fetch_v661: true" in state
    assert "local_exact_version_handoff: true" in state and "batch_entry_state_prefetch: true" in state
    assert "local_exact_version_handoff: false" in handoff and "ablation_force_home_exact_v660: true" in handoff
    assert "batch_entry_state_prefetch: true" in handoff

def test_v661_coaccess_ablation_keeps_same_incremental_router() -> None:
    src=read("executor/v5/metatrack_incremental_routing_v650.go")
    helper=read("executor/v5/metatrack_ablation_v659.go")
    assert "ReadyRank:        metaTrackIncrementalReadyRankV651(record, shard, state)," in src
    assert "CoaccessLocality: metaTrackAblationCoaccessLocalityV661(ignoreCoaccessV661, keys, shard, state)," in src
    assert "return metaTrackIncrementalPairLocalityV650(keys, shard, state)" in helper

def test_v661_dual_track_ablation_preserves_classification_but_unifies_runtime_ready_queue() -> None:
    registry=read("executor/v5/registry.go")
    helper=read("executor/v5/metatrack_ablation_v659.go")
    assert 'metaTrackAblationSingleReadyQueueV661         = "ablation_single_ready_queue_v661"' in helper
    assert "singleReadyQueueV661 := boolFromAny(p.config[metaTrackAblationSingleReadyQueueV661])" in registry or "QUEUE_POLICY_V661_SINGLE_READY" in registry
    assert "unified_ready_queue_v661" in registry or "QUEUE_POLICY_V661_SINGLE_READY" in registry
    assert "applyMetaTrackEffectiveFrontierTracks" in registry

def test_v661_state_prefetch_ablation_is_true_on_demand() -> None:
    registry=read("executor/v5/registry.go")
    helper=read("executor/v5/metatrack_ablation_v659.go")
    assert 'metaTrackAblationOnDemandStateFetchV661       = "ablation_on_demand_state_fetch_v661"' in helper
    assert "onDemandStateFetchV661 := boolFromAny(p.config[metaTrackAblationOnDemandStateFetchV661])" in registry or "PREFETCH_POLICY_V661_ON_DEMAND" in registry
    assert "PREFETCH_POLICY_V661_ON_DEMAND" in registry or "startOnDemandStateForTxV661" in registry
    assert "metatrack_on_demand_state_fetch_wait_ms_v661" in registry or "PREFETCH_POLICY_V661_ON_DEMAND" in registry

def test_v661_consensus_full_is_critical_path_preserving_and_ablation_is_single_batch() -> None:
    critical=read("executor/v5/metatrack_critical_width_window_v6568.go")
    client=read("executor/v5/client.go")
    assert "metaTrackCriticalPathPreservingJoinV661" in critical
    assert "candidateL <= maxInt(currentL, batchL)" in critical
    push=critical.split("func (p *metaTrackCriticalWidthWindowPlannerV6568) PushBatch",1)[1]
    assert "metaTrackCriticalWidthImprovesV6568(p.transactionCount" not in push
    assert "PushBatchSingleRouteBatchV660" in client

def test_v661_ui_lists_four_main_ablations_plus_handoff_subablation() -> None:
    catalog=read("frontend/src/v5FormalExperimentCatalog.ts")
    page=read("frontend/src/pages/V5FormalRunPage.tsx")
    for mid in ["metatrack_ab_route","metatrack_ab_track","metatrack_ab_state","metatrack_ab_cons","metatrack_ab_handoff"]:
        assert mid in catalog
    assert "四个主消融 + 一个状态子消融" in page
    assert 'method.method_id === "hash_batch_si"' not in page
