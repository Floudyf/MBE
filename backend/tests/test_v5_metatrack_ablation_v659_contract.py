from pathlib import Path
import re


def read(path: str) -> str:
    return Path(path).read_text(encoding="utf-8")


def method_block(text: str, method_id: str, next_id: str | None = None) -> str:
    out=text.split(f'method_id: "{method_id}"',1)[1]
    return out.split(f'method_id: "{next_id}"',1)[0] if next_id else out


def plugins(block: str) -> dict[str,str]:
    body=block.split("plugin_overrides:",1)[1].split("plugin_config_overrides:",1)[0]
    return dict(re.findall(r'([a-z_]+): "([^"]+)"',body))


def config_text(block: str) -> str:
    return block.split("plugin_config_overrides:",1)[1].split("},",1)[0].replace(" ","").replace("\n","")


def test_v674_formal_ablations_are_single_variable_by_removed_innovation():
    front=read("frontend/src/v5MethodProfile.ts")
    order=["metatrack_latest","metatrack_ab_route","metatrack_ab_track","metatrack_ab_cons","metatrack_ab_state","metatrack_ab_handoff"]
    blocks={mid:method_block(front,mid,order[i+1] if i+1<len(order) else None) for i,mid in enumerate(order[:-1])}
    full=plugins(blocks["metatrack_latest"])
    no_coaccess=plugins(blocks["metatrack_ab_route"])
    assert no_coaccess==full
    assert no_coaccess["routing"]=="metatrack_coaccess_routing"
    assert "ablation_no_coaccess_sharding_v675:true" in config_text(blocks["metatrack_ab_route"])
    assert "ablation_no_coaccess_sharding_v675:true" not in config_text(blocks["metatrack_latest"])
    expected={"metatrack_ab_track":{"execution"},"metatrack_ab_cons":{"block_producer"},"metatrack_ab_state":{"state_access"}}
    for mid,changed in expected.items():
        current=plugins(blocks[mid])
        actual={k for k in set(full)|set(current) if full.get(k)!=current.get(k)}
        assert actual==changed,(mid,actual)
        assert config_text(blocks[mid])==config_text(blocks["metatrack_latest"])
        assert "ablation_" not in blocks[mid]
    assert plugins(blocks["metatrack_ab_track"])["execution"]=="metatrack_single_execution"
    assert plugins(blocks["metatrack_ab_cons"])["block_producer"]=="metatrack_route_batch_producer"
    assert plugins(blocks["metatrack_ab_state"])["state_access"]=="metatrack_home_exact_access"


def test_v663_runtime_has_modular_plugin_ids_and_no_official_flag_dispatch():
    registry=read("executor/v5/registry.go")
    client=read("executor/v5/client.go")
    for token in ["metatrack_hash_routing","metatrack_single_execution","metatrack_dependency_window_producer","metatrack_adaptive_window_producer","metatrack_nl_window_v669","metatrack_route_batch_producer","metatrack_local_exact_access","metatrack_home_exact_access"]:
        assert token in registry
    assert "singleRouteBatchAblationV660" not in client
    assert "PushMetaTrackRouteBatch" in client
    assert "isMetaTrackRoutingPlugin(plugins.Routing)" in client


def test_v675_state_access_remains_modular_and_no_coaccess_sharding_is_the_only_routing_ablation():
    front=read("frontend/src/v5MethodProfile.ts")
    full=method_block(front,"metatrack_latest","metatrack_unified")
    no_coaccess=method_block(front,"metatrack_ab_route","metatrack_ab_track")
    no_state_prefetch=method_block(front,"metatrack_ab_state","metatrack_ab_handoff")
    assert "local_exact_version_handoff:" not in full
    assert "local_exact_version_handoff:" not in no_state_prefetch
    assert 'state_access: "metatrack_local_exact_access"' in full
    assert 'state_access: "metatrack_home_exact_access"' in no_state_prefetch
    assert "ablation_no_coaccess_sharding_v675: true" not in full
    assert "ablation_no_coaccess_sharding_v675: true" in no_coaccess
    assert "ablation_" not in no_state_prefetch


def test_v663_ui_has_exactly_four_official_ablations():
    catalog=read("frontend/src/v5FormalExperimentCatalog.ts")
    ids=catalog.split("export const METATRACK_ABLATION_METHOD_IDS = [",1)[1].split("] as const;",1)[0]
    for mid in ["metatrack_latest","metatrack_ab_route","metatrack_ab_track","metatrack_ab_state","metatrack_ab_cons"]: assert f'"{mid}"' in ids
    assert '"metatrack_ab_handoff"' not in ids


def test_v667_a2_is_single_execution_module_only():
    front=read("frontend/src/v5MethodProfile.ts")
    order=["metatrack_latest","metatrack_ab_route","metatrack_ab_track","metatrack_ab_cons","metatrack_ab_state","metatrack_ab_handoff"]
    blocks={mid:method_block(front,mid,order[i+1]) for i,mid in enumerate(order[:-1])}
    full=plugins(blocks["metatrack_latest"])
    a2=plugins(blocks["metatrack_ab_track"])
    changed={k for k in set(full)|set(a2) if full.get(k)!=a2.get(k)}
    assert changed=={"execution"}
    assert a2["execution"]=="metatrack_single_execution"
    assert config_text(blocks["metatrack_ab_track"])==config_text(blocks["metatrack_latest"])
    assert "ablation_single_ready_queue_v661" not in blocks["metatrack_ab_track"]
    assert 'method_id: "metatrack_unified"' in front


def test_v675_single_execution_owns_strict_serial_policy():
    registry=read("executor/v5/registry.go")
    assert 'const metaTrackSingleExecutionID = "metatrack_single_execution"' in registry
    assert 'const metaTrackUnifiedReadyPolicyV667 = "single_conservative_serial_v675"' in registry
    assert 'MetaTrackReadyQueuePolicy() string' in registry
    assert 'func metaTrackExecutionUsesUnifiedReadyV667' in registry
    single=registry.split('type metaTrackSingleExecution struct',1)[1].split('type metaTrackReadyQueuePolicyCapability',1)[0]
    assert 'dualTrackExecution{basicPlugin:p.basicPlugin}.ClassifyBatch' not in single
    assert 'applyMetaTrackEffectiveFrontierTracks' not in single
    assert 'single_conservative_serial_v675' in single
    assert 'unified_ready_single_track_v667' not in single
    assert 'result.Dependencies' in single and 'result.StateWaitKeys' in single


def test_v669_full_uses_adaptive_consensus_and_fixed_batch_consensus_is_the_only_fixed_window_substitution():
    front=read("frontend/src/v5MethodProfile.ts")
    order=["metatrack_latest","metatrack_unified","metatrack_ab_route","metatrack_ab_track","metatrack_ab_cons","metatrack_ab_state","metatrack_ab_handoff"]
    blocks={mid:method_block(front,mid,order[i+1]) for i,mid in enumerate(order[:-1])}
    full=plugins(blocks["metatrack_latest"])
    assert full["block_producer"]=="metatrack_nl_window_v669"
    assert plugins(blocks["metatrack_ab_route"])["block_producer"]=="metatrack_nl_window_v669"
    assert plugins(blocks["metatrack_ab_track"])["block_producer"]=="metatrack_nl_window_v669"
    assert plugins(blocks["metatrack_ab_state"])["block_producer"]=="metatrack_nl_window_v669"
    assert plugins(blocks["metatrack_ab_cons"])["block_producer"]=="metatrack_route_batch_producer"
    assert plugins(blocks["metatrack_unified"])["block_producer"]=="metatrack_adaptive_window_producer"
    for mid in ["metatrack_latest","metatrack_ab_route","metatrack_ab_track","metatrack_ab_cons","metatrack_ab_state"]:
        assert "micro_batch_size: 100" in blocks[mid]
        assert "micro_batch_size: 0" not in blocks[mid]


def test_v669_formal_adaptive_producer_is_version_separated_threshold_free_and_dependency_closed():
    registry=read("executor/v5/registry.go")
    planner=read("executor/v5/metatrack_critical_width_window_v6568.go")
    assert 'dependency_closed_adaptive_n_over_l_consensus_window_v669' in registry
    assert 'metatrack_nl_window_v669' in registry
    assert 'PushBatchAdaptiveNLV669' in registry
    formal=planner.split('func (p *metaTrackCriticalWidthWindowPlannerV6568) PushBatchAdaptiveNLV669',1)[1].split('func (p *metaTrackCriticalWidthWindowPlannerV6568) PushBatchFixedRouteBatchV663',1)[0]
    assert 'metaTrackDependencyClosedJoinV662(p.records, batch)' in formal
    assert 'metaTrackCriticalWidthImprovesV6568(p.transactionCount, p.criticalPath, candidateN, candidateL)' in formal
    assert 'metaTrackCountsFitBlockV6568(candidateCounts, blockLimit)' in formal
    historical=planner.split('func (p *metaTrackCriticalWidthWindowPlannerV6568) PushBatchAdaptiveNLV668',1)[1].split('func (p *metaTrackCriticalWidthWindowPlannerV6568) PushBatchAdaptiveNLV669',1)[0]
    assert 'metaTrackDependencyClosedJoinV662(p.records, batch)' not in historical


def test_v669_formal_route_batch_keeps_validated_fixed_100_across_named_ablations():
    front=read("frontend/src/v5MethodProfile.ts")
    scheduler=read("backend/app/services/v5_formal_scheduler.py")
    client=read("executor/v5/client.go")
    for mid,next_mid in [
        ("metatrack_latest","metatrack_unified"),
        ("metatrack_ab_route","metatrack_ab_track"),
        ("metatrack_ab_track","metatrack_ab_cons"),
        ("metatrack_ab_cons","metatrack_ab_state"),
        ("metatrack_ab_state","metatrack_ab_handoff"),
    ]:
        method=method_block(front,mid,next_mid)
        assert "micro_batch_size: 100" in method
        assert "micro_batch_size: 0" not in method
    assert 'for key in ("block_size", "interval_ms", "block_interval_ms"):' in scheduler
    assert 'config[key] = base_config[key]' in scheduler
    assert 'batchSize := plugins.BlockProducer.BlockSize()' in client
    assert 'batchSize = provider.RoutingBatchSize(batchSize)' in client
