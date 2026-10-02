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


def test_v663_formal_ablations_are_real_plugin_substitutions():
    front=read("frontend/src/v5MethodProfile.ts")
    order=["metatrack_latest","metatrack_ab_route","metatrack_ab_track","metatrack_ab_cons","metatrack_ab_state","metatrack_ab_handoff"]
    blocks={mid:method_block(front,mid,order[i+1] if i+1<len(order) else None) for i,mid in enumerate(order[:-1])}
    full=plugins(blocks["metatrack_latest"])
    expected={"metatrack_ab_route":{"routing"},"metatrack_ab_track":{"execution"},"metatrack_ab_cons":{"block_producer"},"metatrack_ab_state":{"state_access"}}
    for mid,changed in expected.items():
        current=plugins(blocks[mid])
        actual={k for k in set(full)|set(current) if full.get(k)!=current.get(k)}
        assert actual==changed,(mid,actual)
        assert config_text(blocks[mid])==config_text(blocks["metatrack_latest"])
        assert "ablation_" not in blocks[mid]
    assert full["routing"]=="metatrack_coaccess_routing"
    assert plugins(blocks["metatrack_ab_route"])["routing"]=="metatrack_hash_routing"
    assert plugins(blocks["metatrack_ab_track"])["execution"]=="metatrack_single_execution"
    assert plugins(blocks["metatrack_ab_cons"])["block_producer"]=="metatrack_route_batch_producer"
    assert plugins(blocks["metatrack_ab_state"])["state_access"]=="metatrack_home_exact_access"


def test_v663_runtime_has_modular_plugin_ids_and_no_official_flag_dispatch():
    registry=read("executor/v5/registry.go")
    client=read("executor/v5/client.go")
    for token in ["metatrack_hash_routing","metatrack_single_conservative_execution","metatrack_dependency_window_producer","metatrack_route_batch_producer","metatrack_local_exact_access","metatrack_home_exact_access"]:
        assert token in registry
    assert "singleRouteBatchAblationV660" not in client
    assert "PushMetaTrackRouteBatch" in client
    assert "isMetaTrackRoutingPlugin(plugins.Routing)" in client


def test_v663_state_access_is_a_module_not_block_executor_ablation():
    front=read("frontend/src/v5MethodProfile.ts")
    official=front.split('method_id: "metatrack_latest"',1)[1].split('method_id: "metatrack_ab_handoff"',1)[0]
    assert "ablation_" not in official
    assert "local_exact_version_handoff:" not in official
    assert 'state_access: "metatrack_local_exact_access"' in official
    assert 'state_access: "metatrack_home_exact_access"' in official


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


def test_v667_single_execution_owns_unified_ready_policy():
    registry=read("executor/v5/registry.go")
    assert 'const metaTrackSingleExecutionID = "metatrack_single_execution"' in registry
    assert 'MetaTrackReadyQueuePolicy() string' in registry
    assert 'func metaTrackExecutionUsesUnifiedReadyV667' in registry
    single=registry.split('type metaTrackSingleExecution struct',1)[1].split('type metaTrackReadyQueuePolicyCapability',1)[0]
    assert 'dualTrackExecution{basicPlugin:p.basicPlugin}.ClassifyBatch' not in single
    assert 'applyMetaTrackEffectiveFrontierTracks' not in single
    assert 'unified_ready_single_track_v667' in single
    assert 'result.Dependencies' in single and 'result.StateWaitKeys' in single



def test_v668_experimental_adaptive_consensus_card_is_isolated():
    front=read("frontend/src/v5MethodProfile.ts")
    full=method_block(front,"metatrack_latest","metatrack_unified")
    exp=method_block(front,"metatrack_unified","metatrack_ab_route")
    fp=plugins(full); ep=plugins(exp)
    changed={k for k in set(fp)|set(ep) if fp.get(k)!=ep.get(k)}
    assert changed=={"block_producer"}
    assert fp["block_producer"]=="metatrack_dependency_window_producer"
    assert ep["block_producer"]=="metatrack_adaptive_window_producer"
    assert config_text(exp)==config_text(full)
    assert 'role: "custom"' in exp
    assert 'role: "experiment"' not in exp
    backend=read("backend/app/services/v5_formal_plan_validator.py")
    exp_backend=backend.split('"metatrack_unified": V5FormalMethod(',1)[1].split('"metatrack_ab_route": V5FormalMethod(',1)[0]
    assert 'role="custom"' in exp_backend
    assert 'role="experiment"' not in exp_backend
    catalog=read("frontend/src/v5FormalExperimentCatalog.ts")
    ids=catalog.split("export const METATRACK_ABLATION_METHOD_IDS = [",1)[1].split("] as const;",1)[0]
    assert '"metatrack_unified"' not in ids


def test_v668_experimental_producer_restores_original_n_over_l_rule_only():
    registry=read("executor/v5/registry.go")
    planner=read("executor/v5/metatrack_critical_width_window_v6568.go")
    assert 'const metaTrackAdaptiveWindowProducerID = "metatrack_adaptive_window_producer"' in registry
    assert 'PushBatchAdaptiveNLV668' in registry
    adaptive=planner.split('func (p *metaTrackCriticalWidthWindowPlannerV6568) PushBatchAdaptiveNLV668',1)[1].split('func (p *metaTrackCriticalWidthWindowPlannerV6568) PushBatchFixedRouteBatchV663',1)[0]
    assert 'metaTrackCriticalWidthImprovesV6568(p.transactionCount, p.criticalPath, candidateN, candidateL)' in adaptive
    assert 'metaTrackCountsFitBlockV6568(candidateCounts, blockLimit)' in adaptive
    full=planner.split('func (p *metaTrackCriticalWidthWindowPlannerV6568) PushBatch(',1)[1].split('func (p *metaTrackCriticalWidthWindowPlannerV6568) Flush',1)[0]
    assert 'metaTrackCriticalPathPreservingJoinV661' in full
