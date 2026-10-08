from pathlib import Path
import re

ROOT = Path(__file__).resolve().parents[2]

def read(path: str) -> str:
    return (ROOT / path).read_text(encoding="utf-8")

def front_block(text: str, method_id: str, next_id: str) -> str:
    return text.split(f'method_id: "{method_id}"', 1)[1].split(f'method_id: "{next_id}"', 1)[0]

def backend_block(text: str, method_id: str, next_id: str) -> str:
    return text.split(f'"{method_id}": V5FormalMethod(', 1)[1].split(f'"{next_id}": V5FormalMethod(', 1)[0]

def compact(text: str) -> str:
    return re.sub(r"\s+", "", text).lower()

def test_v671_official_v669_uses_complete_window_not_streaming_prefix():
    client = read("executor/v5/client.go")
    registry = read("executor/v5/registry.go")
    planner = read("executor/v5/metatrack_critical_width_window_v6568.go")

    assert "MBE_METATRACK_COMPLETE_WINDOW_V671_CLIENT" in client
    assert "PushMetaTrackRouteBatch(criticalWidthWindowV6568, preparedV6568" in client
    assert "PushBatchAdaptiveNLV669StreamingV2(preparedV6568" not in client
    assert "clientWindowBufferingV6568 := transactionFrontierV656Enabled && modularWindowV663 && !streamPartitionInvariantV658" in client

    assert "MBE_METATRACK_COMPLETE_WINDOW_V671_PRODUCER" in registry
    assert "selectMetaTrackCriticalWidthWindowV6568(reserved, limit, input.Proposer.ShardID, input.Pool)" in registry

    formal = planner.split("func (p *metaTrackCriticalWidthWindowPlannerV6568) PushBatchAdaptiveNLV669", 1)[1].split(
        "func (p *metaTrackCriticalWidthWindowPlannerV6568) PushBatchFixedRouteBatchV663", 1
    )[0]
    assert "metaTrackDependencyClosedJoinV662(p.records, batch)" in formal
    assert "metaTrackCriticalWidthImprovesV6568(p.transactionCount, p.criticalPath, candidateN, candidateL)" in formal
    assert "metaTrackCountsFitBlockV6568(candidateCounts, blockLimit)" in formal

def test_v671_state_locality_ablation_keeps_full_routing():
    front = read("frontend/src/v5MethodProfile.ts")
    backend = read("backend/app/services/v5_formal_plan_validator.py")

    full_f = compact(front_block(front, "metatrack_latest", "metatrack_unified"))
    state_f = compact(front_block(front, "metatrack_ab_state", "metatrack_ab_handoff"))
    full_b = compact(backend_block(backend, "metatrack_latest", "metatrack_unified"))
    state_b = compact(backend_block(backend, "metatrack_ab_state", "metatrack_ab_handoff"))

    for full, state in ((full_f, state_f), (full_b, state_b)):
        assert "ablation_no_coaccess_sharding_v675:true" not in state
        assert '"ablation_no_coaccess_sharding_v675":true' not in state
        assert "incremental_exact_continuity_routing_v65:true" in state or '"incremental_exact_continuity_routing_v65":true' in state
        assert "metatrack_home_exact_access" in state
        assert "batch_entry_state_prefetch:false" in state or '"batch_entry_state_prefetch":false' in state
        assert "ablation_on_demand_state_fetch_v661:true" in state or '"ablation_on_demand_state_fetch_v661":true' in state
        assert "batch_remote_writeback:false" in state or '"batch_remote_writeback":false' in state
        assert "final_version_batch_writeback:false" in state or '"final_version_batch_writeback":false' in state
        assert "ablation_no_coaccess_sharding_v675:true" not in full
        assert '"ablation_no_coaccess_sharding_v675":true' not in full

def test_v671_no_consensus_ablation_remains_single_projection_control():
    front = read("frontend/src/v5MethodProfile.ts")
    no_cons = compact(front_block(front, "metatrack_ab_cons", "metatrack_ab_state"))
    assert 'block_producer:"time_or_count_block_producer"' in no_cons
    assert "dependency_closed_consensus:false" in no_cons
    assert "micro_batch_size:100" in no_cons
