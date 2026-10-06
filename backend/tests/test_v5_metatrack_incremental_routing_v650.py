from pathlib import Path

def test_v675_routing_ablation_uses_historical_frequency_load_only_no_coaccess_path():
    registry=Path("executor/v5/registry.go").read_text(encoding="utf-8")
    assert "planNoCoaccessShardingV675" in registry
    assert "historical_frequency_load_only_no_coaccess_v675" in registry
    path=registry.split("func (p *metaTrackRouting) planNoCoaccessShardingV675",1)[1].split("func chooseAdmissibleStatePlacement",1)[0]
    assert "CoaccessEdges = append" not in path
    assert "topCooccurNeighbors" not in path
    assert "coaccessLocalityGainForCandidate" not in path
    assert "leastLoadedShard" in path
    assert "metaTrackNoCoaccessExecutionShardV675" in path
    front=Path("frontend/src/v5MethodProfile.ts").read_text(encoding="utf-8")
    full=front.split('method_id: "metatrack_latest"',1)[1].split('method_id: "metatrack_unified"',1)[0]
    route=front.split('method_id: "metatrack_ab_route"',1)[1].split('method_id: "metatrack_ab_track"',1)[0]
    assert 'routing: "metatrack_coaccess_routing"' in full
    assert 'routing: "metatrack_coaccess_routing"' in route
    assert "ablation_no_coaccess_sharding_v675: true" not in full
    assert "ablation_no_coaccess_sharding_v675: true" in route
    assert "ablation_ignore_coaccess_routing_v661: true" not in route
    assert 'block_producer: "metatrack_nl_window_v669"' in route
