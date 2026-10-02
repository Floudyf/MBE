from pathlib import Path

def test_v663_hash_ablation_is_a_distinct_routing_plugin():
    front=Path("frontend/src/v5MethodProfile.ts").read_text(encoding="utf-8")
    full=front.split('method_id: "metatrack_latest"',1)[1].split('method_id: "metatrack_ab_route"',1)[0]
    route=front.split('method_id: "metatrack_ab_route"',1)[1].split('method_id: "metatrack_ab_track"',1)[0]
    assert 'routing: "metatrack_coaccess_routing"' in full
    assert 'routing: "metatrack_hash_routing"' in route
    assert 'ablation_' not in route
    registry=Path("executor/v5/registry.go").read_text(encoding="utf-8")
    assert 'type metaTrackHashRouting struct' in registry
    assert 'statelessHashRouting' in registry
    assert 'MetaTrackRoutingFamily() bool' in registry
