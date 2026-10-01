from pathlib import Path


def test_v661_incremental_routing_ablation_neutralizes_only_coaccess():
    front=Path("frontend/src/v5MethodProfile.ts").read_text(encoding="utf-8")
    full=front.split('method_id: "metatrack_latest"',1)[1].split('method_id: "metatrack_ab_route"',1)[0]
    route=front.split('method_id: "metatrack_ab_route"',1)[1].split('method_id: "metatrack_ab_track"',1)[0]
    assert 'incremental_exact_continuity_routing_v65: true' in full and 'incremental_exact_continuity_routing_v65: true' in route
    assert 'ablation_ignore_coaccess_routing_v661: true' not in full
    assert 'ablation_ignore_coaccess_routing_v661: true' in route
    assert 'ablation_ignore_exact_ready_routing_v660: true' not in route
