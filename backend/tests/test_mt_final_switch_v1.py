"""Final MetaTrack P2 public method and three-core ablation contract."""
from pathlib import Path
from backend.app.services.v5_formal_plan_validator import BUILTIN_METHODS
from backend.app.services.v5_plugin_manifest_store import STORE

ROOT = Path(__file__).resolve().parents[2]
IDS = ("metatrack_latest", "metatrack_ab_flow", "metatrack_ab_supply", "metatrack_ab_cons")

def test_official_three_core_switch():
    full, flow, supply, cons = (BUILTIN_METHODS[mid] for mid in IDS)
    assert full.display_name == "MetaTrack"
    for m in (full, supply, cons):
        assert m.plugin_config_overrides["routing"]["load_iso_shadow_v1"] is True
        assert m.plugin_config_overrides["routing"]["bounded_batch_routing_v1"] is True
    assert flow.plugin_config_overrides["routing"]["ablation_hash_routing_v662"] is True
    assert not flow.plugin_config_overrides["routing"].get("load_iso_shadow_v1",False)
    assert full.plugin_overrides == supply.plugin_overrides | {"state_access":"metatrack_local_exact_access"}
    assert cons.plugin_overrides["block_producer"] == "time_or_count_block_producer"
    assert full.plugin_overrides["block_producer"] == "metatrack_nl_window_v669"
    assert full.plugin_overrides["routing"] == "metatrack_coaccess_routing"
    assert all(m.plugin_overrides["routing"] == full.plugin_overrides["routing"] for m in (flow,supply,cons))
    assert full.plugin_config_overrides["block_executor"] == flow.plugin_config_overrides["block_executor"] == cons.plugin_config_overrides["block_executor"]
    assert full.plugin_config_overrides["block_executor"] != supply.plugin_config_overrides["block_executor"]
    schema=STORE.get("metatrack_coaccess_routing").config_schema["properties"]
    assert "load_iso_shadow_v1" in schema and "bounded_batch_routing_v1" in schema

def test_frontend_method_and_visible_cards():
    profile=(ROOT/"frontend/src/v5MethodProfile.ts").read_text(encoding="utf8")
    catalog=(ROOT/"frontend/src/v5FormalExperimentCatalog.ts").read_text(encoding="utf8")
    for mid in IDS:
        f=profile.split('method_id: "'+mid+'"',1)[1].split('\n  },',1)[0]
        b=BUILTIN_METHODS[mid].plugin_config_overrides["routing"]
        assert ('load_iso_shadow_v1: true' in f)==(b.get("load_iso_shadow_v1") is True)
    for line in catalog.splitlines():
        if 'family: "metatrack"' not in line or 'methodId:' not in line:continue
        if 'methodId: "metatrack_latest"' in line:
            assert 'comparisonVisible: true' in line and 'mainVisible: true' in line
        else:
            assert 'comparisonVisible: true' not in line and 'mainVisible: true' not in line
