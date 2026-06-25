from fastapi.testclient import TestClient

from backend.app.main import app
from backend.app.services.plugin_registry import REQUIRED_PLUGIN_FIELDS, load_registry


client = TestClient(app)


def test_v2_plugin_registry_loads_and_declares_required_shape() -> None:
    registry = load_registry()

    assert registry.version == "v2.1"
    assert registry.stage == "V2.1"
    assert registry.plugins
    for plugin in registry.plugins:
        assert REQUIRED_PLUGIN_FIELDS <= plugin.keys()
        assert {"trace_fields", "capabilities"} <= plugin["requires"].keys()
        assert {"capabilities"} <= plugin["provides"].keys()
        if plugin["status"] == "planned":
            assert plugin["reason"]


def test_v2_plugins_api_lists_all_and_filters_by_type() -> None:
    all_response = client.get("/api/v2/plugins")
    topology_response = client.get("/api/v2/plugins/topology")

    assert all_response.status_code == 200
    plugins = all_response.json()["plugins"]
    assert {plugin["type"] for plugin in plugins} >= {"topology", "trace_source", "workload", "routing", "execution", "commit", "cross_chain_protocol", "metrics"}
    assert topology_response.status_code == 200
    topology_names = {plugin["name"] for plugin in topology_response.json()["plugins"]}
    assert {"single_chain", "dual_chain", "cross_chain_replay", "multi_chain"} <= topology_names


def test_v2_plugins_api_unknown_type_returns_empty_list() -> None:
    response = client.get("/api/v2/plugins/not_a_type")

    assert response.status_code == 200
    assert response.json() == {"type": "not_a_type", "plugins": []}


def test_v2_cross_chain_protocols_are_not_runnable_except_disabled() -> None:
    registry = load_registry()
    protocols = {plugin["name"]: plugin for plugin in registry.list_plugins("cross_chain_protocol")}

    assert protocols["disabled"]["status"] == "runnable"
    for name in ("lock_mint_serial", "lock_mint_pipeline", "fixed_window_baseline", "committee_bridge_basic", "metaflow"):
        assert protocols[name]["status"] == "planned"
