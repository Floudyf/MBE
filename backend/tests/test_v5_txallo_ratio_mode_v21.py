from backend.app.services.v5_plugin_manifest_store import STORE

def test_txallo_ratio_mode_schema_matches_runtime_contract():
    manifest = STORE.get("txallo_account_sharding")
    props = manifest.config_schema["properties"]
    field = props["allocation_mode"]
    assert field["enum"] == ["paper_g_ratio_snapshot"]
    assert field["default"] == "paper_g_ratio_snapshot"
    assert manifest.default_config["allocation_mode"] == "paper_g_ratio_snapshot"
    assert manifest.default_config["history_ratio"] == 0.10
