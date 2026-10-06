from backend.app.services.v5_formal_plan_validator import ALL_BUILTIN_METHODS
from backend.app.services.v5_plugin_manifest_store import STORE


def test_porygon_fix_v36_builtin_composes_admission_and_sharding_plugins() -> None:
    method = ALL_BUILTIN_METHODS["stateless_porygon"]
    assert method.plugin_overrides["transaction_admission"] == "porygon_access_admission"
    assert method.plugin_overrides["sharding"] == "porygon_object_sharding"
    assert method.plugin_overrides["routing"] == "porygon_stateless_routing"


def test_porygon_fix_v36_manifests_expose_full_plugin_family() -> None:
    expected = {
        "porygon_access_admission": "transaction_admission",
        "porygon_object_sharding": "sharding",
        "porygon_stateless_routing": "routing",
    }
    for plugin_id, category in expected.items():
        manifest = STORE.get(plugin_id)
        assert manifest.category == category
        assert manifest.supported_backends == ["real_cluster"]
        assert manifest.source.get("doi") == "10.1109/ICDE60146.2024.00153"
