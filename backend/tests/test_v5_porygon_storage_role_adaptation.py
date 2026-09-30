from pathlib import Path
from backend.app.services.v5_formal_plan_validator import PORYGON_BUILTIN_METHODS
from backend.app.services.v5_plugin_manifest_store import PluginManifestStore


from backend.app.services.v5_formal_plan_validator import ALL_BUILTIN_METHODS
def test_porygon_uses_partition_state_store_without_touching_other_profiles():
    method = PORYGON_BUILTIN_METHODS["stateless_porygon"]
    assert method.plugin_overrides["state_storage"] == "porygon_partition_state_store"
    assert method.plugin_overrides["routing"] == "porygon_stateless_routing"
    assert method.plugin_overrides["block_executor"] == "porygon_block_executor"


def test_porygon_partition_state_store_manifest_is_private_to_porygon():
    manifests = PluginManifestStore().list()
    matches = [m for m in manifests if m.plugin_id == "porygon_partition_state_store"]
    assert len(matches) == 1
    manifest = matches[0]
    assert manifest.category == "state_storage"
    assert "execution_shard_storage_identity" in manifest.capabilities
    assert manifest.requirements == ["routing:porygon_stateless_routing"]


def test_porygon_frontend_builtin_state_storage_matches_backend_registry() -> None:
    root = Path(__file__).resolve().parents[2]
    source = (root / "frontend" / "src" / "v5MethodProfile.ts").read_text(encoding="utf-8")
    line = next(item for item in source.splitlines() if 'method_id: "stateless_porygon"' in item)
    assert 'state_storage: "porygon_partition_state_store"' in line
    method = ALL_BUILTIN_METHODS["stateless_porygon"]
    assert method.plugin_overrides["state_storage"] == "porygon_partition_state_store"
