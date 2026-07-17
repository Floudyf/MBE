from __future__ import annotations

from backend.app.models.v5_experiment_spec import V5PluginSelection
from backend.app.services.v5_plugin_manifest_store import CATEGORIES, STORE


_LEGACY_CATEGORY_MAP = {
    "routing": "routing", "execution": "execution", "scheduler": "scheduler", "state_access": "state_access",
    "commit": "commit", "consensus": "consensus", "workload": "workload", "sharding": "sharding",
}


def adapt_saved_method(config: dict) -> tuple[list[V5PluginSelection], list[str]]:
    """Adapt V3SavedConfig payloads at compile time without mutating stored templates."""
    payload = config.get("payload", config)
    direct = payload.get("plugin_selections")
    if isinstance(direct, list):
        result = [V5PluginSelection(**item) for item in direct]
        if not any(item.category == "block_executor" for item in result):
            result.append(_default_selection("block_executor", migrated=True))
        return result, []
    selections: dict[str, str] = {}
    for source in (payload.get("module_plugins", {}), payload.get("plugin_selection", {}), payload.get("modules", {})):
        if isinstance(source, dict):
            for category, plugin_id in source.items():
                if category in _LEGACY_CATEGORY_MAP and isinstance(plugin_id, str):
                    selections[_LEGACY_CATEGORY_MAP[category]] = plugin_id
    warnings: list[str] = []
    result: list[V5PluginSelection] = []
    for category in CATEGORIES:
        candidate = selections.get(category)
        if candidate:
            try:
                manifest = STORE.get(candidate)
                if manifest.category == category:
                    result.append(V5PluginSelection(category=category, plugin_id=manifest.plugin_id))
                    continue
            except ValueError:
                warnings.append(f"legacy plugin {candidate} is blocked for real_cluster")
        defaults = [item for item in STORE.list() if item.category == category and item.implementation_status == "implemented"]
        result.append(_default_selection(category, migrated=category == "block_executor"))
    return result, warnings


def _default_selection(category: str, *, migrated: bool = False) -> V5PluginSelection:
    defaults = [item for item in STORE.list() if item.category == category and item.implementation_status == "implemented"]
    config = dict(defaults[0].default_config)
    if migrated:
        config["migrated_default"] = True
    return V5PluginSelection(category=category, plugin_id=defaults[0].plugin_id, config=config)
