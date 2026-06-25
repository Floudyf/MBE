from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml

ROOT = Path(__file__).resolve().parents[3]
DEFAULT_REGISTRY_PATH = ROOT / "configs/plugins/v2_plugin_registry.yaml"

ALLOWED_STATUS = {"runnable", "planned", "experimental", "invalid"}
ALLOWED_MATURITY = {"stable", "experimental", "planned"}
REQUIRED_PLUGIN_FIELDS = {
    "name",
    "type",
    "version",
    "status",
    "maturity",
    "description",
    "requires",
    "provides",
    "compatible_topologies",
    "compatible_trace_sources",
    "forbidden_with",
    "metrics",
    "reason",
}


class PluginRegistryError(ValueError):
    """Raised when the V2 plugin registry declaration is invalid."""


@dataclass(frozen=True)
class PluginRegistry:
    version: str
    stage: str
    plugins: list[dict[str, Any]]

    def list_plugins(self, plugin_type: str | None = None) -> list[dict[str, Any]]:
        if plugin_type is None:
            return list(self.plugins)
        return [plugin for plugin in self.plugins if plugin["type"] == plugin_type]

    def get_plugin(self, plugin_type: str, name: str) -> dict[str, Any]:
        for plugin in self.plugins:
            if plugin["type"] == plugin_type and plugin["name"] == name:
                return plugin
        raise KeyError(f"unknown plugin {plugin_type}/{name}")


def load_registry(path: Path = DEFAULT_REGISTRY_PATH) -> PluginRegistry:
    try:
        document = yaml.safe_load(path.read_text(encoding="utf-8"))
    except OSError as exc:
        raise PluginRegistryError(f"cannot load plugin registry: {path}") from exc
    if not isinstance(document, dict):
        raise PluginRegistryError("plugin registry must be a mapping")
    plugins = document.get("plugins")
    if not isinstance(plugins, list) or not plugins:
        raise PluginRegistryError("plugin registry must contain a non-empty plugins list")
    for index, plugin in enumerate(plugins):
        validate_plugin_declaration(plugin, index)
    return PluginRegistry(version=str(document.get("version", "")), stage=str(document.get("stage", "")), plugins=plugins)


def validate_plugin_declaration(plugin: Any, index: int = 0) -> None:
    if not isinstance(plugin, dict):
        raise PluginRegistryError(f"plugin #{index} must be a mapping")
    missing = REQUIRED_PLUGIN_FIELDS - plugin.keys()
    if missing:
        raise PluginRegistryError(f"plugin {plugin.get('name', index)} missing fields: {sorted(missing)}")
    if plugin["status"] not in ALLOWED_STATUS:
        raise PluginRegistryError(f"plugin {plugin['name']} has invalid status {plugin['status']}")
    if plugin["maturity"] not in ALLOWED_MATURITY:
        raise PluginRegistryError(f"plugin {plugin['name']} has invalid maturity {plugin['maturity']}")
    for section, nested_fields in {"requires": {"trace_fields", "capabilities"}, "provides": {"capabilities"}}.items():
        value = plugin[section]
        if not isinstance(value, dict) or not nested_fields <= value.keys():
            raise PluginRegistryError(f"plugin {plugin['name']} has invalid {section} declaration")
        for field in nested_fields:
            if not isinstance(value[field], list):
                raise PluginRegistryError(f"plugin {plugin['name']} {section}.{field} must be a list")
    for field in ("compatible_topologies", "compatible_trace_sources", "forbidden_with", "metrics"):
        if not isinstance(plugin[field], list):
            raise PluginRegistryError(f"plugin {plugin['name']} field {field} must be a list")
    if plugin["status"] == "planned" and not str(plugin["reason"]).strip():
        raise PluginRegistryError(f"planned plugin {plugin['name']} must declare reason")


def registry_payload(registry: PluginRegistry | None = None) -> dict[str, Any]:
    registry = registry or load_registry()
    return {"version": registry.version, "stage": registry.stage, "plugins": registry.plugins}
