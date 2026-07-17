from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, Field


V5Backend = Literal["preview", "simulation", "real_cluster"]
V5PluginStatus = Literal["implemented", "experimental", "planned", "blocked"]


class V5MetricMetadata(BaseModel):
    key: str
    type: Literal["number", "integer", "ratio", "string", "time_series"] = "number"
    unit: str = ""
    aggregation: str = "last"
    visualization: str = "summary"
    description: str = ""


class V5PluginManifest(BaseModel):
    plugin_id: str
    category: str
    version: str = "1.0.0"
    display_name: str
    description: str
    display_name_zh: str = ""
    description_zh: str = ""
    implementation_status: V5PluginStatus = "implemented"
    supported_backends: list[V5Backend] = Field(default_factory=lambda: ["preview"])
    config_schema: dict[str, Any] = Field(default_factory=lambda: {"type": "object", "properties": {}})
    default_config: dict[str, Any] = Field(default_factory=dict)
    capabilities: list[str] = Field(default_factory=list)
    requirements: list[str] = Field(default_factory=list)
    conflicts: list[str] = Field(default_factory=list)
    metrics: list[V5MetricMetadata] = Field(default_factory=list)
    runtime_factory: str = ""
    runtime_adapter: str = ""
    truth_boundary: str = "v5_real_cluster_candidate"
    source: dict[str, Any] = Field(default_factory=dict)
    legacy_aliases: list[str] = Field(default_factory=list)
    schema_version: str = "v5_plugin_manifest_v1"
