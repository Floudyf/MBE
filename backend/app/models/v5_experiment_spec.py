from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, Field

from backend.app.models.v5_plugin import V5Backend


class V5PluginSelection(BaseModel):
    category: str
    plugin_id: str
    config: dict[str, Any] = Field(default_factory=dict)


class V5Topology(BaseModel):
    nodes: int = Field(ge=1, le=16)
    shards: int = Field(ge=1, le=4)
    validators_per_shard: int = Field(ge=1, le=16)


class V5ExperimentSpec(BaseModel):
    schema_version: Literal["v5_experiment_spec_v1"] = "v5_experiment_spec_v1"
    name: str = Field(default="v5_real_cluster_validation", min_length=1, max_length=160)
    execution_backend: V5Backend = "preview"
    plugin_selections: list[V5PluginSelection]
    topology: V5Topology
    tx_count: int = Field(default=100, ge=1, le=10000)
    seed: int = 1
    duration_ms: int = Field(default=5000, ge=1000, le=3600000)
    fault_policy: dict[str, Any] = Field(default_factory=lambda: {"mode": "disabled"})
    requested_metrics: list[str] = Field(default_factory=list)
    saved_config_id: str | None = None
    source_composer_draft: dict[str, Any] = Field(default_factory=dict)


class V5CompatibilityResult(BaseModel):
    valid: bool
    blockers: list[str] = Field(default_factory=list)
    warnings: list[str] = Field(default_factory=list)
    resolved_plugins: list[V5PluginSelection] = Field(default_factory=list)
    resource_estimate: dict[str, Any] = Field(default_factory=dict)
