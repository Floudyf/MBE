from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, Field


V3DraftModuleStatus = Literal["default", "fixed", "variable", "disabled", "planned", "output"]


class V3ComposerDraftModule(BaseModel):
    module_id: str
    status: V3DraftModuleStatus
    plugin: str
    params: dict[str, Any] = Field(default_factory=dict)


class V3RuntimeTopology(BaseModel):
    shard_count: int = 4
    validators_per_shard: int = 4
    executors_per_shard: int = 1
    storage_nodes_per_shard: int = 1
    supervisor_enabled: bool = True
    node_runtime_mode: str = "logical_single_process"
    network_mode: str = "in_memory_message_bus"
    network_adapter: str = "in_memory_message_bus"
    cross_shard_protocol: str = "none"


class V3ComposerDraftRequest(BaseModel):
    template_id: str
    preset_id: str | None = None
    modules: dict[str, V3ComposerDraftModule]
    topology: V3RuntimeTopology | None = None


class V3DraftValidationResponse(BaseModel):
    is_valid: bool
    is_runnable: bool
    run_mode: str = "draft_smoke"
    normalized_draft: dict[str, Any] | None = None
    variable_modules: list[str] = Field(default_factory=list)
    fixed_modules: list[str] = Field(default_factory=list)
    disabled_modules: list[str] = Field(default_factory=list)
    planned_modules: list[str] = Field(default_factory=list)
    output_modules: list[str] = Field(default_factory=list)
    errors: list[str] = Field(default_factory=list)
    warnings: list[str] = Field(default_factory=list)
