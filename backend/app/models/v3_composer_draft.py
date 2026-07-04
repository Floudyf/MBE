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
    process_runtime_mode: str = "dry_run"
    max_local_processes: int = 8
    enable_committee_epoch: bool = True
    epoch_count: int = 1
    network_mode: str = "in_memory_message_bus"
    network_adapter: str = "in_memory_message_bus"
    cross_shard_protocol: str = "none"
    relay_failure_mode: str = "none"
    relay_force_proof_fail_every_n: int = 0
    relay_force_timeout_every_n: int = 0
    relay_timeout_ms: int = 0
    state_backend: str = "memory_kv"
    benchmark_template: str = "full_stack_v3_template"
    baseline_profile: str = "baseline_simple_chain"
    repeat_count: int = 1


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
