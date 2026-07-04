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
    metaverse_suite_enabled: bool = False
    metaverse_scenario: str = "mixed_metaverse"
    user_count: int = 100
    asset_count: int = 1000
    item_count: int = 1000
    avatar_count: int = 100
    scene_count: int = 16
    metaverse_count: int = 2
    tx_count: int = 10000
    seed: int = 42
    hotspot_ratio: float = 0.2
    cross_scene_ratio: float = 0.15
    cross_shard_ratio: float = 0.2
    burst_rate: float = 0.0
    read_write_ratio: float = 0.3
    asset_skew: float = 0.2
    scene_skew: float = 0.2
    offchain_confirmation_enabled: bool = True
    offchain_confirm_delay_ms: int = 100
    offchain_failure_ratio: float = 0.0
    cross_metaverse_enabled: bool = True
    benchmark_suite_enabled: bool = False
    baseline_matrix_enabled: bool = False
    multi_seed_enabled: bool = False
    paper_export_enabled: bool = False
    sweep_seed_count: int = 3
    sweep_shard_counts: list[int] = Field(default_factory=lambda: [1, 2, 4])
    sweep_cross_shard_ratios: list[float] = Field(default_factory=lambda: [0.0, 0.2, 0.5])
    sweep_hotspot_ratios: list[float] = Field(default_factory=lambda: [0.0, 0.2, 0.5])


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
