from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field


class V5CompiledNodeConfig(BaseModel):
    node_id: str
    shard_id: str
    role: str
    leader: bool
    listen_addr: str
    data_dir: str
    validators: list[str]
    plugin_profile: dict[str, dict[str, Any]]


class V5CompiledRunPlan(BaseModel):
    schema_version: str = "v5_compiled_run_plan_v1"
    plan_id: str
    plan_digest: str
    runtime_stage: str = "v5_1_real_plugin_driven_multi_process_multishard_runtime"
    runtime_truth: str = "v5_real_cluster_candidate"
    execution_backend: str
    duration_ms: int
    source_saved_config_id: str | None = None
    formal_plan_config_id: str | None = None
    method_config_id: str | None = None
    experiment_spec: dict[str, Any]
    plugin_snapshot: list[dict[str, Any]]
    node_configs: list[V5CompiledNodeConfig]
    workload_plan: dict[str, Any]
    fault_plan: dict[str, Any]
    expected_artifacts: list[str]
    resource_estimate: dict[str, Any]
    no_fallback: bool = True
