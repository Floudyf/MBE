from __future__ import annotations

from pydantic import BaseModel

from backend.app.models.v4_realism import V4RealismSmokeRequest


class ExperimentProfile(BaseModel):
    profile_id: str
    label: str
    description: str
    runtime_target: str
    mechanism_tags: list[str]
    default_topology_id: str
    default_workload_id: str
    runnable: bool


class ExperimentTopology(BaseModel):
    topology_id: str
    label: str
    nodes: int
    shards: int
    validators_per_shard: int
    runtime_mode: str
    description: str
    runnable: bool


class ExperimentWorkload(BaseModel):
    workload_id: str
    label: str
    source_type: str
    scale_label: str
    skew_label: str
    description: str
    planned: bool
    default_tx_count: int
    default_blockemulator_tx_limit: int
    csv_required: bool


class ExperimentRunPlanRequest(BaseModel):
    profile_id: str
    topology_id: str
    workload_id: str
    blockemulator_csv: str | None = None
    tx_count_override: int | None = None
    fault_profile_override: str | None = None


class ExperimentRunPlanPreview(BaseModel):
    profile: ExperimentProfile
    topology: ExperimentTopology
    workload: ExperimentWorkload
    runtime: str
    recommended_v4_request: V4RealismSmokeRequest
    runnable: bool
    warnings: list[str]
    next_step: str

