from __future__ import annotations

from pydantic import BaseModel, Field

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


class ExperimentMethod(BaseModel):
    method_id: str
    label: str
    role: str
    description: str
    module_overrides: dict[str, str] = Field(default_factory=dict)
    runnable: bool = True


class ExperimentSuiteRequest(BaseModel):
    plan_name: str | None = None
    selected_method_ids: list[str] = Field(default_factory=list)
    selected_suite_types: list[str] = Field(default_factory=list)
    workload_ids: list[str] = Field(default_factory=list)
    topology_ids: list[str] = Field(default_factory=list)
    seeds: list[int] = Field(default_factory=lambda: [1])
    include_v4_realism: bool = False
    composer_draft: dict | None = None
    formal_config: dict | None = None
    blockemulator_csv: str | None = None


class ExperimentMatrixRow(BaseModel):
    row_id: str
    suite_type: str
    method_id: str
    method_role: str
    workload_id: str
    topology_id: str
    seed: int
    runtime_target: str
    runnable: bool
    warnings: list[str]


class ExperimentRunMatrixPreview(BaseModel):
    plan_name: str
    suite_types: list[str]
    methods: list[ExperimentMethod]
    rows: list[ExperimentMatrixRow]
    runnable_row_count: int
    blocked_row_count: int
    warnings: list[str]
    v4_realism_candidates: list[dict]
    next_step: str


class V4DerivedRequestPreview(BaseModel):
    source: str
    v4_request: V4RealismSmokeRequest
    runnable: bool
    warnings: list[str]
