from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, Field

from backend.app.models.v5_experiment_spec import V5ExperimentSpec

V5Suite = Literal["main_experiment", "comparison_experiment", "ablation_experiment", "workload_sensitivity", "topology_scaling", "fault_recovery_experiment"]


class V5FormalMethod(BaseModel):
    method_id: str = Field(min_length=1, max_length=120)
    display_name: str = Field(min_length=1, max_length=160)
    plugin_overrides: dict[str, str] = Field(default_factory=dict)
    role: Literal["main", "baseline", "ablation", "custom"] = "custom"


class V5FormalExperimentPlan(BaseModel):
    name: str = Field(min_length=1, max_length=160)
    saved_config_id: str = ""
    base_spec: V5ExperimentSpec
    suites: list[V5Suite] = Field(default_factory=lambda: ["main_experiment"])
    methods: list[V5FormalMethod] = Field(default_factory=list)
    seeds: list[int] = Field(default_factory=lambda: [42])
    repeats: int = Field(default=1, ge=1, le=20)
    topology_points: list[dict[str, int]] = Field(default_factory=list)
    workload_points: list[dict[str, int | float]] = Field(default_factory=list)
    fault_points: list[dict[str, object]] = Field(default_factory=list)
    source_label: Literal["user", "e2e", "script"] = "user"
    tags: list[str] = Field(default_factory=list)


class V5FormalRunRequest(BaseModel):
    execution_backend: Literal["preview", "simulation", "real_cluster"] = "preview"
    plan: V5FormalExperimentPlan
