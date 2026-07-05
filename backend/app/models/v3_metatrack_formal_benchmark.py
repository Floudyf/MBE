from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, Field

from backend.app.models.v3_composer_draft import V3ComposerDraftRequest


V3FormalExperimentType = Literal[
    "ablation",
    "hotspot_sensitivity",
    "cross_shard_sensitivity",
    "shard_scalability",
    "control_overhead",
    "workload_comparison",
]

V3RuntimeEvidenceMode = Literal["logical_single_process", "local_multi_process_validation"]


class V3FormalMetatrackBenchmarkRequest(BaseModel):
    draft: V3ComposerDraftRequest
    experiment_type: V3FormalExperimentType = "ablation"
    formal_tx_count: int = Field(default=10000, ge=1000, le=1000000)
    seed_base: int = 42
    seed_count: int = Field(default=5, ge=1, le=10)
    baseline_ids: list[str] = Field(default_factory=lambda: [
        "baseline_hash_serial",
        "baseline_hash_prefetch",
        "baseline_hash_dual_track",
        "baseline_hash_aggregation",
        "metatrack_full",
    ])
    hotspot_ratio_points: list[float] = Field(default_factory=lambda: [0.0, 0.2, 0.4, 0.6, 0.8])
    cross_shard_ratio_points: list[float] = Field(default_factory=lambda: [0.0, 0.2, 0.4, 0.6])
    shard_count_points: list[int] = Field(default_factory=lambda: [1, 2, 4, 8])
    workload_scenario_points: list[str] = Field(default_factory=lambda: ["scene_hotspot", "cross_scene_migration", "mixed_metaverse"])
    method_config_ids: list[str] = Field(default_factory=list)
    workload_config_ids: list[str] = Field(default_factory=list)
    topology_config_ids: list[str] = Field(default_factory=list)
    zipf_alpha: float = Field(default=0.8, ge=0.0, le=2.0)
    runtime_evidence_mode: V3RuntimeEvidenceMode = "logical_single_process"
    enable_faults_for_formal_run: bool = False
    max_run_count: int = 200
    max_total_tx_count: int = 20000000


class V3FormalMetatrackBenchmarkPreview(BaseModel):
    is_valid: bool
    is_runnable: bool
    errors: list[str] = Field(default_factory=list)
    warnings: list[str] = Field(default_factory=list)
    matrix: list[dict] = Field(default_factory=list)
    seed_list: list[int] = Field(default_factory=list)
    run_count: int = 0
    total_tx_count: int = 0
    baseline_count: int = 0
    scan_point_count: int = 0
    experiment_type: str = "ablation"
    runtime_evidence_mode: str = "logical_single_process"
    contains_preview_or_planned_plugin: bool = False
    exceeds_recommended_range: bool = False
    truth_boundary: str = "local_emulator_controlled_benchmark_not_production_chain"


class V3FormalMetatrackBenchmarkRunResponse(BaseModel):
    run_id: str
    status: str
    run_mode: str = "formal_metatrack_benchmark"
    output_dir: str
    summary: dict
    preview: dict
    artifacts: list[dict] = Field(default_factory=list)
