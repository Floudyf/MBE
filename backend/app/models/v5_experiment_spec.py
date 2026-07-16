from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, Field, model_validator

from backend.app.models.v5_plugin import V5Backend


class V5PluginSelection(BaseModel):
    category: str
    plugin_id: str
    config: dict[str, Any] = Field(default_factory=dict)


class V5Topology(BaseModel):
    nodes: int = Field(ge=1, le=16)
    shards: int = Field(ge=1, le=4)
    validators_per_shard: int = Field(ge=1, le=16)


class V5WorkloadSourceSpec(BaseModel):
    """Run-level workload source. Method profiles never own workload data."""
    schema_version: Literal["mbe_workload_source_v1"] = "mbe_workload_source_v1"
    source_type: Literal["synthetic", "dataset"] = "synthetic"
    plugin_id: Literal["deterministic_signed_synthetic", "canonical_trace_replay"] = "deterministic_signed_synthetic"
    dataset_id: str | None = None
    variant_mode: Literal["original_window", "contract_zipf"] | None = None
    variant_id: str | None = None
    requested_tx_count: int = Field(ge=1, le=271868)
    use_full_dataset: bool = False
    seed: int
    selection_mode: Literal["contiguous_window"] = "contiguous_window"
    replay_mode: Literal["max_throughput"] = "max_throughput"
    skew_axis: Literal["contract"] | None = None
    target_alpha: float | None = None
    materialized_id: str | None = None
    source_sha256: str | None = None

    @model_validator(mode="after")
    def _validate_dataset_shape(self) -> "V5WorkloadSourceSpec":
        if self.source_type == "synthetic":
            if self.plugin_id != "deterministic_signed_synthetic":
                raise ValueError("synthetic workload_source requires deterministic_signed_synthetic")
            if self.dataset_id or self.source_sha256:
                raise ValueError("synthetic workload_source must not carry dataset provenance")
            if self.target_alpha is not None:
                raise ValueError("synthetic workload_source must not carry target_alpha")
            return self
        if self.plugin_id != "canonical_trace_replay" or not self.dataset_id or not self.source_sha256:
            raise ValueError("dataset workload_source requires dataset_id, source_sha256, and canonical_trace_replay")
        if self.variant_mode is None:
            self.variant_mode = "original_window"
        if self.variant_mode == "original_window" and self.target_alpha is not None:
            raise ValueError("original_window workload_source does not allow target_alpha")
        if self.variant_mode == "contract_zipf" and self.target_alpha is None:
            raise ValueError("contract_zipf workload_source requires target_alpha")
        if self.variant_mode == "contract_zipf" and self.target_alpha not in {0.0, 0.2, 0.4, 0.6, 0.8, 1.0, 1.2, 1.4}:
            raise ValueError("contract_zipf workload_source target_alpha is not supported")
        if self.variant_mode == "contract_zipf" and self.skew_axis != "contract":
            raise ValueError("contract_zipf workload_source requires skew_axis=contract")
        if self.use_full_dataset and self.requested_tx_count < 1:
            raise ValueError("full dataset workload_source requires a positive requested_tx_count mirror")
        return self


class V5ExperimentSpec(BaseModel):
    schema_version: Literal["v5_experiment_spec_v1"] = "v5_experiment_spec_v1"
    name: str = Field(default="v5_real_cluster_validation", min_length=1, max_length=160)
    execution_backend: V5Backend = "preview"
    plugin_selections: list[V5PluginSelection]
    topology: V5Topology
    tx_count: int = Field(default=100, ge=1, le=271868)
    seed: int = 1
    workload_source: V5WorkloadSourceSpec | None = None
    duration_ms: int = Field(default=5000, ge=1000, le=3600000)
    fault_policy: dict[str, Any] = Field(default_factory=lambda: {"mode": "disabled"})
    requested_metrics: list[str] = Field(default_factory=list)
    saved_config_id: str | None = None
    formal_plan_config_id: str | None = None
    method_config_id: str | None = None
    source_composer_draft: dict[str, Any] = Field(default_factory=dict)

    @model_validator(mode="after")
    def _normalize_workload_source(self) -> "V5ExperimentSpec":
        if self.workload_source is None:
            self.workload_source = V5WorkloadSourceSpec(requested_tx_count=self.tx_count, seed=self.seed)
        if self.workload_source.requested_tx_count != self.tx_count or self.workload_source.seed != self.seed:
            raise ValueError("top-level tx_count and seed must match workload_source compatibility mirrors")
        return self


class V5CompatibilityResult(BaseModel):
    valid: bool
    blockers: list[str] = Field(default_factory=list)
    warnings: list[str] = Field(default_factory=list)
    resolved_plugins: list[V5PluginSelection] = Field(default_factory=list)
    resource_estimate: dict[str, Any] = Field(default_factory=dict)
