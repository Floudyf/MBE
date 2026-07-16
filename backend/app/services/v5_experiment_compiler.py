from __future__ import annotations

import hashlib
import json
from pathlib import Path

from backend.app.models.v5_compiled_run_plan import V5CompiledNodeConfig, V5CompiledRunPlan
from backend.app.models.v5_experiment_spec import V5ExperimentSpec
from backend.app.services.v5_compatibility_engine import validate
from backend.app.services.v5_plugin_manifest_store import STORE
from backend.app.services import v5_workload_data_plane as workload_plane
from backend.app.services.v5_workload_data_plane import WorkloadPreviewRequest


EXPECTED_ARTIFACTS = ["compiled_run_plan.json", "process_manifest.json", "real_cluster_summary.json", "client_submission_log.csv", "artifact_catalog.json"]
DATASET_ARTIFACTS = [
    "workload_manifest_snapshot.json",
    "workload_source_spec.json",
    "workload_selection.json",
    "workload_skew_report.json",
    "workload_materialization_summary.json",
    "workload_identity_mapping_summary.json",
    "workload_replay_summary.json",
]


def requested_cross_shard_count(tx_count: int, ratio: float) -> int:
    return int(tx_count * ratio + 0.5)


def compile_plan(spec: V5ExperimentSpec, run_dir: Path, *, source_saved_config_id: str | None = None) -> V5CompiledRunPlan:
    compatibility = validate(spec)
    if not compatibility.valid:
        raise ValueError("; ".join(compatibility.blockers))
    normalized = spec.model_dump()
    raw = json.dumps(normalized, sort_keys=True, separators=(",", ":"))
    digest = hashlib.sha256(raw.encode("utf-8")).hexdigest()
    profile = {selection.category: {"plugin_id": selection.plugin_id, "config": selection.config} for selection in compatibility.resolved_plugins}
    nodes: list[V5CompiledNodeConfig] = []
    for index in range(spec.topology.nodes):
        shard_index = index // spec.topology.validators_per_shard
        node_id = f"n{index}"
        validators = [f"n{shard_index * spec.topology.validators_per_shard + offset}" for offset in range(spec.topology.validators_per_shard)]
        nodes.append(V5CompiledNodeConfig(node_id=node_id, shard_id=f"s{shard_index}", role="leader" if node_id == validators[0] else "validator", leader=node_id == validators[0], listen_addr="127.0.0.1:0", data_dir=str(run_dir / "nodes" / node_id), validators=validators, plugin_profile=profile))
    snapshot = [STORE.get(item.plugin_id).model_dump() | {"selected_config": item.config} for item in compatibility.resolved_plugins]
    workload = _compile_workload_plan(spec, profile, run_dir)
    expected_artifacts = EXPECTED_ARTIFACTS + (DATASET_ARTIFACTS if workload.get("source_type") == "dataset" else [])
    return V5CompiledRunPlan(
        plan_id=f"v5plan_{digest[:16]}", plan_digest=digest,
        execution_backend=spec.execution_backend, duration_ms=spec.duration_ms,
        source_saved_config_id=source_saved_config_id,
        formal_plan_config_id=spec.formal_plan_config_id,
        method_config_id=spec.method_config_id,
        experiment_spec=normalized, plugin_snapshot=snapshot, node_configs=nodes,
        workload_plan=workload, fault_plan=spec.fault_policy,
        expected_artifacts=expected_artifacts, resource_estimate=compatibility.resource_estimate,
    )


def _compile_workload_plan(spec: V5ExperimentSpec, profile: dict[str, dict], run_dir: Path) -> dict[str, object]:
    source = spec.workload_source
    if source is None:
        raise ValueError("normalized workload_source is required")
    if source.source_type == "synthetic":
        workload_config = profile["workload"]["config"]
        ratio = float(workload_config.get("cross_shard_ratio", 0.0))
        if not 0 <= ratio <= 1:
            raise ValueError("cross_shard_ratio must be between 0 and 1")
        if ratio > 0 and spec.topology.shards < 2:
            raise ValueError("cross_shard_ratio requires at least 2 shards")
        return workload_config | {
            "compiled_workload_plan_version": "mbe_compiled_workload_plan_v1",
            "plugin_id": "deterministic_signed_synthetic",
            "source_type": "synthetic",
            "tx_count": source.requested_tx_count,
            "requested_tx_count": source.requested_tx_count,
            "actual_tx_count": source.requested_tx_count,
            "seed": source.seed,
            "requested_cross_shard_ratio": ratio,
            "requested_cross_shard_count": requested_cross_shard_count(source.requested_tx_count, ratio),
            "truth_label": "synthetic_generated",
            "no_fallback": True,
        }
    if profile["workload"]["plugin_id"] != "canonical_trace_replay":
        raise ValueError("dataset workload_source requires canonical_trace_replay workload plugin")
    request = WorkloadPreviewRequest(
        source_type="dataset",
        plugin_id="canonical_trace_replay",
        dataset_id=source.dataset_id,
        requested_tx_count=source.requested_tx_count,
        use_full_dataset=source.use_full_dataset,
        seed=source.seed,
        variant_mode=source.variant_mode,
        target_alpha=source.target_alpha,
        skew_axis=source.skew_axis,
        source_sha256=source.source_sha256,
    )
    materialized = workload_plane.materialize_request(request)
    manifest = workload_plane.load_manifest(source.dataset_id or "")
    artifacts = workload_plane.workload_artifact_snapshots(source.model_dump(), materialized.summary | materialized.model_dump(), manifest)
    run_dir.mkdir(parents=True, exist_ok=True)
    for name, payload in artifacts.items():
        (run_dir / name).write_text(json.dumps(payload, sort_keys=True, indent=2) + "\n", encoding="utf-8")
    expected_cross = materialized.summary.get("expected_cross_shard_count", 0)
    actual = int(materialized.actual_tx_count)
    plan = {
        "compiled_workload_plan_version": "mbe_compiled_workload_plan_v1",
        "plugin_id": "canonical_trace_replay",
        "source_type": "dataset",
        "dataset_id": materialized.dataset_id,
        "variant_id": materialized.variant_id,
        "variant_mode": materialized.variant_mode,
        "materialized_id": materialized.materialized_id,
        "canonical_relative_path": materialized.canonical_relative_path,
        "materialized_relative_path": materialized.materialized_relative_path,
        "source_sha256": materialized.source_sha256,
        "canonical_sha256": materialized.canonical_sha256,
        "materialized_sha256": materialized.materialized_sha256,
        "requested_tx_count": materialized.requested_tx_count,
        "actual_tx_count": actual,
        "tx_count": actual,
        "seed": materialized.seed,
        "truth_label": "real_observed" if materialized.variant_mode == "original_window" else "real_derived_resampled",
        "selection_mode": source.selection_mode,
        "replay_mode": source.replay_mode,
        "skew_axis": materialized.summary.get("skew_axis") or source.skew_axis,
        "target_alpha": materialized.target_alpha,
        "realized_skew": {
            "gini": materialized.summary.get("gini"),
            "hhi": materialized.summary.get("hhi"),
            "top_1_ratio": materialized.summary.get("top_1_ratio"),
        },
        "base_window_sha256": materialized.summary.get("base_window_sha256"),
        "base_window_hash": materialized.summary.get("base_window_sha256"),
        "expected_cross_shard_count": expected_cross,
        "expected_cross_shard_ratio": (float(expected_cross) / actual) if actual else 0,
        "identity_mapping_version": "mbe_dataset_identity_v1",
        "generator_version": workload_plane.GENERATOR_VERSION,
        "no_fallback": True,
    }
    (run_dir / "compiled_workload_plan.json").write_text(json.dumps(plan, sort_keys=True, indent=2) + "\n", encoding="utf-8")
    return plan
