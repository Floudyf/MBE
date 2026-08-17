from __future__ import annotations

import hashlib
import json
from pathlib import Path

from backend.app.models.v5_compiled_run_plan import V5CompiledNodeConfig, V5CompiledRunPlan
from backend.app.models.v5_experiment_spec import V5ExperimentSpec
from backend.app.services.v5_compatibility_engine import V5CompatibilityError, validate, validate_materialized_workload
from backend.app.services.v5_plugin_manifest_store import STORE
from backend.app.services import v5_workload_data_plane as workload_plane
from backend.app.services.v5_workload_data_plane import WorkloadPreviewRequest


EXPECTED_ARTIFACTS = [
    "compiled_run_plan.json",
    "process_manifest.json",
    "real_cluster_summary.json",
    "artifact_catalog.json",
    "finality_summary.json",
    "transaction_lifecycle.jsonl",
    "transaction_lifecycle.csv",
    "transaction_finality.csv",
    "client_receipt_log.csv",
    "drain_status.json",
    "throughput_windows.csv",
    "physical_remote_state_operations.csv",
    "client/client_submission_log.csv",
    "client/client_lifecycle.csv",
    "client/resolved_access_lists.jsonl.gz",
    "client/workload_replay_summary.json",
    "client/workload_identity_mapping_summary.json",
    "aggregate/block_production_summary.json",
    "aggregate/mechanism_metrics_summary.json",
    "aggregate/remote_state_metrics_summary.json",
    "aggregate/replica_deduplicated_remote_operations.csv",
    "aggregate/metatrack_aggregate_summary.json",
    "aggregate/block_stm_aggregate_summary.json",
]
NODE_EXPECTED_ARTIFACTS = [
    "block_execution_summary.json",
    "blocks.jsonl",
    "commit_markers.jsonl",
    "committed_chain.csv",
    "execution_log.csv",
    "execution_plan.jsonl",
    "node_summary.json",
    "plan_digest_consistency.csv",
    "receipts.jsonl",
    "runtime_events.csv",
    "state_delta_log.csv",
    "state_wal_manifest.json",
    "transaction_execution_trace.csv",
    "transaction_lifecycle.csv",
    "transaction_lifecycle.jsonl",
    "tx_index.jsonl",
]
DATASET_ARTIFACTS = [
    "workload_manifest_snapshot.json",
    "workload_source_spec.json",
    "workload_selection.json",
    "workload_skew_report.json",
    "workload_materialization_summary.json",
    "workload_identity_mapping_summary.json",
    "workload_replay_summary.json",
]
METATRACK_CLIENT_ARTIFACTS = [
    "metatrack_batch_plan.jsonl",
    "access_matrix_summary.csv",
    "state_frequency.csv",
    "coaccess_matrix_edges.csv",
    "placement_plan.csv",
    "transaction_placement.csv",
    "dependency_graph.csv",
    "predicted_remote_access.csv",
]

METATRACK_NODE_ARTIFACTS = [
    "track_classification.csv",
    "metatrack_scheduler_trace.csv",
    "aggregation_plan.csv",
    "logical_physical_update_mapping.csv",
]

BLOCK_STM_NODE_ARTIFACTS = [
    "block_stm_summary.json",
    "block_stm_task_trace.csv",
    "block_stm_validation_trace.csv",
    "block_stm_abort_trace.csv",
    "block_stm_dependency_trace.csv",
    "incarnation_summary.csv",
    "serial_equivalence.json",
]


def requested_cross_shard_count(tx_count: int, ratio: float) -> int:
    return int(tx_count * ratio + 0.5)


def compile_artifact_contract(expected_artifacts: list[str], nodes: list[V5CompiledNodeConfig]) -> list[dict[str, object]]:
    """Compile the formal v2 contract from the producer-facing file list.

    Node evidence is represented once as a scoped wildcard plus the topology's
    actual node ids.  This avoids a basename contract while retaining the
    exact list consumed by the runtime producer.
    """
    node_ids = [node.node_id for node in nodes]
    grouped_node_artifacts = sorted({path.split("/", 2)[2] for path in expected_artifacts if path.startswith("nodes/") and path.count("/") >= 2})
    entries: list[dict[str, object]] = []
    for path in expected_artifacts:
        if path.startswith("nodes/"):
            continue
        if path.startswith("client/"):
            scope = "client"
        elif path.startswith("aggregate/"):
            scope = "aggregate"
        else:
            scope = "child_root"
        entries.append({
            "path_pattern": path,
            "scope": scope,
            "per_node": False,
            "node_ids": [],
            "min_count": 1,
            "required_for_formal_eligibility": True,
        })
    entries.extend({
        "path_pattern": f"nodes/*/{artifact}",
        "scope": "node",
        "per_node": True,
        "node_ids": node_ids,
        "min_count": 1,
        "required_for_formal_eligibility": True,
    } for artifact in grouped_node_artifacts)
    return entries


def compile_plan(spec: V5ExperimentSpec, run_dir: Path, *, source_saved_config_id: str | None = None) -> V5CompiledRunPlan:
    compatibility = validate(spec)
    if not compatibility.valid:
        raise V5CompatibilityError(compatibility.blockers)
    normalized = spec.model_dump()
    raw = json.dumps(normalized, sort_keys=True, separators=(",", ":"))
    digest = hashlib.sha256(raw.encode("utf-8")).hexdigest()
    profile = {}
    for selection in compatibility.resolved_plugins:
        entry = {"plugin_id": selection.plugin_id, "config": selection.config}
        if selection.category == "block_executor":
            entry["migrated_default"] = bool(selection.config.get("migrated_default"))
        profile[selection.category] = entry
    nodes: list[V5CompiledNodeConfig] = []
    for index in range(spec.topology.nodes):
        shard_index = index // spec.topology.validators_per_shard
        node_id = f"n{index}"
        validators = [f"n{shard_index * spec.topology.validators_per_shard + offset}" for offset in range(spec.topology.validators_per_shard)]
        nodes.append(V5CompiledNodeConfig(node_id=node_id, shard_id=f"s{shard_index}", role="leader" if node_id == validators[0] else "validator", leader=node_id == validators[0], listen_addr="127.0.0.1:0", data_dir=str(run_dir / "nodes" / node_id), validators=validators, plugin_profile=profile))
    snapshot = [STORE.get(item.plugin_id).model_dump() | {"selected_config": item.config} for item in compatibility.resolved_plugins]
    workload = _compile_workload_plan(spec, profile, run_dir)
    materialized_blockers = validate_materialized_workload(spec, profile, workload)
    if materialized_blockers:
        (run_dir / "workload_compatibility_blockers.json").write_text(
            json.dumps({"status": "blocked_incompatible_workload", "blockers": materialized_blockers, "workload_plan": workload}, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        raise V5CompatibilityError(materialized_blockers, code="v5_materialized_workload_incompatible")
    node_expected_artifacts = [f"nodes/{node.node_id}/{artifact}" for node in nodes for artifact in NODE_EXPECTED_ARTIFACTS]
    expected_artifacts = EXPECTED_ARTIFACTS + node_expected_artifacts + (DATASET_ARTIFACTS if workload.get("source_type") == "dataset" else [])
    if profile.get("routing", {}).get("plugin_id") in {"metatrack_coaccess_routing", "stateless_hash_routing"}:
        expected_artifacts += [
            f"client/{artifact}"
            for artifact in METATRACK_CLIENT_ARTIFACTS
        ]
        expected_artifacts += [
            f"nodes/{node.node_id}/{artifact}"
            for node in nodes
            for artifact in METATRACK_NODE_ARTIFACTS
        ]

    if profile.get("block_executor", {}).get("plugin_id") == "block_stm_block_executor":
        expected_artifacts += [
            f"nodes/{node.node_id}/{artifact}"
            for node in nodes
            for artifact in BLOCK_STM_NODE_ARTIFACTS
        ]
    artifact_contract = compile_artifact_contract(expected_artifacts, nodes)
    return V5CompiledRunPlan(
        plan_id=f"v5plan_{digest[:16]}", plan_digest=digest,
        execution_backend=spec.execution_backend, duration_ms=spec.duration_ms,
        source_saved_config_id=source_saved_config_id,
        formal_plan_config_id=spec.formal_plan_config_id,
        method_config_id=spec.method_config_id,
        experiment_spec=normalized, plugin_snapshot=snapshot, node_configs=nodes,
        workload_plan=workload, fault_plan=spec.fault_policy,
        expected_artifacts=expected_artifacts,
        artifact_contract_version=2,
        artifact_contract=artifact_contract,
        resource_estimate=compatibility.resource_estimate,
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
            "replay_mode": source.replay_mode,
            "target_submission_tps": source.target_submission_tps,
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
        selection_mode=source.selection_mode,
        replay_mode=source.replay_mode,
        target_submission_tps=source.target_submission_tps,
        target_alpha=source.target_alpha,
        skew_axis=source.skew_axis,
        variant_parameters=source.variant_parameters,
        source_sha256=source.source_sha256,
    )
    materialized = workload_plane.materialize_request(request)
    topology_preview_cross_shard_count = None
    topology_preview_cross_shard_ratio = None
    topology_preview_status = "unavailable"
    try:
        topology_preview = workload_plane.preview_workload(request, shards=spec.topology.shards)
        selected_preview = topology_preview.selected_window_preview or {}
        preview_count = selected_preview.get("cross_shard_count")
        preview_ratio = selected_preview.get("cross_shard_ratio")
        if isinstance(preview_count, (int, float)) and not isinstance(preview_count, bool):
            topology_preview_cross_shard_count = int(preview_count)
            topology_preview_cross_shard_ratio = float(preview_ratio or 0.0)
            topology_preview_status = "topology_specific_preview"
    except Exception:
        # The exact Go preflight runs before node process creation and remains
        # authoritative when a raw source is absent but materialization is cached.
        topology_preview_status = "deferred_to_go_preflight"
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
        "truth_label": materialized.truth_label,
        "selection_mode": materialized.selection_mode,
        "variant_parameters": materialized.variant_parameters,
        "audit_metadata": materialized.summary.get("audit_metadata") or {},
        "source_file_sha256": materialized.source_file_sha256,
        "replay_mode": source.replay_mode,
        "target_submission_tps": source.target_submission_tps,
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
        "topology_preview_cross_shard_count": topology_preview_cross_shard_count,
        "topology_preview_cross_shard_ratio": topology_preview_cross_shard_ratio,
        "topology_preview_status": topology_preview_status,
        "identity_mapping_version": "mbe_dataset_identity_v1",
        "generator_version": workload_plane.GENERATOR_VERSION,
        "no_fallback": True,
    }
    (run_dir / "compiled_workload_plan.json").write_text(json.dumps(plan, sort_keys=True, indent=2) + "\n", encoding="utf-8")
    return plan
