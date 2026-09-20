from __future__ import annotations

from typing import Any

from backend.app.models.v5_experiment_spec import V5CompatibilityResult, V5ExperimentSpec, V5PluginSelection
from backend.app.services.v5_plugin_manifest_store import CATEGORIES, STORE


REQUIRED_CATEGORIES = set(CATEGORIES)
RESOURCE_POLICY = {
    "max_nodes": 64,
    "max_total_processes": 64,
    "max_tx_count": 10000,
    "max_runtime_seconds": 3600,
    "max_output_bytes": 512 * 1024 * 1024,
    "graceful_shutdown_timeout": 15,
    "forced_kill_timeout": 5,
    "max_concurrent_real_clusters": 1,
}


class V5CompatibilityError(ValueError):
    def __init__(self, blockers: list[str], *, code: str = "v5_compatibility_blocked") -> None:
        self.blockers = [str(item) for item in blockers if str(item).strip()]
        self.code = code
        super().__init__("; ".join(self.blockers) or code)


def validate_materialized_workload(spec: V5ExperimentSpec, profile: dict[str, dict[str, Any]], workload_plan: dict[str, Any]) -> list[str]:
    """Validate method capabilities using evidence produced after workload compilation.

    Static plugin config validation cannot infer the realized cross-shard content
    of a canonical dataset. This guard consumes topology-specific preview or
    exact preflight evidence when available.
    """
    block_executor = str((profile.get("block_executor") or {}).get("plugin_id") or "")
    shard_local_only = {
        "groundhog_block_executor": "Groundhog",
        "batch_si_block_executor": "Batch-SI",
    }
    method_name = shard_local_only.get(block_executor)
    if not method_name:
        return []
    # Only exact materialization/runtime evidence may block a run here. The
    # Python topology preview hashes source identifiers, while the Go replay
    # iterator derives runtime identities before sharding; its preview is
    # useful as an advisory signal but is not authoritative for eligibility.
    evidence = [
        ("actual_cross_shard_count", workload_plan.get("actual_cross_shard_count")),
        ("exact_preflight_cross_shard_count", workload_plan.get("exact_preflight_cross_shard_count")),
        ("requested_cross_shard_count", workload_plan.get("requested_cross_shard_count") if workload_plan.get("source_type") != "dataset" else None),
    ]
    for source, raw in evidence:
        if isinstance(raw, bool) or not isinstance(raw, (int, float)):
            continue
        count = int(raw)
        if count > 0:
            total = int(workload_plan.get("actual_tx_count") or workload_plan.get("tx_count") or spec.tx_count or 0)
            ratio = float(count) / float(total) if total else 0.0
            return [
                f"{method_name} is incompatible with the compiled workload: {count} cross-shard transactions "
                f"(ratio={ratio:.6f}, evidence={source}); {method_name} currently supports multi-shard deployment "
                "with shard-local transactions only"
            ]
    return []


def validate(spec: V5ExperimentSpec) -> V5CompatibilityResult:
    blockers: list[str] = []
    warnings: list[str] = []
    by_category: dict[str, V5PluginSelection] = {}
    for selection in spec.plugin_selections:
        if selection.category not in REQUIRED_CATEGORIES:
            blockers.append(f"unknown plugin category: {selection.category}")
            continue
        if selection.category in by_category:
            blockers.append(f"duplicate plugin category: {selection.category}")
            continue
        try:
            manifest = STORE.get(selection.plugin_id)
        except ValueError as exc:
            blockers.append(str(exc))
            continue
        if manifest.category != selection.category:
            blockers.append(f"plugin {selection.plugin_id} does not belong to {selection.category}")
            continue
        by_category[selection.category] = V5PluginSelection(category=selection.category, plugin_id=manifest.plugin_id, config={**manifest.default_config, **selection.config})
        if spec.execution_backend not in manifest.supported_backends:
            blockers.append(f"plugin {manifest.plugin_id} does not support {spec.execution_backend}")
        if manifest.implementation_status != "implemented":
            blockers.append(f"plugin {manifest.plugin_id} is {manifest.implementation_status}")
        _validate_schema(selection.config, manifest.config_schema, manifest.plugin_id, blockers)
    missing = sorted(REQUIRED_CATEGORIES - set(by_category))
    blockers.extend(f"missing required plugin category: {category}" for category in missing)
    if spec.topology.nodes != spec.topology.shards * spec.topology.validators_per_shard:
        blockers.append("nodes must equal shards * validators_per_shard for V5 committee topology")
    if spec.topology.nodes > RESOURCE_POLICY["max_nodes"]:
        blockers.append("topology exceeds max_nodes resource policy")
    if spec.tx_count > RESOURCE_POLICY["max_tx_count"]:
        blockers.append("tx_count exceeds max_tx_count resource policy")
    if spec.duration_ms > RESOURCE_POLICY["max_runtime_seconds"] * 1000:
        blockers.append("duration exceeds max_runtime_seconds resource policy")
    workload_config = by_category.get("workload").config if by_category.get("workload") else {}
    source = spec.workload_source
    if source and source.source_type == "dataset":
        if by_category.get("workload") and by_category["workload"].plugin_id != "canonical_trace_replay":
            blockers.append("dataset workload_source requires canonical_trace_replay workload plugin")
        if source.variant_mode == "original_window" and source.target_alpha is not None:
            blockers.append("original_window workload_source does not allow target_alpha")
        if source.variant_mode in {"contract_zipf", "key_zipf"} and source.target_alpha is None:
            blockers.append("derived workload_source requires target_alpha")
        if source.variant_mode in {"contract_zipf", "key_zipf"} and not source.skew_axis:
            blockers.append("derived workload_source requires skew_axis")
        if float(workload_config.get("cross_shard_ratio", 0.0) or 0.0) != 0:
            blockers.append("dataset workload_source does not allow cross_shard_ratio")
        if int(workload_config.get("timeout_every", 0) or 0) != 0:
            blockers.append("dataset workload_source does not allow timeout_every")
    ratio = float(workload_config.get("cross_shard_ratio", 0.0) or 0.0)
    if spec.execution_backend == "real_cluster" and ratio > 0 and _cross_shard_fault_unsupported(spec.fault_policy):
        blockers.append("cross-shard experiments with message loss or node restart are not supported because Relay/SourceFinalize reliable retransmission is not implemented")
    scheduler = by_category.get("scheduler")
    execution = by_category.get("execution")
    routing = by_category.get("routing")
    if routing and routing.plugin_id == "metatrack_coaccess_routing" and routing.config.get("control_policy") == "logical_domain_frontier_v1":
        required = {
            "transaction_admission": "metatrack_strict_admission_v1",
            "execution": "dual_track_execution",
            "scheduler": "fast_first_scheduler",
            "block_executor": "metatrack_block_executor",
        }
        for category, plugin_id in required.items():
            selected = by_category.get(category)
            if not selected or selected.plugin_id != plugin_id:
                blockers.append(f"MetaTrack logical_domain_frontier_v1 requires {category}:{plugin_id}")
        if int(routing.config.get("logical_domain_count", 0) or 0) < 1:
            blockers.append("MetaTrack logical_domain_frontier_v1 requires logical_domain_count >= 1")
        selected_executor = by_category.get("block_executor")
        if selected_executor and selected_executor.plugin_id == "metatrack_block_executor" and selected_executor.config.get("control_policy") != "logical_domain_frontier_v1":
            blockers.append("MetaTrack routing and block executor control_policy must match logical_domain_frontier_v1")
    if routing and routing.plugin_id == "metatrack_coaccess_routing" and routing.config.get("control_policy") == "declared_access_frontier_v2":
        required = {
            "transaction_admission": "metatrack_strict_admission_v1",
            "execution": "dual_track_execution",
            "scheduler": "fast_first_scheduler",
            "block_executor": "metatrack_block_executor",
        }
        for category, plugin_id in required.items():
            selected = by_category.get(category)
            if not selected or selected.plugin_id != plugin_id:
                blockers.append(f"MetaTrack declared_access_frontier_v2 requires {category}:{plugin_id}")
        selected_executor = by_category.get("block_executor")
        if selected_executor and selected_executor.plugin_id == "metatrack_block_executor" and selected_executor.config.get("control_policy") != "declared_access_frontier_v2":
            blockers.append("MetaTrack routing and block executor control_policy must match declared_access_frontier_v2")
    if scheduler and scheduler.plugin_id == "fast_first_scheduler" and (not execution or execution.plugin_id != "dual_track_execution"):
        blockers.append("fast_first_scheduler requires dual_track_execution")
    block_producer = by_category.get("block_producer")
    block_executor = by_category.get("block_executor")
    aria_producer = bool(block_producer and block_producer.plugin_id == "aria_block_producer")
    aria_executor = bool(block_executor and block_executor.plugin_id == "aria_block_executor")
    if aria_producer != aria_executor:
        blockers.append("aria_block_producer and aria_block_executor must be selected together")
    if aria_producer and aria_executor:
        for key in ("reordering", "read_only_optimization", "retry_nonce_gaps"):
            if block_producer.config.get(key) != block_executor.config.get(key):
                blockers.append(f"Aria producer and executor {key} must match")
        required = {
            "routing": "hash_routing_baseline",
            "execution": "serial_execution_baseline",
            "scheduler": "fifo_serial_scheduler",
            "state_access": "direct_state_access",
            "state_storage": "persistent_local_state_store",
            "commit": "normal_commit",
        }
        for category, plugin_id in required.items():
            selected = by_category.get(category)
            if not selected or selected.plugin_id != plugin_id:
                blockers.append(f"Aria requires {category}:{plugin_id}")
        warnings.append("Aria uses one consensus block per deterministic batch; conflict-deferred transactions remain FIFO in the mempool for a later block and fallback is disabled")
    groundhog_producer = bool(block_producer and block_producer.plugin_id == "groundhog_block_producer")
    groundhog_executor = bool(block_executor and block_executor.plugin_id == "groundhog_block_executor")
    if groundhog_producer != groundhog_executor:
        blockers.append("groundhog_block_producer and groundhog_block_executor must be selected together")
    if groundhog_producer and groundhog_executor:
        producer_set_limit = int(block_producer.config.get("ordered_set_limit", 64) or 64)
        executor_set_limit = int(block_executor.config.get("ordered_set_limit", 64) or 64)
        if producer_set_limit != executor_set_limit:
            blockers.append("Groundhog producer and executor ordered_set_limit must match")
        if producer_set_limit < 1 or producer_set_limit > 65535:
            blockers.append("Groundhog ordered_set_limit must be between 1 and 65535")
        required = {
            "routing": "hash_routing_baseline",
            "execution": "serial_execution_baseline",
            "scheduler": "fifo_serial_scheduler",
            "state_access": "direct_state_access",
            "state_storage": "persistent_local_state_store",
            "commit": "normal_commit",
        }
        for category, plugin_id in required.items():
            selected = by_category.get(category)
            if not selected or selected.plugin_id != plugin_id:
                blockers.append(f"Groundhog requires {category}:{plugin_id}")
        if ratio != 0:
            blockers.append("Groundhog core reproduction requires cross_shard_ratio=0")
        warnings.append("Groundhog runs independently inside each selected shard with unordered typed-commutative block semantics and is comparison-limited against sequential-state methods")
    batch_si_execution = bool(execution and execution.plugin_id == "batch_si_execution")
    batch_si_scheduler = bool(scheduler and scheduler.plugin_id == "batch_si_scheduler")
    batch_si_executor = bool(block_executor and block_executor.plugin_id == "batch_si_block_executor")
    if any((batch_si_execution, batch_si_scheduler, batch_si_executor)) and not all((batch_si_execution, batch_si_scheduler, batch_si_executor)):
        blockers.append("batch_si_execution, batch_si_scheduler, and batch_si_block_executor must be selected together")
    if batch_si_execution and batch_si_scheduler and batch_si_executor:
        required = {
            "routing": "hash_routing_baseline",
            "block_producer": "time_or_count_block_producer",
            "execution": "batch_si_execution",
            "state_access": "direct_state_access",
            "state_storage": "persistent_local_state_store",
            "commit": "normal_commit",
        }
        for category, plugin_id in required.items():
            selected = by_category.get(category)
            if not selected or selected.plugin_id != plugin_id:
                blockers.append(f"Batch-SI requires {category}:{plugin_id}")
        for key in ("partition_mode", "ordering_mode", "priority_mode"):
            scheduler_value = scheduler.config.get(key)
            executor_value = block_executor.config.get(key)
            if scheduler_value != executor_value:
                blockers.append(f"Batch-SI scheduler and executor {key} must match")
        if ratio != 0:
            blockers.append("Batch-SI core reproduction requires cross_shard_ratio=0")
        if spec.topology.shards != 1:
            blockers.append("Batch-SI core literature reproduction requires exactly 1 shard; multi-shard Batch-SI must be labeled as an extension")
        warnings.append("Batch-SI literature baseline uses one shard; batches are sequential and transactions inside one batch use a common immutable snapshot")
    # MBE_PORYGON_PAPER_REPRO_20260920_V7: additive Porygon composition guard.
    porygon_selected = any(
        selected and selected.plugin_id.startswith("porygon_")
        for selected in by_category.values()
    )
    if porygon_selected:
        required = {
            "routing": "porygon_stateless_routing",
            "block_producer": "porygon_transaction_block_producer",
            "consensus": "pbft_style_consensus",
            "execution": "porygon_execution",
            "scheduler": "porygon_pipeline_scheduler",
            "block_executor": "porygon_block_executor",
            "state_access": "porygon_remote_state_access",
            "state_storage": "persistent_local_state_store",
            "cross_shard": "porygon_cross_shard_coordinator",
            "commit": "normal_commit",
        }
        for category, plugin_id in required.items():
            selected = by_category.get(category)
            if not selected or selected.plugin_id != plugin_id:
                blockers.append(f"Porygon requires {category}:{plugin_id}")
        scheduler_cfg = by_category.get("scheduler").config if by_category.get("scheduler") else {}
        cross_cfg = by_category.get("cross_shard").config if by_category.get("cross_shard") else {}
        shard_count = int(scheduler_cfg.get("execution_shard_count", 4) or 4)
        if int(cross_cfg.get("execution_shard_count", shard_count) or shard_count) != shard_count:
            blockers.append("Porygon scheduler and cross-shard execution_shard_count must match")
        warnings.append("Porygon uses the common MBE PBFT consensus for fair comparison; W/O/E/M pipeline timing is exported as logical protocol-slot evidence and is not claimed as wall-clock overlap")

    if spec.execution_backend == "real_cluster" and blockers:
        warnings.append("real_cluster is blocked and will not fall back to simulation or V4 smoke")
    estimate = {**RESOURCE_POLICY, "estimated_processes": spec.topology.nodes, "estimated_ports": spec.topology.nodes, "estimate_only": True}
    return V5CompatibilityResult(valid=not blockers, blockers=blockers, warnings=warnings, resolved_plugins=list(by_category.values()), resource_estimate=estimate)


def _cross_shard_fault_unsupported(policy: dict[str, Any]) -> bool:
    if not policy:
        return False
    drop_message_types = policy.get("drop_message_types")
    return bool(
        float(policy.get("drop_rate", 0) or 0) > 0
        or int(policy.get("drop_every", 0) or 0) > 0
        or drop_message_types
        or int(policy.get("kill_node_after_ms", 0) or 0) > 0
        or int(policy.get("restart_node_after_ms", 0) or 0) > 0
        or str(policy.get("mode", "")).lower() in {"kill_node", "restart_node", "node_kill", "node_restart", "network_drop"}
    )


def _validate_schema(config: dict[str, Any], schema: dict[str, Any], plugin_id: str, blockers: list[str]) -> None:
    for name, field in schema.get("properties", {}).items():
        if name not in config:
            continue
        value = config[name]
        kind = field.get("type")
        if kind == "integer" and (not isinstance(value, int) or isinstance(value, bool)):
            blockers.append(f"{plugin_id}.{name} must be integer")
        if kind == "number" and (not isinstance(value, (int, float)) or isinstance(value, bool)):
            blockers.append(f"{plugin_id}.{name} must be number")
        if "minimum" in field and isinstance(value, (int, float)) and value < field["minimum"]:
            blockers.append(f"{plugin_id}.{name} is below minimum")
        if "maximum" in field and isinstance(value, (int, float)) and value > field["maximum"]:
            blockers.append(f"{plugin_id}.{name} is above maximum")
        if "enum" in field and value not in field["enum"]:
            blockers.append(f"{plugin_id}.{name} must be one of {field['enum']}")
