from __future__ import annotations

from typing import Any

from backend.app.models.v5_experiment_spec import V5CompatibilityResult, V5ExperimentSpec, V5PluginSelection
from backend.app.services.v5_plugin_manifest_store import CATEGORIES, STORE


REQUIRED_CATEGORIES = set(CATEGORIES)
RESOURCE_POLICY = {
    "max_nodes": 16,
    "max_total_processes": 16,
    "max_tx_count": 10000,
    "max_runtime_seconds": 3600,
    "max_output_bytes": 512 * 1024 * 1024,
    "graceful_shutdown_timeout": 15,
    "forced_kill_timeout": 5,
    "max_concurrent_real_clusters": 1,
}


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
    scheduler = by_category.get("scheduler")
    execution = by_category.get("execution")
    if scheduler and scheduler.plugin_id == "fast_first_scheduler" and (not execution or execution.plugin_id != "dual_track_execution"):
        blockers.append("fast_first_scheduler requires dual_track_execution")
    if spec.execution_backend == "real_cluster" and blockers:
        warnings.append("real_cluster is blocked and will not fall back to simulation or V4 smoke")
    estimate = {**RESOURCE_POLICY, "estimated_processes": spec.topology.nodes, "estimated_ports": spec.topology.nodes, "estimate_only": True}
    return V5CompatibilityResult(valid=not blockers, blockers=blockers, warnings=warnings, resolved_plugins=list(by_category.values()), resource_estimate=estimate)


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
