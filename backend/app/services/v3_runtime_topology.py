from __future__ import annotations

from typing import Any

from backend.app.models.v3_composer_draft import V3RuntimeTopology


CURRENT_STAGE = "V3.5.1"
LATEST_RUNTIME_STAGE = "V3.5.1 logical node topology runtime"
RUNTIME_TRUTH = "single_process_logical_node_topology_runtime"
NEXT_STAGE = "V3.5.2 Local Multi-process Launcher Preview"


def default_topology() -> V3RuntimeTopology:
    return V3RuntimeTopology()


def stage_metadata() -> dict[str, str]:
    return {
        "current_stage": CURRENT_STAGE,
        "latest_runtime_stage": LATEST_RUNTIME_STAGE,
        "runtime_truth": RUNTIME_TRUTH,
        "next_stage": NEXT_STAGE,
    }


def normalize_topology(value: V3RuntimeTopology | dict[str, Any] | None) -> tuple[dict[str, Any], list[str]]:
    topology = value if isinstance(value, V3RuntimeTopology) else V3RuntimeTopology(**(value or {}))
    data = topology.model_dump() if hasattr(topology, "model_dump") else topology.dict()
    errors: list[str] = []
    _range(errors, data, "shard_count", 1, 32)
    _range(errors, data, "validators_per_shard", 1, 64)
    _range(errors, data, "executors_per_shard", 0, 64)
    _range(errors, data, "storage_nodes_per_shard", 0, 64)
    if not isinstance(data.get("supervisor_enabled"), bool):
        errors.append("topology.supervisor_enabled must be bool")
    if data.get("node_runtime_mode") != "logical_single_process":
        errors.append("topology.node_runtime_mode currently only allows logical_single_process")
    if data.get("network_mode") != "in_memory_message_bus":
        errors.append("topology.network_mode currently only allows in_memory_message_bus")
    return data, errors


def topology_summary(topology: dict[str, Any]) -> dict[str, int | str | bool]:
    shard_count = int(topology["shard_count"])
    validators = int(topology["validators_per_shard"])
    executors = int(topology["executors_per_shard"])
    storage = int(topology["storage_nodes_per_shard"])
    supervisor = 1 if bool(topology["supervisor_enabled"]) else 0
    validator_count = shard_count * validators
    executor_count = shard_count * executors
    storage_count = shard_count * storage
    return {
        **topology,
        "total_logical_nodes": validator_count + executor_count + storage_count + supervisor,
        "logical_node_count": validator_count + executor_count + storage_count + supervisor,
        "validator_node_count": validator_count,
        "executor_node_count": executor_count,
        "storage_node_count": storage_count,
        "supervisor_node_count": supervisor,
        "consensus_domain_count": shard_count,
    }


def _range(errors: list[str], data: dict[str, Any], key: str, minimum: int, maximum: int) -> None:
    value = data.get(key)
    if not isinstance(value, int) or value < minimum or value > maximum:
        errors.append(f"topology.{key} must be between {minimum} and {maximum}")
