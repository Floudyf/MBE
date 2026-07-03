from __future__ import annotations

from typing import Any

from backend.app.models.v3_composer_draft import V3RuntimeTopology


CURRENT_STAGE = "V3.10 Benchmark / Experiment Template Hardening Closure"
LATEST_RUNTIME_STAGE = "benchmark template catalog, baseline profile catalog, local sweep runner, reproducibility manifest, and benchmark report artifacts"
CURRENT_CAPABILITY = "benchmark template catalog, baseline profile catalog, local controlled sweep runner, repeatability manifest, and benchmark report artifacts"
RUNTIME_TRUTH = "benchmark_template_hardening_not_paper_grade_benchmark"
NEXT_STAGE = "V3.11 CrossShard Protocol Hardening"

BENCHMARK_TEMPLATES = {
    "metatrack_hotspot_template",
    "pbft_network_template",
    "cross_shard_relay_preview_template",
    "state_authenticity_template",
    "full_stack_v3_template",
}
BASELINE_PROFILES = {
    "baseline_simple_chain",
    "baseline_hash_sharding",
    "baseline_no_prefetch",
    "baseline_no_cross_shard_protocol",
    "baseline_memory_kv",
    "baseline_no_state_authenticity",
}


def default_topology() -> V3RuntimeTopology:
    return V3RuntimeTopology()


def stage_metadata() -> dict[str, str]:
    return {
        "current_stage": CURRENT_STAGE,
        "latest_runtime_stage": LATEST_RUNTIME_STAGE,
        "latest_completed_runtime_stage": LATEST_RUNTIME_STAGE,
        "current_capability": CURRENT_CAPABILITY,
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
    mode = data.get("network_mode") or "in_memory_message_bus"
    adapter = data.get("network_adapter") or mode
    if adapter == "in_memory_message_bus" and mode != "in_memory_message_bus":
        adapter = mode
    data["network_adapter"] = adapter
    data["network_mode"] = adapter
    if adapter not in {"in_memory_message_bus", "localhost_tcp_preview"}:
        errors.append("topology.network_adapter currently only allows in_memory_message_bus or localhost_tcp_preview")
    protocol = data.get("cross_shard_protocol") or "none"
    data["cross_shard_protocol"] = protocol
    if protocol not in {"none", "relay_preview", "broker_preview", "two_phase_commit_preview"}:
        errors.append("topology.cross_shard_protocol currently allows none, relay_preview, broker_preview, or two_phase_commit_preview")
    if protocol in {"broker_preview", "two_phase_commit_preview"}:
        errors.append(f"topology.cross_shard_protocol={protocol} is planned only and not runnable in V3.9")
    state_backend = data.get("state_backend") or "memory_kv"
    data["state_backend"] = state_backend
    if state_backend not in {"memory_kv", "persistent_kv", "merkle_trie_mvp", "ethereum_mpt_compatible"}:
        errors.append("topology.state_backend currently allows memory_kv, persistent_kv, merkle_trie_mvp, or ethereum_mpt_compatible")
    if state_backend == "ethereum_mpt_compatible":
        errors.append("topology.state_backend=ethereum_mpt_compatible is planned only and not runnable in V3.10")
    benchmark_template = data.get("benchmark_template") or "full_stack_v3_template"
    data["benchmark_template"] = benchmark_template
    if benchmark_template not in BENCHMARK_TEMPLATES:
        errors.append("topology.benchmark_template must be one of the V3.10 benchmark templates")
    baseline_profile = data.get("baseline_profile") or "baseline_simple_chain"
    data["baseline_profile"] = baseline_profile
    if baseline_profile not in BASELINE_PROFILES:
        errors.append("topology.baseline_profile must be one of the V3.10 baseline profiles")
    _range(errors, data, "repeat_count", 1, 20)
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
