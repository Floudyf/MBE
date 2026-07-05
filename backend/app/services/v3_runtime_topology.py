from __future__ import annotations

from typing import Any

from backend.app.models.v3_composer_draft import V3RuntimeTopology


CURRENT_STAGE = "V3-final Fault, Observability, and Reproducibility Closure"
LATEST_RUNTIME_STAGE = "deterministic fault injection MVP, observability summary, final artifact catalog, reproducibility guide, experiment manual, and paper experiment mapping"
CURRENT_CAPABILITY = "deterministic fault injection, local observability summary, component health status, final artifact catalog, reproducibility bundle, experiment manual, and paper experiment mapping"
RUNTIME_TRUTH = "v3_final_emulator_closure_not_production_system"
NEXT_STAGE = "V3 maintenance only; do not start V4 unless explicitly requested"
METAVERSE_SCENARIOS = {
    "asset_transfer",
    "avatar_update",
    "scene_hotspot",
    "item_transfer",
    "cross_scene_migration",
    "onchain_offchain_confirmation",
    "cross_metaverse_transfer",
    "mixed_metaverse",
}

BENCHMARK_TEMPLATES = {
    "metatrack_hotspot_template",
    "pbft_network_template",
    "cross_shard_relay_preview_template",
    "cross_shard_relay_mvp_template",
    "state_authenticity_template",
    "full_stack_v3_template",
    "metaverse_mixed_template",
    "metaverse_asset_transfer_template",
    "metaverse_cross_scene_template",
    "metaverse_cross_metaverse_template",
}
BASELINE_PROFILES = {
    "baseline_simple_chain",
    "baseline_hash_sharding",
    "baseline_no_prefetch",
    "baseline_no_cross_shard_protocol",
    "baseline_memory_kv",
    "baseline_no_state_authenticity",
}
FAULT_PROFILES = {
    "none",
    "node_failure",
    "node_recovery",
    "network_delay",
    "network_drop",
    "target_congestion",
    "relay_fault",
    "mixed_fault",
}
RELAY_FAULT_MODES = {"none", "proof_fail", "timeout", "target_reject"}
OBSERVABILITY_LEVELS = {"basic", "detailed"}
WORKLOAD_SOURCES = {"synthetic", "metaverse", "saved_workload", "existing_trace_preview"}


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
    node_runtime_mode = data.get("node_runtime_mode") or "logical_single_process"
    data["node_runtime_mode"] = node_runtime_mode
    if node_runtime_mode not in {"logical_single_process", "local_multi_process"}:
        errors.append("topology.node_runtime_mode currently allows logical_single_process or local_multi_process")
    process_runtime_mode = data.get("process_runtime_mode") or "dry_run"
    data["process_runtime_mode"] = process_runtime_mode
    if process_runtime_mode not in {"dry_run", "smoke"}:
        errors.append("topology.process_runtime_mode currently allows dry_run or smoke")
    _range(errors, data, "max_local_processes", 1, 32)
    if not isinstance(data.get("enable_committee_epoch"), bool):
        errors.append("topology.enable_committee_epoch must be bool")
    _range(errors, data, "epoch_count", 1, 5)
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
    if protocol not in {"none", "relay_preview", "relay_mvp", "broker_preview", "two_phase_commit_preview"}:
        errors.append("topology.cross_shard_protocol currently allows none, relay_preview, relay_mvp, broker_preview, or two_phase_commit_preview")
    if protocol in {"broker_preview", "two_phase_commit_preview"}:
        errors.append(f"topology.cross_shard_protocol={protocol} is planned only and not runnable in V3.11")
    relay_failure_mode = data.get("relay_failure_mode") or "none"
    data["relay_failure_mode"] = relay_failure_mode
    if relay_failure_mode not in {"none", "proof_fail", "timeout", "target_reject"}:
        errors.append("topology.relay_failure_mode currently allows none, proof_fail, timeout, or target_reject")
    _range(errors, data, "relay_force_proof_fail_every_n", 0, 1000000)
    _range(errors, data, "relay_force_timeout_every_n", 0, 1000000)
    _range(errors, data, "relay_timeout_ms", 0, 1000000)
    state_backend = data.get("state_backend") or "memory_kv"
    data["state_backend"] = state_backend
    if state_backend not in {"memory_kv", "persistent_kv", "merkle_trie_mvp", "ethereum_mpt_compatible"}:
        errors.append("topology.state_backend currently allows memory_kv, persistent_kv, merkle_trie_mvp, or ethereum_mpt_compatible")
    if state_backend == "ethereum_mpt_compatible":
        errors.append("topology.state_backend=ethereum_mpt_compatible is planned only and not runnable in V3.10")
    benchmark_template = data.get("benchmark_template") or "full_stack_v3_template"
    data["benchmark_template"] = benchmark_template
    if benchmark_template not in BENCHMARK_TEMPLATES:
        errors.append("topology.benchmark_template must be one of the V3.13 benchmark templates")
    baseline_profile = data.get("baseline_profile") or "baseline_simple_chain"
    data["baseline_profile"] = baseline_profile
    if baseline_profile not in BASELINE_PROFILES:
        errors.append("topology.baseline_profile must be one of the V3.10 baseline profiles")
    _range(errors, data, "repeat_count", 1, 20)
    _bool(errors, data, "controlled_experiment_enabled")
    data["plugin_selection_mode"] = "controlled" if bool(data.get("controlled_experiment_enabled")) else "free"
    _bool(errors, data, "metaverse_suite_enabled")
    workload_source = data.get("workload_source") or ("metaverse" if data.get("metaverse_suite_enabled") else "synthetic")
    data["workload_source"] = workload_source
    if workload_source not in WORKLOAD_SOURCES:
        errors.append("topology.workload_source must be synthetic, metaverse, saved_workload, or existing_trace_preview")
    scenario = data.get("metaverse_scenario") or "mixed_metaverse"
    data["metaverse_scenario"] = scenario
    if scenario not in METAVERSE_SCENARIOS:
        errors.append("topology.metaverse_scenario must be one of the V3.13 metaverse scenarios")
    _range(errors, data, "user_count", 1, 100000)
    _range(errors, data, "asset_count", 1, 1000000)
    _range(errors, data, "item_count", 0, 1000000)
    _range(errors, data, "avatar_count", 1, 100000)
    _range(errors, data, "scene_count", 1, 10000)
    _range(errors, data, "metaverse_count", 1, 100)
    _range(errors, data, "tx_count", 1, 10000000)
    _range(errors, data, "seed", 0, 2147483647)
    for key in ("hotspot_ratio", "cross_scene_ratio", "cross_shard_ratio", "burst_rate", "read_write_ratio", "asset_skew", "scene_skew", "offchain_failure_ratio"):
        _ratio(errors, data, key)
    _ratio(errors, data, "zipf_alpha", maximum=2.0)
    _range_float(errors, data, "submit_rate", 0.0, 1000000.0)
    _range_float(errors, data, "arrival_rate", 0.0, 1000000.0)
    _range(errors, data, "key_space_size", 1, 100000000)
    _bool(errors, data, "offchain_confirmation_enabled")
    _range(errors, data, "offchain_confirm_delay_ms", 0, 600000)
    _bool(errors, data, "cross_metaverse_enabled")
    _bool(errors, data, "benchmark_suite_enabled")
    _bool(errors, data, "baseline_matrix_enabled")
    _bool(errors, data, "multi_seed_enabled")
    _bool(errors, data, "paper_export_enabled")
    _range(errors, data, "sweep_seed_count", 1, 20)
    _int_list(errors, data, "sweep_shard_counts", 1, 32)
    _float_list(errors, data, "sweep_cross_shard_ratios", 0.0, 1.0)
    _float_list(errors, data, "sweep_hotspot_ratios", 0.0, 1.0)
    _bool(errors, data, "fault_injection_enabled")
    fault_profile = data.get("fault_profile") or "none"
    data["fault_profile"] = fault_profile
    if fault_profile not in FAULT_PROFILES:
        errors.append("topology.fault_profile must be one of none, node_failure, node_recovery, network_delay, network_drop, target_congestion, relay_fault, or mixed_fault")
    _range(errors, data, "fault_seed", 0, 2147483647)
    _range(errors, data, "fault_start_round", 0, 1000000)
    _range(errors, data, "fault_duration_rounds", 0, 1000000)
    _range(errors, data, "failed_node_count", 0, 10000)
    _range(errors, data, "message_delay_ms", 0, 600000)
    _ratio(errors, data, "message_drop_ratio")
    _ratio(errors, data, "target_congestion_ratio")
    relay_fault_mode = data.get("relay_fault_mode") or "none"
    data["relay_fault_mode"] = relay_fault_mode
    if relay_fault_mode not in RELAY_FAULT_MODES:
        errors.append("topology.relay_fault_mode currently allows none, proof_fail, timeout, or target_reject")
    _bool(errors, data, "observability_enabled")
    observability_level = data.get("observability_level") or "basic"
    data["observability_level"] = observability_level
    if observability_level not in OBSERVABILITY_LEVELS:
        errors.append("topology.observability_level currently allows basic or detailed")
    _bool(errors, data, "reproducibility_bundle_enabled")
    _bool(errors, data, "paper_mapping_enabled")
    _bool(errors, data, "final_artifact_catalog_enabled")
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
        "process_runtime_mode": topology.get("process_runtime_mode", "dry_run"),
        "max_local_processes": int(topology.get("max_local_processes", 8)),
        "committee_epoch_enabled": bool(topology.get("enable_committee_epoch", True)),
        "epoch_count": int(topology.get("epoch_count", 1)),
        "metaverse_suite_enabled": bool(topology.get("metaverse_suite_enabled", False)),
        "workload_source": str(topology.get("workload_source", "synthetic")),
        "metaverse_scenario": str(topology.get("metaverse_scenario", "mixed_metaverse")),
        "metaverse_tx_count": int(topology.get("tx_count", 10000)),
        "benchmark_suite_enabled": bool(topology.get("benchmark_suite_enabled", False)),
        "baseline_matrix_enabled": bool(topology.get("baseline_matrix_enabled", False)),
        "controlled_experiment_enabled": bool(topology.get("controlled_experiment_enabled", False)),
        "plugin_selection_mode": "controlled" if bool(topology.get("controlled_experiment_enabled", False)) else "free",
        "multi_seed_enabled": bool(topology.get("multi_seed_enabled", False)),
        "paper_export_enabled": bool(topology.get("paper_export_enabled", False)),
        "fault_injection_enabled": bool(topology.get("fault_injection_enabled", False)),
        "fault_profile": str(topology.get("fault_profile", "none")),
        "observability_enabled": bool(topology.get("observability_enabled", True)),
        "observability_level": str(topology.get("observability_level", "basic")),
        "reproducibility_bundle_enabled": bool(topology.get("reproducibility_bundle_enabled", True)),
        "paper_mapping_enabled": bool(topology.get("paper_mapping_enabled", True)),
        "final_artifact_catalog_enabled": bool(topology.get("final_artifact_catalog_enabled", True)),
    }


def _range(errors: list[str], data: dict[str, Any], key: str, minimum: int, maximum: int) -> None:
    value = data.get(key)
    if not isinstance(value, int) or value < minimum or value > maximum:
        errors.append(f"topology.{key} must be between {minimum} and {maximum}")


def _ratio(errors: list[str], data: dict[str, Any], key: str, maximum: float = 1.0) -> None:
    value = data.get(key)
    if not isinstance(value, (int, float)) or float(value) < 0.0 or float(value) > maximum:
        errors.append(f"topology.{key} must be between 0.0 and {maximum}")


def _range_float(errors: list[str], data: dict[str, Any], key: str, minimum: float, maximum: float) -> None:
    value = data.get(key)
    if not isinstance(value, (int, float)) or float(value) < minimum or float(value) > maximum:
        errors.append(f"topology.{key} must be between {minimum} and {maximum}")


def _bool(errors: list[str], data: dict[str, Any], key: str) -> None:
    if not isinstance(data.get(key), bool):
        errors.append(f"topology.{key} must be bool")


def _int_list(errors: list[str], data: dict[str, Any], key: str, minimum: int, maximum: int) -> None:
    values = data.get(key)
    if not isinstance(values, list) or not values:
        errors.append(f"topology.{key} must be a non-empty list")
        return
    for value in values:
        if not isinstance(value, int) or value < minimum or value > maximum:
            errors.append(f"topology.{key} values must be between {minimum} and {maximum}")
            return


def _float_list(errors: list[str], data: dict[str, Any], key: str, minimum: float, maximum: float) -> None:
    values = data.get(key)
    if not isinstance(values, list) or not values:
        errors.append(f"topology.{key} must be a non-empty list")
        return
    for value in values:
        if not isinstance(value, (int, float)) or float(value) < minimum or float(value) > maximum:
            errors.append(f"topology.{key} values must be between {minimum} and {maximum}")
            return
