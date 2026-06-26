from __future__ import annotations

from copy import deepcopy
from typing import Any

from backend.app.services.v3_profile_loader import V3ProfileStore

CURRENT_STAGE = "v3.3.1"
ALLOWED_STATUS = {"planned", "runnable", "invalid"}
ALLOWED_TRUTH_LABELS = {
    "synthetic_replay",
    "existing_trace_replay",
    "fabric_chain_backed_trace_replay",
    "fabric_live_validation",
    "modular_runtime",
    "modular_runtime_calibrated",
    "public_chain_imported_trace_semantic_unknown",
    "planned_cross_chain_replay",
}
ALLOWED_BACKEND_TYPES = {
    "local_virtual",
    "trace_replay",
    "modular_research_chain",
    "fabric_validation",
    "fabric_live_planned",
    "evm_live_planned",
}
RESULT_LIKE_FIELDS = {
    "max_tps",
    "stable_tps",
    "peak_tps",
    "throughput_tps",
    "p99_latency_ms",
    "avg_latency_ms",
    "aggregation_ratio",
}
CHAIN_REQUIRED_SECTIONS = {
    "chain",
    "deployment",
    "node",
    "block",
    "consensus",
    "finality",
    "tx_pool",
    "sharding",
    "execution",
    "state",
    "commit",
    "network",
    "cross_chain",
    "application",
    "fault_safety",
    "metrics",
    "capability",
}
CHAIN_REQUIRED_FIELDS = {
    "chain": {"chain_id", "chain_name", "role", "chain_type", "truth_label"},
    "deployment": {"mode", "node_count", "validator_count", "executor_count", "hardware_profile"},
    "node": {"node_id_prefix", "node_roles", "clock_mode", "storage_root"},
    "block": {"block_interval_ms", "max_tx_per_block", "block_cut_policy", "empty_block_enabled"},
    "consensus": {"plugin", "leader_policy", "ordering_policy", "fault_model", "config"},
    "finality": {"finality_type", "finality_depth", "finality_timeout_ms", "commit_rule"},
    "tx_pool": {"plugin", "max_pool_size", "batch_selection_policy", "dedup_enabled", "backpressure_policy"},
    "sharding": {"enabled", "shard_count", "plugin", "routing_policy", "cross_shard_policy"},
    "execution": {"plugin", "parallelism", "access_list_enabled", "dependency_check_enabled", "dual_track_enabled"},
    "state": {"model", "backend", "key_count", "read_set_required", "write_set_required", "remote_fetch_cost_ms"},
    "commit": {"plugin", "aggregation_enabled", "commit_batch_size", "state_commit_log_enabled"},
    "network": {"delay_ms", "jitter_ms", "loss_rate", "bandwidth_limit"},
    "cross_chain": {"cross_chain_enabled", "peer_chain_ids", "finality_export_enabled", "bridge_adapter"},
    "application": {"chaincode_or_contract", "operation_types", "asset_model", "hotspot_model"},
    "fault_safety": {"fault_injection_enabled", "crash_faults", "byzantine_faults", "safety_assertions_enabled"},
    "metrics": {"trace_enabled", "report_enabled", "mechanism_metrics_enabled", "block_log_enabled", "tx_results_enabled"},
    "capability": {"status", "min_stage", "backend_type", "runnable", "blocking_reasons"},
}
LEGAL_PLUGIN_IDS = {
    "TxPoolPlugin": {"fifo_pool"},
    "BlockProducer": {"time_or_count_block_producer"},
    "ConsensusPlugin": {"simple_leader"},
    "ShardingPlugin": {"hash_sharding", "co_access_sharding"},
    "ExecutionSchedulerPlugin": {"serial_execution", "dual_track_execution"},
    "StateAccessPlugin": {"direct_fetch", "access_list_prefetch"},
    "CommitPlugin": {"normal_commit", "hot_update_aggregation_commit"},
    "MetricsPlugin": {"basic_metrics"},
    "CrossChainProtocolPlugin": {"lock_mint_serial", "lock_mint_pipeline", "fixed_window_baseline", "committee_bridge_basic", "metaflow_basic", "metaflow_afs_fda"},
}
METATRACK_ALLOWED_CLASSES = {"ShardingPlugin", "ExecutionSchedulerPlugin", "StateAccessPlugin", "CommitPlugin"}
METAFLOW_ALLOWED_CLASSES = {"CrossChainProtocolPlugin"}
RUNNABLE_EXPERIMENT_PROFILE_IDS = {
    "single_chain_runtime_smoke",
    "metatrack_go_backed_ablation_smoke",
    "single_chain_role_separation_smoke",
}
V32_RUNTIME_ALLOWED_CLASSES = {
    "TxPoolPlugin",
    "BlockProducer",
    "ConsensusPlugin",
    "ShardingPlugin",
    "ExecutionSchedulerPlugin",
    "StateAccessPlugin",
    "CommitPlugin",
    "MetricsPlugin",
}
V32_MINIMAL_PLUGIN_IDS = {
    "TxPoolPlugin": "fifo_pool",
    "BlockProducer": "time_or_count_block_producer",
    "ConsensusPlugin": "simple_leader",
    "ShardingPlugin": "hash_sharding",
    "ExecutionSchedulerPlugin": "serial_execution",
    "StateAccessPlugin": "direct_fetch",
    "CommitPlugin": "normal_commit",
    "MetricsPlugin": "basic_metrics",
}
FUTURE_STAGE_REQUIREMENTS = {
    "modular_research_chain": "requires V3.2 minimal single-chain modular runtime",
    "fabric_validation": "requires V3.4 Fabric-backed validation",
    "fabric_live_planned": "requires future Fabric live backend implementation",
    "evm_live_planned": "requires future EVM live backend implementation",
}
STAGE_ORDER = {"v3.1": 10, "v3.2": 20, "v3.3": 30, "v3.3.1": 31, "v3.4": 40, "v3.5": 50, "v3.6": 60}


def validate_chain_profile(profile: dict[str, Any]) -> dict[str, Any]:
    errors: list[str] = []
    warnings: list[str] = []
    _check_result_like_fields(profile, errors)
    _require_sections(profile, CHAIN_REQUIRED_SECTIONS, errors)
    for section, fields in CHAIN_REQUIRED_FIELDS.items():
        if isinstance(profile.get(section), dict):
            _require_fields(profile[section], fields, f"{section}.", errors)
    capability = profile.get("capability", {})
    _validate_capability(capability, errors)
    truth_label = profile.get("chain", {}).get("truth_label")
    if truth_label not in ALLOWED_TRUTH_LABELS:
        errors.append(f"invalid chain.truth_label: {truth_label}")
    role_config = normalized_chain_role_config(profile)
    committee = role_config["committee"]
    if committee["enabled"] is True and committee["status"] in {"planned", "disabled"}:
        errors.append("planned or disabled committee must not be enabled")
    _v31_future_guard(capability, errors, warnings)
    return _result("chain_profile", profile.get("profile_id", ""), errors, warnings, capability)


def validate_plugin_profile(profile: dict[str, Any]) -> dict[str, Any]:
    errors: list[str] = []
    warnings: list[str] = []
    _require_fields(profile, {"plugin_profile_id", "domain", "status", "min_stage", "runnable", "plugins"}, "", errors)
    domain = profile.get("domain")
    plugins = profile.get("plugins", {})
    if not isinstance(plugins, dict) or not plugins:
        errors.append("plugins must be a non-empty mapping")
    if domain == "metatrack":
        allowed_classes = METATRACK_ALLOWED_CLASSES
    elif domain == "metaflow":
        allowed_classes = METAFLOW_ALLOWED_CLASSES
    elif domain == "v3_runtime":
        allowed_classes = V32_RUNTIME_ALLOWED_CLASSES
    else:
        allowed_classes = set()
    if not allowed_classes:
        errors.append(f"invalid plugin profile domain: {domain}")
    for plugin_class, plugin_id in plugins.items():
        if plugin_class not in allowed_classes:
            errors.append(f"{domain} plugin profile cannot use {plugin_class}")
        if plugin_id not in LEGAL_PLUGIN_IDS.get(plugin_class, set()):
            errors.append(f"unknown plugin id {plugin_class}:{plugin_id}")
    if domain == "v3_runtime" and profile.get("plugin_profile_id") != "v3_2_minimal_single_chain":
        errors.append("only v3_2_minimal_single_chain is allowed as a V3.2 runtime plugin profile")
    if domain == "v3_runtime" and plugins != V32_MINIMAL_PLUGIN_IDS:
        errors.append("V3.2 runtime plugin profile must use only the minimal supported plugin set")
    description = str(profile.get("description", "")).lower()
    if plugins.get("CrossChainProtocolPlugin") == "committee_bridge_basic" and "production bridge" in description and "not a production bridge" not in description:
        errors.append("committee_bridge_basic must not be described as a production bridge")
    capability = {"status": profile.get("status"), "min_stage": profile.get("min_stage"), "runnable": profile.get("runnable"), "backend_type": "modular_research_chain", "blocking_reasons": profile.get("blocking_reasons", [])}
    _validate_capability(capability, errors)
    _v31_future_guard(capability, errors, warnings)
    return _result("plugin_profile", profile.get("plugin_profile_id", ""), errors, warnings, capability)


def validate_experiment_profile(profile: dict[str, Any], store: V3ProfileStore) -> dict[str, Any]:
    errors: list[str] = []
    warnings: list[str] = []
    _require_sections(profile, {"experiment", "plugin_profiles", "workload", "fairness", "outputs", "capability"}, errors)
    experiment = profile.get("experiment", {})
    capability = profile.get("capability", {})
    _require_fields(experiment, {"experiment_id", "stage", "type", "truth_label", "backend_type", "runnable"}, "experiment.", errors)
    _validate_capability(capability, errors)
    if experiment.get("truth_label") not in ALLOWED_TRUTH_LABELS:
        errors.append(f"invalid experiment.truth_label: {experiment.get('truth_label')}")
    if experiment.get("backend_type") not in ALLOWED_BACKEND_TYPES:
        errors.append(f"invalid experiment.backend_type: {experiment.get('backend_type')}")
    if experiment.get("runnable") is True and profile.get("profile_id") not in RUNNABLE_EXPERIMENT_PROFILE_IDS:
        errors.append("only V3.2 smoke, V3.3 Go-backed MetaTrack smoke, and V3.3.1 role separation smoke may be declared runnable")
    _validate_profile_references(profile, store, errors)
    exp_type = experiment.get("type")
    if exp_type == "metatrack_plugin_ablation":
        _validate_metatrack_fairness(profile, store, errors, warnings)
    elif exp_type == "metatrack_go_backed_ablation_smoke":
        _validate_metatrack_go_backed_smoke(profile, store, errors, warnings)
    elif exp_type == "metaflow_protocol_comparison":
        _validate_metaflow_fairness(profile, store, errors, warnings)
    elif exp_type == "fabric_backed_validation":
        warnings.append("Fabric validation profile is planned in V3.1 and must not start Fabric.")
    elif exp_type == "single_chain_modular_runtime_smoke":
        _validate_v32_smoke_profile(profile, store, errors, warnings)
    elif exp_type == "single_chain_role_separation_smoke":
        _validate_v331_role_separation_smoke(profile, store, errors, warnings)
    else:
        errors.append(f"unknown experiment type: {exp_type}")
    _v31_future_guard({**capability, "backend_type": experiment.get("backend_type")}, errors, warnings)
    return _result("experiment_profile", profile.get("profile_id", ""), errors, warnings, capability)


def validate_any_profile(profile: dict[str, Any], store: V3ProfileStore | None = None) -> dict[str, Any]:
    profile_type = profile.get("profile_type")
    if profile_type == "chain_profile":
        return validate_chain_profile(profile)
    if profile_type == "plugin_profile":
        return validate_plugin_profile(profile)
    if profile_type == "experiment_profile":
        if store is None:
            raise ValueError("experiment profile validation requires a V3ProfileStore")
        return validate_experiment_profile(profile, store)
    return {"profile_type": str(profile_type), "profile_id": profile.get("profile_id", ""), "valid": False, "status": "invalid", "runnable": False, "errors": [f"unknown profile_type: {profile_type}"], "warnings": [], "blocking_reasons": ["unknown_profile_type"]}


def _validate_profile_references(profile: dict[str, Any], store: V3ProfileStore, errors: list[str]) -> None:
    if profile.get("chain_profile") and profile["chain_profile"] not in store.chains:
        errors.append(f"unknown chain_profile: {profile['chain_profile']}")
    chain_profiles = profile.get("chain_profiles", {})
    for field in ("source_chain_profile", "target_chain_profile"):
        if chain_profiles.get(field) and chain_profiles[field] not in store.chains:
            errors.append(f"unknown {field}: {chain_profiles[field]}")
    for section in ("baselines", "proposed"):
        for plugin_id in profile.get("plugin_profiles", {}).get(section, []):
            if plugin_id not in store.plugins:
                errors.append(f"unknown plugin profile: {plugin_id}")


def _validate_metatrack_fairness(profile: dict[str, Any], store: V3ProfileStore, errors: list[str], warnings: list[str]) -> None:
    fairness = profile.get("fairness", {})
    required_flags = ["same_workload", "same_seed", "same_tx_count", "same_chain_profile", "same_hardware_profile", "same_submit_rate", "same_block_config", "same_consensus_config", "same_network_profile", "same_calibration_profile", "only_plugin_diff"]
    for flag in required_flags:
        if fairness.get(flag) is not True:
            errors.append(f"MetaTrack fairness requires {flag}=true")
    allowed = set(fairness.get("allowed_plugin_diff_classes", []))
    if allowed != METATRACK_ALLOWED_CLASSES:
        errors.append("MetaTrack fairness allowed_plugin_diff_classes must be ShardingPlugin, ExecutionSchedulerPlugin, StateAccessPlugin, CommitPlugin")
    for plugin_id in _experiment_plugin_ids(profile):
        plugin = store.plugins.get(plugin_id)
        if plugin and plugin.get("domain") != "metatrack":
            errors.append("MetaTrack experiment cannot vary CrossChainProtocolPlugin or MetaFlow plugin profiles")
    if profile.get("workload", {}).get("seed") is None:
        errors.append("MetaTrack workload seed is required")
    if profile.get("calibration", {}).get("enabled") and profile.get("calibration", {}).get("required_for_all_baselines") is not True:
        errors.append("calibration profile must be required for all MetaTrack baselines when enabled")
    warnings.append("MetaTrack profile is preview-only in V3.1; requires V3.3 for execution.")


def _validate_metaflow_fairness(profile: dict[str, Any], store: V3ProfileStore, errors: list[str], warnings: list[str]) -> None:
    fairness = profile.get("fairness", {})
    required_flags = ["same_workload", "same_seed", "same_tx_count", "same_chain_profile", "same_hardware_profile", "same_submit_rate", "same_network_profile", "same_calibration_profile", "only_plugin_diff"]
    for flag in required_flags:
        if fairness.get(flag) is not True:
            errors.append(f"MetaFlow fairness requires {flag}=true")
    allowed = set(fairness.get("allowed_plugin_diff_classes", []))
    if not {"CrossChainProtocolPlugin", "control_policy", "BDT_adaptation_logic"} <= allowed:
        errors.append("MetaFlow fairness must allow only CrossChainProtocolPlugin, control_policy, and B/D/T adaptation logic")
    if not fairness.get("timeout_baseline_ms"):
        errors.append("MetaFlow fairness requires a shared timeout_baseline_ms")
    if not fairness.get("finality_profile"):
        errors.append("MetaFlow fairness requires a shared finality_profile")
    for plugin_id in _experiment_plugin_ids(profile):
        plugin = store.plugins.get(plugin_id)
        if plugin and plugin.get("domain") != "metaflow":
            errors.append("MetaFlow experiment cannot vary MetaTrack plugin profiles")
        if plugin and plugin.get("plugins", {}).get("CrossChainProtocolPlugin") == "committee_bridge_basic":
            description = str(plugin.get("description", "")).lower()
            if "not a production bridge" not in description:
                errors.append("committee_bridge_basic must be marked as not a production bridge")
    warnings.append("MetaFlow profile is preview-only in V3.1; requires V3.6 for execution.")


def _validate_v32_smoke_profile(profile: dict[str, Any], store: V3ProfileStore, errors: list[str], warnings: list[str]) -> None:
    if profile.get("profile_id") != "single_chain_runtime_smoke":
        errors.append("V3.2 runtime smoke must use profile_id single_chain_runtime_smoke")
    experiment = profile.get("experiment", {})
    if experiment.get("stage") != "v3.2":
        errors.append("V3.2 smoke experiment.stage must be v3.2")
    if experiment.get("truth_label") != "modular_runtime":
        errors.append("V3.2 smoke truth_label must be modular_runtime")
    if experiment.get("backend_type") != "modular_research_chain":
        errors.append("V3.2 smoke backend_type must be modular_research_chain")
    if profile.get("chain_profile") != "chain_x_default":
        errors.append("V3.2 smoke must use chain_x_default")
    plugin_ids = _experiment_plugin_ids(profile)
    if plugin_ids != ["v3_2_minimal_single_chain"]:
        errors.append("V3.2 smoke may only use v3_2_minimal_single_chain")
    plugin = store.plugins.get("v3_2_minimal_single_chain")
    if plugin and plugin.get("plugins") != V32_MINIMAL_PLUGIN_IDS:
        errors.append("V3.2 smoke plugin set is not the minimal supported plugin set")
    if profile.get("workload", {}).get("source") != "synthetic":
        errors.append("V3.2 smoke must use synthetic workload")
    warnings.append("V3.2 smoke validates the minimal runtime pipeline only; it is not MetaTrack full evaluation or Fabric validation.")


def _validate_metatrack_go_backed_smoke(profile: dict[str, Any], store: V3ProfileStore, errors: list[str], warnings: list[str]) -> None:
    if profile.get("profile_id") != "metatrack_go_backed_ablation_smoke":
        errors.append("V3.3 Go-backed MetaTrack smoke must use profile_id metatrack_go_backed_ablation_smoke")
    experiment = profile.get("experiment", {})
    if experiment.get("stage") != "v3.3":
        errors.append("V3.3 Go-backed MetaTrack smoke experiment.stage must be v3.3")
    if experiment.get("truth_label") != "modular_runtime":
        errors.append("V3.3 Go-backed MetaTrack smoke truth_label must be modular_runtime")
    if experiment.get("backend_type") != "modular_research_chain":
        errors.append("V3.3 Go-backed MetaTrack smoke backend_type must be modular_research_chain")
    if experiment.get("runtime_mode") != "go_backed":
        errors.append("V3.3 Go-backed MetaTrack smoke runtime_mode must be go_backed")
    if profile.get("chain_profile") not in {"chain_x_default", "single_chain_research_default"}:
        errors.append("V3.3 Go-backed MetaTrack smoke must use chain_x_default or single_chain_research_default")
    expected_plugins = ["baseline_hash_only", "co_access_only", "co_access_dual_track", "full_MetaTrack"]
    if _experiment_plugin_ids(profile) != expected_plugins:
        errors.append("V3.3 Go-backed MetaTrack smoke must run the four approved MetaTrack combinations")
    _validate_metatrack_fairness(profile, store, errors, warnings)
    if profile.get("workload", {}).get("source") != "synthetic":
        errors.append("V3.3 Go-backed MetaTrack smoke must use synthetic workload")
    if profile.get("calibration", {}).get("enabled") is True:
        errors.append("V3.3 Go-backed MetaTrack smoke must not require Fabric calibration")
    warnings.append("V3.3 Go-backed MetaTrack smoke is controlled evaluation only; it is not Fabric validation or final paper-scale evidence.")


def _validate_v331_role_separation_smoke(profile: dict[str, Any], store: V3ProfileStore, errors: list[str], warnings: list[str]) -> None:
    if profile.get("profile_id") != "single_chain_role_separation_smoke":
        errors.append("V3.3.1 role separation smoke must use profile_id single_chain_role_separation_smoke")
    experiment = profile.get("experiment", {})
    if experiment.get("stage") != "v3.3.1":
        errors.append("V3.3.1 role separation smoke experiment.stage must be v3.3.1")
    if experiment.get("truth_label") != "modular_runtime":
        errors.append("V3.3.1 role separation smoke truth_label must be modular_runtime")
    if experiment.get("backend_type") != "modular_research_chain":
        errors.append("V3.3.1 role separation smoke backend_type must be modular_research_chain")
    if experiment.get("runtime_mode") != "go_backed":
        errors.append("V3.3.1 role separation smoke runtime_mode must be go_backed")
    if profile.get("chain_profile") != "single_chain_research_default":
        errors.append("V3.3.1 role separation smoke must use single_chain_research_default")
    plugin_ids = _experiment_plugin_ids(profile)
    if plugin_ids != ["v3_2_minimal_single_chain"]:
        errors.append("V3.3.1 role separation smoke may only use v3_2_minimal_single_chain")
    if profile.get("workload", {}).get("source") != "synthetic":
        errors.append("V3.3.1 role separation smoke must use synthetic workload")
    chain = store.chains.get(str(profile.get("chain_profile")), {})
    role_config = normalized_chain_role_config(chain)
    committee = role_config["committee"]
    if committee["enabled"] is True or committee["status"] not in {"planned", "disabled"}:
        errors.append("V3.3.1 role separation smoke requires committee disabled/planned placeholder")
    warnings.append("V3.3.1 role separation smoke validates role-separated single-chain runtime structure only; it is not Fabric validation or frontend integration.")


def _experiment_plugin_ids(profile: dict[str, Any]) -> list[str]:
    plugins = profile.get("plugin_profiles", {})
    return list(plugins.get("baselines", [])) + list(plugins.get("proposed", []))


def _validate_capability(capability: dict[str, Any], errors: list[str]) -> None:
    if capability.get("status") not in ALLOWED_STATUS:
        errors.append(f"invalid capability.status: {capability.get('status')}")
    if not isinstance(capability.get("blocking_reasons", []), list):
        errors.append("capability.blocking_reasons must be a list")
    backend_type = capability.get("backend_type")
    if backend_type is not None and backend_type not in ALLOWED_BACKEND_TYPES:
        errors.append(f"invalid backend_type: {backend_type}")
    if capability.get("status") == "planned" and capability.get("runnable") is True:
        errors.append("planned profile must not be runnable")


def _v31_future_guard(capability: dict[str, Any], errors: list[str], warnings: list[str]) -> None:
    backend_type = capability.get("backend_type")
    min_stage = str(capability.get("min_stage", ""))
    if backend_type in FUTURE_STAGE_REQUIREMENTS and capability.get("runnable") is True and _stage_after(min_stage, CURRENT_STAGE):
        errors.append(f"{backend_type} is not runnable before its implemented stage")
    if min_stage and min_stage != CURRENT_STAGE:
        warnings.append(f"requires {min_stage}; V3.3.1 supports role-separated Go-backed smoke execution beyond V3.2/V3.3 smoke")


def _stage_after(candidate: str, current: str) -> bool:
    return STAGE_ORDER.get(candidate, 999) > STAGE_ORDER.get(current, 0)


def _require_sections(profile: dict[str, Any], sections: set[str], errors: list[str]) -> None:
    missing = sorted(section for section in sections if section not in profile)
    if missing:
        errors.append(f"missing required sections: {missing}")


def _require_fields(section: dict[str, Any], fields: set[str], prefix: str, errors: list[str]) -> None:
    missing = sorted(field for field in fields if field not in section)
    if missing:
        errors.append(f"missing required fields: {[prefix + field for field in missing]}")


def _check_result_like_fields(value: Any, errors: list[str], path: str = "") -> None:
    if isinstance(value, dict):
        for key, nested in value.items():
            next_path = f"{path}.{key}" if path else str(key)
            if key in RESULT_LIKE_FIELDS:
                errors.append(f"result-like field is not allowed in ChainProfile: {next_path}")
            _check_result_like_fields(nested, errors, next_path)
    elif isinstance(value, list):
        for index, item in enumerate(value):
            _check_result_like_fields(item, errors, f"{path}[{index}]")


def _result(profile_type: str, profile_id: Any, errors: list[str], warnings: list[str], capability: dict[str, Any]) -> dict[str, Any]:
    blocking_reasons = list(capability.get("blocking_reasons") or [])
    if errors:
        blocking_reasons.extend(errors)
    return {
        "profile_type": profile_type,
        "profile_id": str(profile_id),
        "valid": not errors,
        "status": "invalid" if errors else str(capability.get("status", "planned")),
        "runnable": False if errors else bool(capability.get("runnable", False)),
        "backend_type": capability.get("backend_type", ""),
        "declared_stage": capability.get("min_stage", ""),
        "errors": errors,
        "warnings": warnings,
        "blocking_reasons": blocking_reasons,
    }


def clone_profile(profile: dict[str, Any]) -> dict[str, Any]:
    return deepcopy(profile)


def normalized_chain_role_config(profile: dict[str, Any]) -> dict[str, Any]:
    deployment = _mapping(profile.get("deployment"))
    consensus = _mapping(profile.get("consensus"))
    committee = _mapping(profile.get("committee"))
    execution = _mapping(profile.get("execution"))
    state = _mapping(profile.get("state"))
    sharding = _mapping(profile.get("sharding"))
    routing = _mapping(profile.get("routing"))
    network = _mapping(profile.get("network"))
    validator_count = int(consensus.get("validator_count") or deployment.get("validator_count") or 1)
    execution_shard_count = int(execution.get("shard_count") or sharding.get("shard_count") or execution.get("parallelism") or deployment.get("executor_count") or 1)
    state_storage_unit_count = int(state.get("storage_unit_count") or sharding.get("shard_count") or execution_shard_count or 1)
    return {
        "consensus": {
            "domain_count": int(consensus.get("domain_count") or 1),
            "domain_ids": [f"consensus_{index}" for index in range(int(consensus.get("domain_count") or 1))],
            "plugin": consensus.get("plugin", "simple_leader"),
            "validator_count": validator_count,
        },
        "committee": {
            "enabled": bool(committee.get("enabled", False)),
            "status": committee.get("status", "planned"),
            "epoch_enabled": bool(committee.get("epoch_enabled", False)),
            "lifecycle_plugin": committee.get("lifecycle_plugin", "none"),
        },
        "execution": {
            "shard_count": execution_shard_count,
            "executor_count": int(execution.get("executor_count") or deployment.get("executor_count") or execution.get("parallelism") or execution_shard_count),
        },
        "state": {
            "storage_unit_count": state_storage_unit_count,
            "placement_policy": state.get("placement_policy", "hash_state_storage"),
            "backend": state.get("backend", "memory"),
            "remote_fetch_cost_ms": int(state.get("remote_fetch_cost_ms") or 1),
        },
        "routing": {
            "plugin": routing.get("plugin") or sharding.get("plugin", "hash_sharding"),
            "routing_scope": routing.get("routing_scope", "execution_shard"),
        },
        "network": {
            "plugin": network.get("plugin", "fixed_delay"),
            "base_delay_ms": int(network.get("base_delay_ms") or network.get("delay_ms") or 0),
        },
    }


def _mapping(value: Any) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}
