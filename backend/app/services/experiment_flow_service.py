from __future__ import annotations

from datetime import datetime, timezone
from uuid import uuid4

from backend.app.models.experiment_flow import (
    ChildRunResult,
    ExperimentConditions,
    ExperimentMatrixRow,
    ExperimentMethod,
    ExperimentProfile,
    ExperimentRunMatrixPreview,
    ExperimentRunPlanPreview,
    ExperimentRunPlanRequest,
    ExperimentSuiteRequest,
    ExperimentTopology,
    ExperimentWorkload,
    RunSuiteExecutionRequest,
    RunSuiteExecutionResponse,
    V4DerivedRequestPreview,
)
from backend.app.models.v4_realism import V4RealismSmokeRequest
from backend.app.services import v4_realism_runner
from backend.app.services.v3_controlled_smoke_runner import ControlledSmokeError, run_v3_4_10_controlled_smoke
from backend.app.services.v3_saved_config_store import SavedConfigNotFound, get_saved_config, list_saved_configs


PROFILES = {
    "v4_3_realism_default": ExperimentProfile(
        profile_id="v4_3_realism_default",
        label="V4.3 真实度默认方案",
        description="V4.3 signed tx, localhost TCP P2P, PBFT-style commit, state root, cross-shard, fault, and BlockEmulator bridge validation profile.",
        runtime_target="v4.3",
        mechanism_tags=["signed_tx", "real_p2p", "pbft_style", "state_root", "cross_shard", "faults", "blockemulator_bridge"],
        default_topology_id="local_8_nodes_2_shards",
        default_workload_id="small_test",
        runnable=True,
    ),
    "metatrack_v3_mechanism_profile": ExperimentProfile(
        profile_id="metatrack_v3_mechanism_profile",
        label="MetaTrack 机制实验方案",
        description="V3-final local modular runtime mechanism profile for MetaTrack controlled experiments.",
        runtime_target="v3",
        mechanism_tags=["metatrack", "routing", "dual_track", "aggregation", "local_emulator"],
        default_topology_id="local_4_nodes_1_shard",
        default_workload_id="small_test",
        runnable=True,
    ),
    "baseline_hash_profile": ExperimentProfile(
        profile_id="baseline_hash_profile",
        label="Baseline Hash 对照方案",
        description="Baseline hash routing comparison profile for V3/V4-oriented experiment planning.",
        runtime_target="v3/v4",
        mechanism_tags=["baseline", "hash_routing", "comparison"],
        default_topology_id="local_4_nodes_1_shard",
        default_workload_id="small_test",
        runnable=True,
    ),
}

TOPOLOGIES = {
    "local_4_nodes_1_shard": ExperimentTopology(
        topology_id="local_4_nodes_1_shard",
        label="Local 4 nodes / 1 shard",
        nodes=4,
        shards=1,
        validators_per_shard=4,
        runtime_mode="v4.3",
        description="Smallest V4.3 PBFT-style smoke topology.",
        runnable=True,
    ),
    "local_8_nodes_2_shards": ExperimentTopology(
        topology_id="local_8_nodes_2_shards",
        label="Local 8 nodes / 2 shards",
        nodes=8,
        shards=2,
        validators_per_shard=4,
        runtime_mode="v4.3",
        description="Recommended small real-node validation topology.",
        runnable=True,
    ),
    "local_8_nodes_4_shards": ExperimentTopology(
        topology_id="local_8_nodes_4_shards",
        label="Local 8 nodes / 4 shards",
        nodes=8,
        shards=4,
        validators_per_shard=2,
        runtime_mode="v4.3",
        description="Runnable stress topology with fewer validators per shard.",
        runnable=True,
    ),
}

WORKLOADS = {
    "small_test": ExperimentWorkload(
        workload_id="small_test",
        label="Small test workload",
        source_type="sample",
        scale_label="small",
        skew_label="basic",
        description="Built-in small validation workload for V4.3 smoke.",
        planned=False,
        default_tx_count=20,
        default_blockemulator_tx_limit=20,
        csv_required=False,
    ),
    "blockemulator_sample": ExperimentWorkload(
        workload_id="blockemulator_sample",
        label="BlockEmulator sample subset",
        source_type="sample_csv",
        scale_label="small",
        skew_label="basic",
        description="Small BlockEmulator-style sample subset for bridge validation.",
        planned=False,
        default_tx_count=20,
        default_blockemulator_tx_limit=20,
        csv_required=False,
    ),
    "blockemulator_csv": ExperimentWorkload(
        workload_id="blockemulator_csv",
        label="BlockEmulator external CSV",
        source_type="external_csv",
        scale_label="user_selected",
        skew_label="user_selected",
        description="User-selected BlockEmulator CSV subset path.",
        planned=False,
        default_tx_count=20,
        default_blockemulator_tx_limit=20,
        csv_required=True,
    ),
    "real_skew_low": ExperimentWorkload(
        workload_id="real_skew_low",
        label="Real workload skew low",
        source_type="real_dataset",
        scale_label="small/medium/large",
        skew_label="low",
        description="Planned filtered real workload with low skew; dataset not attached yet.",
        planned=True,
        default_tx_count=20,
        default_blockemulator_tx_limit=20,
        csv_required=False,
    ),
    "real_skew_medium": ExperimentWorkload(
        workload_id="real_skew_medium",
        label="Real workload skew medium",
        source_type="real_dataset",
        scale_label="small/medium/large",
        skew_label="medium",
        description="Planned filtered real workload with medium skew; dataset not attached yet.",
        planned=True,
        default_tx_count=20,
        default_blockemulator_tx_limit=20,
        csv_required=False,
    ),
    "real_skew_high": ExperimentWorkload(
        workload_id="real_skew_high",
        label="Real workload skew high",
        source_type="real_dataset",
        scale_label="small/medium/large",
        skew_label="high",
        description="Planned filtered real workload with high skew; dataset not attached yet.",
        planned=True,
        default_tx_count=20,
        default_blockemulator_tx_limit=20,
        csv_required=False,
    ),
    "extreme_hotspot": ExperimentWorkload(
        workload_id="extreme_hotspot",
        label="Extreme hotspot real workload",
        source_type="real_dataset",
        scale_label="small/medium/large",
        skew_label="extreme_hotspot",
        description="Planned filtered real workload with extreme hotspot skew; dataset not attached yet.",
        planned=True,
        default_tx_count=20,
        default_blockemulator_tx_limit=20,
        csv_required=False,
    ),
}

METHODS = {
    "metatrack_full": ExperimentMethod(
        method_id="metatrack_full",
        label="MetaTrack full",
        role="main",
        description="Full MetaTrack method using routing, execution, state access, and commit optimizations.",
        module_overrides={
            "Routing": "metatrack_coaccess_routing",
            "Execution": "metatrack_dual_track_execution",
            "StateAccess": "access_list_prefetch",
            "Commit": "constraint_checked_aggregation",
        },
        runnable=True,
    ),
    "baseline_hash": ExperimentMethod(
        method_id="baseline_hash",
        label="Baseline hash",
        role="baseline",
        description="Hash routing baseline for comparison.",
        module_overrides={"Routing": "hash_sharding"},
        runnable=True,
    ),
    "baseline_serial": ExperimentMethod(
        method_id="baseline_serial",
        label="Baseline serial",
        role="baseline",
        description="Serial execution baseline for comparison.",
        module_overrides={"Execution": "serial_execution"},
        runnable=True,
    ),
    "baseline_no_prefetch": ExperimentMethod(
        method_id="baseline_no_prefetch",
        label="Baseline no prefetch",
        role="baseline",
        description="State access baseline without prefetch.",
        module_overrides={"StateAccess": "direct_fetch"},
        runnable=True,
    ),
    "metatrack_routing_only": ExperimentMethod(
        method_id="metatrack_routing_only",
        label="MetaTrack routing only",
        role="ablation",
        description="Ablation with MetaTrack routing enabled only.",
        module_overrides={"Routing": "metatrack_coaccess_routing"},
        runnable=True,
    ),
    "metatrack_routing_execution": ExperimentMethod(
        method_id="metatrack_routing_execution",
        label="MetaTrack routing + execution",
        role="ablation",
        description="Ablation with MetaTrack routing and dual-track execution.",
        module_overrides={"Routing": "metatrack_coaccess_routing", "Execution": "metatrack_dual_track_execution"},
        runnable=True,
    ),
    "metatrack_no_aggregation": ExperimentMethod(
        method_id="metatrack_no_aggregation",
        label="MetaTrack without aggregation",
        role="ablation",
        description="Ablation that disables commit aggregation while keeping other MetaTrack parts.",
        module_overrides={"Commit": "normal_commit"},
        runnable=True,
    ),
}


def list_profiles() -> list[ExperimentProfile]:
    return list(PROFILES.values())


def list_topologies() -> list[ExperimentTopology]:
    return list(TOPOLOGIES.values())


def list_workloads() -> list[ExperimentWorkload]:
    return list(WORKLOADS.values())


def list_default_methods() -> list[ExperimentMethod]:
    return list(METHODS.values())


def list_methods(include_saved: bool = True) -> list[ExperimentMethod]:
    methods = list_default_methods()
    if not include_saved:
        return methods
    for config in list_saved_configs("method"):
        methods.append(_saved_config_to_experiment_method(config))
    return methods


def recommended_run() -> ExperimentRunPlanPreview:
    profile = PROFILES["v4_3_realism_default"]
    return preview_run_plan(
        ExperimentRunPlanRequest(
            profile_id=profile.profile_id,
            topology_id=profile.default_topology_id,
            workload_id=profile.default_workload_id,
        )
    )


def preview_run_plan(request: ExperimentRunPlanRequest) -> ExperimentRunPlanPreview:
    profile = _profile(request.profile_id)
    topology = _topology(request.topology_id)
    workload = _workload(request.workload_id)
    warnings: list[str] = []
    runnable = bool(profile.runnable and topology.runnable and not workload.planned)

    if workload.planned:
        runnable = False
        warnings.append(f"{workload.workload_id}: dataset not attached yet; workload is planned and not runnable.")
    if workload.csv_required and not request.blockemulator_csv:
        runnable = False
        warnings.append(f"{workload.workload_id}: blockemulator_csv is required before this workload can run.")
    if topology.shards > topology.nodes:
        runnable = False
        warnings.append(f"{topology.topology_id}: shard count cannot exceed node count.")
    if topology.topology_id == "local_8_nodes_4_shards":
        warnings.append("local_8_nodes_4_shards: 每片 validator 较少，不适合作为默认 PBFT-style realism 配置。")

    tx_count = request.tx_count_override or workload.default_tx_count
    fault_profile = request.fault_profile_override or "mixed_light"
    recommended = V4RealismSmokeRequest(
        nodes=topology.nodes,
        shards=topology.shards,
        tx_count=tx_count,
        enable_cross_shard=True,
        enable_faults=True,
        fault_profile=fault_profile,
        blockemulator_csv=request.blockemulator_csv,
        blockemulator_tx_limit=workload.default_blockemulator_tx_limit,
        run_duration_ms=1000,
    )
    next_step = "Apply recommended_v4_request in V4 realism mode." if runnable else "Resolve warnings before running V4 realism."
    return ExperimentRunPlanPreview(
        profile=profile,
        topology=topology,
        workload=workload,
        runtime=profile.runtime_target,
        recommended_v4_request=recommended,
        runnable=runnable,
        warnings=warnings,
        next_step=next_step,
    )


def preview_run_matrix(request: ExperimentSuiteRequest) -> ExperimentRunMatrixPreview:
    suite_types = request.selected_suite_types or ["quick_validation"]
    method_ids = request.selected_method_ids or ["metatrack_full"]
    workload_ids = request.workload_ids or ["small_test"]
    seeds = request.seeds or [1]
    methods = [_method(method_id) for method_id in method_ids]
    topologies = _matrix_topologies(request)
    repeat_count = request.conditions.repeat_count if request.conditions else 1
    warnings: list[str] = []
    rows: list[ExperimentMatrixRow] = []

    for suite_type in suite_types:
        for method in methods:
            for workload_id in workload_ids:
                workload = _workload(workload_id)
                for topology_mode, topology in topologies:
                    for seed in seeds:
                        for repeat_index in range(1, repeat_count + 1):
                            row_warnings = _matrix_row_warnings(workload, topology, request.blockemulator_csv)
                            row_warnings.extend(_method_warnings(method))
                            runnable = method.runnable and method.previewable and topology.runnable and not any(_warning_blocks_run(item) for item in row_warnings)
                            runtime_target = "v4.3" if suite_type == "v4_realism_validation" else "v3-formal-preview"
                            tx_count = request.conditions.tx_count if request.conditions else workload.default_tx_count
                            rows.append(
                                ExperimentMatrixRow(
                                    row_id=f"{suite_type}:{method.method_id}:{workload.workload_id}:{topology.topology_id}:seed{seed}:repeat{repeat_index}",
                                    suite_type=suite_type,
                                    method_id=method.method_id,
                                    method_role=method.role,
                                    config_source=method.config_source,
                                    method_config_id=method.config_id,
                                    resolved_method_name=method.label,
                                    validation_status=method.validation_status,
                                    workload_id=workload.workload_id,
                                    topology_id=topology.topology_id,
                                    topology_mode=topology_mode,
                                    nodes=topology.nodes,
                                    shards=topology.shards,
                                    validators_per_shard=topology.validators_per_shard,
                                    tx_count=tx_count,
                                    seed=seed,
                                    repeat_index=repeat_index,
                                    runtime_target=runtime_target,
                                    runnable=runnable,
                                    warnings=row_warnings,
                                )
                            )

    for row in rows:
        for warning in row.warnings:
            if warning not in warnings:
                warnings.append(warning)

    v4_candidates = [
        {
            "row_id": row.row_id,
            "method_id": row.method_id,
            "workload_id": row.workload_id,
            "topology_id": row.topology_id,
            "topology_mode": row.topology_mode,
            "nodes": row.nodes,
            "shards": row.shards,
            "validators_per_shard": row.validators_per_shard,
            "tx_count": row.tx_count,
            "seed": row.seed,
            "repeat_index": row.repeat_index,
        }
        for row in rows
        if row.suite_type == "v4_realism_validation" and row.runnable
    ]
    runnable_count = sum(1 for row in rows if row.runnable)
    blocked_count = len(rows) - runnable_count
    return ExperimentRunMatrixPreview(
        plan_name=request.plan_name or "current_experiment_plan",
        suite_types=suite_types,
        methods=methods,
        rows=rows,
        runnable_row_count=runnable_count,
        blocked_row_count=blocked_count,
        warnings=warnings,
        v4_realism_candidates=v4_candidates,
        next_step="Run matrix preview only; use formal runner or derive V4 request for execution details.",
    )


def derive_v4_realism_request(request: ExperimentSuiteRequest) -> V4DerivedRequestPreview:
    workload_ids = request.workload_ids or ["small_test"]
    warnings: list[str] = []
    selected_workload: ExperimentWorkload | None = None
    selected_topology: ExperimentTopology | None = None

    for workload_id in workload_ids:
        workload = _workload(workload_id)
        if workload.planned:
            warnings.append(f"{workload.workload_id}: dataset not attached yet; workload is planned and not runnable.")
            continue
        if workload.csv_required and not request.blockemulator_csv:
            warnings.append(f"{workload.workload_id}: blockemulator_csv is required before this workload can run.")
            continue
        selected_workload = workload
        break

    for _, topology in _matrix_topologies(request):
        if topology.shards > topology.nodes:
            warnings.append(f"{topology.topology_id}: shard count cannot exceed node count.")
            continue
        selected_topology = topology
        if topology.topology_id == "local_8_nodes_4_shards":
            warnings.append("local_8_nodes_4_shards: fewer validators per shard; use as stress preview, not the default realism configuration.")
        break

    runnable = bool(selected_workload and selected_topology)
    if not selected_workload:
        selected_workload = WORKLOADS["small_test"]
        runnable = False
    if not selected_topology:
        selected_topology = TOPOLOGIES["local_8_nodes_2_shards"]
        runnable = False
    if not runnable and not warnings:
        warnings.append("No runnable topology/workload combination could be derived.")

    requested_tx_count = request.conditions.tx_count if request.conditions else selected_workload.default_tx_count
    v4_nodes = selected_topology.nodes
    v4_shards = selected_topology.shards
    v4_tx_count = requested_tx_count
    if not 4 <= v4_nodes <= 8:
        warnings.append(f"custom nodes={v4_nodes} is outside V4 realism request range 4..8; matrix preview remains available, but V4 derivation is blocked.")
        runnable = False
        v4_nodes = min(max(v4_nodes, 4), 8)
    if not 1 <= v4_shards <= 4:
        warnings.append(f"custom shards={v4_shards} is outside V4 realism request range 1..4; V4 derivation is blocked.")
        runnable = False
        v4_shards = min(max(v4_shards, 1), 4)
    if not 1 <= v4_tx_count <= 100:
        warnings.append(f"tx_count={v4_tx_count} is outside V4 realism request range 1..100; matrix preview remains available, but V4 derivation is blocked.")
        runnable = False
        v4_tx_count = min(max(v4_tx_count, 1), 100)

    v4_request = V4RealismSmokeRequest(
        nodes=v4_nodes,
        shards=v4_shards,
        tx_count=v4_tx_count,
        enable_cross_shard=True,
        enable_faults=True,
        fault_profile="mixed_light",
        blockemulator_csv=request.blockemulator_csv,
        blockemulator_tx_limit=selected_workload.default_blockemulator_tx_limit,
        run_duration_ms=1000,
    )
    return V4DerivedRequestPreview(
        source="experiment_suite_preview",
        v4_request=v4_request,
        runnable=runnable,
        warnings=warnings,
    )


def execute_selected_run_matrix(request: RunSuiteExecutionRequest) -> RunSuiteExecutionResponse:
    if request.run_mode not in {"dry_run", "execute"}:
        raise ValueError("run_mode must be dry_run or execute")
    max_execute_rows = max(0, min(request.max_execute_rows, 10))
    run_group_id = "run_suite_" + datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S_") + uuid4().hex[:6]
    child_runs: list[ChildRunResult] = []
    warnings: list[str] = []
    executed_count = 0

    for row in request.selected_rows:
        blocked_reason = _selected_row_blocked_reason(row)
        if blocked_reason:
            child_runs.append(_blocked_child(row, blocked_reason))
            continue

        if request.run_mode == "dry_run":
            child = _dry_run_child(row)
            child_runs.append(child)
            warnings.extend(warning for warning in child.warnings if warning not in warnings)
            continue

        if executed_count >= max_execute_rows:
            reason = f"max_execute_rows={max_execute_rows} reached; row was not started."
            child_runs.append(_blocked_child(row, reason))
            warnings.append(reason)
            continue

        child = _execute_supported_child(row, request)
        child_runs.append(child)
        if child.status not in {"blocked", "preview_only", "dry_run"}:
            executed_count += 1
        warnings.extend(warning for warning in child.warnings if warning not in warnings)

    blocked_count = sum(1 for child in child_runs if child.status == "blocked")
    started_count = sum(1 for child in child_runs if child.status not in {"blocked", "preview_only", "dry_run"})
    return RunSuiteExecutionResponse(
        run_group_id=run_group_id,
        run_mode=request.run_mode,
        selected_row_count=len(request.selected_rows),
        started_row_count=started_count,
        blocked_row_count=blocked_count,
        child_runs=child_runs,
        warnings=warnings,
        next_step="Inspect child run ids and artifact hints; use V4 details or existing formal panels for deeper results.",
    )


def _profile(profile_id: str) -> ExperimentProfile:
    try:
        return PROFILES[profile_id]
    except KeyError as exc:
        raise ValueError(f"unknown profile_id: {profile_id}") from exc


def _topology(topology_id: str) -> ExperimentTopology:
    try:
        return TOPOLOGIES[topology_id]
    except KeyError as exc:
        raise ValueError(f"unknown topology_id: {topology_id}") from exc


def _workload(workload_id: str) -> ExperimentWorkload:
    try:
        return WORKLOADS[workload_id]
    except KeyError as exc:
        raise ValueError(f"unknown workload_id: {workload_id}") from exc


def _method(method_id: str) -> ExperimentMethod:
    if method_id in METHODS:
        return METHODS[method_id]
    if method_id.startswith("v3cfg_"):
        try:
            config = get_saved_config(method_id)
        except SavedConfigNotFound as exc:
            raise ValueError(f"unknown method_id: {method_id}") from exc
        if config.get("config_kind") != "method":
            raise ValueError(f"saved config is not a method: {method_id}")
        return _saved_config_to_experiment_method(config)
    raise ValueError(f"unknown method_id: {method_id}")


def _saved_config_to_experiment_method(config: dict) -> ExperimentMethod:
    validation_status = str(config.get("validation_status") or "unknown")
    payload = config.get("payload") if isinstance(config.get("payload"), dict) else {}
    modules = payload.get("modules") if isinstance(payload.get("modules"), dict) else {}
    if not modules:
        draft = payload.get("draft") if isinstance(payload.get("draft"), dict) else {}
        modules = draft.get("modules") if isinstance(draft.get("modules"), dict) else {}
    module_overrides = {
        str(module_id): str(module.get("plugin"))
        for module_id, module in modules.items()
        if isinstance(module, dict) and module.get("plugin")
    }
    tags = [str(tag) for tag in config.get("tags", [])]
    return ExperimentMethod(
        method_id=str(config["config_id"]),
        label=str(config.get("name") or config["config_id"]),
        role=_saved_method_role(config),
        description=str(config.get("description") or "Saved V3 Composer method template."),
        module_overrides=module_overrides,
        runnable=validation_status == "runnable",
        config_source="saved_config",
        config_id=str(config["config_id"]),
        validation_status=validation_status,
        tags=tags,
        previewable=validation_status in {"runnable", "valid", "unknown"},
    )


def _saved_method_role(config: dict) -> str:
    tags = {str(tag).strip().lower() for tag in config.get("tags", [])}
    for role in ("main", "baseline", "ablation", "custom"):
        if role in tags:
            return role
    return "custom"


def _matrix_topologies(request: ExperimentSuiteRequest) -> list[tuple[str, ExperimentTopology]]:
    conditions = request.conditions
    if conditions is None:
        topology_ids = request.topology_ids or ["local_8_nodes_2_shards"]
        return [("preset", _topology(topology_id)) for topology_id in topology_ids]
    if conditions.topology_mode == "preset":
        topology_id = conditions.topology_id or (request.topology_ids[0] if request.topology_ids else "local_8_nodes_2_shards")
        return [("preset", _topology(topology_id))]
    if conditions.nodes is None or conditions.shards is None or conditions.validators_per_shard is None:
        raise ValueError("custom topology requires nodes, shards, and validators_per_shard")
    if conditions.shards > conditions.nodes:
        raise ValueError("custom topology shard count cannot exceed node count")
    topology_id = f"custom_{conditions.nodes}n_{conditions.shards}s_{conditions.validators_per_shard}v"
    return [("custom", ExperimentTopology(
        topology_id=topology_id,
        label=f"Custom {conditions.nodes} nodes / {conditions.shards} shards / {conditions.validators_per_shard} validators",
        nodes=conditions.nodes,
        shards=conditions.shards,
        validators_per_shard=conditions.validators_per_shard,
        runtime_mode="custom_preview",
        description="User-provided experiment-flow topology preview.",
        runnable=True,
    ))]


def _method_warnings(method: ExperimentMethod) -> list[str]:
    if method.validation_status == "blocked" or not method.previewable:
        return ["method template is blocked."]
    if method.validation_status in {"valid", "unknown"}:
        return ["method template is not validated as runnable."]
    return []


def _matrix_row_warnings(workload: ExperimentWorkload, topology: ExperimentTopology, blockemulator_csv: str | None) -> list[str]:
    warnings: list[str] = []
    if workload.planned:
        warnings.append(f"{workload.workload_id}: dataset not attached yet; workload is planned and not runnable.")
    if workload.csv_required and not blockemulator_csv:
        warnings.append(f"{workload.workload_id}: blockemulator_csv is required before this workload can run.")
    if topology.shards > topology.nodes:
        warnings.append(f"{topology.topology_id}: shard count cannot exceed node count.")
    if topology.topology_id == "local_8_nodes_4_shards":
        warnings.append("local_8_nodes_4_shards: fewer validators per shard; not recommended as the default PBFT-style realism topology.")
    return warnings


def _warning_blocks_run(warning: str) -> bool:
    return any(marker in warning for marker in (
        "dataset not attached yet",
        "required before this workload can run",
        "cannot exceed node count",
        "method template is not validated as runnable",
        "method template is blocked",
    ))


def _selected_row_blocked_reason(row) -> str | None:
    if not row.runnable:
        return "selected row is not runnable"
    try:
        workload = _workload(row.workload_id)
    except ValueError as exc:
        return str(exc)
    if workload.planned:
        return f"{workload.workload_id}: dataset not attached yet; workload is planned and blocked."
    if row.topology_mode == "custom":
        if min(row.nodes, row.shards, row.validators_per_shard) < 1:
            return "custom topology values must be positive"
        if row.shards > row.nodes:
            return f"{row.topology_id}: shard count cannot exceed node count."
    else:
        try:
            topology = _topology(row.topology_id)
        except ValueError as exc:
            return str(exc)
        if topology.shards > topology.nodes:
            return f"{topology.topology_id}: shard count cannot exceed node count."
    if any(_warning_blocks_run(warning) for warning in row.warnings):
        return "; ".join(row.warnings)
    return None


def _blocked_child(row, reason: str) -> ChildRunResult:
    return ChildRunResult(
        row_id=row.row_id,
        suite_type=row.suite_type,
        method_id=row.method_id,
        status="blocked",
        runner="experiment_flow_execution_bridge",
        warnings=list(row.warnings),
        blocked_reason=reason,
    )


def _dry_run_child(row) -> ChildRunResult:
    warnings: list[str] = []
    status = "dry_run"
    runner = "experiment_flow_execution_bridge"
    if row.suite_type in _FORMAL_PREVIEW_ONLY_SUITES:
        status = "preview_only"
        warnings.append(_FORMAL_PREVIEW_ONLY_WARNING)
    if row.suite_type == "quick_validation":
        runner = "v3_controlled_smoke_runner"
    if row.suite_type == "v4_realism_validation":
        runner = "v4_realism_runner"
    return ChildRunResult(
        row_id=row.row_id,
        suite_type=row.suite_type,
        method_id=row.method_id,
        status=status,
        runner=runner,
        warnings=warnings,
    )


def _execute_supported_child(row, request: RunSuiteExecutionRequest) -> ChildRunResult:
    if row.suite_type == "quick_validation":
        return _execute_quick_validation(row)
    if row.suite_type == "v4_realism_validation":
        return _execute_v4_realism_validation(row, request)
    if row.suite_type in _FORMAL_PREVIEW_ONLY_SUITES:
        return ChildRunResult(
            row_id=row.row_id,
            suite_type=row.suite_type,
            method_id=row.method_id,
            status="preview_only",
            runner="formal_metatrack_runner",
            warnings=[_FORMAL_PREVIEW_ONLY_WARNING],
        )
    return ChildRunResult(
        row_id=row.row_id,
        suite_type=row.suite_type,
        method_id=row.method_id,
        status="preview_only",
        runner="experiment_flow_execution_bridge",
        warnings=[f"{row.suite_type}: execution bridge is not supported in V4.3.4."],
    )


def _execute_quick_validation(row) -> ChildRunResult:
    try:
        result = run_v3_4_10_controlled_smoke()
        return ChildRunResult(
            row_id=row.row_id,
            suite_type=row.suite_type,
            method_id=row.method_id,
            status=str(result.get("status", "completed")),
            runner="v3_controlled_smoke_runner",
            run_id=str(result.get("run_id") or "") or None,
            summary={
                "run_mode": result.get("run_mode"),
                "preset_count": len(result.get("preset_order", [])),
                "backend_type": result.get("backend_type"),
                "data_truth_label": result.get("data_truth_label"),
            },
            artifacts=list(result.get("artifacts", [])),
        )
    except (ControlledSmokeError, ValueError, RuntimeError, OSError) as exc:
        return ChildRunResult(
            row_id=row.row_id,
            suite_type=row.suite_type,
            method_id=row.method_id,
            status="failed",
            runner="v3_controlled_smoke_runner",
            warnings=[str(exc)],
        )


def _execute_v4_realism_validation(row, request: RunSuiteExecutionRequest) -> ChildRunResult:
    try:
        v4_request = request.v4_request_override
        if v4_request is None:
            derived = derive_v4_realism_request(
                ExperimentSuiteRequest(
                    selected_suite_types=[row.suite_type],
                    selected_method_ids=[row.method_id],
                    workload_ids=[row.workload_id],
                    topology_ids=[row.topology_id],
                    seeds=[row.seed],
                    include_v4_realism=True,
                    conditions=ExperimentConditions(
                        topology_mode=row.topology_mode,
                        topology_id=row.topology_id if row.topology_mode == "preset" else None,
                        nodes=row.nodes if row.topology_mode == "custom" else None,
                        shards=row.shards if row.topology_mode == "custom" else None,
                        validators_per_shard=row.validators_per_shard if row.topology_mode == "custom" else None,
                        tx_count=row.tx_count,
                        repeat_count=1,
                    ),
                )
            )
            if not derived.runnable:
                return ChildRunResult(
                    row_id=row.row_id,
                    suite_type=row.suite_type,
                    method_id=row.method_id,
                    status="blocked",
                    runner="v4_realism_runner",
                    warnings=derived.warnings,
                    blocked_reason="derived V4 realism request is not runnable",
                )
            v4_request = derived.v4_request
        result = v4_realism_runner.run_smoke(v4_request)
        return ChildRunResult(
            row_id=row.row_id,
            suite_type=row.suite_type,
            method_id=row.method_id,
            status=str(result.get("status", "completed")),
            runner="v4_realism_runner",
            run_id=str(result.get("run_id") or "") or None,
            summary=dict(result.get("summary") or {}),
            artifacts=list(result.get("artifacts") or []),
            warnings=[str(result.get("stderr") or result.get("stdout") or "")] if result.get("status") == "failed" else [],
        )
    except (ValueError, RuntimeError, OSError) as exc:
        return ChildRunResult(
            row_id=row.row_id,
            suite_type=row.suite_type,
            method_id=row.method_id,
            status="failed",
            runner="v4_realism_runner",
            warnings=[str(exc)],
        )


_FORMAL_PREVIEW_ONLY_SUITES = {
    "main_experiment",
    "comparison_experiment",
    "ablation_experiment",
    "workload_sensitivity",
    "topology_scaling",
}
_FORMAL_PREVIEW_ONLY_WARNING = "formal matrix execution bridge is planned for a later stage; use FormalMetatrackExperimentPanel for current formal runs."
