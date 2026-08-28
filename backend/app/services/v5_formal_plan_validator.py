from __future__ import annotations

from dataclasses import dataclass
import json
from numbers import Real

from backend.app.models.v5_formal_experiment import V5FormalExperimentPlan, V5FormalMethod, V5FormalRunRequest
from backend.app.services.v3_saved_config_store import SavedConfigNotFound, get_saved_config
from backend.app.services.v5_compatibility_engine import validate
from backend.app.services.v5_formal_scheduler import _spec_for, expand
from backend.app.services.v5_plugin_manifest_store import STORE


CATALOG_DEFAULT_METHOD_ID = "v5_catalog_default"
METHOD_EXCLUDED_CATEGORIES = {"workload", "fault_injection"}
MAX_CHILD_RUNS = 1024


def _batch_si_method(
    method_id: str,
    display_name: str,
    *,
    role: str,
    partition_mode: str = "wrbp",
    ordering_mode: str = "ofas",
    priority_mode: str = "paper",
    execution_mode: str = "snapshot_parallel",
    worker_count: int = 4,
) -> V5FormalMethod:
    shared = {
        "partition_mode": partition_mode,
        "ordering_mode": ordering_mode,
        "priority_mode": priority_mode,
    }
    return V5FormalMethod(
        method_id=method_id,
        display_name=display_name,
        role=role,
        plugin_overrides={
            "routing": "hash_routing_baseline",
            "execution": "batch_si_execution",
            "scheduler": "batch_si_scheduler",
            "block_executor": "batch_si_block_executor",
            "commit": "normal_commit",
        },
        plugin_config_overrides={
            "scheduler": dict(shared),
            "block_executor": {
                **shared,
                "worker_count": worker_count,
                "execution_mode": execution_mode,
            },
        },
    )


BUILTIN_METHODS: dict[str, V5FormalMethod] = {
    "hash_serial": V5FormalMethod(
        method_id="hash_serial",
        display_name="Stateful Hash + Serial Reference",
        role="baseline",
        plugin_overrides={
            "routing": "hash_routing_baseline",
            "execution": "serial_execution_baseline",
            "scheduler": "fifo_serial_scheduler",
            "block_executor": "serial_block_executor",
            "commit": "normal_commit",
        },
        plugin_config_overrides={"block_executor": {"worker_count": 1}},
    ),
    "hash_block_stm": V5FormalMethod(
        method_id="hash_block_stm",
        display_name="Stateful Hash + Block-STM Reference",
        role="main",
        plugin_overrides={
            "routing": "hash_routing_baseline",
            "execution": "serial_execution_baseline",
            "scheduler": "fifo_serial_scheduler",
            "block_executor": "block_stm_block_executor",
            "commit": "normal_commit",
        },
        plugin_config_overrides={"block_executor": {"worker_count": 4, "execution_mode": "performance", "oracle_mode": "off", "maximum_incarnations": 0, "incarnation_limit_action": "fail"}},
    ),
    "hash_aria": V5FormalMethod(
        method_id="hash_aria",
        display_name="Aria",
        role="baseline",
        plugin_overrides={
            "routing": "hash_routing_baseline",
            "block_producer": "aria_block_producer",
            "execution": "serial_execution_baseline",
            "scheduler": "fifo_serial_scheduler",
            "block_executor": "aria_block_executor",
            "commit": "normal_commit",
        },
        plugin_config_overrides={
            "block_producer": {
                "candidate_scan_multiplier": 1,
                "reordering": True,
                "read_only_optimization": True,
                "retry_nonce_gaps": True,
            },
            "block_executor": {
                "worker_count": 4,
                "reordering": True,
                "read_only_optimization": True,
                "retry_nonce_gaps": True,
            },
        },
    ),
    "hash_groundhog": V5FormalMethod(
        method_id="hash_groundhog",
        display_name="Groundhog",
        role="baseline",
        plugin_overrides={
            "routing": "hash_routing_baseline",
            "block_producer": "groundhog_block_producer",
            "execution": "serial_execution_baseline",
            "scheduler": "fifo_serial_scheduler",
            "block_executor": "groundhog_block_executor",
            "commit": "normal_commit",
        },
        plugin_config_overrides={
            "block_producer": {
                "candidate_scan_multiplier": 4,
                "ordered_set_limit": 64,
            },
            "block_executor": {
                "worker_count": 4,
                "ordered_set_limit": 64,
            },
        },
    ),
    # MBE_META_TRACK_RAPID_FIX_V3
    "metatrack_serial": V5FormalMethod(
        method_id="metatrack_serial",
        display_name="MetaTrack",
        role="main",
        plugin_overrides={
            "routing": "metatrack_coaccess_routing",
            "execution": "dual_track_execution",
            "scheduler": "fast_first_scheduler",
            "block_executor": "metatrack_block_executor",
            "commit": "commutative_hot_update_aggregation",
        },
        plugin_config_overrides={"execution": {"access_size_threshold": 4}, "block_executor": {"worker_count": 4}},
    ),
    "metatrack_block_stm": V5FormalMethod(
        method_id="metatrack_block_stm",
        display_name="MetaTrack with Block-STM backend",
        role="compatibility",
        plugin_overrides={
            "routing": "metatrack_coaccess_routing",
            "execution": "dual_track_execution",
            "scheduler": "fast_first_scheduler",
            "block_executor": "block_stm_block_executor",
            "commit": "commutative_hot_update_aggregation",
        },
        plugin_config_overrides={"execution": {"access_size_threshold": 4}, "block_executor": {"worker_count": 4, "execution_mode": "performance", "oracle_mode": "off", "maximum_incarnations": 0, "incarnation_limit_action": "fail"}},
    ),
}

LITERATURE_BUILTIN_METHODS: dict[str, V5FormalMethod] = {
    "hash_fabricpp_cg": V5FormalMethod(
        method_id="hash_fabricpp_cg", display_name="Fabric++ Traditional CG (SIGMOD 2019)", role="baseline",
        plugin_overrides={"routing":"hash_routing_baseline","execution":"fabricpp_cg_execution","scheduler":"fabricpp_cg_scheduler","block_executor":"fabricpp_cg_block_executor","commit":"normal_commit"},
        plugin_config_overrides={"block_executor":{"worker_count":1}},
    ),
    "hash_cg": V5FormalMethod(
        method_id="hash_cg", display_name="CG / Nezha", role="baseline",
        plugin_overrides={"routing":"hash_routing_baseline","execution":"cg_execution","scheduler":"cg_scheduler","block_executor":"cg_block_executor","commit":"normal_commit"},
        plugin_config_overrides={"block_executor":{"worker_count":4}},
    ),
    "hash_acg": V5FormalMethod(
        method_id="hash_acg", display_name="Address Conflict Graph (ACG/Nezha)", role="baseline",
        plugin_overrides={"routing":"hash_routing_baseline","execution":"acg_execution","scheduler":"acg_scheduler","block_executor":"acg_block_executor","commit":"normal_commit"},
        plugin_config_overrides={"block_executor":{"worker_count":4}},
    ),
    "hash_bsx": V5FormalMethod(
        method_id="hash_bsx", display_name="Batch-Schedule-Execute (BSX)", role="baseline",
        plugin_overrides={"routing":"hash_routing_baseline","execution":"bsx_execution","scheduler":"bsx_scheduler","block_executor":"bsx_block_executor","commit":"normal_commit"},
        plugin_config_overrides={"block_executor":{"worker_count":4}},
    ),
}


BATCH_SI_BUILTIN_METHODS: dict[str, V5FormalMethod] = {
    "hash_batch_si": _batch_si_method(
        "hash_batch_si", "Batch-SI", role="main"
    ),
    "hash_batch_si_no_wrbp": _batch_si_method(
        "hash_batch_si_no_wrbp", "Batch-SI w/o WRBP", role="ablation", partition_mode="sequential"
    ),
    "hash_batch_si_no_ofas": _batch_si_method(
        "hash_batch_si_no_ofas", "Batch-SI w/o OFAS", role="ablation", ordering_mode="dependency_graph"
    ),
    "hash_batch_si_serial_batch": _batch_si_method(
        "hash_batch_si_serial_batch", "Batch-SI w/o Snapshot Parallelism", role="ablation", execution_mode="snapshot_serial"
    ),
    "hash_batch_si_txid_priority": _batch_si_method(
        "hash_batch_si_txid_priority", "Batch-SI w/o OFAS Priority", role="ablation", priority_mode="txid"
    ),
}


STATELESS_BUILTIN_METHODS: dict[str, V5FormalMethod] = {
    "stateless_hash_serial": V5FormalMethod(
        method_id="stateless_hash_serial",
        display_name="Stateless Hash + Serial",
        role="baseline",
        plugin_overrides={
            "routing": "stateless_hash_routing",
            "execution": "serial_execution_baseline",
            "scheduler": "fifo_serial_scheduler",
            "block_executor": "serial_block_executor",
            "commit": "normal_commit",
        },
        plugin_config_overrides={"block_executor": {"worker_count": 1}},
    ),
    "stateless_hash_block_stm": V5FormalMethod(
        method_id="stateless_hash_block_stm",
        display_name="Stateless Hash + Block-STM",
        role="compatibility",
        plugin_overrides={
            "routing": "stateless_hash_routing",
            "execution": "serial_execution_baseline",
            "scheduler": "fifo_serial_scheduler",
            "block_executor": "block_stm_block_executor",
            "commit": "normal_commit",
        },
        plugin_config_overrides={"block_executor": {"worker_count": 4, "execution_mode": "performance", "oracle_mode": "off", "maximum_incarnations": 0, "incarnation_limit_action": "fail"}},
    ),
}

ALL_BUILTIN_METHODS: dict[str, V5FormalMethod] = {**BUILTIN_METHODS, **LITERATURE_BUILTIN_METHODS, **BATCH_SI_BUILTIN_METHODS, **STATELESS_BUILTIN_METHODS}



class FormalPlanValidationError(ValueError):
    pass


def validate_child_run_capacity(child_count: int) -> None:
    # Pure capacity guard; scheduler execution semantics remain unchanged.
    if child_count < 1:
        raise FormalPlanValidationError("formal matrix must contain at least one Child Run")
    if child_count > MAX_CHILD_RUNS:
        raise FormalPlanValidationError(
            f"formal matrix exceeds the {MAX_CHILD_RUNS} Child Run limit"
        )


@dataclass(frozen=True)
class ValidatedFormalPlan:
    plan: V5FormalExperimentPlan
    rows: list[dict]


def validate_request(payload: V5FormalRunRequest, *, allow_blocked_rows: bool = False) -> ValidatedFormalPlan:
    if payload.execution_backend != payload.plan.base_spec.execution_backend:
        raise FormalPlanValidationError("execution_backend must match plan.base_spec.execution_backend")
    if payload.execution_backend != "real_cluster":
        raise FormalPlanValidationError("formal RunGroup execution only supports real_cluster")

    plan = payload.plan.model_copy(deep=True)
    plan.methods = [_verified_method(method) for method in plan.methods]
    _validate_fault_points(plan)
    _validate_suite_shape(plan)
    rows = expand(plan, payload.execution_backend)
    validate_child_run_capacity(len(rows))

    row_errors: list[str] = []
    for row in rows:
        try:
            result = validate(_spec_for(plan, row))
            if not result.valid:
                row["blockers"] = sorted(set(row.get("blockers", []) + result.blockers))
                row["warnings"] = sorted(set(row.get("warnings", []) + result.warnings))
                row["runnable"] = False
        except (ValueError, TypeError) as exc:
            row["blockers"] = sorted(set(row.get("blockers", []) + [str(exc)]))
            row["runnable"] = False
        if row.get("blockers"):
            row_errors.extend(f"{row['child_run_id']}: {message}" for message in row["blockers"])
    if row_errors and not allow_blocked_rows:
        raise FormalPlanValidationError("; ".join(row_errors))
    return ValidatedFormalPlan(plan=plan, rows=rows)


def _verified_method(method: V5FormalMethod) -> V5FormalMethod:
    if method.method_id == CATALOG_DEFAULT_METHOD_ID:
        if method.plugin_overrides:
            raise FormalPlanValidationError("v5_catalog_default must not contain plugin overrides")
        return V5FormalMethod(method_id=CATALOG_DEFAULT_METHOD_ID, display_name="V5 Catalog Default", plugin_overrides={}, role="baseline")
    if method.method_id in ALL_BUILTIN_METHODS:
        expected = ALL_BUILTIN_METHODS[method.method_id]
        if not _builtin_method_payload_matches(method, expected):
            raise FormalPlanValidationError(f"builtin method payload does not match registry: {method.method_id}")
        verified = expected.model_copy(deep=True)
        verified.plugin_config_overrides = {category: dict(config) for category, config in method.plugin_config_overrides.items()}
        return verified
    try:
        saved = get_saved_config(method.method_id)
    except SavedConfigNotFound as exc:
        raise FormalPlanValidationError(f"unknown saved V5 method: {method.method_id}") from exc
    payload = saved.get("payload") if isinstance(saved.get("payload"), dict) else {}
    snapshot = payload.get("compatibility_snapshot") if isinstance(payload.get("compatibility_snapshot"), dict) else {}
    selections = payload.get("plugin_selections")
    if saved.get("config_kind") != "method" or saved.get("validation_status") != "runnable":
        raise FormalPlanValidationError(f"saved method is not runnable: {method.method_id}")
    if payload.get("schema_version") != "v5_plugin_profile_v1" or snapshot.get("valid") is not True or not isinstance(selections, list) or not selections:
        raise FormalPlanValidationError(f"saved method is not a valid V5 plugin profile: {method.method_id}")
    expected: dict[str, str] = {}
    for selection in selections:
        if not isinstance(selection, dict):
            raise FormalPlanValidationError(f"saved method has malformed plugin selection: {method.method_id}")
        category, plugin_id = selection.get("category"), selection.get("plugin_id")
        if not isinstance(category, str) or not category or not isinstance(plugin_id, str) or not plugin_id or category in METHOD_EXCLUDED_CATEGORIES or category in expected:
            raise FormalPlanValidationError(f"saved method has invalid plugin selection: {method.method_id}")
        try:
            manifest = STORE.get(plugin_id)
        except ValueError as exc:
            raise FormalPlanValidationError(f"saved method references unknown plugin: {plugin_id}") from exc
        if manifest.category != category:
            raise FormalPlanValidationError(f"saved method plugin category mismatch: {category}/{plugin_id}")
        expected[category] = manifest.plugin_id
    if "block_executor" not in expected:
        expected["block_executor"] = _default_plugin("block_executor")
    expected_config_overrides = payload.get("plugin_config_overrides") if isinstance(payload.get("plugin_config_overrides"), dict) else {}
    if method.display_name != saved.get("name") or method.plugin_overrides != expected or method.plugin_config_overrides != expected_config_overrides:
        raise FormalPlanValidationError(f"method profile payload does not match saved config: {method.method_id}")
    draft = payload.get("source_composer_draft") if isinstance(payload.get("source_composer_draft"), dict) else {}
    roles = {"main", "baseline", "ablation", "compatibility", "custom"}
    role = draft.get("role") if draft.get("role") in roles else next((tag for tag in saved.get("tags", []) if tag in roles), "custom")
    return V5FormalMethod(method_id=method.method_id, display_name=saved["name"], plugin_overrides=expected, plugin_config_overrides=expected_config_overrides, role=role)


def _builtin_method_payload_matches(method: V5FormalMethod, expected: V5FormalMethod) -> bool:
    return (
        method.display_name == expected.display_name
        and method.plugin_overrides == expected.plugin_overrides
        and method.plugin_config_overrides == expected.plugin_config_overrides
    )


def _validate_suite_shape(plan: V5FormalExperimentPlan) -> None:
    if not plan.methods:
        raise FormalPlanValidationError("at least one execution method is required")
    if not plan.suites:
        raise FormalPlanValidationError("at least one experiment suite is required")
    method_ids = [method.method_id for method in plan.methods]
    if len(set(method_ids)) != len(method_ids):
        raise FormalPlanValidationError("formal methods must be unique")
    for suite in plan.suites:
        if suite == "comparison_experiment" and len(plan.methods) < 2:
            raise FormalPlanValidationError("comparison_experiment requires at least two methods")
        if suite == "ablation_experiment":
            mains = [method for method in plan.methods if method.role == "main"]
            controls = [method for method in plan.methods if method.role in {"ablation", "baseline"}]
            if len(plan.methods) < 2 or len(mains) != 1 or not controls:
                raise FormalPlanValidationError("ablation_experiment requires one main method and at least one ablation or baseline method")
            if not any(method.method_id != CATALOG_DEFAULT_METHOD_ID for method in mains):
                raise FormalPlanValidationError("ablation_experiment requires a saved main method")
            snapshots = {tuple(sorted(_effective_snapshot(plan, method).items())) for method in plan.methods}
            if len(snapshots) < 2:
                raise FormalPlanValidationError("ablation_experiment methods must differ in plugin selections")
            main_snapshot = _effective_snapshot(plan, mains[0])
            for method in (method for method in plan.methods if method.role in {"ablation", "baseline"}):
                if not _changed_categories(main_snapshot, _effective_snapshot(plan, method)):
                    raise FormalPlanValidationError("each ablation or baseline method must differ from the main method")
        if suite == "workload_sensitivity" and len(plan.workload_points) < 2:
            raise FormalPlanValidationError("workload_sensitivity requires at least two workload points")
        if suite == "topology_scaling" and len(plan.topology_points) < 2:
            raise FormalPlanValidationError("topology_scaling requires at least two topology points")
        if suite == "fault_recovery_experiment":
            if len(plan.fault_points) < 2:
                raise FormalPlanValidationError("fault_recovery_experiment requires at least two fault points")
            if not any(str(point.get("mode", "disabled")) == "disabled" for point in plan.fault_points):
                raise FormalPlanValidationError("fault_recovery_experiment requires a disabled baseline fault point")


def _effective_snapshot(plan: V5FormalExperimentPlan, method: V5FormalMethod) -> dict[str, str]:
    snapshot: dict[str, str] = {}
    for selection in plan.base_spec.plugin_selections:
        if selection.category in METHOD_EXCLUDED_CATEGORIES:
            continue
        plugin_id = method.plugin_overrides.get(selection.category, selection.plugin_id)
        manifest = STORE.get(plugin_id)
        config = dict(manifest.default_config)
        if plugin_id == selection.plugin_id:
            config |= dict(selection.config)
        config |= dict(method.plugin_config_overrides.get(selection.category, {}))
        snapshot[selection.category] = json.dumps(
            {"plugin_id": manifest.plugin_id, "config": config},
            sort_keys=True,
            separators=(",", ":"),
            default=str,
        )
    return snapshot


def _changed_categories(left: dict[str, str], right: dict[str, str]) -> list[str]:
    return sorted(category for category in sorted(set(left) | set(right)) if left.get(category) != right.get(category))


def _default_plugin(category: str) -> str:
    return next(item.plugin_id for item in STORE.list() if item.category == category and item.implementation_status == "implemented")


_FAULT_FIELDS = {
    "disabled": {"mode"},
    "delay_only": {"mode", "delay_ms"},
    "network_drop": {"mode", "delay_ms", "drop_rate", "drop_message_types", "drop_every"},
}


def _validate_fault_points(plan: V5FormalExperimentPlan) -> None:
    for point in plan.fault_points:
        if not isinstance(point, dict):
            raise FormalPlanValidationError("fault point must be an object")
        mode = point.get("mode")
        if mode not in _FAULT_FIELDS:
            if mode in {"kill_node", "restart_node"}:
                raise FormalPlanValidationError(f"{mode} is not connected to the V5 real_cluster node lifecycle and cannot run as a formal experiment")
            raise FormalPlanValidationError("fault point has an unsupported mode")
        if set(point) - _FAULT_FIELDS[mode]:
            raise FormalPlanValidationError(f"fault point fields do not match mode {mode}")
        if "drop_every" in point:
            raise FormalPlanValidationError("drop_every is not consumed by the V5 Runtime; use drop_rate")
        for key in {"delay_ms"} & set(point):
            value = point[key]
            if isinstance(value, bool) or not isinstance(value, int) or value < 0 or (key == "delay_ms" and value > 1000):
                raise FormalPlanValidationError(f"fault point {key} must be an in-range integer")
        if "drop_rate" in point:
            value = point["drop_rate"]
            if isinstance(value, bool) or not isinstance(value, Real) or not 0 < value <= 1:
                raise FormalPlanValidationError("fault point drop_rate must be greater than 0 and at most 1")
        if mode == "delay_only" and "delay_ms" not in point:
            raise FormalPlanValidationError("delay_only requires delay_ms")
        if mode == "network_drop" and "drop_rate" not in point:
            raise FormalPlanValidationError("network_drop requires drop_rate")
