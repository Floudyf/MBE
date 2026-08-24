from __future__ import annotations

from backend.app.models.v5_experiment_spec import V5ExperimentSpec, V5PluginSelection, V5Topology
from backend.app.models.v5_formal_experiment import V5FormalExperimentPlan, V5FormalRunRequest
from backend.app.services.v5_compatibility_engine import validate
from backend.app.services.v5_formal_plan_validator import (
    ALL_BUILTIN_METHODS,
    BATCH_SI_BUILTIN_METHODS,
    FormalPlanValidationError,
    _builtin_method_payload_matches,
    _validate_suite_shape,
    validate_request,
)
from backend.app.services.v5_formal_scheduler import _spec_for, expand
from backend.app.services.v5_plugin_manifest_store import CATEGORIES, STORE


def _selections() -> list[V5PluginSelection]:
    preferred = {
        "workload": "deterministic_signed_synthetic",
        "transaction_admission": "signature_nonce_admission",
        "txpool": "fifo_per_node_mempool",
        "sharding": "deterministic_state_key_sharding",
        "routing": "hash_routing_baseline",
        "block_producer": "time_or_count_block_producer",
        "consensus": "pbft_style_consensus",
        "network": "localhost_tcp_typed_network",
        "execution": "serial_execution_baseline",
        "scheduler": "fifo_serial_scheduler",
        "block_executor": "serial_block_executor",
        "state_access": "direct_state_access",
        "state_storage": "persistent_local_state_store",
        "cross_shard": "relay_certificate_protocol",
        "commit": "normal_commit",
        "fault_injection": "faults_disabled",
        "metrics": "runtime_core_metrics",
        "observability": "node_network_consensus_observer",
    }
    out: list[V5PluginSelection] = []
    for category in CATEGORIES:
        manifest = STORE.get(preferred[category])
        config = dict(manifest.default_config)
        if category == "workload":
            config |= {"cross_shard_ratio": 0, "timeout_every": 0}
        if category == "block_producer":
            config |= {"block_size": 100, "interval_ms": 75}
        out.append(V5PluginSelection(category=category, plugin_id=manifest.plugin_id, config=config))
    return out


def _base_spec() -> V5ExperimentSpec:
    return V5ExperimentSpec(
        name="batch-si-plugin-test",
        execution_backend="real_cluster",
        plugin_selections=_selections(),
        topology=V5Topology(nodes=4, shards=1, validators_per_shard=4),
        tx_count=100,
        seed=7,
        duration_ms=9000,
    )


def _plan(method_ids: list[str], suite: str = "main_experiment") -> V5FormalExperimentPlan:
    return V5FormalExperimentPlan(
        name="batch-si-plan",
        base_spec=_base_spec(),
        methods=[BATCH_SI_BUILTIN_METHODS[item].model_copy(deep=True) for item in method_ids],
        suites=[suite],
        seeds=[7],
        repeats=1,
    )


def test_batch_si_manifests_are_private_and_paired() -> None:
    execution = STORE.get("batch_si_execution")
    scheduler = STORE.get("batch_si_scheduler")
    executor = STORE.get("batch_si_block_executor")
    assert execution.category == "execution"
    assert scheduler.category == "scheduler"
    assert executor.category == "block_executor"
    assert execution.supported_backends == ["real_cluster"]
    assert "no_cross_scheme_algorithm_reuse" in execution.capabilities
    assert "execution:batch_si_execution" in scheduler.requirements
    assert "execution:batch_si_execution" in executor.requirements
    assert executor.default_config["worker_count"] == 4
    assert executor.config_schema["properties"]["worker_count"]["maximum"] == 32
    assert STORE.get("metatrack_block_executor").config_schema["properties"]["worker_count"]["maximum"] == 8


def test_batch_si_builtin_registry_contains_full_and_four_ablations() -> None:
    assert set(BATCH_SI_BUILTIN_METHODS) == {
        "hash_batch_si",
        "hash_batch_si_no_wrbp",
        "hash_batch_si_no_ofas",
        "hash_batch_si_serial_batch",
        "hash_batch_si_txid_priority",
    }
    full = BATCH_SI_BUILTIN_METHODS["hash_batch_si"]
    assert full.plugin_overrides["execution"] == "batch_si_execution"
    assert full.plugin_overrides["scheduler"] == "batch_si_scheduler"
    assert full.plugin_overrides["block_executor"] == "batch_si_block_executor"
    assert BATCH_SI_BUILTIN_METHODS["hash_batch_si_no_wrbp"].plugin_config_overrides["scheduler"]["partition_mode"] == "sequential"
    assert BATCH_SI_BUILTIN_METHODS["hash_batch_si_no_ofas"].plugin_config_overrides["scheduler"]["ordering_mode"] == "dependency_graph"
    assert BATCH_SI_BUILTIN_METHODS["hash_batch_si_serial_batch"].plugin_config_overrides["block_executor"]["execution_mode"] == "snapshot_serial"
    assert BATCH_SI_BUILTIN_METHODS["hash_batch_si_txid_priority"].plugin_config_overrides["scheduler"]["priority_mode"] == "txid"


def test_batch_si_builtin_payload_is_registry_locked() -> None:
    method = BATCH_SI_BUILTIN_METHODS["hash_batch_si"].model_copy(deep=True)
    method.plugin_config_overrides["block_executor"]["worker_count"] = 8
    assert not _builtin_method_payload_matches(method, BATCH_SI_BUILTIN_METHODS["hash_batch_si"])

    forged = BATCH_SI_BUILTIN_METHODS["hash_batch_si"].model_copy(deep=True)
    forged.plugin_config_overrides["scheduler"]["partition_mode"] = "sequential"
    assert not _builtin_method_payload_matches(forged, BATCH_SI_BUILTIN_METHODS["hash_batch_si"])


def test_batch_si_worker_override_is_applied_by_formal_plan() -> None:
    plan = _plan(["hash_batch_si"])
    plan.worker_count = 8
    row = expand(plan, "real_cluster")[0]
    assert row["topology_point"]["worker_count"] == 8
    spec = _spec_for(plan, row)
    executor = next(item for item in spec.plugin_selections if item.category == "block_executor")
    assert executor.config["worker_count"] == 8


def test_batch_si_compatibility_accepts_core_profile_and_rejects_mismatches() -> None:
    plan = _plan(["hash_batch_si"])
    row = expand(plan, "real_cluster")[0]
    spec = _spec_for(plan, row)
    result = validate(spec)
    assert result.valid, result.blockers
    assert any("common immutable snapshot" in item for item in result.warnings)

    wrong_execution = spec.model_copy(deep=True)
    next(item for item in wrong_execution.plugin_selections if item.category == "execution").plugin_id = "serial_execution_baseline"
    wrong_result = validate(wrong_execution)
    assert not wrong_result.valid
    assert any("must be selected together" in item for item in wrong_result.blockers)

    execution_only = spec.model_copy(deep=True)
    next(item for item in execution_only.plugin_selections if item.category == "scheduler").plugin_id = "fifo_serial_scheduler"
    next(item for item in execution_only.plugin_selections if item.category == "block_executor").plugin_id = "serial_block_executor"
    execution_only_result = validate(execution_only)
    assert not execution_only_result.valid
    assert any("must be selected together" in item for item in execution_only_result.blockers)

    config_mismatch = spec.model_copy(deep=True)
    next(item for item in config_mismatch.plugin_selections if item.category == "scheduler").config["partition_mode"] = "sequential"
    mismatch_result = validate(config_mismatch)
    assert not mismatch_result.valid
    assert any("partition_mode must match" in item for item in mismatch_result.blockers)

    cross_shard = spec.model_copy(deep=True)
    next(item for item in cross_shard.plugin_selections if item.category == "workload").config["cross_shard_ratio"] = 0.25
    cross_result = validate(cross_shard)
    assert not cross_result.valid
    assert any("cross_shard_ratio=0" in item for item in cross_result.blockers)


def test_batch_si_ablation_recognizes_config_only_differences() -> None:
    plan = _plan(["hash_batch_si", "hash_batch_si_no_wrbp", "hash_batch_si_no_ofas"], "ablation_experiment")
    _validate_suite_shape(plan)
    request = V5FormalRunRequest(execution_backend="real_cluster", plan=plan)
    validated = validate_request(request)
    assert len(validated.rows) == 3
    assert all(row["runnable"] for row in validated.rows)
    changed = {row["method_config_id"]: set(row["changed_plugin_categories"]) for row in validated.rows}
    assert changed["hash_batch_si_no_wrbp"] == {"scheduler", "block_executor"}
    assert changed["hash_batch_si_no_ofas"] == {"scheduler", "block_executor"}


def test_batch_si_topology_worker_scan_changes_executor_not_process_count() -> None:
    plan = _plan(["hash_batch_si"], "topology_scaling")
    plan.topology_points = [
        {"nodes": 4, "shards": 1, "validators_per_shard": 4, "worker_count": 1},
        {"nodes": 4, "shards": 1, "validators_per_shard": 4, "worker_count": 8},
    ]
    rows = expand(plan, "real_cluster")
    assert len(rows) == 2
    assert {row["estimated_processes"] for row in rows} == {4}
    worker_counts = set()
    for row in rows:
        spec = _spec_for(plan, row)
        executor = next(item for item in spec.plugin_selections if item.category == "block_executor")
        worker_counts.add(executor.config["worker_count"])
        assert spec.topology.nodes == 4
    assert worker_counts == {1, 8}
    assert {row["comparison_semantics_class"] for row in rows} == {"batch_si_common_batch_snapshot_v1"}


def test_batch_si_ablation_requires_full_main_and_a_real_variant() -> None:
    invalid = _plan(["hash_batch_si_no_wrbp", "hash_batch_si_no_ofas"], "ablation_experiment")
    try:
        _validate_suite_shape(invalid)
    except FormalPlanValidationError as exc:
        assert "one main method" in str(exc)
    else:
        raise AssertionError("ablation without full Batch-SI must be rejected")


def test_batch_si_method_comparison_uses_its_own_semantics_class_without_blocking_the_matrix() -> None:
    method_ids = [
        "hash_serial",
        "hash_block_stm",
        "hash_groundhog",
        "hash_batch_si",
        "stateless_hash_serial",
        "stateless_hash_block_stm",
        "metatrack_serial",
        "metatrack_block_stm",
    ]
    plan = V5FormalExperimentPlan(
        name="batch-si-eight-method-comparison",
        base_spec=_base_spec(),
        methods=[ALL_BUILTIN_METHODS[item].model_copy(deep=True) for item in method_ids],
        suites=["comparison_experiment"],
        seeds=[7],
        repeats=1,
        worker_count=4,
    )

    rows = expand(plan, "real_cluster")

    assert len(rows) == 8
    assert all(row["runnable"] for row in rows), {
        row["method_config_id"]: row["blockers"] for row in rows if row["blockers"]
    }
    batch_si = next(row for row in rows if row["method_config_id"] == "hash_batch_si")
    assert batch_si["comparison_semantics_class"] == "batch_si_common_batch_snapshot_v1"
    assert batch_si["state_access_semantics"] == "sequential_batches_common_batch_snapshot"
    assert batch_si["proof_policy"] == "pre_consensus_batch_plan_digest"
    assert batch_si["legacy_cross_shard_protocol"] is False
    assert batch_si["measurement_boundary"] == "client_submit_to_batch_si_terminal"
    assert batch_si["performance_comparison_valid"] is False
    assert batch_si["comparison_warning"] == "execution semantics differ; direct performance uplift is invalid"
