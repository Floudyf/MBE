from __future__ import annotations

from backend.app.api.v5_formal_experiments import _formal_experiment_profile
from backend.app.models.v5_experiment_spec import V5ExperimentSpec, V5PluginSelection, V5Topology
from backend.app.models.v5_formal_experiment import V5FormalExperimentPlan
from backend.app.services.v5_compatibility_engine import validate as validate_compatibility
from backend.app.services.v5_formal_plan_validator import ALL_BUILTIN_METHODS
from backend.app.services.v5_formal_scheduler import _spec_for, expand
from backend.app.services.v5_plugin_manifest_store import CATEGORIES, STORE


def _base_selections() -> list[V5PluginSelection]:
    selections: list[V5PluginSelection] = []
    for category in CATEGORIES:
        manifest = next(item for item in STORE.list() if item.category == category)
        config = {"cross_shard_ratio": 0.0, "timeout_every": 0} if category == "workload" else {}
        selections.append(V5PluginSelection(category=category, plugin_id=manifest.plugin_id, config=config))
    return selections


def _fabricpp_plan(worker_count: int = 8) -> V5FormalExperimentPlan:
    base = V5ExperimentSpec(
        name="fabricpp-fixed-worker-contract",
        execution_backend="real_cluster",
        plugin_selections=_base_selections(),
        topology=V5Topology(nodes=8, shards=1, validators_per_shard=8),
        tx_count=1000,
        seed=11,
        duration_ms=3_600_000,
    )
    return V5FormalExperimentPlan(
        name="fabricpp-fixed-worker-contract",
        base_spec=base,
        methods=[ALL_BUILTIN_METHODS["hash_fabricpp_cg"].model_copy(deep=True)],
        worker_count=worker_count,
        seeds=[11],
        repeats=1,
        suites=["comparison_experiment"],
    )


def test_fabricpp_manifest_declares_fixed_single_worker_contract() -> None:
    manifest = STORE.get("fabricpp_cg_block_executor")
    worker = manifest.config_schema["properties"]["worker_count"]
    assert manifest.default_config["worker_count"] == 1
    assert worker["minimum"] == 1
    assert worker["maximum"] == 1
    assert worker["default"] == 1
    assert worker["readOnly"] is True


def test_formal_matrix_worker_request_does_not_override_fabricpp_effective_worker() -> None:
    plan = _fabricpp_plan(worker_count=8)
    rows = expand(plan, "real_cluster")
    assert len(rows) == 1
    row = rows[0]
    assert row["topology_point"]["worker_count"] == 8

    spec = _spec_for(plan, row)
    block_executor = next(item for item in spec.plugin_selections if item.category == "block_executor")
    assert block_executor.plugin_id == "fabricpp_cg_block_executor"
    assert block_executor.config["worker_count"] == 1

    compatibility = validate_compatibility(spec)
    assert compatibility.valid is True
    assert not any("worker_count is above maximum" in item for item in compatibility.blockers)


def test_formal_profile_records_requested_8_but_effective_1_for_fabricpp() -> None:
    plan = _fabricpp_plan(worker_count=8)
    rows = expand(plan, "real_cluster")
    profile = _formal_experiment_profile(plan.model_dump(), rows)
    truth = profile["worker_execution_truth"]["hash_fabricpp_cg"]
    assert truth["registered_default_worker_count"] == 1
    assert truth["requested_worker_count"] == 8
    assert truth["effective_worker_count"] == 1
    assert truth["effective_worker_counts"] == [1]
