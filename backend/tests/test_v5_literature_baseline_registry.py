from backend.app.models.v5_experiment_spec import V5ExperimentSpec, V5PluginSelection, V5Topology
from backend.app.models.v5_formal_experiment import V5FormalExperimentPlan, V5FormalRunRequest
from backend.app.services.v5_formal_plan_validator import ALL_BUILTIN_METHODS, BUILTIN_METHODS, validate_request
from backend.app.services.v5_plugin_manifest_store import CATEGORIES, STORE


EXPECTED = {
    "hash_cg": ("cg_execution", "cg_scheduler", "cg_block_executor"),
    "hash_acg": ("acg_execution", "acg_scheduler", "acg_block_executor"),
    "hash_bsx": ("bsx_execution", "bsx_scheduler", "bsx_block_executor"),
}


def test_cg_acg_bsx_builtins_and_manifests_are_registered():
    for method_id, (execution, scheduler, block_executor) in EXPECTED.items():
        method = ALL_BUILTIN_METHODS[method_id]
        assert method.plugin_overrides["execution"] == execution
        assert method.plugin_overrides["scheduler"] == scheduler
        assert method.plugin_overrides["block_executor"] == block_executor
        assert method.plugin_config_overrides["block_executor"]["worker_count"] == 4
        assert STORE.get(execution).supported_backends == ["real_cluster"]
        assert STORE.get(scheduler).supported_backends == ["real_cluster"]
        assert STORE.get(block_executor).supported_backends == ["real_cluster"]


def test_truth_boundaries_disclose_reimplementation_scope():
    assert STORE.get("cg_scheduler").truth_boundary == "nezha_cg_official_multigraph_bounded_johnson_retry_mbe_worker_v4"
    assert STORE.get("acg_scheduler").truth_boundary == "nezha_acg_hs_official_reference_mbe_retry_v2"
    assert STORE.get("bsx_scheduler").truth_boundary == "bsx_homogeneous_conflict_graph_coloring_dsatur_v1"


def test_existing_builtin_family_is_not_rewritten_by_literature_baselines():
    # The three added paper baselines live in their own registry family. Existing
    # Serial/Block-STM/Aria/Groundhog/MetaTrack payloads remain registry-locked.
    assert list(BUILTIN_METHODS) == [
        "hash_serial",
        "hash_block_stm",
        "hash_aria",
        "hash_groundhog",
        "metatrack_serial",
        "metatrack_block_stm",
    ]


def _canonical_base_spec() -> V5ExperimentSpec:
    canonical = {
        "workload": "deterministic_signed_synthetic",
        "routing": "hash_routing_baseline",
        "block_producer": "time_or_count_block_producer",
        "execution": "serial_execution_baseline",
        "scheduler": "fifo_serial_scheduler",
        "block_executor": "serial_block_executor",
        "state_access": "direct_state_access",
        "state_storage": "persistent_local_state_store",
        "commit": "normal_commit",
        "fault_injection": "faults_disabled",
    }
    selections = []
    for category in CATEGORIES:
        plugin_id = canonical.get(category)
        manifest = STORE.get(plugin_id) if plugin_id else next(item for item in STORE.list() if item.category == category)
        config = dict(manifest.default_config)
        if category == "workload":
            config |= {"cross_shard_ratio": 0.0, "timeout_every": 0}
        selections.append(V5PluginSelection(category=category, plugin_id=manifest.plugin_id, config=config))
    return V5ExperimentSpec(
        name="literature-baseline-registry",
        execution_backend="real_cluster",
        plugin_selections=selections,
        topology=V5Topology(nodes=4, shards=1, validators_per_shard=4),
        tx_count=1000,
        seed=11,
        duration_ms=3600000,
    )


def test_literature_baselines_validate_as_independent_real_cluster_methods():
    plan = V5FormalExperimentPlan(
        name="literature-baseline-validation",
        base_spec=_canonical_base_spec(),
        methods=[ALL_BUILTIN_METHODS[method_id] for method_id in EXPECTED],
        suites=["comparison_experiment"],
        seeds=[11],
        repeats=1,
        worker_count=4,
    )
    checked = validate_request(V5FormalRunRequest(execution_backend="real_cluster", plan=plan))
    assert [row["method_config_id"] for row in checked.rows] == list(EXPECTED)
    assert all(row["runnable"] for row in checked.rows)
    semantics = {row["method_config_id"]: row["comparison_semantics_class"] for row in checked.rows}
    assert semantics["hash_cg"] == "nezha_cg_johnson_retryable_v4"
    assert semantics["hash_acg"] == "nezha_acg_hs_retryable_v2"
    # BSX may realize a different deterministic serializable order than the
    # consensus input order, so its correctness oracle is its own bound plan.
    assert semantics["hash_bsx"] == "bsx_deterministic_coloring_serializable_v1"
