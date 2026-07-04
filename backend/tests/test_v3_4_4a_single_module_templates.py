from __future__ import annotations

from backend.app.models.v3_composer_draft import V3ComposerDraftModule, V3ComposerDraftRequest, V3RuntimeTopology
from backend.app.services.v3_composer_draft_runner import build_experiment_profile, merge_run_metadata
from backend.app.services.v3_composer_draft_validator import validate_v3_composer_draft
from backend.app.services.v3_experiment_templates import load_templates


def draft(template_id: str, preset_id: str | None = None, **overrides: tuple[str, str]) -> V3ComposerDraftRequest:
    plugins = {
        "Workload": ("fixed", "synthetic_hotspot"),
        "TxPool": ("fixed", "fifo_pool"),
        "BlockProducer": ("fixed", "time_or_count_block_producer"),
        "Consensus": ("fixed", "simple_leader"),
        "CommitteeEpoch": ("disabled", "disabled"),
        "Routing": ("fixed", "co_access_sharding"),
        "Execution": ("fixed", "dual_track_execution"),
        "StateAccess": ("fixed", "access_list_prefetch"),
        "StateStorage": ("fixed", "hash_state_storage"),
        "Commit": ("fixed", "hot_update_aggregation_commit"),
        "MetricsReport": ("output", "basic_metrics"),
    }
    plugins.update(overrides)
    return V3ComposerDraftRequest(
        template_id=template_id,
        preset_id=preset_id,
        topology=V3RuntimeTopology(controlled_experiment_enabled=True),
        modules={
            module_id: V3ComposerDraftModule(module_id=module_id, status=status, plugin=plugin)
            for module_id, (status, plugin) in plugins.items()
        },
    )


def test_catalog_returns_single_module_templates() -> None:
    templates = load_templates()

    assert "single_module_txpool" in templates
    assert "single_module_blockproducer" in templates
    assert "single_module_consensus" in templates
    assert "single_module_routing" in templates
    assert "single_module_execution" in templates
    assert "single_module_state_access" in templates
    assert "single_module_commit" in templates
    assert "metatrack_ablation" in templates
    assert {preset["preset_id"] for preset in templates["metatrack_ablation"]["presets"]} == {
        "metatrack_baseline_smoke",
        "metatrack_routing_only_smoke",
        "metatrack_routing_execution_smoke",
        "metatrack_routing_execution_state_access_smoke",
        "metatrack_full_smoke",
    }
    assert templates["single_module_consensus"]["variable_module"] == "Consensus"
    assert templates["single_module_consensus"]["allowed_variable_plugins"] == [
        "simple_leader",
        "poa_light",
        "pbft_light_model",
        "blockemulator_aligned_pbft_preview",
    ]
    assert templates["single_module_routing"]["variable_module"] == "Routing"
    assert templates["single_module_routing"]["allowed_variable_plugins"] == [
        "hash_sharding",
        "metatrack_coaccess_routing",
        "hotspot_aware_routing",
    ]
    assert templates["single_module_execution"]["variable_module"] == "Execution"
    assert templates["single_module_execution"]["allowed_variable_plugins"] == [
        "serial_execution",
        "parallel_light_execution",
        "metatrack_dual_track_execution",
    ]
    assert templates["single_module_state_access"]["variable_module"] == "StateAccess"
    assert templates["single_module_state_access"]["allowed_variable_plugins"] == [
        "direct_fetch",
        "remote_state_access_model",
        "cached_state_access",
        "access_list_prefetch",
    ]
    assert templates["single_module_commit"]["variable_module"] == "Commit"
    assert templates["single_module_commit"]["allowed_variable_plugins"] == [
        "normal_commit",
        "conservative_commit",
        "hot_update_aggregation",
        "constraint_checked_aggregation",
    ]


def test_single_module_templates_have_default_presets() -> None:
    templates = load_templates()
    expected = {
        "metatrack_ablation": "metatrack_baseline_smoke",
        "single_module_txpool": "txpool_fifo_smoke",
        "single_module_blockproducer": "blockproducer_time_or_count_smoke",
        "single_module_consensus": "consensus_light_smoke",
        "single_module_routing": "routing_coaccess_smoke",
        "single_module_execution": "execution_dual_track_smoke",
        "single_module_state_access": "state_access_remote_prefetch_smoke",
        "single_module_commit": "commit_hot_update_smoke",
    }

    for template_id, preset_id in expected.items():
        template = templates[template_id]
        presets = {preset["preset_id"]: preset for preset in template["presets"]}
        preset = presets[preset_id]

        assert template["default_preset_id"] == preset_id
        assert preset["primary_metrics"]
        assert preset["expected_artifacts"]
        assert preset["truthfulness_note"]
        assert preset["result_guide"]


def test_single_module_templates_auto_fill_default_presets() -> None:
    cases = {
        "single_module_txpool": ("TxPool", "txpool_fifo_smoke", {"TxPool": ("variable", "fifo_pool")}),
        "single_module_blockproducer": (
            "BlockProducer",
            "blockproducer_time_or_count_smoke",
            {"BlockProducer": ("variable", "time_or_count_block_producer")},
        ),
        "single_module_consensus": ("Consensus", "consensus_light_smoke", {"Consensus": ("variable", "simple_leader")}),
        "single_module_routing": ("Routing", "routing_coaccess_smoke", {"Routing": ("variable", "metatrack_coaccess_routing")}),
        "single_module_execution": ("Execution", "execution_dual_track_smoke", {"Execution": ("variable", "metatrack_dual_track_execution")}),
        "single_module_state_access": ("StateAccess", "state_access_remote_prefetch_smoke", {"StateAccess": ("variable", "access_list_prefetch")}),
        "single_module_commit": ("Commit", "commit_hot_update_smoke", {"Commit": ("variable", "hot_update_aggregation")}),
    }

    for template_id, (variable_module, preset_id, overrides) in cases.items():
        result = validate_v3_composer_draft(draft(template_id, **overrides))

        assert result.is_valid is True
        assert result.normalized_draft is not None
        assert result.normalized_draft["preset_id"] == preset_id
        assert result.normalized_draft["variable_module"] == variable_module
        assert result.normalized_draft["primary_metrics"]
        assert result.normalized_draft["expected_artifacts"]


def test_single_module_template_rejects_mismatched_preset() -> None:
    result = validate_v3_composer_draft(
        draft(
            "single_module_txpool",
            preset_id="consensus_light_smoke",
            TxPool=("variable", "fifo_pool"),
        )
    )

    assert result.is_valid is False
    assert any("Invalid preset" in error and "single_module_txpool" in error for error in result.errors)


def test_metatrack_template_rejects_mismatched_preset() -> None:
    result = validate_v3_composer_draft(
        draft(
            "metatrack_ablation",
            preset_id="commit_hot_update_smoke",
            Routing=("variable", "hash_sharding"),
            Execution=("variable", "serial_execution"),
            StateAccess=("variable", "direct_fetch"),
            Commit=("variable", "normal_commit"),
        )
    )

    assert result.is_valid is False
    assert any("Invalid preset" in error and "metatrack_ablation" in error for error in result.errors)


def test_single_module_routing_allows_runtime_routing_plugins() -> None:
    for plugin_id in ("hash_sharding", "metatrack_coaccess_routing", "hotspot_aware_routing"):
        result = validate_v3_composer_draft(
            draft("single_module_routing", Routing=("variable", plugin_id))
        )

        assert result.is_valid is True
        assert result.is_runnable is True
        assert result.normalized_draft is not None
        assert result.normalized_draft["variable_module"] == "Routing"
        assert result.normalized_draft["preset_id"] == "routing_coaccess_smoke"


def test_single_module_routing_rejects_planned_cross_shard_protocols() -> None:
    for plugin_id in ("clpa_like_partitioning", "shardcutter_like_partitioning", "relay_cross_shard", "broker_cross_shard", "two_phase_commit"):
        result = validate_v3_composer_draft(
            draft("single_module_routing", Routing=("variable", plugin_id))
        )

        assert result.is_valid is False
        assert result.is_runnable is False


def test_single_module_execution_allows_runtime_execution_plugins() -> None:
    for plugin_id in ("serial_execution", "parallel_light_execution", "metatrack_dual_track_execution"):
        result = validate_v3_composer_draft(
            draft("single_module_execution", Execution=("variable", plugin_id))
        )

        assert result.is_valid is True
        assert result.is_runnable is True
        assert result.normalized_draft is not None
        assert result.normalized_draft["variable_module"] == "Execution"
        assert result.normalized_draft["preset_id"] == "execution_dual_track_smoke"


def test_single_module_execution_rejects_planned_execution_plugins() -> None:
    for plugin_id in ("block_stm_like_model", "calvin_like_model", "real_optimistic_execution", "real_rollback_engine"):
        result = validate_v3_composer_draft(
            draft("single_module_execution", Execution=("variable", plugin_id))
        )

        assert result.is_valid is False
        assert result.is_runnable is False


def test_single_module_execution_rejects_routing_or_consensus_change() -> None:
    routing_result = validate_v3_composer_draft(
        draft(
            "single_module_execution",
            Execution=("variable", "parallel_light_execution"),
            Routing=("fixed", "hash_sharding"),
        )
    )
    consensus_result = validate_v3_composer_draft(
        draft(
            "single_module_execution",
            Execution=("variable", "metatrack_dual_track_execution"),
            Consensus=("fixed", "poa_light"),
        )
    )

    assert routing_result.is_valid is False
    assert any("Fairness violation" in error and "ShardingPlugin" in error for error in routing_result.errors)
    assert consensus_result.is_valid is False
    assert any("Fairness violation" in error and "ConsensusPlugin" in error for error in consensus_result.errors)


def test_single_module_state_access_allows_runtime_state_access_plugins() -> None:
    for plugin_id in ("direct_fetch", "remote_state_access_model", "cached_state_access", "access_list_prefetch"):
        result = validate_v3_composer_draft(
            draft("single_module_state_access", StateAccess=("variable", plugin_id))
        )

        assert result.is_valid is True
        assert result.is_runnable is True
        assert result.normalized_draft is not None
        assert result.normalized_draft["variable_module"] == "StateAccess"
        assert result.normalized_draft["preset_id"] == "state_access_remote_prefetch_smoke"


def test_single_module_state_access_rejects_planned_state_access_plugins() -> None:
    for plugin_id in ("real_witness_fetch", "real_proof_fetch", "mpt_proof_model", "persistent_kv_access", "snapshot_access"):
        result = validate_v3_composer_draft(
            draft("single_module_state_access", StateAccess=("variable", plugin_id))
        )

        assert result.is_valid is False
        assert result.is_runnable is False


def test_single_module_state_access_rejects_execution_or_routing_change() -> None:
    execution_result = validate_v3_composer_draft(
        draft(
            "single_module_state_access",
            StateAccess=("variable", "cached_state_access"),
            Execution=("fixed", "serial_execution"),
        )
    )
    routing_result = validate_v3_composer_draft(
        draft(
            "single_module_state_access",
            StateAccess=("variable", "remote_state_access_model"),
            Routing=("fixed", "hash_sharding"),
        )
    )

    assert execution_result.is_valid is False
    assert any("Fairness violation" in error and "ExecutionSchedulerPlugin" in error for error in execution_result.errors)
    assert routing_result.is_valid is False
    assert any("Fairness violation" in error and "ShardingPlugin" in error for error in routing_result.errors)


def test_single_module_commit_allows_runtime_commit_plugins() -> None:
    for plugin_id in ("normal_commit", "conservative_commit", "hot_update_aggregation", "constraint_checked_aggregation"):
        result = validate_v3_composer_draft(
            draft("single_module_commit", Commit=("variable", plugin_id))
        )

        assert result.is_valid is True
        assert result.is_runnable is True
        assert result.normalized_draft is not None
        assert result.normalized_draft["variable_module"] == "Commit"
        assert result.normalized_draft["preset_id"] == "commit_hot_update_smoke"


def test_single_module_commit_rejects_planned_commit_plugins() -> None:
    for plugin_id in ("atomic_reservation_commit", "batch_commit", "real_db_lock_commit"):
        result = validate_v3_composer_draft(
            draft("single_module_commit", Commit=("variable", plugin_id))
        )

        assert result.is_valid is False
        assert result.is_runnable is False


def test_single_module_commit_rejects_state_access_or_execution_change() -> None:
    state_access_result = validate_v3_composer_draft(
        draft(
            "single_module_commit",
            Commit=("variable", "hot_update_aggregation"),
            StateAccess=("fixed", "direct_fetch"),
        )
    )
    execution_result = validate_v3_composer_draft(
        draft(
            "single_module_commit",
            Commit=("variable", "constraint_checked_aggregation"),
            Execution=("fixed", "serial_execution"),
        )
    )

    assert state_access_result.is_valid is False
    assert any("Fairness violation" in error and "StateAccessPlugin" in error for error in state_access_result.errors)
    assert execution_result.is_valid is False
    assert any("Fairness violation" in error and "ExecutionSchedulerPlugin" in error for error in execution_result.errors)


def test_single_module_consensus_allows_consensus_light_plugins() -> None:
    for plugin_id in ("simple_leader", "poa_light", "pbft_light_model"):
        result = validate_v3_composer_draft(
            draft("single_module_consensus", Consensus=("variable", plugin_id))
        )

        assert result.is_valid is True
        assert result.is_runnable is True
        assert result.normalized_draft is not None
        assert result.normalized_draft["fairness_validated"] is True
        assert result.normalized_draft["variable_module"] == "Consensus"


def test_single_module_consensus_rejects_real_consensus_plugins() -> None:
    for plugin_id in ("pbft", "hotstuff", "raft"):
        result = validate_v3_composer_draft(
            draft("single_module_consensus", Consensus=("variable", plugin_id))
        )

        assert result.is_valid is False
        assert result.is_runnable is False


def test_single_module_consensus_rejects_txpool_change() -> None:
    result = validate_v3_composer_draft(
        draft(
            "single_module_consensus",
            Consensus=("variable", "poa_light"),
            TxPool=("fixed", "priority_pool"),
        )
    )

    assert result.is_valid is False
    assert any("Fairness violation" in error and "TxPoolPlugin" in error for error in result.errors)


def test_single_module_txpool_rejects_consensus_change() -> None:
    result = validate_v3_composer_draft(
        draft(
            "single_module_txpool",
            TxPool=("variable", "fifo_pool"),
            Consensus=("fixed", "poa_light"),
        )
    )

    assert result.is_valid is False
    assert any("Fairness violation" in error and "ConsensusPlugin" in error for error in result.errors)


def test_single_module_blockproducer_rejects_txpool_or_consensus_change() -> None:
    txpool_result = validate_v3_composer_draft(
        draft(
            "single_module_blockproducer",
            BlockProducer=("variable", "time_or_count_block_producer"),
            TxPool=("fixed", "priority_pool"),
        )
    )
    consensus_result = validate_v3_composer_draft(
        draft(
            "single_module_blockproducer",
            BlockProducer=("variable", "time_or_count_block_producer"),
            Consensus=("fixed", "pbft_light_model"),
        )
    )

    assert txpool_result.is_valid is False
    assert any("Fairness violation" in error and "TxPoolPlugin" in error for error in txpool_result.errors)
    assert consensus_result.is_valid is False
    assert any("Fairness violation" in error and "ConsensusPlugin" in error for error in consensus_result.errors)


def test_metatrack_ablation_presets_validate_expected_combinations() -> None:
    cases = {
        "metatrack_baseline_smoke": ("baseline", [], "hash_sharding", "serial_execution", "direct_fetch", "normal_commit"),
        "metatrack_routing_only_smoke": ("routing_only", ["routing"], "metatrack_coaccess_routing", "serial_execution", "direct_fetch", "normal_commit"),
        "metatrack_routing_execution_smoke": ("routing_execution", ["routing", "execution"], "metatrack_coaccess_routing", "metatrack_dual_track_execution", "direct_fetch", "normal_commit"),
        "metatrack_routing_execution_state_access_smoke": ("routing_execution_state_access", ["routing", "execution", "state_access"], "metatrack_coaccess_routing", "metatrack_dual_track_execution", "access_list_prefetch", "normal_commit"),
        "metatrack_full_smoke": ("full", ["routing", "execution", "state_access", "commit"], "metatrack_coaccess_routing", "metatrack_dual_track_execution", "access_list_prefetch", "constraint_checked_aggregation"),
    }
    for preset_id, (stage, components, routing, execution, state_access, commit) in cases.items():
        result = validate_v3_composer_draft(
            draft(
                "metatrack_ablation",
                preset_id=preset_id,
                Routing=("variable", routing),
                Execution=("variable", execution),
                StateAccess=("variable", state_access),
                Commit=("variable", commit),
            )
        )

        assert result.is_valid is True
        assert result.is_runnable is True
        assert result.normalized_draft is not None
        assert result.normalized_draft["fairness_validated"] is True
        assert result.normalized_draft["ablation_stage"] == stage
        assert result.normalized_draft["enabled_metatrack_components"] == components


def test_metatrack_ablation_rejects_plugin_combo_outside_selected_preset() -> None:
    result = validate_v3_composer_draft(
        draft(
            "metatrack_ablation",
            preset_id="metatrack_routing_only_smoke",
            Routing=("variable", "metatrack_coaccess_routing"),
            Execution=("variable", "metatrack_dual_track_execution"),
            StateAccess=("variable", "direct_fetch"),
            Commit=("variable", "normal_commit"),
        )
    )

    assert result.is_valid is False
    assert any("preset metatrack_routing_only_smoke requires ExecutionSchedulerPlugin=serial_execution" in error for error in result.errors)


def test_runner_summary_and_experiment_profile_include_preset_metadata() -> None:
    validation = validate_v3_composer_draft(
        draft("single_module_consensus", Consensus=("variable", "pbft_light_model"))
    )
    assert validation.normalized_draft is not None
    normalized = validation.normalized_draft

    profile = build_experiment_profile(normalized)
    summary = merge_run_metadata({"tx_count": 24}, normalized)

    for payload in (profile, summary):
        assert payload["experiment_template"] == "single_module_consensus"
        assert payload["preset_id"] == "consensus_light_smoke"
        assert payload["preset_name"] == "Consensus-light smoke"
        assert payload["variable_module"] == "Consensus"
        assert payload["fairness_validated"] is True
        assert "avg_consensus_latency_ms" in payload["primary_metrics"]
        assert "consensus_log.csv" in payload["expected_artifacts"]
        assert "PBFT" in payload["truthfulness_note"]


def test_metatrack_runner_summary_and_experiment_profile_include_ablation_metadata() -> None:
    validation = validate_v3_composer_draft(
        draft(
            "metatrack_ablation",
            preset_id="metatrack_full_smoke",
            Routing=("variable", "metatrack_coaccess_routing"),
            Execution=("variable", "metatrack_dual_track_execution"),
            StateAccess=("variable", "access_list_prefetch"),
            Commit=("variable", "constraint_checked_aggregation"),
        )
    )
    assert validation.normalized_draft is not None
    normalized = validation.normalized_draft

    profile = build_experiment_profile(normalized)
    summary = merge_run_metadata({"tx_count": 24}, normalized)

    for payload in (profile, summary):
        assert payload["experiment_template"] == "metatrack_ablation"
        assert payload["preset_id"] == "metatrack_full_smoke"
        assert payload["ablation_stage"] == "full"
        assert payload["enabled_metatrack_components"] == ["routing", "execution", "state_access", "commit"]
        assert "Routing" in payload["controlled_modules"]
        assert payload["fairness_validated"] is True
        assert "avg_commit_latency_ms" in payload["primary_metrics"]
        assert "state_commit_log.csv" in payload["expected_artifacts"]
        assert "paper-ready benchmark" in payload["truthfulness_note"]


def test_routing_runner_summary_and_experiment_profile_include_preset_metadata() -> None:
    validation = validate_v3_composer_draft(
        draft("single_module_routing", Routing=("variable", "hotspot_aware_routing"))
    )
    assert validation.normalized_draft is not None
    normalized = validation.normalized_draft

    profile = build_experiment_profile(normalized)
    summary = merge_run_metadata({"tx_count": 24}, normalized)

    for payload in (profile, summary):
        assert payload["experiment_template"] == "single_module_routing"
        assert payload["preset_id"] == "routing_coaccess_smoke"
        assert payload["preset_name"] == "Routing co-access smoke"
        assert payload["variable_module"] == "Routing"
        assert payload["fairness_validated"] is True
        assert "cross_shard_ratio" in payload["primary_metrics"]
        assert "routing_log.csv" in payload["expected_artifacts"]
        assert "real cross-shard" in payload["truthfulness_note"]


def test_execution_runner_summary_and_experiment_profile_include_preset_metadata() -> None:
    validation = validate_v3_composer_draft(
        draft("single_module_execution", Execution=("variable", "parallel_light_execution"))
    )
    assert validation.normalized_draft is not None
    normalized = validation.normalized_draft

    profile = build_experiment_profile(normalized)
    summary = merge_run_metadata({"tx_count": 24}, normalized)

    for payload in (profile, summary):
        assert payload["experiment_template"] == "single_module_execution"
        assert payload["preset_id"] == "execution_dual_track_smoke"
        assert payload["preset_name"] == "Execution dual-track smoke"
        assert payload["variable_module"] == "Execution"
        assert payload["fairness_validated"] is True
        assert "fast_track_count" in payload["primary_metrics"]
        assert "execution_log.csv" in payload["expected_artifacts"]
        assert "real concurrent execution" in payload["truthfulness_note"]


def test_state_access_runner_summary_and_experiment_profile_include_preset_metadata() -> None:
    validation = validate_v3_composer_draft(
        draft("single_module_state_access", StateAccess=("variable", "remote_state_access_model"))
    )
    assert validation.normalized_draft is not None
    normalized = validation.normalized_draft

    profile = build_experiment_profile(normalized)
    summary = merge_run_metadata({"tx_count": 24}, normalized)

    for payload in (profile, summary):
        assert payload["experiment_template"] == "single_module_state_access"
        assert payload["preset_id"] == "state_access_remote_prefetch_smoke"
        assert payload["preset_name"] == "StateAccess remote/prefetch smoke"
        assert payload["variable_module"] == "StateAccess"
        assert payload["fairness_validated"] is True
        assert "remote_state_access_ratio" in payload["primary_metrics"]
        assert "state_access_log.csv" in payload["expected_artifacts"]
        assert "real proofs" in payload["truthfulness_note"]


def test_commit_runner_summary_and_experiment_profile_include_preset_metadata() -> None:
    validation = validate_v3_composer_draft(
        draft("single_module_commit", Commit=("variable", "constraint_checked_aggregation"))
    )
    assert validation.normalized_draft is not None
    normalized = validation.normalized_draft

    profile = build_experiment_profile(normalized)
    summary = merge_run_metadata({"tx_count": 24}, normalized)

    for payload in (profile, summary):
        assert payload["experiment_template"] == "single_module_commit"
        assert payload["preset_id"] == "commit_hot_update_smoke"
        assert payload["preset_name"] == "Commit hot-update smoke"
        assert payload["variable_module"] == "Commit"
        assert payload["fairness_validated"] is True
        assert "aggregation_ratio" in payload["primary_metrics"]
        assert "state_commit_log.csv" in payload["expected_artifacts"]
        assert "real database locking" in payload["truthfulness_note"]
