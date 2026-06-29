from __future__ import annotations

from backend.app.models.v3_composer_draft import V3ComposerDraftModule, V3ComposerDraftRequest
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
    assert templates["single_module_consensus"]["variable_module"] == "Consensus"
    assert templates["single_module_consensus"]["allowed_variable_plugins"] == [
        "simple_leader",
        "poa_light",
        "pbft_light_model",
    ]
    assert templates["single_module_routing"]["variable_module"] == "Routing"
    assert templates["single_module_routing"]["allowed_variable_plugins"] == [
        "hash_sharding",
        "metatrack_coaccess_routing",
        "hotspot_aware_routing",
    ]


def test_single_module_templates_have_default_presets() -> None:
    templates = load_templates()
    expected = {
        "single_module_txpool": "txpool_fifo_smoke",
        "single_module_blockproducer": "blockproducer_time_or_count_smoke",
        "single_module_consensus": "consensus_light_smoke",
        "single_module_routing": "routing_coaccess_smoke",
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


def test_legacy_metatrack_draft_remains_compatible() -> None:
    result = validate_v3_composer_draft(
        draft(
            "metatrack_ablation",
            Routing=("variable", "co_access_sharding"),
            Execution=("variable", "dual_track_execution"),
            StateAccess=("variable", "access_list_prefetch"),
            Commit=("variable", "hot_update_aggregation_commit"),
        )
    )

    assert result.is_valid is True
    assert result.is_runnable is True
    assert result.normalized_draft is not None
    assert result.normalized_draft["fairness_validated"] is False


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
