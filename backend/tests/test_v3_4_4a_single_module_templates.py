from __future__ import annotations

from backend.app.models.v3_composer_draft import V3ComposerDraftModule, V3ComposerDraftRequest
from backend.app.services.v3_composer_draft_validator import validate_v3_composer_draft
from backend.app.services.v3_experiment_templates import load_templates


def draft(template_id: str, **overrides: tuple[str, str]) -> V3ComposerDraftRequest:
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
    assert templates["single_module_consensus"]["variable_module"] == "Consensus"
    assert templates["single_module_consensus"]["allowed_variable_plugins"] == [
        "simple_leader",
        "poa_light",
        "pbft_light_model",
    ]


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
