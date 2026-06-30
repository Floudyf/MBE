from __future__ import annotations

from backend.app.models.v3_composer_draft import V3ComposerDraftModule, V3ComposerDraftRequest, V3RuntimeTopology
from backend.app.services.v3_composer_draft_validator import validate_v3_composer_draft


def valid_draft(template_id: str = "metatrack_ablation", **overrides: tuple[str, str]) -> V3ComposerDraftRequest:
    plugins = {
        "Workload": ("fixed", "synthetic_hotspot"),
        "TxPool": ("fixed", "fifo_pool"),
        "BlockProducer": ("fixed", "time_or_count_block_producer"),
        "Consensus": ("fixed", "simple_leader"),
        "CommitteeEpoch": ("disabled", "disabled"),
        "Routing": ("variable", "co_access_sharding"),
        "Execution": ("variable", "dual_track_execution"),
        "StateAccess": ("variable", "access_list_prefetch"),
        "StateStorage": ("fixed", "hash_state_storage"),
        "Commit": ("variable", "hot_update_aggregation_commit"),
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


def test_valid_full_metatrack_draft_is_runnable() -> None:
    result = validate_v3_composer_draft(valid_draft())

    assert result.is_valid is True
    assert result.is_runnable is True
    assert set(result.variable_modules) == {"Routing", "Execution", "StateAccess", "Commit"}
    assert result.normalized_draft is not None
    assert result.normalized_draft["plugin_selection"]["Commit"] == "hot_update_aggregation_commit"
    assert result.normalized_draft["topology_summary"]["logical_node_count"] == 25
    assert result.normalized_draft["current_stage"] == "V3.5.1"


def test_valid_draft_accepts_custom_logical_topology() -> None:
    draft = valid_draft()
    draft.topology = V3RuntimeTopology(shard_count=2, validators_per_shard=3, executors_per_shard=2, storage_nodes_per_shard=1, supervisor_enabled=False)
    result = validate_v3_composer_draft(draft)

    assert result.is_valid is True
    assert result.normalized_draft is not None
    assert result.normalized_draft["topology"]["shard_count"] == 2
    assert result.normalized_draft["topology_summary"]["logical_node_count"] == 12


def test_invalid_topology_is_rejected() -> None:
    draft = valid_draft()
    draft.topology = V3RuntimeTopology(shard_count=33)
    result = validate_v3_composer_draft(draft)

    assert result.is_valid is False
    assert any("topology.shard_count" in error for error in result.errors)


def test_valid_baseline_draft_is_runnable() -> None:
    result = validate_v3_composer_draft(
        valid_draft(
            Routing=("variable", "hash_sharding"),
            Execution=("variable", "serial_execution"),
            StateAccess=("variable", "direct_fetch"),
            Commit=("variable", "normal_commit"),
        )
    )

    assert result.is_valid is True
    assert result.is_runnable is True


def test_consensus_light_plugins_are_runnable_when_fixed() -> None:
    for plugin_id in ("simple_leader", "poa_light", "pbft_light_model"):
        result = validate_v3_composer_draft(valid_draft(Consensus=("fixed", plugin_id)))

        assert result.is_valid is True
        assert result.is_runnable is True
        assert result.normalized_draft is not None
        assert result.normalized_draft["plugin_selection"]["Consensus"] == plugin_id


def test_required_module_disabled_fails() -> None:
    result = validate_v3_composer_draft(valid_draft(Consensus=("disabled", "simple_leader")))

    assert result.is_valid is False
    assert any("共识排序是必需模块，不能关闭" in error for error in result.errors)


def test_metrics_report_cannot_be_variable() -> None:
    result = validate_v3_composer_draft(valid_draft(MetricsReport=("variable", "basic_metrics")))

    assert result.is_valid is False
    assert any("输出模块" in error for error in result.errors)


def test_committee_epoch_cannot_be_variable() -> None:
    result = validate_v3_composer_draft(valid_draft(CommitteeEpoch=("variable", "disabled")))

    assert result.is_valid is False
    assert any("委员会 / Epoch 当前不能作为可运行实验变量" in error for error in result.errors)


def test_planned_plugin_is_not_valid_or_runnable() -> None:
    result = validate_v3_composer_draft(valid_draft(Consensus=("fixed", "hotstuff_model")))

    assert result.is_valid is False
    assert result.is_runnable is False
    assert any("规划中插件" in error for error in result.errors)


def test_real_pbft_hotstuff_and_raft_remain_unsupported() -> None:
    for plugin_id in ("pbft", "real_pbft", "hotstuff", "raft"):
        result = validate_v3_composer_draft(valid_draft(Consensus=("fixed", plugin_id)))

        assert result.is_valid is False
        assert result.is_runnable is False


def test_preview_only_plugin_is_valid_but_not_runnable() -> None:
    result = validate_v3_composer_draft(valid_draft(Workload=("fixed", "existing_trace")))

    assert result.is_valid is True
    assert result.is_runnable is False
    assert any("仅用于预览" in warning for warning in result.warnings)


def test_unknown_plugin_fails() -> None:
    result = validate_v3_composer_draft(valid_draft(Routing=("variable", "mystery_router")))

    assert result.is_valid is False
    assert any("未知插件" in error for error in result.errors)


def test_plugin_in_wrong_module_fails() -> None:
    result = validate_v3_composer_draft(valid_draft(Consensus=("fixed", "dual_track_execution")))

    assert result.is_valid is False
    assert any("只能用于交易执行模块" in error for error in result.errors)


def test_metatrack_template_rejects_consensus_variable() -> None:
    result = validate_v3_composer_draft(valid_draft(Consensus=("variable", "simple_leader")))

    assert result.is_valid is False
    assert any("共识排序属于固定环境" in error for error in result.errors)
