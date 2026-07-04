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
    assert result.normalized_draft["current_stage"] == "V3-final Fault, Observability, and Reproducibility Closure"
    assert result.normalized_draft["runtime_truth"] == "v3_final_emulator_closure_not_production_system"
    assert result.normalized_draft["topology"]["cross_shard_protocol"] == "none"
    assert result.normalized_draft["topology"]["state_backend"] == "memory_kv"
    assert result.normalized_draft["topology"]["benchmark_template"] == "full_stack_v3_template"
    assert result.normalized_draft["topology"]["baseline_profile"] == "baseline_simple_chain"
    assert result.normalized_draft["topology"]["repeat_count"] == 1


def test_valid_draft_accepts_state_backend_mvp_options() -> None:
    for backend in ("memory_kv", "persistent_kv", "merkle_trie_mvp"):
        draft = valid_draft()
        draft.topology = V3RuntimeTopology(state_backend=backend)
        result = validate_v3_composer_draft(draft)

        assert result.is_valid is True
        assert result.normalized_draft is not None
        assert result.normalized_draft["topology"]["state_backend"] == backend
        assert result.normalized_draft["topology_summary"]["state_backend"] == backend


def test_ethereum_mpt_compatible_is_planned_only() -> None:
    draft = valid_draft()
    draft.topology = V3RuntimeTopology(state_backend="ethereum_mpt_compatible")
    result = validate_v3_composer_draft(draft)

    assert result.is_valid is False
    assert any("planned only" in error for error in result.errors)


def test_valid_draft_accepts_benchmark_template_and_baseline_profile() -> None:
    draft = valid_draft()
    draft.topology = V3RuntimeTopology(
        benchmark_template="state_authenticity_template",
        baseline_profile="baseline_memory_kv",
        repeat_count=3,
    )
    result = validate_v3_composer_draft(draft)

    assert result.is_valid is True
    assert result.normalized_draft is not None
    assert result.normalized_draft["topology"]["benchmark_template"] == "state_authenticity_template"
    assert result.normalized_draft["topology"]["baseline_profile"] == "baseline_memory_kv"
    assert result.normalized_draft["topology"]["repeat_count"] == 3


def test_valid_draft_accepts_metaverse_suite_topology() -> None:
    draft = valid_draft()
    draft.topology = V3RuntimeTopology(
        metaverse_suite_enabled=True,
        metaverse_scenario="cross_metaverse_transfer",
        tx_count=128,
        user_count=20,
        asset_count=50,
        item_count=10,
        avatar_count=20,
        scene_count=8,
        metaverse_count=2,
        hotspot_ratio=0.3,
        cross_scene_ratio=0.4,
        cross_shard_ratio=0.5,
        offchain_confirmation_enabled=True,
        offchain_confirm_delay_ms=200,
        offchain_failure_ratio=0.1,
        cross_metaverse_enabled=True,
        benchmark_suite_enabled=True,
        baseline_matrix_enabled=True,
        multi_seed_enabled=True,
        paper_export_enabled=True,
        sweep_seed_count=2,
        sweep_shard_counts=[1, 2],
        sweep_cross_shard_ratios=[0.0, 0.5],
        sweep_hotspot_ratios=[0.0, 0.3],
    )
    result = validate_v3_composer_draft(draft)

    assert result.is_valid is True
    assert result.normalized_draft is not None
    topology = result.normalized_draft["topology"]
    assert topology["metaverse_suite_enabled"] is True
    assert topology["metaverse_scenario"] == "cross_metaverse_transfer"
    assert topology["paper_export_enabled"] is True


def test_invalid_metaverse_scenario_and_ranges_are_rejected() -> None:
    draft = valid_draft()
    draft.topology = V3RuntimeTopology(metaverse_scenario="real_platform_trace", tx_count=0, hotspot_ratio=1.2, sweep_seed_count=21)
    result = validate_v3_composer_draft(draft)

    assert result.is_valid is False
    assert any("metaverse_scenario" in error for error in result.errors)
    assert any("tx_count" in error for error in result.errors)
    assert any("hotspot_ratio" in error for error in result.errors)
    assert any("sweep_seed_count" in error for error in result.errors)


def test_invalid_benchmark_template_is_rejected() -> None:
    draft = valid_draft()
    draft.topology = V3RuntimeTopology(benchmark_template="paper_grade_claim")
    result = validate_v3_composer_draft(draft)

    assert result.is_valid is False
    assert any("topology.benchmark_template" in error for error in result.errors)


def test_valid_draft_accepts_custom_logical_topology() -> None:
    draft = valid_draft()
    draft.topology = V3RuntimeTopology(shard_count=2, validators_per_shard=3, executors_per_shard=2, storage_nodes_per_shard=1, supervisor_enabled=False)
    result = validate_v3_composer_draft(draft)

    assert result.is_valid is True
    assert result.normalized_draft is not None
    assert result.normalized_draft["topology"]["shard_count"] == 2
    assert result.normalized_draft["topology_summary"]["logical_node_count"] == 12


def test_valid_draft_accepts_localhost_tcp_network_adapter() -> None:
    draft = valid_draft()
    draft.topology = V3RuntimeTopology(network_adapter="localhost_tcp_preview", network_mode="localhost_tcp_preview")
    result = validate_v3_composer_draft(draft)

    assert result.is_valid is True
    assert result.normalized_draft is not None
    assert result.normalized_draft["topology"]["network_adapter"] == "localhost_tcp_preview"
    assert result.normalized_draft["topology_summary"]["network_adapter"] == "localhost_tcp_preview"


def test_invalid_network_adapter_is_rejected() -> None:
    draft = valid_draft()
    draft.topology = V3RuntimeTopology(network_adapter="raw_tcp")
    result = validate_v3_composer_draft(draft)

    assert result.is_valid is False
    assert any("topology.network_adapter" in error for error in result.errors)


def test_valid_draft_accepts_relay_preview_cross_shard_protocol() -> None:
    draft = valid_draft()
    draft.topology = V3RuntimeTopology(cross_shard_protocol="relay_preview")
    result = validate_v3_composer_draft(draft)

    assert result.is_valid is True
    assert result.normalized_draft is not None
    assert result.normalized_draft["topology"]["cross_shard_protocol"] == "relay_preview"
    assert result.normalized_draft["topology_summary"]["cross_shard_protocol"] == "relay_preview"


def test_valid_draft_accepts_relay_mvp_cross_shard_protocol() -> None:
    draft = valid_draft()
    draft.topology = V3RuntimeTopology(cross_shard_protocol="relay_mvp", benchmark_template="cross_shard_relay_mvp_template")
    result = validate_v3_composer_draft(draft)

    assert result.is_valid is True
    assert result.normalized_draft is not None
    assert result.normalized_draft["topology"]["cross_shard_protocol"] == "relay_mvp"
    assert result.normalized_draft["topology_summary"]["cross_shard_protocol"] == "relay_mvp"
    assert result.normalized_draft["topology"]["benchmark_template"] == "cross_shard_relay_mvp_template"


def test_planned_cross_shard_protocols_are_rejected() -> None:
    for protocol in ("broker_preview", "two_phase_commit_preview"):
        draft = valid_draft()
        draft.topology = V3RuntimeTopology(cross_shard_protocol=protocol)
        result = validate_v3_composer_draft(draft)

        assert result.is_valid is False
        assert any("planned only" in error for error in result.errors)


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


def test_consensus_runtime_plugins_are_runnable_when_fixed() -> None:
    for plugin_id in ("simple_leader", "poa_light", "pbft_light_model", "blockemulator_aligned_pbft_preview"):
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
    for plugin_id in ("pbft", "real_pbft", "hotstuff", "raft", "future_hotstuff_preview", "future_raft_preview"):
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
