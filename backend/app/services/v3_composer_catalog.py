from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class V3PluginCapability:
    plugin_id: str
    label: str
    runnable: bool
    preview_only: bool = False
    planned: bool = False
    runtime_supported: bool = False


@dataclass(frozen=True)
class V3ModuleCapability:
    module_id: str
    label: str
    required: bool
    allow_disabled: bool
    allow_variable: bool
    output_only: bool
    default_plugin: str
    plugins: dict[str, V3PluginCapability]


def runnable(plugin_id: str, label: str) -> V3PluginCapability:
    return V3PluginCapability(plugin_id=plugin_id, label=label, runnable=True, runtime_supported=True)


def preview(plugin_id: str, label: str) -> V3PluginCapability:
    return V3PluginCapability(plugin_id=plugin_id, label=label, runnable=False, preview_only=True)


def planned(plugin_id: str, label: str) -> V3PluginCapability:
    return V3PluginCapability(plugin_id=plugin_id, label=label, runnable=False, planned=True)


def disabled_plugin() -> V3PluginCapability:
    return V3PluginCapability(plugin_id="disabled", label="已关闭", runnable=True, runtime_supported=True)


def module(
    module_id: str,
    label: str,
    *,
    required: bool,
    allow_disabled: bool = False,
    allow_variable: bool = True,
    output_only: bool = False,
    default_plugin: str,
    plugins: list[V3PluginCapability],
) -> V3ModuleCapability:
    return V3ModuleCapability(
        module_id=module_id,
        label=label,
        required=required,
        allow_disabled=allow_disabled,
        allow_variable=allow_variable,
        output_only=output_only,
        default_plugin=default_plugin,
        plugins={plugin.plugin_id: plugin for plugin in plugins},
    )


CATALOG: dict[str, V3ModuleCapability] = {
    "Workload": module(
        "Workload",
        "负载生成",
        required=True,
        allow_variable=False,
        default_plugin="synthetic_hotspot",
        plugins=[
            runnable("default_synthetic", "默认合成负载"),
            runnable("synthetic_hotspot", "合成热点负载"),
            preview("existing_trace", "已有 trace"),
            preview("saved_workload", "已保存 workload"),
            planned("fabric_observation_trace", "Fabric 观测 trace"),
        ],
    ),
    "TxPool": module(
        "TxPool",
        "交易池",
        required=True,
        allow_variable=False,
        default_plugin="fifo_pool",
        plugins=[
            runnable("fifo_pool", "FIFO 交易池"),
            planned("priority_pool", "优先级交易池"),
            planned("hotspot_aware_pool", "热点感知交易池"),
            planned("fee_based_pool", "费用交易池"),
        ],
    ),
    "BlockProducer": module(
        "BlockProducer",
        "区块生成",
        required=True,
        allow_variable=False,
        default_plugin="time_or_count_block_producer",
        plugins=[
            runnable("time_or_count_block_producer", "时间或数量触发"),
            planned("fixed_size_block", "固定大小区块"),
            planned("adaptive_block_cut", "自适应切块"),
        ],
    ),
    "Consensus": module(
        "Consensus",
        "共识排序",
        required=True,
        allow_variable=False,
        default_plugin="simple_leader",
        plugins=[
            runnable("simple_leader", "简单 Leader 排序"),
            planned("pbft_model", "PBFT 模型"),
            planned("hotstuff_model", "HotStuff 模型"),
            planned("raft_model", "Raft 模型"),
            planned("committee_consensus", "委员会共识"),
        ],
    ),
    "CommitteeEpoch": module(
        "CommitteeEpoch",
        "委员会 / Epoch",
        required=False,
        allow_disabled=True,
        allow_variable=False,
        default_plugin="disabled",
        plugins=[
            disabled_plugin(),
            preview("fixed_epoch_placeholder", "固定 Epoch 占位"),
            planned("committee_lifecycle_planned", "委员会生命周期"),
            planned("adaptive_committee_lifecycle", "自适应委员会生命周期"),
        ],
    ),
    "Routing": module(
        "Routing",
        "分片 / 路由",
        required=True,
        default_plugin="hash_sharding",
        plugins=[
            runnable("hash_sharding", "哈希分片"),
            runnable("co_access_sharding", "共访问分片"),
            planned("range_sharding", "范围分片"),
            planned("dynamic_resharding", "动态重分片"),
        ],
    ),
    "Execution": module(
        "Execution",
        "交易执行",
        required=True,
        default_plugin="serial_execution",
        plugins=[
            runnable("serial_execution", "串行执行"),
            runnable("dual_track_execution", "双轨执行"),
            planned("parallel_naive", "朴素并行"),
            planned("block_stm_like", "Block-STM-like"),
        ],
    ),
    "StateAccess": module(
        "StateAccess",
        "状态访问",
        required=True,
        default_plugin="direct_fetch",
        plugins=[
            runnable("direct_fetch", "直接拉取"),
            runnable("access_list_prefetch", "访问列表预取"),
            planned("cache_prefetch", "缓存预取"),
            planned("witness_prefetch", "Witness 预取"),
        ],
    ),
    "StateStorage": module(
        "StateStorage",
        "状态存储",
        required=True,
        allow_variable=False,
        default_plugin="hash_state_storage",
        plugins=[
            runnable("memory_kv", "内存 KV"),
            runnable("hash_state_storage", "哈希状态存储"),
            planned("lsm_model", "LSM 模型"),
            planned("partitioned_state_storage", "分区状态存储"),
        ],
    ),
    "Commit": module(
        "Commit",
        "状态提交",
        required=True,
        default_plugin="normal_commit",
        plugins=[
            runnable("normal_commit", "普通提交"),
            runnable("hot_update_aggregation_commit", "热点更新聚合提交"),
            planned("batch_commit", "批量提交"),
            planned("two_phase_commit", "两阶段提交"),
        ],
    ),
    "MetricsReport": module(
        "MetricsReport",
        "指标 / 报告",
        required=True,
        allow_variable=False,
        output_only=True,
        default_plugin="basic_metrics",
        plugins=[
            runnable("basic_metrics", "基础指标"),
            runnable("metatrack_metrics", "MetaTrack 指标"),
            planned("consensus_metrics", "共识指标"),
            planned("sharding_metrics", "分片指标"),
            planned("committee_metrics", "委员会指标"),
        ],
    ),
}


REQUIRED_MODULES = [module_id for module_id, item in CATALOG.items() if item.required]
METATRACK_VARIABLE_MODULES = {"Routing", "Execution", "StateAccess", "Commit"}
METATRACK_FIXED_MODULES = {"Workload", "TxPool", "BlockProducer", "Consensus", "StateStorage"}
OUTPUT_MODULES = {"MetricsReport"}

GO_RUNTIME_PLUGIN_CLASSES = {
    "TxPool": "TxPoolPlugin",
    "BlockProducer": "BlockProducer",
    "Consensus": "ConsensusPlugin",
    "Routing": "ShardingPlugin",
    "Execution": "ExecutionSchedulerPlugin",
    "StateAccess": "StateAccessPlugin",
    "Commit": "CommitPlugin",
    "MetricsReport": "MetricsPlugin",
}


def plugin_owner(plugin_id: str) -> str | None:
    for module_id, item in CATALOG.items():
        if plugin_id in item.plugins:
            return module_id
    return None


def module_label(module_id: str) -> str:
    item = CATALOG.get(module_id)
    return item.label if item else module_id
