from __future__ import annotations

from dataclasses import dataclass
from typing import Any


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
            runnable("poa_light", "PoA light authority model"),
            runnable("pbft_light_model", "PBFT-light stage/quorum model"),
            planned("pbft_model", "PBFT 模型"),
            planned("pbft", "Real PBFT unsupported"),
            planned("real_pbft", "Real PBFT unsupported"),
            planned("hotstuff_model", "HotStuff 模型"),
            planned("hotstuff", "HotStuff unsupported"),
            planned("raft_model", "Raft 模型"),
            planned("raft", "Raft unsupported"),
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
            runnable("metatrack_coaccess_routing", "MetaTrack 共访问路由 light model"),
            runnable("hotspot_aware_routing", "热点感知路由 light model"),
            runnable("co_access_sharding", "共访问分片"),
            planned("clpa_like_partitioning", "CLPA-like 分区"),
            planned("shardcutter_like_partitioning", "ShardCutter-like 分区"),
            planned("relay_cross_shard", "Relay 跨片策略"),
            planned("broker_cross_shard", "Broker 跨片策略"),
            planned("two_phase_commit", "2PC 跨片策略"),
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
            runnable("dual_track_execution", "MetaTrack 双轨执行（兼容别名）"),
            runnable("parallel_light_execution", "轻量并行执行模型"),
            runnable("metatrack_dual_track_execution", "MetaTrack 双轨轻量执行模型"),
            planned("parallel_naive", "朴素并行"),
            planned("block_stm_like", "Block-STM-like"),
            planned("block_stm_like_model", "Block-STM-like model"),
            planned("calvin_like_model", "Calvin-like model"),
            planned("real_optimistic_execution", "真实乐观执行"),
            planned("real_rollback_engine", "真实 rollback engine"),
            planned("real_thread_pool_execution", "真实线程池执行"),
        ],
    ),
    "StateAccess": module(
        "StateAccess",
        "状态访问",
        required=True,
        default_plugin="direct_fetch",
        plugins=[
            runnable("remote_state_access_model", "Remote state access light model"),
            runnable("cached_state_access", "Cached state access light model"),
            planned("real_witness_fetch", "Real witness fetch"),
            planned("real_proof_fetch", "Real proof fetch"),
            planned("mpt_proof_model", "MPT proof model"),
            planned("persistent_kv_access", "Persistent KV access"),
            planned("snapshot_access", "Snapshot access"),
            planned("real_remote_storage", "Real remote storage"),
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

SINGLE_MODULE_LOCKED_PLUGIN_VALUES = {
    "Workload": "synthetic_hotspot",
    "TxPool": "fifo_pool",
    "BlockProducer": "time_or_count_block_producer",
    "Consensus": "simple_leader",
    "CommitteeEpoch": "disabled",
    "Routing": "co_access_sharding",
    "Execution": "dual_track_execution",
    "StateAccess": "access_list_prefetch",
    "StateStorage": "hash_state_storage",
    "Commit": "hot_update_aggregation_commit",
    "MetricsReport": "basic_metrics",
}

SINGLE_MODULE_TEMPLATE_CATALOG: dict[str, dict[str, Any]] = {
    "single_module_txpool": {
        "template_id": "single_module_txpool",
        "template_name": "Single-module TxPool",
        "stage": "V3.4.4b",
        "chain_mode": "single_chain",
        "status": "runnable",
        "runnable": True,
        "preview_only": False,
        "description": "Fair single-module template for TxPool hardening checks. Only TxPool may vary.",
        "variable_module": "TxPool",
        "allowed_variable_plugins": ["fifo_pool"],
        "locked_plugin_values": {key: value for key, value in SINGLE_MODULE_LOCKED_PLUGIN_VALUES.items() if key != "TxPool"},
        "fairness_rule": "Only TxPoolPlugin may vary; all other modules, workload, seed, submit rate, block config, and network profile stay fixed.",
        "truthfulness_note": "fifo_pool is runtime-realized. priority_pool, hotspot_aware_pool, and fee_based_pool remain planned.",
        "default_preset_id": "txpool_fifo_smoke",
        "presets": [
            {
                "preset_id": "txpool_fifo_smoke",
                "preset_name": "FIFO TxPool smoke",
                "description": "Runs a local Draft Smoke that focuses on FIFO TxPool queue admission, selection, wait, and rejection observability.",
                "default_chain_profile": "single_chain_research_default",
                "default_plugin_selection": dict(SINGLE_MODULE_LOCKED_PLUGIN_VALUES),
                "variable_module": "TxPool",
                "locked_modules": {key: value for key, value in SINGLE_MODULE_LOCKED_PLUGIN_VALUES.items() if key != "TxPool"},
                "primary_metrics": [
                    "txpool_admitted_count",
                    "txpool_rejected_count",
                    "txpool_peak_size",
                    "txpool_avg_wait_ms",
                    "txpool_p95_wait_ms",
                    "queue_wait_ms",
                ],
                "secondary_metrics": ["tx_count", "success_count", "failure_count", "avg_latency_ms"],
                "expected_artifacts": ["txpool_log.csv", "summary.csv", "summary.json", "block_log.csv", "tx_results.csv"],
                "result_guide": "Focus on queue wait, admitted/rejected count, peak pool size, and txpool_log.csv.",
                "truthfulness_note": "This preset validates FIFO TxPool behavior in the local Go-backed runtime. It is not a full network mempool experiment.",
            }
        ],
    },
    "single_module_blockproducer": {
        "template_id": "single_module_blockproducer",
        "template_name": "Single-module BlockProducer",
        "stage": "V3.4.4b",
        "chain_mode": "single_chain",
        "status": "runnable",
        "runnable": True,
        "preview_only": False,
        "description": "Fair single-module template for BlockProducer hardening checks. Only BlockProducer may vary.",
        "variable_module": "BlockProducer",
        "allowed_variable_plugins": ["time_or_count_block_producer"],
        "locked_plugin_values": {key: value for key, value in SINGLE_MODULE_LOCKED_PLUGIN_VALUES.items() if key != "BlockProducer"},
        "fairness_rule": "Only BlockProducer may vary; all other modules, workload, seed, submit rate, consensus config, and network profile stay fixed.",
        "truthfulness_note": "time_or_count_block_producer is runtime-realized. fixed_size_block and adaptive_block_cut remain planned.",
        "default_preset_id": "blockproducer_time_or_count_smoke",
        "presets": [
            {
                "preset_id": "blockproducer_time_or_count_smoke",
                "preset_name": "Time-or-count BlockProducer smoke",
                "description": "Runs a local Draft Smoke that focuses on time/count/drain block production and block_log explainability.",
                "default_chain_profile": "single_chain_research_default",
                "default_plugin_selection": dict(SINGLE_MODULE_LOCKED_PLUGIN_VALUES),
                "variable_module": "BlockProducer",
                "locked_modules": {key: value for key, value in SINGLE_MODULE_LOCKED_PLUGIN_VALUES.items() if key != "BlockProducer"},
                "primary_metrics": [
                    "block_count",
                    "empty_block_count",
                    "avg_block_size",
                    "max_block_size",
                    "avg_block_interval_ms",
                    "blockproducer_count_cut_count",
                    "blockproducer_time_cut_count",
                    "blockproducer_drain_cut_count",
                    "blockproducer_empty_cut_count",
                ],
                "secondary_metrics": ["tx_count", "success_count", "queue_wait_ms", "avg_latency_ms"],
                "expected_artifacts": ["block_log.csv", "txpool_log.csv", "summary.csv", "summary.json", "tx_results.csv"],
                "result_guide": "Focus on block size, block interval, cut reason counts, and block_log.csv.",
                "truthfulness_note": "This preset validates local time/count block production behavior. It is not a multi-node proposer network.",
            }
        ],
    },
    "single_module_consensus": {
        "template_id": "single_module_consensus",
        "template_name": "Single-module Consensus-light",
        "stage": "V3.4.4b",
        "chain_mode": "single_chain",
        "status": "runnable",
        "runnable": True,
        "preview_only": False,
        "description": "Fair single-module template for Consensus-light checks. Only Consensus may vary.",
        "variable_module": "Consensus",
        "allowed_variable_plugins": ["simple_leader", "poa_light", "pbft_light_model"],
        "locked_plugin_values": {key: value for key, value in SINGLE_MODULE_LOCKED_PLUGIN_VALUES.items() if key != "Consensus"},
        "fairness_rule": "Only ConsensusPlugin may vary; all other modules, workload, seed, submit rate, block config, and network profile stay fixed.",
        "truthfulness_note": "pbft_light_model is PBFT-style stage/quorum/message-count model only. It is not real PBFT.",
        "default_preset_id": "consensus_light_smoke",
        "presets": [
            {
                "preset_id": "consensus_light_smoke",
                "preset_name": "Consensus-light smoke",
                "description": "Runs a local Draft Smoke that focuses on simple_leader, PoA-light, or PBFT-light consensus metrics.",
                "default_chain_profile": "single_chain_research_default",
                "default_plugin_selection": dict(SINGLE_MODULE_LOCKED_PLUGIN_VALUES),
                "variable_module": "Consensus",
                "locked_modules": {key: value for key, value in SINGLE_MODULE_LOCKED_PLUGIN_VALUES.items() if key != "Consensus"},
                "primary_metrics": [
                    "consensus_latency_ms",
                    "avg_consensus_latency_ms",
                    "p95_consensus_latency_ms",
                    "consensus_message_count",
                    "avg_consensus_message_count",
                    "consensus_round_count",
                    "view_change_count",
                    "finalized_block_count",
                    "failed_block_count",
                ],
                "secondary_metrics": ["block_count", "avg_block_size", "avg_latency_ms", "success_count"],
                "expected_artifacts": ["consensus_log.csv", "block_log.csv", "summary.csv", "summary.json", "tx_results.csv"],
                "result_guide": "Focus on consensus latency, message count, round count, finalized blocks, and consensus_log.csv.",
                "truthfulness_note": "pbft_light_model is a PBFT-style stage/quorum/message-count model only. It is not real PBFT or production BFT consensus.",
            }
        ],
    },
    "single_module_routing": {
        "template_id": "single_module_routing",
        "template_name": "Single-module Routing / Sharding",
        "stage": "V3.4.5",
        "chain_mode": "single_chain",
        "status": "runnable",
        "runnable": True,
        "preview_only": False,
        "description": "Fair single-module template for Routing/Sharding hardening checks. Only Routing may vary.",
        "variable_module": "Routing",
        "allowed_variable_plugins": ["hash_sharding", "metatrack_coaccess_routing", "hotspot_aware_routing"],
        "locked_plugin_values": {key: value for key, value in SINGLE_MODULE_LOCKED_PLUGIN_VALUES.items() if key != "Routing"},
        "fairness_rule": "Only ShardingPlugin / Routing may vary; all other modules, workload, seed, submit rate, block config, consensus config, and network profile stay fixed.",
        "truthfulness_note": "Routing light models estimate shard assignment and routing decisions only. They do not implement relay, broker, 2PC, CLPA, ShardCutter, state migration, or real cross-shard protocols.",
        "default_preset_id": "routing_coaccess_smoke",
        "presets": [
            {
                "preset_id": "routing_coaccess_smoke",
                "preset_name": "Routing co-access smoke",
                "description": "Runs a local Draft Smoke that focuses on Routing/Sharding decision records, touched shards, cross-shard ratio, hotspot keys, and co-access groups.",
                "default_chain_profile": "single_chain_research_default",
                "default_plugin_selection": dict(SINGLE_MODULE_LOCKED_PLUGIN_VALUES) | {"Routing": "metatrack_coaccess_routing"},
                "variable_module": "Routing",
                "locked_modules": {key: value for key, value in SINGLE_MODULE_LOCKED_PLUGIN_VALUES.items() if key != "Routing"},
                "primary_metrics": [
                    "cross_shard_ratio",
                    "cross_shard_tx_count",
                    "remote_state_access_count",
                    "avg_touched_shards",
                    "hotspot_key_count",
                    "coaccess_group_count",
                    "avg_routing_overhead_ms",
                ],
                "secondary_metrics": ["routing_decision_count", "local_tx_count", "max_touched_shards", "routing_plugin"],
                "expected_artifacts": ["routing_log.csv", "summary.csv", "summary.json", "block_log.csv", "tx_results.csv"],
                "result_guide": "Focus on routing_plugin, cross_shard_ratio, touched shard counts, hotspot/co-access counts, and routing_log.csv.",
                "truthfulness_note": "This preset validates routing/sharding decision behavior in the local Go-backed runtime. It does not implement real cross-shard transaction protocols, relay, broker, or state migration.",
            }
        ],
    },
    "single_module_execution": {
        "template_id": "single_module_execution",
        "template_name": "Single-module Execution",
        "stage": "V3.4.6",
        "chain_mode": "single_chain",
        "status": "runnable",
        "runnable": True,
        "preview_only": False,
        "description": "Fair single-module template for Execution runtime hardening checks. Only Execution may vary.",
        "variable_module": "Execution",
        "allowed_variable_plugins": ["serial_execution", "parallel_light_execution", "metatrack_dual_track_execution"],
        "locked_plugin_values": {key: value for key, value in SINGLE_MODULE_LOCKED_PLUGIN_VALUES.items() if key != "Execution"},
        "fairness_rule": "Only ExecutionSchedulerPlugin / Execution may vary; all other modules, workload, seed, submit rate, routing config, consensus config, and network profile stay fixed.",
        "truthfulness_note": "Execution light models estimate deterministic scheduling, dependency edges, blocking, and fast/conservative track assignment only. They do not implement real concurrent execution, rollback, Block-STM, or Calvin.",
        "default_preset_id": "execution_dual_track_smoke",
        "presets": [
            {
                "preset_id": "execution_dual_track_smoke",
                "preset_name": "Execution dual-track smoke",
                "description": "Runs a local Draft Smoke that focuses on Execution records, dependency edges, logical workers, blocked transactions, and fast/conservative tracks.",
                "default_chain_profile": "single_chain_research_default",
                "default_plugin_selection": dict(SINGLE_MODULE_LOCKED_PLUGIN_VALUES) | {"Execution": "metatrack_dual_track_execution"},
                "variable_module": "Execution",
                "locked_modules": {key: value for key, value in SINGLE_MODULE_LOCKED_PLUGIN_VALUES.items() if key != "Execution"},
                "primary_metrics": [
                    "fast_track_count",
                    "conservative_track_count",
                    "blocked_tx_count",
                    "dependency_edge_count",
                    "avg_dependency_edges_per_tx",
                    "avg_execution_latency_ms",
                    "p95_execution_latency_ms",
                    "parallelizable_tx_count",
                ],
                "secondary_metrics": ["execution_plugin", "execution_tx_count", "logical_worker_count", "serial_tx_count", "conflict_count"],
                "expected_artifacts": ["execution_log.csv", "routing_log.csv", "summary.csv", "summary.json", "block_log.csv", "tx_results.csv"],
                "result_guide": "Focus on fast/conservative track counts, dependency edges, blocked tx count, logical workers, execution latency, and execution_log.csv.",
                "truthfulness_note": "This preset validates deterministic execution scheduling behavior in the local Go-backed runtime. It does not implement real concurrent execution, real rollback, or a full Block-STM/Calvin engine.",
            }
        ],
    },
    "single_module_state_access": {
        "template_id": "single_module_state_access",
        "template_name": "Single-module StateAccess",
        "stage": "V3.4.7",
        "chain_mode": "single_chain",
        "status": "runnable",
        "runnable": True,
        "preview_only": False,
        "description": "Fair single-module template for StateAccess runtime hardening checks. Only StateAccess may vary.",
        "variable_module": "StateAccess",
        "allowed_variable_plugins": ["direct_fetch", "remote_state_access_model", "cached_state_access", "access_list_prefetch"],
        "locked_plugin_values": {key: value for key, value in SINGLE_MODULE_LOCKED_PLUGIN_VALUES.items() if key != "StateAccess"},
        "fairness_rule": "Only StateAccessPlugin / StateAccess may vary; all other modules, workload, seed, submit rate, routing config, execution config, consensus config, and network profile stay fixed.",
        "truthfulness_note": "StateAccess light models estimate local/remote access, deterministic cache/prefetch, and proof/witness sizes only. They do not implement real remote storage, proofs, witnesses, MPT, state root, persistent KV, or snapshots.",
        "default_preset_id": "state_access_remote_prefetch_smoke",
        "presets": [
            {
                "preset_id": "state_access_remote_prefetch_smoke",
                "preset_name": "StateAccess remote/prefetch smoke",
                "description": "Runs a local Draft Smoke that focuses on state access records, local/remote access, cache/prefetch hits, latency, and proof/witness estimates.",
                "default_chain_profile": "single_chain_research_default",
                "default_plugin_selection": dict(SINGLE_MODULE_LOCKED_PLUGIN_VALUES) | {"StateAccess": "access_list_prefetch"},
                "variable_module": "StateAccess",
                "locked_modules": {key: value for key, value in SINGLE_MODULE_LOCKED_PLUGIN_VALUES.items() if key != "StateAccess"},
                "primary_metrics": [
                    "remote_state_access_count",
                    "remote_state_access_ratio",
                    "cache_hit_count",
                    "cache_miss_count",
                    "cache_hit_rate",
                    "prefetch_hit_count",
                    "prefetch_miss_count",
                    "prefetch_hit_rate",
                    "avg_state_access_latency_ms",
                    "p95_state_access_latency_ms",
                    "witness_estimated_count",
                    "proof_estimated_count",
                ],
                "secondary_metrics": ["state_access_plugin", "state_access_count", "local_state_access_count", "estimated_witness_bytes", "estimated_proof_bytes"],
                "expected_artifacts": ["state_access_log.csv", "execution_log.csv", "routing_log.csv", "summary.csv", "summary.json", "block_log.csv", "tx_results.csv"],
                "result_guide": "Focus on local/remote state access, cache/prefetch hit rates, state access latency, proof/witness estimates, and state_access_log.csv.",
                "truthfulness_note": "This preset validates deterministic state access behavior in the local Go-backed runtime. It models local/remote state access, cache, prefetch, and proof/witness estimates, but does not generate real proofs, witnesses, MPTs, or remote storage IO.",
            }
        ],
    },
}


def experiment_template_catalog() -> dict[str, dict[str, Any]]:
    return {template_id: dict(template) for template_id, template in SINGLE_MODULE_TEMPLATE_CATALOG.items()}


def plugin_owner(plugin_id: str) -> str | None:
    for module_id, item in CATALOG.items():
        if plugin_id in item.plugins:
            return module_id
    return None


def module_label(module_id: str) -> str:
    item = CATALOG.get(module_id)
    return item.label if item else module_id
