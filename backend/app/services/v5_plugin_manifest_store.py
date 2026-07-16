from __future__ import annotations

from collections.abc import Iterable

from backend.app.models.v5_plugin import V5PluginManifest


CATEGORIES = (
    "workload", "transaction_admission", "txpool", "sharding", "routing",
    "block_producer", "consensus", "network", "execution", "scheduler",
    "state_access", "state_storage", "cross_shard", "commit", "fault_injection",
    "metrics", "observability",
)


def _schema(properties: dict) -> dict:
    return {"type": "object", "properties": properties}


_ZH = {
    "deterministic_signed_synthetic": ("确定性签名合成负载", "生成带签名的确定性片内、跨片及超时退款交易。"),
    "signature_nonce_admission": ("签名与随机数准入", "校验 Ed25519 签名、发送方公钥绑定和随机数。"),
    "fifo_per_node_mempool": ("每节点 FIFO 交易池", "每个节点维护独立的 FIFO 交易池。"),
    "deterministic_state_key_sharding": ("确定性状态键分片", "将账户和状态键映射到配置的分片。"),
    "hash_routing_baseline": ("哈希路由基线", "稳定的哈希路由基线。"),
    "metatrack_coaccess_routing": ("MetaTrack 共访存路由", "带确定性分片偏移的共访问感知路由配置。"),
    "time_or_count_block_producer": ("时间或数量出块器", "Leader 按时间间隔或交易池数量持续提议区块。"),
    "pbft_style_consensus": ("PBFT 风格共识", "基于本地 TCP 的 PrePrepare、Prepare 和 Commit 法定人数消息流程。"),
    "localhost_tcp_typed_network": ("本地 TCP 类型化网络", "在独立 localhost TCP 监听器上传输类型化消息。"),
    "serial_execution_baseline": ("串行执行基线", "确定性的串行执行基线。"),
    "dual_track_execution": ("双轨执行", "记录快速轨与保守轨执行证据的确定性执行插件。"),
    "fifo_serial_scheduler": ("FIFO 串行调度器", "对已准入交易进行 FIFO 调度。"),
    "fast_first_scheduler": ("快速优先调度器", "用于双轨执行的快速优先调度器。"),
    "direct_state_access": ("直接状态访问", "直接访问确定性状态数据库。"),
    "persistent_local_state_store": ("本地持久化状态存储", "每个节点持久化状态、区块、回执和交易索引。"),
    "relay_certificate_protocol": ("中继证书跨片协议", "提供 SourceLock、中继证书、TargetCommit、SourceFinalize 与超时退款证据。"),
    "normal_commit": ("普通提交", "确定性的持久化提交。"),
    "commutative_hot_update_aggregation": ("可交换热点更新聚合", "记录热点更新聚合决策的提交配置。"),
    "faults_disabled": ("禁用故障注入", "不施加网络故障策略。"),
    "network_delay_drop": ("网络延迟与丢包", "确定性的真实 TCP 延迟和丢包策略。"),
    "runtime_core_metrics": ("运行时核心指标", "运行时计数器和状态根汇总。"),
    "node_network_consensus_observer": ("节点网络共识观测器", "生成节点、TCP、共识日志和产物目录。"),
}


def _manifest(category: str, plugin_id: str, display_name: str, description: str, *,
              config: dict | None = None, schema: dict | None = None, capabilities: list[str] | None = None,
              requirements: list[str] | None = None, metrics: list[dict] | None = None,
              aliases: list[str] | None = None) -> V5PluginManifest:
    display_name_zh, description_zh = _ZH.get(plugin_id, (display_name, description))
    return V5PluginManifest(
        plugin_id=plugin_id, category=category, display_name=display_name, description=description,
        display_name_zh=display_name_zh, description_zh=description_zh,
        supported_backends=["preview", "simulation", "real_cluster"],
        config_schema=schema or _schema({}), default_config=config or {}, capabilities=capabilities or [],
        requirements=requirements or [], metrics=metrics or [], runtime_factory=f"builtin:{plugin_id}",
        runtime_adapter="go_factory_registry", legacy_aliases=aliases or [],
    )


_MANIFESTS = [
    _manifest("workload", "deterministic_signed_synthetic", "Deterministic Signed Synthetic", "Deterministic signed workload with intra-shard, relay, and timeout cases.", config={"cross_shard_ratio": 0.25, "timeout_every": 17}, schema=_schema({"cross_shard_ratio": {"type": "number", "minimum": 0, "maximum": 1, "default": 0.25}, "timeout_every": {"type": "integer", "minimum": 0, "maximum": 1000, "default": 17}}), metrics=[{"key": "submitted_tx_count", "type": "integer", "unit": "tx", "aggregation": "sum", "visualization": "summary", "description": "Transactions submitted over TCP."}]),
    _manifest("workload", "canonical_trace_replay", "Canonical Trace Replay", "Streams deterministic materialized dataset workload records.", config={}, schema=_schema({"dataset_id": {"type": "string"}, "variant_mode": {"type": "string", "enum": ["original_window", "contract_zipf"]}, "target_alpha": {"type": ["number", "null"]}}), capabilities=["dataset_replay", "gzip_streaming", "no_fallback"], metrics=[{"key": "workload_replay_read_count", "type": "integer", "unit": "tx", "aggregation": "sum", "visualization": "summary", "description": "Canonical workload records read by the client."}]),
    _manifest("transaction_admission", "signature_nonce_admission", "Signature and Nonce Admission", "Ed25519 signature, sender/public-key binding, and nonce admission.", capabilities=["signed_tx", "nonce_validation"]),
    _manifest("txpool", "fifo_per_node_mempool", "FIFO Per-node Mempool", "Independent FIFO mempool on every node.", config={"capacity": 10000}, schema=_schema({"capacity": {"type": "integer", "minimum": 100, "maximum": 100000, "default": 10000}})),
    _manifest("sharding", "deterministic_state_key_sharding", "Deterministic State-key Sharding", "Maps account/state keys to a configured shard.", capabilities=["multi_shard"]),
    _manifest("routing", "hash_routing_baseline", "Hash Routing Baseline", "Stable hash routing baseline.", aliases=["hash"], metrics=[{"key": "routing_decision_count", "type": "integer", "unit": "decision", "aggregation": "sum", "visualization": "summary", "description": "Routing decisions made by the client."}]),
    _manifest("routing", "metatrack_coaccess_routing", "MetaTrack Co-access Routing", "Co-access-aware routing profile with a deterministic shard offset.", aliases=["co_access", "metatrack"], metrics=[{"key": "metatrack_routed_tx_count", "type": "integer", "unit": "tx", "aggregation": "sum", "visualization": "summary", "description": "Transactions routed through the MetaTrack profile."}]),
    _manifest("block_producer", "time_or_count_block_producer", "Time or Count Block Producer", "Leader proposes blocks repeatedly by interval or mempool count.", config={"block_size": 10, "interval_ms": 150}, schema=_schema({"block_size": {"type": "integer", "minimum": 1, "maximum": 1000, "default": 10}, "interval_ms": {"type": "integer", "minimum": 25, "maximum": 5000, "default": 150}})),
    _manifest("consensus", "pbft_style_consensus", "PBFT-style Consensus", "Local TCP PBFT-style PrePrepare, Prepare, and Commit quorum runtime.", capabilities=["pbft_messages", "quorum_commit"]),
    _manifest("network", "localhost_tcp_typed_network", "Localhost TCP Typed Network", "Typed message transport on distinct localhost TCP listeners.", capabilities=["tcp", "typed_messages"]),
    _manifest("execution", "serial_execution_baseline", "Serial Execution", "Deterministic serial execution baseline."),
    _manifest("execution", "dual_track_execution", "Dual-track Execution", "Deterministic execution with fast/conservative track evidence.", aliases=["dual_track"]),
    _manifest("scheduler", "fifo_serial_scheduler", "FIFO Serial Scheduler", "FIFO scheduler for admitted transactions."),
    _manifest("scheduler", "fast_first_scheduler", "Fast-first Scheduler", "Fast-first scheduler for dual-track execution.", requirements=["execution:dual_track_execution"]),
    _manifest("state_access", "direct_state_access", "Direct State Access", "Direct deterministic state database access."),
    _manifest("state_storage", "persistent_local_state_store", "Persistent Local State Store", "Per-node persisted state, block, receipt, and transaction index files.", capabilities=["persistent_state", "state_root"]),
    _manifest("cross_shard", "relay_certificate_protocol", "Relay Certificate Cross-shard", "SourceLock, relay certificate, target commit, source finalization, and timeout/refund evidence.", capabilities=["cross_shard", "relay_certificate"]),
    _manifest("commit", "normal_commit", "Normal Commit", "Durable deterministic commit."),
    _manifest("commit", "commutative_hot_update_aggregation", "Commutative Hot-update Aggregation", "Commit profile that records aggregation decisions for hot updates.", aliases=["aggregation"]),
    _manifest("fault_injection", "faults_disabled", "Faults Disabled", "No network fault policy."),
    _manifest("fault_injection", "network_delay_drop", "Network Delay and Drop", "Deterministic real TCP delay/drop policy.", config={"delay_ms": 5, "drop_every": 0}, schema=_schema({"delay_ms": {"type": "integer", "minimum": 0, "maximum": 1000, "default": 5}, "drop_every": {"type": "integer", "minimum": 0, "maximum": 10000, "default": 0}})),
    _manifest("metrics", "runtime_core_metrics", "Runtime Core Metrics", "Runtime counters and state-root summaries.", metrics=[{"key": "committed_block_count", "type": "integer", "unit": "block", "aggregation": "sum", "visualization": "summary", "description": "Committed blocks."}, {"key": "state_root_consistent", "type": "ratio", "unit": "boolean", "aggregation": "all", "visualization": "summary", "description": "Validator roots agree within every shard."}]),
    _manifest("observability", "node_network_consensus_observer", "Node, Network, and Consensus Observer", "Node logs, TCP logs, consensus logs, and artifact catalog."),
]


class PluginManifestStore:
    def __init__(self, manifests: Iterable[V5PluginManifest] = _MANIFESTS):
        self._items = list(manifests)
        seen: set[tuple[str, str, str]] = set()
        for item in self._items:
            if item.category not in CATEGORIES:
                raise ValueError(f"unknown plugin category: {item.category}")
            key = (item.plugin_id, item.category, item.version)
            if key in seen:
                raise ValueError(f"duplicate plugin manifest: {item.plugin_id}")
            seen.add(key)

    def list(self) -> list[V5PluginManifest]:
        return list(self._items)

    def get(self, plugin_id: str) -> V5PluginManifest:
        for item in self._items:
            if item.plugin_id == plugin_id or plugin_id in item.legacy_aliases:
                return item
        raise ValueError(f"unknown plugin: {plugin_id}")


STORE = PluginManifestStore()
