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


def _manifest(category: str, plugin_id: str, display_name: str, description: str, *,
              config: dict | None = None, schema: dict | None = None, capabilities: list[str] | None = None,
              requirements: list[str] | None = None, metrics: list[dict] | None = None,
              aliases: list[str] | None = None) -> V5PluginManifest:
    return V5PluginManifest(
        plugin_id=plugin_id, category=category, display_name=display_name, description=description,
        supported_backends=["preview", "simulation", "real_cluster"],
        config_schema=schema or _schema({}), default_config=config or {}, capabilities=capabilities or [],
        requirements=requirements or [], metrics=metrics or [], runtime_factory=f"builtin:{plugin_id}",
        runtime_adapter="go_factory_registry", legacy_aliases=aliases or [],
    )


_MANIFESTS = [
    _manifest("workload", "deterministic_signed_synthetic", "Deterministic Signed Synthetic", "Deterministic signed workload with intra-shard, relay, and timeout cases.", config={"cross_shard_ratio": 0.25, "timeout_every": 17}, schema=_schema({"cross_shard_ratio": {"type": "number", "minimum": 0, "maximum": 1, "default": 0.25}, "timeout_every": {"type": "integer", "minimum": 0, "maximum": 1000, "default": 17}}), metrics=[{"key": "submitted_tx_count", "type": "integer", "unit": "tx", "aggregation": "sum", "visualization": "summary", "description": "Transactions submitted over TCP."}]),
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
