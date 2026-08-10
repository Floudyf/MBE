from __future__ import annotations

from collections.abc import Iterable

from backend.app.models.v5_plugin import V5PluginManifest


CATEGORIES = (
    "workload", "transaction_admission", "txpool", "sharding", "routing",
    "block_producer", "consensus", "network", "execution", "scheduler",
    "block_executor", "state_access", "state_storage", "cross_shard", "commit", "fault_injection",
    "metrics", "observability",
)


def _schema(properties: dict) -> dict:
    return {"type": "object", "properties": properties}


_ZH = {
    "deterministic_signed_synthetic": ("确定性签名合成负载", "生成带签名的确定性片内、跨片及超时退款交易。"),
    "signature_nonce_admission": ("签名与随机数准入", "校验 Ed25519 签名、发送方公钥绑定和随机数。"),
    "fifo_per_node_mempool": ("每节点 FIFO 交易池", "每个节点维护独立的 FIFO 交易池。"),
    "deterministic_state_key_sharding": ("确定性状态键分片", "将账户和状态键映射到配置的分片。"),
    "hash_routing_baseline": ("有状态哈希路由参考", "按源分片执行并使用本地状态的历史参考路由。"),
    "stateless_hash_routing": ("无状态哈希路由基线", "使用确定性状态归属、远程状态获取与写回的公平无状态哈希路由。"),
    "metatrack_coaccess_routing": ("MetaTrack 共访存路由", "按批次访问频率与状态共现构造有容量约束的执行侧状态放置，并以 MajorityPlace 决定交易执行分片。"),
    "time_or_count_block_producer": ("时间或数量出块器", "Leader 按时间间隔或交易池数量持续提议区块。"),
    "aria_block_producer": ("Aria 批次区块装配器", "按同一状态快照执行一个 Aria 批次，将可提交交易组装入当前区块，并将冲突交易按原顺序留到后续区块。"),
    "groundhog_block_producer": ("Groundhog 区块装配器", "基于同一快照解释候选交易并通过类型化状态预留筛选可共同成立的区块交易。"),
    "pbft_style_consensus": ("PBFT 风格共识", "基于本地 TCP 的 PrePrepare、Prepare 和 Commit 法定人数消息流程。"),
    "localhost_tcp_typed_network": ("本地 TCP 类型化网络", "在独立 localhost TCP 监听器上传输类型化消息。"),
    "serial_execution_baseline": ("串行执行基线", "确定性的串行执行基线。"),
    "batch_si_execution": ("Batch-SI 执行分类器", "仅为 Batch-SI 提供独立的执行轨分类，不调用其他方案算法代码。"),
    "dual_track_execution": ("双轨执行", "记录快速轨与保守轨执行证据的确定性执行插件。"),
    "fifo_serial_scheduler": ("FIFO 串行调度器", "对已准入交易进行 FIFO 调度。"),
    "fast_first_scheduler": ("快速优先调度器", "用于双轨执行的快速优先调度器。"),
    "batch_si_scheduler": ("Batch-SI 批次规划器", "在共识前独立执行 AWRT、WRBP 与 OFAS，并将确定性批次计划绑定到区块。"),
    "batch_si_block_executor": ("Batch-SI 批快照执行器", "按批次顺序、批内统一快照并行执行 Batch-SI 计划，不调用其他方案算法代码。"),
    "cg_execution": ("CG 执行分类", "论文对照组：完整事务冲突图。"),
    "cg_scheduler": ("CG 冲突图调度", "对所有事务对执行完整读写冲突检测并构造原顺序依赖 DAG。"),
    "cg_block_executor": ("CG 并行执行器", "按冲突 DAG 的零入度层并行执行。"),
    "acg_execution": ("ACG 执行分类", "论文对照组：地址冲突图。"),
    "acg_scheduler": ("ACG 地址冲突图调度", "按地址访问索引构造冲突先序并执行层次调度。"),
    "acg_block_executor": ("ACG 并行执行器", "按 ACG 层次并行执行。"),
    "bsx_execution": ("BSX 执行分类", "论文对照组：无向冲突图着色调度。"),
    "bsx_scheduler": ("BSX 图着色调度", "构造无向冲突图并使用确定性 DSATUR 启发式着色。"),
    "bsx_block_executor": ("BSX 并行执行器", "同色无冲突事务并行、颜色批次顺序推进。"),
    "aria_block_executor": ("Aria 批量乐观执行器", "验证固定 Aria 批次在同一快照、读写预留与 Rule 2 下可以单纪元提交。"),
    "groundhog_block_executor": ("Groundhog 无序区块执行器", "对固定区块执行同快照类型化修改验证、约束合并和确定性状态物化。"),
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
              aliases: list[str] | None = None, supported_backends: list[str] | None = None,
              truth_boundary: str = "v5_real_cluster_candidate", source: dict | None = None) -> V5PluginManifest:
    display_name_zh, description_zh = _ZH.get(plugin_id, (display_name, description))
    return V5PluginManifest(
        plugin_id=plugin_id, category=category, display_name=display_name, description=description,
        display_name_zh=display_name_zh, description_zh=description_zh,
        supported_backends=supported_backends or ["preview", "simulation", "real_cluster"],
        config_schema=schema or _schema({}), default_config=config or {}, capabilities=capabilities or [],
        requirements=requirements or [], metrics=metrics or [], runtime_factory=f"builtin:{plugin_id}",
        runtime_adapter="go_factory_registry", truth_boundary=truth_boundary, source=source or {}, legacy_aliases=aliases or [],
    )


_MANIFESTS = [
    _manifest("workload", "deterministic_signed_synthetic", "Deterministic Signed Synthetic", "Deterministic signed workload with intra-shard, relay, and timeout cases.", config={"cross_shard_ratio": 0.25, "timeout_every": 17}, schema=_schema({"cross_shard_ratio": {"type": "number", "minimum": 0, "maximum": 1, "default": 0.25}, "timeout_every": {"type": "integer", "minimum": 0, "maximum": 1000, "default": 17}}), metrics=[{"key": "submitted_tx_count", "type": "integer", "unit": "tx", "aggregation": "sum", "visualization": "summary", "description": "Transactions submitted over TCP."}]),
    _manifest("workload", "canonical_trace_replay", "Canonical Trace Replay", "Streams deterministic materialized dataset workload records through a manifest-owned universal access-list contract.", config={}, schema=_schema({"dataset_id": {"type": "string"}, "variant_mode": {"type": "string"}, "selection_mode": {"type": "string", "enum": ["contiguous_window", "validated_prefix"]}, "skew_axis": {"type": ["string", "null"]}, "target_alpha": {"type": ["number", "null"]}, "variant_parameters": {"type": "object"}}), capabilities=["dataset_replay", "gzip_streaming", "manifest_variants", "direct_access_list_v3", "no_fallback"], metrics=[{"key": "workload_replay_read_count", "type": "integer", "unit": "tx", "aggregation": "sum", "visualization": "summary", "description": "Canonical workload records read by the client."}]),
    _manifest("transaction_admission", "signature_nonce_admission", "Signature and Nonce Admission", "Ed25519 signature, sender/public-key binding, and nonce admission.", capabilities=["signed_tx", "nonce_validation"]),
    _manifest("txpool", "fifo_per_node_mempool", "FIFO Per-node Mempool", "Independent FIFO mempool on every node.", config={"capacity": 10000}, schema=_schema({"capacity": {"type": "integer", "minimum": 100, "maximum": 100000, "default": 10000}})),
    _manifest("sharding", "deterministic_state_key_sharding", "Deterministic State-key Sharding", "Maps account/state keys to a configured shard.", capabilities=["multi_shard"]),
    _manifest("routing", "hash_routing_baseline", "Stateful Hash Routing Reference", "Historical source-shard execution reference with local state access.", aliases=["hash"], capabilities=["stateful_local_execution"], truth_boundary="stateful_reference_not_directly_comparable_to_stateless_methods", metrics=[{"key": "routing_decision_count", "type": "integer", "unit": "decision", "aggregation": "sum", "visualization": "summary", "description": "Routing decisions made by the client."}]),
    _manifest("routing", "stateless_hash_routing", "Stateless Hash Routing Baseline", "Fair stateless hash baseline with deterministic state homes, exact per-key logical-version fetch for non-commutative business state, and home-shard writeback.", aliases=["stateless_hash"], capabilities=["batch_routing", "stateless_direct_execution", "remote_state_fetch", "remote_state_writeback", "exact_state_version_fetch", "per_key_version_writeback_frontier"], truth_boundary="stateless_hash_remote_home_exact_version_v2", metrics=[{"key": "routing_decision_count", "type": "integer", "unit": "decision", "aggregation": "sum", "visualization": "summary", "description": "Stateless hash routing decisions made by the client."}]),
    _manifest(
        "routing", "metatrack_coaccess_routing", "MetaTrack Co-access Routing",
        "Batch-level execution sharding from state access frequency and co-access weights, with an explicit admissible state-load capacity and MajorityPlace transaction routing.",
        config={"routing_epoch": 0},
        schema=_schema({
            "routing_epoch": {"type": "integer", "minimum": 0, "maximum": 1000000000, "default": 0},
        }),
        aliases=["co_access", "metatrack"],
        capabilities=["batch_routing", "batch_frequency_coaccess", "admissible_state_load_capacity", "per_seed_top_cooccur_budget", "majority_place_queue_tie", "routing_epoch_evidence", "predicted_remote_access_accounting", "stateless_direct_execution", "remote_state_fetch", "remote_state_writeback", "exact_state_version_fetch"],
        truth_boundary="metatrack_batch_execution_sharding_admissible_capacity_v2",
        source={"source_type": "user_manuscript_algorithm_reimplementation", "source_name": "MetaTrack batch-level execution sharding"},
        metrics=[{"key": "metatrack_routed_tx_count", "type": "integer", "unit": "tx", "aggregation": "sum", "visualization": "summary", "description": "Transactions routed through the MetaTrack profile."}],
    ),
    _manifest("block_producer", "time_or_count_block_producer", "Time or Count Block Producer", "Leader proposes blocks repeatedly by interval or mempool count.", config={"block_size": 100, "interval_ms": 75}, schema=_schema({"block_size": {"type": "integer", "minimum": 10, "maximum": 5000, "default": 100}, "interval_ms": {"type": "integer", "minimum": 25, "maximum": 5000, "default": 75}})),
    _manifest(
        "block_producer", "aria_block_producer", "Aria Block Producer",
        "Executes one deterministic Aria batch over the block-start snapshot, selects Rule-2-committable transactions for the current block, and releases deferred transactions back to FIFO mempool order for a later block.",
        config={
            "block_size": 100,
            "interval_ms": 75,
            "candidate_scan_multiplier": 1,
            "reordering": True,
            "read_only_optimization": True,
            "retry_nonce_gaps": True,
        },
        schema=_schema({
            "block_size": {"type": "integer", "minimum": 10, "maximum": 5000, "default": 100},
            "interval_ms": {"type": "integer", "minimum": 25, "maximum": 5000, "default": 75},
            "candidate_scan_multiplier": {"type": "integer", "minimum": 1, "maximum": 32, "default": 1},
            "reordering": {"type": "boolean", "default": True},
            "read_only_optimization": {"type": "boolean", "default": True},
            "retry_nonce_gaps": {"type": "boolean", "default": True},
        }),
        capabilities=["aria_single_batch_assembly", "same_snapshot_execution", "rule2_conflict_selection", "fifo_deferred_transactions", "proposal_selection_evidence"],
        requirements=["block_executor:aria_block_executor", "routing:hash_routing_baseline", "execution:serial_execution_baseline", "scheduler:fifo_serial_scheduler", "state_access:direct_state_access", "state_storage:persistent_local_state_store", "commit:normal_commit"],
        supported_backends=["real_cluster"],
        truth_boundary="aria_rule2_one_consensus_block_per_batch_fallback_disabled",
        source={"source_type": "paper_and_official_source_reimplementation", "source_name": "Aria: A Fast and Practical Deterministic OLTP Database", "doi": "10.14778/3407790.3407808", "source_repository": "https://github.com/luyi0619/aria", "source_commit": "d0508c393ec084582c12e6f3abadab63501eaedd", "reproduction_dossier": "docs/reproductions/aria/source_lock.md"},
    ),
    _manifest(
        "block_producer", "groundhog_block_producer", "Groundhog Block Producer",
        "Builds a Groundhog candidate by interpreting a wider FIFO candidate window against one block-start snapshot, reserving typed modifications, deferring only aggregate constraint conflicts, and preserving terminal application failures for final receipts.",
        config={"block_size": 100, "interval_ms": 75, "candidate_scan_multiplier": 4, "ordered_set_limit": 64},
        schema=_schema({
            "block_size": {"type": "integer", "minimum": 10, "maximum": 5000, "default": 100},
            "interval_ms": {"type": "integer", "minimum": 25, "maximum": 5000, "default": 75},
            "candidate_scan_multiplier": {"type": "integer", "minimum": 1, "maximum": 32, "default": 4},
            "ordered_set_limit": {"type": "integer", "minimum": 1, "maximum": 65535, "default": 64},
        }),
        capabilities=["groundhog_block_assembly", "same_snapshot_candidate_interpretation", "typed_state_reservation", "transaction_atomic_rollback", "constraint_conflict_deferral"],
        requirements=["block_executor:groundhog_block_executor", "routing:hash_routing_baseline", "state_access:direct_state_access", "state_storage:persistent_local_state_store"],
        supported_backends=["real_cluster"],
        truth_boundary="groundhog_deterministic_candidate_assembly_over_concurrent_typed_reservation_validation;official_cpp_mempool_worker_race_not_reproduced",
        source={"source_type": "paper_and_official_source_reimplementation", "source_name": "Groundhog: Linearly-Scalable Smart Contracting via Commutative Transaction Semantics", "arxiv": "2404.03201", "source_repository": "https://github.com/scslab/smart-contract-scalability", "source_commit": "6b357bc206b73ece39fd61fe7dba655352200c0a", "reproduction_dossier": "docs/reproductions/groundhog/source_lock.md"},
    ),
    _manifest("consensus", "pbft_style_consensus", "PBFT-style Consensus", "Local TCP PBFT-style PrePrepare, Prepare, and Commit quorum runtime.", capabilities=["pbft_messages", "quorum_commit"]),
    _manifest("network", "localhost_tcp_typed_network", "Localhost TCP Typed Network", "Typed message transport on distinct localhost TCP listeners.", capabilities=["tcp", "typed_messages"]),
    _manifest("execution", "serial_execution_baseline", "Serial Execution", "Deterministic serial execution baseline."),
    _manifest(
        "execution", "batch_si_execution", "Batch-SI Execution Classifier",
        "Private Batch-SI execution-track classifier. It only identifies transactions for the Batch-SI planner and does not reuse another scheme's algorithm implementation.",
        capabilities=["batch_si_execution_track", "no_cross_scheme_algorithm_reuse"],
        supported_backends=["real_cluster"],
        truth_boundary="batch_si_private_execution_classification_contract",
        source={"source_type": "paper_reimplementation", "source_name": "Batch-SI: Semi-Parallel Concurrency Scheme for Permissioned Blockchains Exploiting Batch Snapshot Isolation", "reproduction_dossier": "docs/reproductions/batch_si/source_lock.md"},
    ),
    _manifest(
        "execution", "dual_track_execution", "Dual-track Execution",
        "Deterministic execution with fast/conservative track evidence using the paper access-size eligibility threshold.",
        aliases=["dual_track"],
        config={"access_size_threshold": 4},
        schema=_schema({"access_size_threshold": {"type": "integer", "minimum": 1, "maximum": 64, "default": 4}}),
        capabilities=["dual_track", "access_size_threshold", "non_commutative_conservative_admission"],
        truth_boundary="metatrack_dual_track_execution_with_tau_access_size_gate",
    ),
    _manifest("scheduler", "fifo_serial_scheduler", "FIFO Serial Scheduler", "FIFO scheduler for admitted transactions."),
    _manifest("scheduler", "fast_first_scheduler", "Fast-first Scheduler", "Fast-first scheduler for dual-track execution.", requirements=["execution:dual_track_execution"]),
    _manifest(
        "scheduler", "batch_si_scheduler", "Batch-SI Batch Planner",
        "Independently constructs AWRT, applies WRBP, performs OFAS, defers cyclic transactions before consensus, and commits a deterministic batch plan to the block.",
        config={"partition_mode": "wrbp", "ordering_mode": "ofas", "priority_mode": "paper"},
        schema=_schema({
            "partition_mode": {"type": "string", "enum": ["wrbp", "sequential"], "default": "wrbp"},
            "ordering_mode": {"type": "string", "enum": ["ofas", "dependency_graph"], "default": "ofas"},
            "priority_mode": {"type": "string", "enum": ["paper", "txid"], "default": "paper"},
        }),
        capabilities=["batch_si", "awrt", "wrbp", "ofas", "pre_consensus_execution_plan", "deterministic_deferral"],
        requirements=["block_executor:batch_si_block_executor", "execution:batch_si_execution", "routing:hash_routing_baseline", "state_access:direct_state_access", "state_storage:persistent_local_state_store", "commit:normal_commit"],
        supported_backends=["real_cluster"],
        truth_boundary="batch_si_ordering_reimplementation_over_declared_mbe_access_lists",
        source={"source_type": "paper_reimplementation", "source_name": "Batch-SI: Semi-Parallel Concurrency Scheme for Permissioned Blockchains Exploiting Batch Snapshot Isolation", "reproduction_dossier": "docs/reproductions/batch_si/source_lock.md"},
    ),
    _manifest(
        "block_executor", "serial_block_executor", "Serial Block Executor",
        "Executes block transactions in original order through the generic block executor contract.",
        config={"worker_count": 1},
        schema=_schema({"worker_count": {"type": "integer", "minimum": 1, "maximum": 1, "default": 1, "readOnly": True}}),
        capabilities=["serial_order", "deterministic_state_delta", "legacy_equivalence_oracle", "execution_plan_digest"],
        metrics=[
            {"key": "block_executor_id", "type": "string", "unit": "", "aggregation": "last", "visualization": "summary", "description": "Selected block executor plugin."},
            {"key": "block_execution_ms", "type": "number", "unit": "ms", "aggregation": "sum", "visualization": "summary", "description": "Block execution wall-clock time."},
            {"key": "executed_transaction_count", "type": "integer", "unit": "tx", "aggregation": "sum", "visualization": "summary", "description": "Transactions executed by the block executor."},
            {"key": "execution_plan_digest", "type": "string", "unit": "", "aggregation": "last", "visualization": "summary", "description": "Deterministic execution plan digest."},
            {"key": "plan_digest_consistent", "type": "ratio", "unit": "boolean", "aggregation": "all", "visualization": "summary", "description": "Committed nodes agree on plan digest evidence."},
        ],
        supported_backends=["real_cluster"],
        truth_boundary="legacy_faithful_reference_baseline",
        source={"source_type": "internal_reference", "source_name": "MBE legacy realism serial execution engine", "source_commit": "84f28f43f2cfe93864fd4e4b68377fcfd31a5595", "reproduction_dossier": "docs/reproductions/serial/source_lock.md"},
    ),
    _manifest(
        "block_executor", "metatrack_block_executor", "MetaTrack Block Executor",
        "Executes MetaTrack fast/conservative ready queues with dependency and transaction-level remote StateReady suspend/resume callbacks over MBE transfer semantics.",
        config={"worker_count": 4},
        schema=_schema({"worker_count": {"type": "integer", "minimum": 1, "maximum": 8, "default": 4}}),
        capabilities=["metatrack_ready_queues", "dependency_release", "transaction_level_remote_state_ready", "state_wait_suspend_resume", "deterministic_state_delta", "execution_plan_digest"],
        requirements=["execution:dual_track_execution", "scheduler:fast_first_scheduler"],
        metrics=[
            {"key": "fast_track_execution_instance_count", "type": "integer", "unit": "tx", "aggregation": "sum", "visualization": "summary", "description": "Fast-track execution instances completed by MetaTrack."},
            {"key": "blocked_queue_max_depth", "type": "integer", "unit": "tx", "aggregation": "max", "visualization": "summary", "description": "Maximum planned/actual dependency queue depth."},
            {"key": "wakeup_count", "type": "integer", "unit": "event", "aggregation": "sum", "visualization": "summary", "description": "Dependency releases that moved transactions into ready queues."},
            {"key": "state_wait_blocked_count", "type": "integer", "unit": "event", "aggregation": "sum", "visualization": "summary", "description": "Transactions suspended because one or more required remote state keys were not StateReady."},
            {"key": "state_ready_wakeup_count", "type": "integer", "unit": "event", "aggregation": "sum", "visualization": "summary", "description": "StateReady arrivals that woke a suspended transaction."},
            {"key": "remote_state_fetch_completed_count", "type": "integer", "unit": "fetch", "aggregation": "sum", "visualization": "summary", "description": "Remote state fetches completed by transaction-level StateReady scheduling."},
        ],
        supported_backends=["real_cluster"],
        truth_boundary="metatrack_native_transaction_level_state_ready_suspend_resume",
    ),
    _manifest(
        "block_executor", "aria_block_executor", "Aria Block Executor",
        "Validates a fixed one-epoch Aria batch over the block-start snapshot using minimum-TID read/write reservations, Rule 2 deterministic reordering, and no serial or Calvin fallback.",
        config={
            "worker_count": 4,
            "reordering": True,
            "read_only_optimization": True,
            "retry_nonce_gaps": True,
        },
        schema=_schema({
            "worker_count": {"type": "integer", "minimum": 1, "maximum": 8, "default": 4},
            "reordering": {"type": "boolean", "default": True},
            "read_only_optimization": {"type": "boolean", "default": True},
            "retry_nonce_gaps": {"type": "boolean", "default": True},
        }),
        capabilities=[
            "aria", "deterministic_batch_occ", "same_snapshot_epoch",
            "read_write_reservation", "deterministic_reordering_rule2",
            "single_epoch_fixed_block_validation", "execution_plan_digest",
        ],
        requirements=["block_producer:aria_block_producer", "routing:hash_routing_baseline", "execution:serial_execution_baseline", "scheduler:fifo_serial_scheduler", "state_access:direct_state_access", "state_storage:persistent_local_state_store", "commit:normal_commit"],
        metrics=[
            {"key": "aria_epoch_count", "type": "integer", "unit": "epoch", "aggregation": "sum", "visualization": "summary", "description": "Aria execution/commit epochs completed; fixed blocks must report exactly one."},
            {"key": "aria_conflict_abort_count", "type": "integer", "unit": "tx", "aggregation": "sum", "visualization": "summary", "description": "Conflict attempts rejected during fixed-block validation; expected to be zero."},
            {"key": "aria_candidate_transaction_count", "type": "integer", "unit": "tx", "aggregation": "sum", "visualization": "summary", "description": "Transactions inspected by the proposer-side Aria batch selection."},
            {"key": "aria_selected_transaction_count", "type": "integer", "unit": "tx", "aggregation": "sum", "visualization": "summary", "description": "Rule-2-committable transactions selected into the consensus block."},
            {"key": "aria_deferred_transaction_count", "type": "integer", "unit": "tx", "aggregation": "sum", "visualization": "summary", "description": "Transactions retained in FIFO mempool order for a later block."},
            {"key": "maximum_parallel_width", "type": "integer", "unit": "tx", "aggregation": "max", "visualization": "summary", "description": "Maximum observed concurrent Aria execution width."},
        ],
        supported_backends=["real_cluster"],
        truth_boundary="aria_rule2_one_consensus_block_per_batch_fallback_disabled",
        source={
            "source_type": "paper_and_official_source_reimplementation",
            "source_name": "Aria: A Fast and Practical Deterministic OLTP Database",
            "doi": "10.14778/3407790.3407808",
            "source_repository": "https://github.com/luyi0619/aria",
            "source_commit": "d0508c393ec084582c12e6f3abadab63501eaedd",
            "reproduction_dossier": "docs/reproductions/aria/source_lock.md",
        },
    ),
    _manifest(
        "block_executor", "groundhog_block_executor", "Groundhog Block Executor",
        "Reimplements Groundhog unordered-block execution over MBE transaction semantics using one block-start snapshot, typed commutative modifications, reserve/rollback validation, deterministic materialization, and no serial fallback.",
        config={"worker_count": 4, "ordered_set_limit": 64},
        schema=_schema({
            "worker_count": {"type": "integer", "minimum": 1, "maximum": 8, "default": 4},
            "ordered_set_limit": {"type": "integer", "minimum": 1, "maximum": 65535, "default": 64},
        }),
        capabilities=["groundhog", "unordered_block", "block_start_snapshot", "typed_commutative_modifications", "nonnegative_int64_set_add", "bytes_set", "ordered_set", "concurrent_reserve_commit_rollback", "transactional_reservation_rewind", "deterministic_materialization", "no_serial_fallback", "execution_plan_digest"],
        requirements=["block_producer:groundhog_block_producer", "routing:hash_routing_baseline", "execution:serial_execution_baseline", "scheduler:fifo_serial_scheduler", "state_access:direct_state_access", "state_storage:persistent_local_state_store", "commit:normal_commit"],
        metrics=[
            {"key": "groundhog_execution_attempt_count", "type": "integer", "unit": "tx", "aggregation": "sum", "visualization": "summary", "description": "Groundhog transaction interpretations against the common block snapshot."},
            {"key": "groundhog_reservation_count", "type": "integer", "unit": "modification", "aggregation": "sum", "visualization": "summary", "description": "Typed modifications successfully reserved."},
            {"key": "groundhog_constraint_conflict_count", "type": "integer", "unit": "conflict", "aggregation": "sum", "visualization": "summary", "description": "Typed-state constraints that rejected a transaction or fixed block."},
            {"key": "groundhog_reservation_rollback_count", "type": "integer", "unit": "rollback", "aggregation": "sum", "visualization": "summary", "description": "Transaction-level reservation rollbacks."},
            {"key": "groundhog_integer_merge_count", "type": "integer", "unit": "modification", "aggregation": "sum", "visualization": "summary", "description": "Nonnegative integer set-add modifications merged."},
            {"key": "groundhog_bytes_merge_count", "type": "integer", "unit": "modification", "aggregation": "sum", "visualization": "summary", "description": "Byte-string set modifications merged."},
            {"key": "groundhog_ordered_set_merge_count", "type": "integer", "unit": "modification", "aggregation": "sum", "visualization": "summary", "description": "Ordered-set insert, clear, and limit modifications merged."},
            {"key": "groundhog_modified_key_count", "type": "integer", "unit": "key", "aggregation": "sum", "visualization": "summary", "description": "Distinct state keys materialized by Groundhog."},
            {"key": "groundhog_reservation_parallel_width", "type": "integer", "unit": "tx", "aggregation": "max", "visualization": "summary", "description": "Maximum concurrent Groundhog reserve/revert/commit width over disjoint object reservations."},
            {"key": "maximum_parallel_width", "type": "integer", "unit": "tx", "aggregation": "max", "visualization": "summary", "description": "Maximum observed concurrent Groundhog interpretation width."},
        ],
        supported_backends=["real_cluster"],
        truth_boundary="groundhog_supported_typed_state_semantics_with_concurrent_transactional_reserve_revert_commit;official_cpp_storage_engine_not_embedded",
        source={"source_type": "paper_and_official_source_reimplementation", "source_name": "Groundhog: Linearly-Scalable Smart Contracting via Commutative Transaction Semantics", "arxiv": "2404.03201", "source_repository": "https://github.com/scslab/smart-contract-scalability", "source_commit": "6b357bc206b73ece39fd61fe7dba655352200c0a", "reproduction_dossier": "docs/reproductions/groundhog/source_lock.md"},
    ),
    _manifest(
        "block_executor", "block_stm_block_executor", "Block-STM Block Executor",
        "Reimplements the Block-STM speculative block execution mechanism over MBE transfer semantics with MVMemory, validation, abort, and deterministic ordered materialization.",
        config={"worker_count": 4, "maximum_incarnations": 0},
        schema=_schema({
            "worker_count": {"type": "integer", "minimum": 1, "maximum": 8, "default": 4},
            "maximum_incarnations": {"type": "integer", "minimum": 0, "maximum": 1000000, "default": 0, "description": "0 disables the artificial per-transaction incarnation cap for formal experiments."},
        }),
        capabilities=["block_stm", "mvmemory", "speculative_execution", "captured_reads", "validation", "abort_reexecution", "estimate_dependency", "serial_equivalence"],
        requirements=["state_access:direct_state_access", "state_storage:persistent_local_state_store"],
        metrics=[
            {"key": "worker_count", "type": "integer", "unit": "worker", "aggregation": "last", "visualization": "summary", "description": "Configured Block-STM workers."},
            {"key": "maximum_parallel_width", "type": "integer", "unit": "tx", "aggregation": "max", "visualization": "summary", "description": "Maximum observed Block-STM parallel width."},
            {"key": "abort_count", "type": "integer", "unit": "tx", "aggregation": "sum", "visualization": "summary", "description": "Validation abort count."},
            {"key": "reexecution_count", "type": "integer", "unit": "tx", "aggregation": "sum", "visualization": "summary", "description": "Transactions re-executed after abort."},
            {"key": "dependency_wait_count", "type": "integer", "unit": "wait", "aggregation": "sum", "visualization": "summary", "description": "ESTIMATE/dependency waits."},
            {"key": "validation_failure_count", "type": "integer", "unit": "failure", "aggregation": "sum", "visualization": "summary", "description": "Read validation failures."},
        ],
        supported_backends=["real_cluster"],
        truth_boundary="block_stm_core_reimplementation_over_mbe_transaction_semantics",
        source={"source_type": "paper_and_source_reimplementation", "source_name": "Block-STM: Scaling Blockchain Execution by Turning Ordering Curse to a Performance Blessing", "arxiv": "2203.06871v3", "source_repository": "https://github.com/aptos-labs/aptos-core", "source_commit": "20f9379515358add43f4042693462aaedd654826", "reproduction_dossier": "docs/reproductions/block_stm/source_lock.md"},
    ),
    _manifest(
        "block_executor", "batch_si_block_executor", "Batch-SI Block Executor",
        "Executes the committed Batch-SI plan with sequential batches, one immutable snapshot per batch, parallel transactions inside a batch, and deterministic state materialization using Batch-SI-owned transaction semantics.",
        config={"worker_count": 4, "partition_mode": "wrbp", "ordering_mode": "ofas", "priority_mode": "paper", "execution_mode": "snapshot_parallel"},
        schema=_schema({
            "worker_count": {"type": "integer", "minimum": 1, "maximum": 8, "default": 4},
            "partition_mode": {"type": "string", "enum": ["wrbp", "sequential"], "default": "wrbp"},
            "ordering_mode": {"type": "string", "enum": ["ofas", "dependency_graph"], "default": "ofas"},
            "priority_mode": {"type": "string", "enum": ["paper", "txid"], "default": "paper"},
            "execution_mode": {"type": "string", "enum": ["snapshot_parallel", "snapshot_serial"], "default": "snapshot_parallel"},
        }),
        capabilities=["batch_si", "batch_snapshot_isolation", "sequential_batches", "intra_batch_parallelism", "deterministic_state_delta", "execution_plan_digest", "no_cross_scheme_algorithm_reuse"],
        requirements=["scheduler:batch_si_scheduler", "routing:hash_routing_baseline", "execution:batch_si_execution", "state_access:direct_state_access", "state_storage:persistent_local_state_store", "commit:normal_commit"],
        metrics=[
            {"key": "configured_worker_count", "type": "integer", "unit": "worker", "aggregation": "last", "visualization": "summary", "description": "Configured Batch-SI intra-batch workers."},
            {"key": "maximum_parallel_width", "type": "integer", "unit": "tx", "aggregation": "max", "visualization": "summary", "description": "Maximum observed intra-batch parallel width."},
            {"key": "batch_count", "type": "integer", "unit": "batch", "aggregation": "sum", "visualization": "summary", "description": "WRBP execution batches."},
            {"key": "maximum_batch_width", "type": "integer", "unit": "tx", "aggregation": "max", "visualization": "summary", "description": "Largest Batch-SI batch width."},
            {"key": "write_opportunity_reuse_count", "type": "integer", "unit": "opportunity", "aggregation": "sum", "visualization": "summary", "description": "WRBP write-opportunity batch assignments reused."},
            {"key": "dependency_edge_count", "type": "integer", "unit": "dependency", "aggregation": "sum", "visualization": "summary", "description": "Intra-batch read/write dependency edges inspected."},
            {"key": "deferred_transaction_count", "type": "integer", "unit": "tx", "aggregation": "sum", "visualization": "summary", "description": "OFAS cycle victims returned to the mempool before PBFT."},
            {"key": "batch_snapshot_create_ms", "type": "number", "unit": "ms", "aggregation": "sum", "visualization": "summary", "description": "Time spent creating immutable batch snapshots."},
        ],
        supported_backends=["real_cluster"],
        truth_boundary="batch_si_core_reimplementation_over_mbe_transaction_and_state_contracts",
        source={"source_type": "paper_reimplementation", "source_name": "Batch-SI: Semi-Parallel Concurrency Scheme for Permissioned Blockchains Exploiting Batch Snapshot Isolation", "reproduction_dossier": "docs/reproductions/batch_si/source_lock.md"},
    ),
    _manifest("execution", "cg_execution", "CG Execution", "Full pairwise conflict-graph baseline classifier.", capabilities=["paper_baseline_cg", "full_pairwise_conflict_detection"], supported_backends=["real_cluster"], truth_boundary="cg_original_order_conflict_dag_v1", source={"source_type": "paper_reimplementation", "source_name": "Batch-SI CG comparison baseline; Piduguralla et al. Euro-Par 2023"}),
    _manifest("scheduler", "cg_scheduler", "CG Scheduler", "Constructs the full transaction conflict DAG using all pairwise RW/WR/WW checks.", capabilities=["consensus_bound_schedule", "full_pairwise_conflict_detection", "topological_waves"], requirements=["execution:cg_execution", "block_executor:cg_block_executor"], supported_backends=["real_cluster"], truth_boundary="cg_original_order_conflict_dag_v1"),
    _manifest("block_executor", "cg_block_executor", "CG Block Executor", "Executes each conflict-DAG frontier in parallel and advances frontiers sequentially.", config={"worker_count": 4}, schema=_schema({"worker_count": {"type": "integer", "minimum": 1, "maximum": 64, "default": 4}}), capabilities=["parallel_execution", "deterministic_materialization"], supported_backends=["real_cluster"], truth_boundary="cg_original_order_conflict_dag_v1"),
    _manifest("execution", "acg_execution", "ACG Execution", "Address-conflict-graph baseline classifier.", capabilities=["paper_baseline_acg", "address_indexed_conflict_detection"], supported_backends=["real_cluster"], truth_boundary="nezha_acg_hs_paper_description_v1", source={"source_type": "paper_description_reimplementation", "source_name": "Nezha: Exploiting Concurrency for Transaction Processing in DAG-based Blockchains", "doi": "10.1109/ICDCS54860.2022.00034"}),
    _manifest("scheduler", "acg_scheduler", "ACG Scheduler", "Builds original-order conflict precedence through address-indexed access frontiers and emits hierarchical waves.", capabilities=["consensus_bound_schedule", "address_indexed_conflict_detection", "hierarchical_waves"], requirements=["execution:acg_execution", "block_executor:acg_block_executor"], supported_backends=["real_cluster"], truth_boundary="nezha_acg_hs_paper_description_v1"),
    _manifest("block_executor", "acg_block_executor", "ACG Block Executor", "Executes address-conflict hierarchy waves in parallel.", config={"worker_count": 4}, schema=_schema({"worker_count": {"type": "integer", "minimum": 1, "maximum": 64, "default": 4}}), capabilities=["parallel_execution", "deterministic_materialization"], supported_backends=["real_cluster"], truth_boundary="nezha_acg_hs_paper_description_v1"),
    _manifest("execution", "bsx_execution", "BSX Execution", "Batch-Schedule-Execute conflict-graph coloring classifier.", capabilities=["paper_baseline_bsx", "undirected_conflict_graph"], supported_backends=["real_cluster"], truth_boundary="bsx_homogeneous_conflict_graph_coloring_dsatur_v1", source={"source_type": "paper_reimplementation", "source_name": "Batch-Schedule-Execute: On Optimizing Concurrent Deterministic Scheduling for Blockchains", "doi": "10.1109/SRDS64841.2024.00025", "arxiv": "2402.05535"}),
    _manifest("scheduler", "bsx_scheduler", "BSX Scheduler", "Builds an undirected conflict graph and applies deterministic DSATUR coloring for the homogeneous-transaction scheduling case.", capabilities=["consensus_bound_schedule", "undirected_conflict_graph", "deterministic_dsatur_coloring"], requirements=["execution:bsx_execution", "block_executor:bsx_block_executor"], supported_backends=["real_cluster"], truth_boundary="bsx_homogeneous_conflict_graph_coloring_dsatur_v1"),
    _manifest("block_executor", "bsx_block_executor", "BSX Block Executor", "Executes one independent color class in parallel before advancing to the next color.", config={"worker_count": 4}, schema=_schema({"worker_count": {"type": "integer", "minimum": 1, "maximum": 64, "default": 4}}), capabilities=["parallel_execution", "deterministic_materialization"], supported_backends=["real_cluster"], truth_boundary="bsx_homogeneous_conflict_graph_coloring_dsatur_v1"),
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
