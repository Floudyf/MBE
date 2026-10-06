// MBE_V5_RESULTS_UI_TRUTH_CN_FINAL_20260814_V5
import { useMemo, useState } from "react";
import type { V5FormalChildRun } from "../../api";
import V5MetricHelp from "./V5MetricHelp";

type MetricDef = { key: string; label: string; unit?: string; help: string };

const COMMON: MetricDef[] = [
  { key: "worker_count", label: "工作线程数", help: "该方法真实执行路径记录的 工作线程数；Serial 通常为 1。" },
  { key: "maximum_parallel_width", label: "最大并行宽度", help: "运行期/计划证据中观察到的最大同时并行交易宽度。" },
  { key: "block_execution_ms", label: "区块执行总耗时", unit: "ms", help: "Leader 区块执行墙钟时间包络累计。" },
  { key: "transaction_execution_ms", label: "交易执行耗时", unit: "ms", help: "统一算法执行时间；Porygon 使用逐区块 replica 最大关键路径后跨区块求和，其他方法保留各自正式执行阶段口径。" },
  { key: "actual_block_fill_ratio", label: "实际区块填充率", help: "实际平均已提交交易数 ÷ 配置 block_size，用于审查不同方法的区块利用率差异。" },
  { key: "deterministic_materialization_ms", label: "确定性物化耗时", unit: "ms", help: "确定性物化/应用阶段累计时间。" },
  { key: "state_commitment_ms", label: "状态承诺耗时", unit: "ms", help: "状态承诺 / 状态根更新阶段累计时间。" },
];

const VERSIONED_STATE_READY: MetricDef[] = [
  { key: "versioned_state_ready_wave_count", label: "版本就绪 Wave 数", help: "exact-version StateReady 前沿实际形成的可执行 Wave 数。" },
  { key: "versioned_state_ready_wait_observation_count", label: "版本等待观测次数", help: "外层版本前沿观察到 required version 尚未满足的次数。" },
  { key: "versioned_state_ready_resolved_token_count", label: "已解析版本令牌数", help: "exact-version StateReady 成功解析的版本令牌数。" },
  { key: "versioned_state_probe_count", label: "版本探测次数", help: "exact-version StateReady 发起的本地/远程版本探测次数。" },
  { key: "versioned_state_probe_latency_ms", label: "版本探测累计延迟", unit: "ms", help: "版本探测累计观测延迟；这是事件累计量，不与区块墙钟耗时直接相加。" },
  { key: "versioned_state_ready_max_wave_width", label: "最大版本就绪 Wave 宽度", help: "单个 exact-version ready Wave 的最大交易宽度。" },
  { key: "versioned_state_ready_execution_ms", label: "版本前沿 / StateReady 包络", unit: "ms", help: "exact-version StateReady 从开始到完成的真实墙钟包络，包含版本探测、等待、Wave 推进和内层执行。" },
];

// MBE_METATRACK_PROJECTION_FRONTIER_V612
const METATRACK_PROJECTION_FRONTIER_V612: MetricDef[] = [
  { key: "metatrack_projection_frontier_projection_count", label: "当前就绪投影前沿数", help: "新版 MetaTrack v6.5.6 保留耐久提交后即时唤醒；RouteBatch 仅作为共现/路由计算窗口，不再作为最新版共识原子边界。每笔交易签名绑定执行前驱、只排序前驱和原 RouteBatch 内自然执行深度，PBFT 前由所有验证者重算闭包后决定是否可进入当前交易前沿。" },
  { key: "metatrack_projection_frontier_layer_count", label: "投影前沿层数", help: "v6.5.6 直接按签名交易前驱形成共识前沿：RAW / exact-version / nonce 是执行屏障，WAW / WAR 只约束跨块顺序；允许跨 RouteBatch 聚合同一时刻独立的交易，但块内执行深度不得超过各交易签名的原批次自然深度。" },
  { key: "metatrack_projection_frontier_max_layer_projection_width", label: "最大同层投影宽度", help: "当前签名依赖图中可同时进入一个 PBFT 单元的独立交易前沿宽度；唯一容量限制仍是已有 block_size，不再由 micro_batch_size=100 人为切断共识聚合，也不增加经验宽度/深度阈值。" },
  { key: "metatrack_projection_frontier_cross_projection_edge_count", label: "跨投影精确版本依赖边", help: "共识块内部真实跨完整投影的 exact-version 读取依赖边数。" },
  { key: "metatrack_projection_frontier_suppressed_remote_dead_intermediate_publish_count", label: "省掉的远端死中间版本发布", help: "直接依据 v6.0.3 已签名并验证的 dead_intermediate 分类，省掉的远端 Home 中间版本发布次数。" },
  { key: "metatrack_version_dead_intermediate_count", label: "死中间版本数", help: "版本存活分析判定为没有本地/远端值或排序后继的 produced version 数。" },
  { key: "metatrack_version_home_writeback_elided_count", label: "Home 写回省略数", help: "版本存活机制已经安全省略的 Home exact-version 写回数量。" },
  { key: "metatrack_closure_boundary_immediate_publish_count", label: "闭包边界立即发布数", help: "依赖闭包模式为了跨投影 StateReady 活性而立即发布的最终持久版本数量。" },
];

// MBE_METATRACK_LIVENESS_SAFE_BOUNDARY_BATCH_V621
const METATRACK_BOUNDARY_BATCH_V621: MetricDef[] = [
  { key: "metatrack_closure_boundary_batch_request_group_count", label: "闭包边界批量发布组数", help: "依赖闭包模式中，在生产者完成这一原有可见时点，把同一 Home 的 exact-version 发布合并成批量 APPLY；不延迟版本可见性。" },
  { key: "metatrack_closure_boundary_batch_item_count", label: "闭包边界批量发布项数", help: "闭包边界批量 APPLY 实际携带的 remote-live / final-persistent exact-version 项数。" },
];

// MBE_METATRACK_ASYNC_VERSION_WRITEBACK_V640
const METATRACK_ASYNC_WRITEBACK_V640: MetricDef[] = [
  { key: "metatrack_implementation_revision", label: "MetaTrack 实现修订", help: "v6.4.0 恢复共享交易级 Full Locality；PBFT 仍使用依赖闭包，不使用投影级执行屏障。" },
  { key: "metatrack_block_remote_value_consumer_edge_count", label: "块内跨执行片精确版本读取边", help: "在当前 dependency-ready PBFT 单元中，确实需要跨执行片读取 produced-version→RequiredVersion 的边数；该关系决定哪些 final-persistent 仍必须即时发布。" },
  { key: "metatrack_block_consumer_critical_final_publish_count", label: "块内消费者关键最终版本数", help: "签名为 final-persistent、但当前依赖就绪 PBFT 单元中存在跨执行片精确值读取者，因此保留生产者完成时即时发布的版本数。" },
  { key: "metatrack_background_final_enqueue_count", label: "后台最终版本写回数", help: "当前聚合块中没有跨执行片精确值读取者的 final-persistent 版本；生产者完成后进入后台耐久写回，不阻塞交易级调度。" },
  { key: "metatrack_async_version_writeback_batch_count", label: "后台写回批次数", help: "后台最终版本按 Home 自然汇聚后实际发送的批次数；无定时等待、无固定批大小。" },
  { key: "metatrack_async_version_writeback_item_count", label: "后台写回版本项数", help: "后台批次实际携带的 final-persistent exact-version 项数。" },
  { key: "metatrack_async_version_writeback_max_queue_depth", label: "后台写回最大排队深度", help: "Leader 观测到的后台最终版本队列最大深度，用于判断耐久写回能否跟上交易执行。" },
  { key: "metatrack_critical_version_publish_wait_ms", label: "关键版本即时发布等待", unit: "ms", help: "remote-live 和块内跨执行片消费者所需 final-persistent 继续走即时发布路径时，实际等待 Home ACK 的累计墙钟时间。" },
  { key: "metatrack_background_final_writeback_ms", label: "后台最终写回工作时间", unit: "ms", help: "后台 final-persistent 批量写回累计网络工作时间；它与交易执行重叠，不直接等同于额外墙钟时间。" },
  { key: "metatrack_final_join_wait_ms", label: "提交前最终等待", unit: "ms", help: "v6.5.6 将后台最终版本 join 后移到真正 durable 屏障前，使本地 commit 规划、状态 Apply/WAL 与远端最终写回重叠；该指标现在表示到 durable 边界仍未被隐藏的残余等待。" },
];

// MBE_METATRACK_CRITICAL_PATH_BALANCED_ROUTING_V651
const METATRACK_INCREMENTAL_ROUTING_V650: MetricDef[] = [
  { key: "metatrack_incremental_routing_policy", label: "持续分流策略", help: "最新版 MetaTrack 跨 RouteBatch 持续维护前驱与共现历史，但不再让 exact-version 连续性绝对优先；候选执行域按预计就绪层级、远程代价、共现局部性和负载做无阈值序位平衡。" },
  { key: "metatrack_incremental_execution_shard_capacity", label: "执行分片容量", help: "由本次实验声明的总交易数与执行分片数直接推导的确定性容量约束，不是经验阈值。" },
  { key: "metatrack_incremental_exact_state_edge_count", label: "精确版本状态边数", help: "规划时实际找到当前运行中生产者的 RequiredVersion 状态边总数。" },
  { key: "metatrack_incremental_exact_cross_shard_state_edge_count", label: "跨执行片精确版本状态边", help: "规划后生产者与后继位于不同执行分片的 exact-version 状态边；这是本轮优化直接压缩的核心量。" },
  { key: "metatrack_incremental_exact_cross_shard_state_edge_rate", label: "跨执行片精确版本率", help: "跨执行片 exact-version 状态边 ÷ exact-version 状态边总数；v6.5.1 不再以最小化该比例作为绝对第一优先级。" },
  { key: "metatrack_incremental_exact_predecessor_edge_count", label: "精确版本前驱边数", help: "按前驱交易去重后的真实 exact-version 前驱→后继边数。" },
  { key: "metatrack_incremental_exact_cross_shard_predecessor_count", label: "跨执行片前驱边", help: "按前驱交易去重后仍跨执行分片的 exact-version 边数。" },
  { key: "metatrack_incremental_coaccess_pair_update_count", label: "增量共现更新数", help: "逐交易更新状态对共现历史的次数；历史跨 RouteBatch 持续保留。" },
  { key: "metatrack_incremental_capacity_forced_choice_count", label: "容量约束改选数", help: "无容量约束时的序位平衡最优分片已达到由实验规模推导的容量，因此改选其它可容纳分片的交易数。" },
  { key: "metatrack_incremental_routing_plan_total_us", label: "持续分流总规划时间", unit: "μs", help: "客户端唯一规划器在所有 RouteBatch 上执行持续分流的累计墙钟时间；不进入签名计划摘要。" },
  { key: "metatrack_incremental_routing_plan_mean_us", label: "单 RouteBatch 平均规划时间", unit: "μs", help: "持续分流 PlanBatch 调用的平均墙钟时间。" },
  { key: "metatrack_incremental_routing_plan_max_us", label: "单 RouteBatch 最大规划时间", unit: "μs", help: "持续分流单次 PlanBatch 的最大墙钟时间，用来确认分析成本没有成为新瓶颈。" },
  { key: "metatrack_incremental_batch_partition_invariant", label: "RouteBatch 边界无关", help: "为真表示该版本合同要求 RouteBatch 只承担签名/存活性打包作用，不重置持续分流状态。" },
];

const METATRACK_NATURAL_WINDOW_V657: MetricDef[] = [
  { key: "metatrack_natural_window_fixed_micro_batch_enabled", label: "固定100笔路由批次", help: "正式 Full 与四个正式消融统一保持 micro_batch_size=100；这是前序规模敏感性实验确定并固定的实验工作点，不作为算法阈值参与 N/L 判定。" },
  { key: "metatrack_natural_window_block_size", label: "复用块容量", help: "直接复用本次实验已有 block_size，不引入新的 MetaTrack 容量阈值。" },
  { key: "metatrack_natural_window_block_interval_ms", label: "复用出块间隔", unit: "ms", help: "直接复用本次实验已有 block_interval_ms；固定速率回放按确定性的逻辑释放时间形成自然时间边界。" },
  { key: "metatrack_natural_window_window_count", label: "自然路由窗口数", help: "本次实验最终形成的自然 RouteBatch/封签窗口数量。" },
  { key: "metatrack_natural_window_time_boundary_count", label: "出块周期切窗次数", help: "固定速率回放中因跨越既有出块周期而封闭窗口的次数。" },
  { key: "metatrack_natural_window_capacity_boundary_count", label: "块容量切窗次数", help: "因达到既有 block_size 而封闭窗口的次数。" },
  { key: "metatrack_natural_window_min_window_size", label: "最小自然窗口交易数", help: "自然路由窗口的最小交易数。" },
  { key: "metatrack_natural_window_average_window_size", label: "平均自然窗口交易数", help: "自然路由窗口的平均交易数。" },
  { key: "metatrack_natural_window_max_window_size", label: "最大自然窗口交易数", help: "自然路由窗口的最大交易数。" },
];

const METATRACK_PARTITION_INVARIANT_V658: MetricDef[] = [
  { key: "metatrack_partition_invariant_routing_mode", label: "分流模式", help: "实验版逐交易增量分流，但持续复用跨交易共现、exact-version 前驱和负载历史；不存在固定分析批大小。" },
  { key: "metatrack_partition_invariant_route_batch_semantic_role", label: "RouteBatch 角色", help: "实验版 RouteBatch 仅保留为签名/证据包装，不再决定共识选择边界。" },
  { key: "metatrack_partition_invariant_consensus_selection", label: "共识候选规则", help: "节点从当前已到达交易中，在现有 block_size 容量内选择最大依赖闭合前沿；不使用 N/L、固定笔数或 worker 带宽阈值。" },
  { key: "metatrack_partition_invariant_version_liveness_mode", label: "版本存活模式", help: "实验版暂时关闭批内 Version Liveness 裁剪，采用保守版本发布，避免把当前工程批内没人读误判为未来永远没人读。" },
  { key: "metatrack_partition_invariant_routing_unit_size", label: "流式分流单位", help: "实验版为 1：每笔交易到达后立即完成增量分流与签名，算法历史跨调用持续保存。" },
  { key: "metatrack_partition_invariant_block_size", label: "物理块容量上限", help: "实际 PBFT 块仍受原有 block_size 硬上限约束，不会因取消分析批而形成无限大块。" },
];

const METATRACK_CONSENSUS_WINDOW_V656813: MetricDef[] = [
  { key: "metatrack_consensus_window_count", label: "共识批次聚合数", help: "从持久提交区块中的签名窗口元数据重建得到的共识批次聚合数量。" },
  { key: "metatrack_consensus_window_average_tx_count", label: "平均交易/窗口", help: "每个全局共识批次聚合包含的平均逻辑交易数。" },
  { key: "metatrack_consensus_window_average_route_batch_count", label: "平均 RouteBatch/窗口", help: "每个共识批次聚合聚合的连续 RouteBatch 平均数量。" },
  { key: "metatrack_consensus_window_max_route_batch_count", label: "最大 RouteBatch/窗口", help: "单个共识批次聚合聚合的最大 RouteBatch 数。" },
  { key: "metatrack_consensus_window_average_critical_path", label: "平均最长依赖链", help: "根据持久化签名前驱元数据重新计算的平均最长执行链 L。" },
  { key: "metatrack_consensus_window_max_critical_path", label: "最大最长依赖链", help: "所有窗口中重新计算得到的最大最长执行链 L。" },
  { key: "metatrack_consensus_window_average_structural_width", label: "平均结构并行宽度 N/L", help: "窗口交易数 N 与重建最长执行链 L 的比值，仅做运行后观测。" },
  { key: "metatrack_consensus_window_critical_width_stop_count", label: "严格V661关键路径切窗次数", help: "仅用于严格临界路径机制对照：加入下一 RouteBatch 会延长已有最大执行关键路径，因此按历史 V661 规则收窗。正式 Full 使用依赖闭包 + N/L。" },
  { key: "metatrack_consensus_window_n_over_l_stop_count", label: "N/L 不再提高切窗次数", help: "正式 Full 重建确认：候选依赖闭包安全且容量允许，但加入下一完整 RouteBatch 后 N/L 不再严格提高，因此在该边界收窗。" },
  { key: "metatrack_consensus_window_block_size_stop_count", label: "block_size 切窗次数", help: "重建确认候选窗口会超过配置 block_size 硬上限的次数。" },
  { key: "metatrack_consensus_window_input_end_stop_count", label: "输入结束收窗次数", help: "没有下一个 RouteBatch、由输入结束形成的最后窗口数量。" },
  { key: "metatrack_consensus_window_pbft_blocks_saved", label: "相对单 RouteBatch 省掉的 PBFT 块", help: "按每个 RouteBatch 实际活跃分片重建的基准块数，减去窗口聚合后的实际分片窗口块数。" },
  { key: "metatrack_consensus_window_aggregation_active", label: "去掉共识聚合处理变量已激活", help: "正式 Full 实际聚合至少两个完整 RouteBatch 且确实减少 PBFT 块；只有为真且去掉共识聚合保持单 RouteBatch 窗口时，其 TPS 差才可归因于去掉共识聚合。" },
  { key: "metatrack_consensus_window_signed_reconstruction_match", label: "签名窗口重建一致", help: "窗口签名元数据、实际分片投影、重算关键路径和 N/L 边界规则是否全部一致。" },
];

const NATIVE_STATE_READY: MetricDef[] = [
  { key: "state_ready_wait_count", label: "StateReady 等待交易数", help: "MetaTrack 原生 suspend/resume 路径进入 StateReady 等待的逻辑交易计数。" },
  { key: "state_ready_resume_count", label: "StateReady 恢复交易数", help: "状态/版本就绪后恢复执行的逻辑交易计数。" },
  { key: "state_prefetch_wait_ms", label: "StateReady 累计等待时间", unit: "ms", help: "逐交易等待时间的累计量，可大于区块墙钟时间，不用于堆叠阶段图。" },
  { key: "scheduler_blocked_count", label: "调度阻塞事件数", help: "双轨调度器记录的阻塞事件数。" },
  { key: "scheduler_wakeup_count", label: "调度唤醒事件数", help: "双轨调度器记录的唤醒事件数。" },
  { key: "metatrack_suspend_resume_execution_ms", label: "双轨 / StateReady 执行包络", unit: "ms", help: "MetaTrack 原生 suspend/resume 执行阶段的高精度墙钟包络。" },
];

const METHOD_METRICS: Array<{ match: (id: string) => boolean; title: string; metrics: MetricDef[] }> = [
  {
    match: (id) => id.includes("stateless_hash_serial"), title: "Stateless Hash + Serial",
    metrics: [
      { key: "stateless_version_admission_candidate_mean_tx_count", label: "准入候选平均大小", help: "进入 exact-version 共识前准入检查的平均候选交易数。" },
      { key: "stateless_version_admission_mean_frontier_width", label: "可执行版本前沿平均宽度", help: "每次候选中实际能安全进入 PBFT 的平均交易数。" },
      { key: "stateless_version_admission_mean_nonempty_frontier_width", label: "非空可执行前沿平均宽度", help: "只统计实际形成 PBFT 提案的候选事件，admitted 交易数 ÷ 非空前沿事件数；可直接解释实际小区块规模。" },
      { key: "stateless_version_admission_zero_frontier_rate", label: "零前沿候选比例", help: "候选检查时没有任何交易可安全进入 PBFT 的事件比例。" },
      { key: "stateless_version_admission_admission_ratio", label: "版本准入率", help: "累计 admitted 交易出现次数 ÷ candidate 交易出现次数。" },
      { key: "stateless_version_admission_external_not_ready_ratio", label: "外部精确版本未就绪比例", help: "候选中外部 exact-version token 未 materialize 的比例。" },
      { key: "stateless_version_admission_deferred_direct_external_not_ready_count", label: "外部版本直接阻塞次数", help: "因外部 required version 未就绪而直接延期的交易出现次数。" },
      { key: "stateless_version_admission_deferred_internal_propagation_count", label: "块内依赖传播阻塞次数", help: "自身无直接外部缺失，但其候选内 producer 尚未可执行而被传播阻塞的交易出现次数。" },
    ],
  },
  {
    match: (id) => id.includes("block_stm"), title: "Block-STM",
    metrics: [
      { key: "abort_count", label: "中止事件数", help: "正式 Block-STM 中止事件计数：每个分片取 PBFT replica 最大值后跨分片求和；同一逻辑交易仍可能多次中止。" },
      { key: "block_stm_abort_events_per_tx", label: "每交易中止事件数", help: "副本去重后的中止事件数 ÷ submitted unique 逻辑交易数；同一交易可多次中止。" },
      { key: "block_stm_unique_aborted_tx_count", label: "发生过中止的唯一交易数", help: "至少经历过一次 dependency/validation abort 的唯一逻辑交易数；每分片 leader 区块证据去重后求和。" },
      { key: "block_stm_unique_aborted_tx_rate", label: "唯一交易中止比例", help: "至少中止过一次的唯一交易数 ÷ submitted unique；适合与 MetaTrack Fast fallback 比较。" },
      { key: "reexecution_count", label: "重执行次数", help: "副本去重后的 Block-STM 重执行事件计数。" },
      { key: "block_stm_unique_reexecuted_tx_count", label: "发生过重执行的唯一交易数", help: "至少执行过 incarnation>0 的唯一逻辑交易数。" },
      { key: "block_stm_unique_reexecuted_tx_rate", label: "唯一交易重执行比例", help: "至少重执行过一次的唯一交易数 ÷ submitted unique。" },
      { key: "reexecution_events_per_tx", label: "每交易重执行次数", help: "副本去重后的重执行事件数 ÷ submitted unique 逻辑交易数。" },
      { key: "validation_failure_count", label: "验证失败次数", help: "副本去重后的 Block-STM 验证失败事件数。" },
      { key: "dependency_wait_count", label: "依赖等待次数", help: "副本去重后的依赖等待事件计数。" },
      { key: "block_stm_internal_version_dependency_delegated_count", label: "块内版本依赖委托数", help: "无状态 exact-version 层识别为同块内部依赖并交给 Block-STM 自行验证/重执行的依赖事件数；按每分片一个 leader 的区块证据去重汇总。" },
      { key: "block_stm_internal_version_dependencies_delegated_per_tx", label: "每交易块内版本依赖委托数", help: "块内版本依赖委托数 ÷ submitted unique 逻辑交易数。" },
      { key: "maximum_incarnation_observed", label: "最大执行版本号", help: "运行中观察到的最大执行版本号。" },
    ],
  },
  {
    match: (id) => id.includes("stateless_hash_block_stm"), title: "Stateless exact-version frontier",
    metrics: [
      { key: "stateless_version_admission_candidate_mean_tx_count", label: "准入候选平均大小", help: "进入 exact-version 共识前准入检查的平均候选交易数。" },
      { key: "stateless_version_admission_mean_frontier_width", label: "可执行版本前沿平均宽度", help: "每次候选中实际能安全进入 PBFT 的平均交易数。" },
      { key: "stateless_version_admission_mean_nonempty_frontier_width", label: "非空可执行前沿平均宽度", help: "只统计实际形成 PBFT 提案的候选事件，admitted 交易数 ÷ 非空前沿事件数；可直接解释实际小区块规模。" },
      { key: "stateless_version_admission_zero_frontier_rate", label: "零前沿候选比例", help: "候选检查时没有任何交易可安全进入 PBFT 的事件比例。" },
      { key: "stateless_version_admission_admission_ratio", label: "版本准入率", help: "累计 admitted 交易出现次数 ÷ candidate 交易出现次数。" },
      { key: "stateless_version_admission_external_not_ready_ratio", label: "外部精确版本未就绪比例", help: "候选中外部 exact-version token 未 materialize 的比例。" },
      { key: "stateless_version_admission_deferred_direct_external_not_ready_count", label: "外部版本直接阻塞次数", help: "因外部 required version 未就绪而直接延期的交易出现次数。" },
      { key: "stateless_version_admission_deferred_internal_propagation_count", label: "块内依赖传播阻塞次数", help: "自身无直接外部缺失，但其候选内 producer 尚未可执行而被传播阻塞的交易出现次数。" },
    ],
  },
  {
    match: (id) => id === "hash_cg", title: "CG / Nezha",
    metrics: [
      { key: "cg_candidate_transaction_count", label: "候选出现次数", help: "所有 CG 规划尝试中的候选事务出现次数；Deferred 事务重试后会再次成为候选。" },
      { key: "cg_cycle_abort_decision_count", label: "周期延期决策次数", help: "JohnsonCE/BreakCycles 作出的 victim 延期决策总次数；这是尝试级证据，不是永久失败交易数。" },
      { key: "cg_cycle_attempt_abort_rate", label: "尝试级延期率", help: "周期延期决策次数 ÷ 候选出现次数。" },
      { key: "cg_cycle_deferred_retry_count", label: "Deferred 重试事件数", help: "cycle victim 被放回 FIFO mempool 等待后续区块的事件次数。" },
      { key: "cg_cycle_unique_deferred_tx_count", label: "唯一 Deferred 交易数", help: "至少经历过一次 CG Deferred 的唯一逻辑交易数。" },
      { key: "cg_cycle_unique_deferred_rate", label: "唯一 Deferred 比例", help: "唯一 Deferred 逻辑交易数 ÷ submitted unique。" },
      { key: "dependency_edge_count", label: "冲突弧数", help: "Nezha NewBuildConflictGraph 邻接多重图弧数。" },
      { key: "pairwise_conflict_check_count", label: "候选交易对数", help: "CG 候选交易对检查规模。" },
      { key: "cg_planning_worker_count", label: "规划线程数", help: "传统 CG 的构图 / Tarjan / JohnsonCE / BreakCycles 路径固定为 1。" },
      { key: "cg_execution_worker_count", label: "执行工作线程数", help: "Figure 13 扫描的真实执行 Worker 数（2/4/8/16/32）；不改变 CG 规划算法。" },
      { key: "cg_worker_truth_valid", label: "线程证据一致", help: "请求 Worker、执行器实际 Worker、规划 Worker=1 及观测并行宽度的交叉验证结果。" },
      { key: "literature_plan_parse_ms", label: "计划解析耗时", unit: "ms", help: "各提交区块执行器解析共识绑定 CG 计划的累计耗时。" },
      { key: "literature_plan_verify_ms", label: "计划验证耗时", unit: "ms", help: "各提交区块验证共识绑定 CG 计划的累计耗时。" },
      { key: "cg_validator_mode", label: "CG 验证模式", help: "运行时使用的 CG 计划验证模式。" },
      { key: "cg_johnson_cycle_budget", label: "Johnson cycle budget", help: "单次 SCC 精确 Johnson 枚举允许的 cycle occurrence 上限。" },
      { key: "cg_johnson_traversal_work_budget", label: "Johnson traversal budget", help: "单次 Johnson 调用的确定性 traversal work 上限。" },
      { key: "cg_johnson_plan_work_budget", label: "Johnson plan budget", help: "整个 CG 规划阶段的确定性 Johnson traversal work 上限。" },
      { key: "cg_cycle_space_policy", label: "周期空间策略", help: "精确 Johnson 与确定性 bounded fallback 的运行策略标识。" },
      { key: "cg_large_rmw_clique_policy", label: "大 RMW 团策略", help: "高冲突同键 RMW clique 的确定性规约策略。" },
      { key: "cg_large_rmw_clique_threshold", label: "大 RMW 团阈值", help: "触发大 RMW clique 规约的阈值。" },
      { key: "cg_cycle_retry_lifecycle", label: "周期 victim 生命周期", help: "应为 FIFO deferred-to-later-block；victim 不作为终态失败交易。" },
      { key: "cg_reference_commit_order_count", label: "参考提交序列累计长度", help: "各区块 Nezha BasicTopologicalSort 参考 commitOrder 的累计交易数。" },
      { key: "wave_count", label: "执行前沿数", help: "残余无环依赖图派生的 dependency-ready frontiers 数。" },
      { key: "maximum_wave_width", label: "最大执行前沿宽度", help: "可并发 ready-set 的最大宽度；实际并行度仍受执行 Worker 限制。" },
    ],
  },
  {
    match: (id) => id.includes("acg"), title: "ACG / Nezha",
    metrics: [
      { key: "nezha_hs_abort_decision_count", label: "HS 中止决策次数", help: "Nezha HS 在各次候选尝试中作出的中止决策总次数；同一逻辑交易可出现多次。" },
      { key: "nezha_hs_candidate_transaction_count", label: "候选出现次数", help: "所有 ACG first-pass 候选出现次数，包含后续重试再次成为候选的次数。" },
      { key: "nezha_hs_attempt_abort_rate", label: "HS 尝试中止率", help: "HS 中止决策次数 ÷ 候选出现次数，对应算法尝试层面的 abort 口径。" },
      { key: "nezha_hs_deferred_retry_count", label: "Deferred 重试次数", help: "HS victim 被放回 FIFO mempool 等待后续区块的事件次数。" },
      { key: "nezha_hs_unique_deferred_tx_count", label: "唯一 Deferred 交易数", help: "至少经历过一次 HS Deferred 的唯一逻辑交易数。" },
      { key: "nezha_hs_unique_deferred_rate", label: "唯一 Deferred 比例", help: "唯一 Deferred 交易数 ÷ submitted unique；与尝试中止率不是同一口径。" },
      { key: "dependency_edge_count", label: "依赖边数", help: "ACG 依赖边原始计数。" },
      { key: "dependency_edges_per_tx", label: "每交易依赖边数", help: "依赖边数 ÷ 已提交交易数。" },
      { key: "wave_count", label: "Wave 数", help: "ACG Wave 数。" },
      { key: "maximum_wave_width", label: "最大 Wave 宽度", help: "ACG 最大 Wave 宽度。" },
    ],
  },
  {
    match: (id) => id.includes("bsx"), title: "BSX",
    metrics: [
      { key: "graph_color_count", label: "图着色颜色数", help: "BSX 冲突图着色使用的颜色数。" },
      { key: "graph_colors_per_block", label: "每区块颜色数", help: "图着色颜色数 ÷ 已提交区块数。" },
      { key: "pairwise_conflict_check_count", label: "冲突检查次数", help: "BSX 两两冲突检查原始计数。" },
      { key: "conflict_checks_per_tx", label: "每交易冲突检查次数", help: "冲突检查次数 ÷ 已提交交易数。" },
      { key: "maximum_wave_width", label: "最大颜色组宽度", help: "单个颜色组 / Wave 的最大交易宽度。" },
    ],
  },
  {
    match: (id) => id.includes("aria"), title: "Aria",
    metrics: [
      { key: "aria_epoch_count", label: "Epoch 数", help: "共识绑定的 Aria 候选证据中记录的 Epoch 数。" },
      { key: "aria_maximum_epoch_width", label: "最大 Epoch 宽度", help: "Aria 最大 Epoch 宽度。" },
      { key: "aria_candidate_transaction_count", label: "候选交易数", help: "Aria 候选交易总数。" },
      { key: "aria_selected_transaction_count", label: "选中交易数", help: "Aria 选择进入区块的交易数。" },
      { key: "aria_deferred_transaction_count", label: "延期交易数", help: "Aria 冲突后延期到后续区块的交易数。" },
      { key: "aria_conflict_abort_count", label: "冲突中止数", help: "Aria 冲突中止原始计数。" },
      { key: "aria_conflict_abort_rate", label: "冲突中止率", help: "冲突中止数 ÷ 候选交易数。" },
      { key: "aria_reexecution_count", label: "重执行次数", help: "Aria 重执行原始计数。" },
      { key: "aria_waw_dependency_count", label: "WAW", help: "Aria 写后写（WAW）依赖数。" },
      { key: "aria_raw_dependency_count", label: "RAW", help: "Aria 读后写（RAW）依赖数。" },
      { key: "aria_war_dependency_count", label: "WAR", help: "Aria 写后读（WAR）依赖数。" },
      { key: "aria_read_only_fast_commit_count", label: "只读快速提交数", help: "Aria 只读快速提交数。" },
    ],
  },
  {
    match: (id) => id.includes("batch_si"), title: "Batch-SI",
    metrics: [
      { key: "batch_count", label: "批次数", help: "Batch-SI 生成的 Batch 总数。" },
      { key: "batch_count_per_block", label: "每区块平均批次数", help: "批次数 ÷ 已提交区块数。" },
      { key: "maximum_batch_width", label: "最大批次宽度", help: "Batch-SI 最大批次宽度。" },
      { key: "batch_si_first_pass_ofas_abort_rate", label: "OFAS 延期率", help: "第一轮候选中被 OFAS 延后到后续提案的比例。" },
      { key: "batch_si_unique_deferral_rate", label: "唯一延期交易比例", help: "唯一延期交易数 ÷ 接受交易数。" },
      { key: "write_reuse_per_tx", label: "每交易写机会复用数", help: "写机会复用数 ÷ 已提交交易数。" },
      { key: "batch_snapshot_count", label: "批快照数", help: "批快照创建次数。" },
      { key: "batch_si_plan_payload_bytes", label: "计划负载字节数", unit: "B", help: "共识绑定 Batch-SI 执行计划负载字节数。" },
      { key: "batch_si_executor_plan_parse_ms", label: "计划解析耗时", unit: "ms", help: "执行器解析 Batch-SI 执行计划的时间。" },
      { key: "batch_si_executor_plan_verify_ms", label: "计划验证耗时", unit: "ms", help: "执行器验证 Batch-SI 执行计划的时间。" },
      { key: "batch_si_executor_full_verify_count", label: "完整重验证次数", help: "发生完整重验证的次数。" },
    ],
  },
  {
    match: (id) => id.includes("groundhog"), title: "Groundhog",
    metrics: [
      { key: "groundhog_reservation_count", label: "预留次数", help: "Groundhog 预留操作原始计数。" },
      { key: "groundhog_reservations_per_tx", label: "每交易预留次数", help: "预留次数 ÷ 已提交交易数。" },
      { key: "groundhog_constraint_conflict_count", label: "约束冲突次数", help: "类型化约束冲突原始计数。" },
      { key: "groundhog_constraint_conflicts_per_attempt", label: "每次尝试约束冲突数", help: "约束冲突次数 ÷ 执行尝试次数。" },
      { key: "groundhog_reservation_rollback_count", label: "预留回滚次数", help: "预留回滚原始计数。" },
      { key: "groundhog_reservation_rollback_rate", label: "预留回滚率", help: "回滚次数 ÷ 预留次数。" },
      { key: "groundhog_proposal_deferral_rate", label: "提案延期率", help: "提案延期事件数 ÷ 提案候选交易数。" },
      { key: "groundhog_reservation_parallel_width", label: "预留阶段并行宽度", help: "Groundhog 预留引擎的最大并行宽度。" },
    ],
  },
  {
    match: (id) => id.includes("optme"), title: "OptME",
    metrics: [
      { key: "optme_simulation_ms", label: "PBFT 后模拟耗时", unit: "ms", help: "OptME 在共识输出后对统一块起始快照做第一次真实模拟的累计时间。" },
      { key: "optme_graph_scheduling_ms", label: "冲突图与调度耗时", unit: "ms", help: "作者 AddressBasedConflictGraph 构建、层次排序、First-Updater-Wins、重排与分组耗时。" },
      // MBE_OPTME_V20_METRICS
      { key: "optme_early_detection_count", label: "提前中止检测数", help: "地址冲突图构造及并行子图归并阶段，由 First-Updater-Wins 提前检测出的交易数。" },
      { key: "optme_hierarchical_abort_count", label: "层次排序中止数", help: "层次排序阶段因反向读写约束等产生的中止交易数；其中一部分随后可被乐观重排救回。" },
      { key: "optme_rescheduled_transaction_count", label: "进入二次执行交易数", help: "完成乐观重排后仍未回到主计划、实际进入作者二次执行路径的交易数。" },
      { key: "optme_early_abort_count", label: "旧版中止计数（兼容）", help: "兼容旧结果字段；v20 起等同最终进入 reschedule 路径的交易数，不再冒充论文 Early Detection。" },
      { key: "optme_simulation_failed_count", label: "首次模拟失败数", help: "第一次业务模拟失败、按作者 filter_map 语义不进入 OptME 冲突图但仍由 MBE 产生终态失败回执的交易数。" },
      { key: "optme_reordered_transaction_count", label: "成功重排交易数", help: "满足作者源码 write-only + 多写单元条件后被重新插回主 schedule 的交易数。" },
      { key: "optme_sequence_count", label: "主 Schedule 序列数", help: "OptME 主执行计划的 sequence 数。" },
      { key: "optme_maximum_sequence_width", label: "最大 Sequence 宽度", help: "同一 OptME sequence 可并行提交的最大交易数。" },
      { key: "optme_rescheduled_epoch_count", label: "二次执行 Epoch 数", help: "剩余 aborted 交易按作者源码冲突规则形成的 re-execution epoch 数。" },
      { key: "optme_reexecution_count", label: "二次执行交易数", help: "进入作者源码第二次执行阶段的交易数。" },
      { key: "optme_reexecution_simulation_failed_count", label: "二次模拟失败数", help: "进入二次执行后业务模拟本身失败的交易数；这些交易不再参加 optimistic write-set 验证。" },
      { key: "optme_reexecution_invalid_count", label: "二次验证失效数", help: "二次执行本身成功，但 optimistic assumption 的写集合互斥验证失败；不私自加入作者源码中已注释掉的第三次 fallback。" },
    ],
  },
  {
    match: (id) => id.includes("txallo"), title: "TxAllo",
    metrics: [
      { key: "txallo_allocation_mode", label: "分配模式", help: "paper_g_snapshot 表示正式评测窗口只使用此前已提交历史运行一次完整 G-TxAllo，然后冻结映射。" },
      { key: "txallo_history_transaction_count", label: "历史交易数", help: "只来自正式评测窗口之前、用于本次 G-TxAllo 快照的历史交易数量。" },
      { key: "txallo_graph_account_count", label: "账户图节点数", help: "TxAllo 历史账户交易图中的账户数量；不是 MetaTrack 状态键数量。" },
      { key: "txallo_graph_edge_count", label: "账户图边数", help: "历史账户对加权边数量。多账户交易按论文 1/C(m,2) 归一化。" },
      { key: "txallo_louvain_level_count", label: "Louvain 层数", help: "完整层次 Louvain 实际执行的层级数；不再只做第一层局部移动。" },
      { key: "txallo_g_txallo_run_count", label: "G-TxAllo 次数", help: "当前评测快照应为 1（有历史时）。" },
      { key: "txallo_a_txallo_run_count", label: "A-TxAllo 次数", help: "当前 G-snapshot 评测模式应为 0；只有真实 committed-block epoch 动态模式才允许增加。" },
      { key: "txallo_mapping_complete", label: "账户映射完整", help: "历史图中的每个账户都必须唯一落到一个合法分片；否则 bootstrap 直接失败。" },
      { key: "txallo_modeled_throughput", label: "论文模型吞吐 Λ", help: "TxAllo 目标函数中的模型值，不等同于 MBE 实测 end-to-end TPS。" },
      { key: "txallo_modeled_cross_shard_ratio", label: "历史模型跨片率 γ", help: "用于冻结映射的历史图上论文定义的跨片边权比例。" },
      { key: "txallo_evaluation_cross_shard_ratio", label: "正式评测跨片率", help: "冻结映射应用到正式评测交易后实际 sender/receiver 跨片比例。" },
      { key: "txallo_future_evaluation_transactions_used", label: "未来评测交易用于训练数", help: "必须为 0；用于证明当前评测 batch 没有回灌进 TxAllo 映射。" },
    ],
  },
  {
    match: (id) => id.includes("porygon"), title: "Porygon",
    metrics: [
      { key: "porygon_execution_shard_count", label: "ESC 数", help: "前端 topology.shards 映射得到的 Porygon Execution Sub-Committee 数。" },
      { key: "porygon_execution_wave_count", label: "执行 Wave 数", help: "Porygon 在 OC 跨 ESC 冲突闭包后实际执行的 Wave 总数。" },
      { key: "porygon_logical_state_cross_shard_ratio", label: "Porygon 逻辑跨片比例", help: "规划阶段 CTx 数量占全部 Porygon 计划交易的比例；不是 workload source/target 跨片比例，也不以成功执行数作分母。" },
      { key: "porygon_executed_cross_shard_ratio", label: "实际执行跨片比例", help: "成功且未被 OC 冲突闭包放弃的 CTx 在实际执行交易中的比例。" },
      { key: "porygon_cross_esc_abandon_ratio", label: "跨 ESC 放弃比例", help: "按 Porygon 论文 OC 冲突规则被标记 Abandoned 的 CTx 占全部计划交易比例。" },
      { key: "porygon_cross_esc_conflict_closure_verified", label: "跨 ESC 冲突闭包", help: "所有未放弃交易在不同 ESC 之间均无剩余读写/写写冲突时为真。" },
      { key: "porygon_full_woec_pipeline_overlap_claimed", label: "完整 W/O/E/M 实际重叠", help: "只有真实运行时间区间证明 Ordering/Execution 与 Execution/Commit 跨高度重叠时才为真；逻辑槽位不计。" },
      { key: "porygon_ordering_execution_wall_clock_overlap_count", label: "排序-执行实际重叠块数", help: "后一高度 Ordering 与前一高度 Execution 在墙钟时间上真实相交的区块数。" },
      { key: "porygon_execution_commit_wall_clock_overlap_count", label: "执行-提交实际重叠块数", help: "后一高度 Execution 与前一高度 Porygon 协议提交阶段在墙钟时间上真实相交的区块数。" },
      { key: "porygon_proposal_carried_update_enforced", label: "Proposal-U 后续 EC 更新", help: "跨片更新由 PBFT 认证的 Proposal.U 携带，并由后续 EC 应用时为真。" },
      { key: "porygon_compact_proposal_txlist_omitted", label: "Compact Proposal", help: "PBFT Proposal 省略完整 TxList，仅携带 TransactionBlock 引用与 L/U/T 时为真。" },
      { key: "porygon_prepared_state_forwarding_used", label: "预备状态前传", help: "Paper2 应始终为假；执行状态只能来自 Proposal.T。" },
      { key: "porygon_exact_multiround_rollback_claimed", label: "原文故障回滚完整复现", help: "只有真实观察到后续同 shard ESC 重试并最终通过 Proposal 携带 rollback transaction 时才可为真；当前 Paper2 正常路径保持 fail-closed。" },
      { key: "porygon_paper_pending_transaction_count_max", label: "跨轮 Pending 峰值", help: "transaction-level 未提交交易集合在本次运行中的最大数量。" },
      { key: "porygon_paper_itx_committed_count", label: "ITx 提交数", help: "满足论文四轮生命周期并提交的片内交易数量。" },
      { key: "porygon_paper_ctx_committed_count", label: "CTx 提交数", help: "满足论文六轮生命周期并提交的跨片交易数量。" },


      { key: "porygon_esc_ownership_verified", label: "ESC 归属核验", help: "逐区块按动态 ESC role 与该区块执行直方图核对各 replica 的本地业务执行数量。" },
      { key: "porygon_business_execution_critical_path_ms", label: "业务执行关键路径", unit: "ms", help: "逐区块取 replica 业务执行最大值后跨区块求和。" },
      { key: "porygon_result_exchange_wait_critical_path_ms", label: "ESC 结果交换等待关键路径", unit: "ms", help: "逐区块取 replica ESC 结果交换等待最大值后跨区块求和。" },
      { key: "porygon_execution_critical_path_ms", label: "Porygon 执行总关键路径", unit: "ms", help: "逐区块 replica 执行关键路径最大值之和，是 Porygon 横向执行时间比较的正式字段。" },
      { key: "porygon_leader_local_transaction_execution_ms", label: "Global Leader 本地业务执行时间", unit: "ms", help: "仅保留诊断用途；不是 Porygon 全局执行墙钟时间。" },
      { key: "porygon_witness_threshold_configured", label: "Witness 配置阈值", help: "真实 EC Witness Certificate 的配置/故障界限阈值。" },
      { key: "porygon_witness_validation_mode", label: "Witness 验证模式", help: "PBFT 排序前由确定性 EC Witness Committee 收集认证 witness vote 并验证阈值证书。" },
    ],
  },
  {
    match: (id) => id.includes("metatrack"), title: "MetaTrack",
    metrics: [
      { key: "fast_track_logical_tx_count", label: "快速轨交易数", help: "MetaTrack 快速轨逻辑交易数。" },
      { key: "conservative_track_logical_tx_count", label: "保守轨交易数", help: "MetaTrack 保守轨逻辑交易数。" },
      { key: "fast_track_ratio", label: "快速轨比例", help: "快速轨交易数 ÷ 已分类逻辑交易数。" },
      { key: "metatrack_fast_fallback_count", label: "快速轨回退次数", help: "Fast tentative result 被丢弃并转入 Conservative 重新执行的次数。" },
      { key: "metatrack_fast_fallback_rate", label: "快速轨回退率", help: "快速轨回退次数 ÷ 初始快速轨交易数；strict frontier 正常应接近 0。" },
      { key: "metatrack_fast_discarded_execution_ms", label: "快速轨丢弃执行耗时", unit: "ms", help: "Fast 回退时已经消耗但最终丢弃的业务执行时间累计。" },
      { key: "metatrack_conservative_reexecution_count", label: "保守轨重执行次数", help: "由 Fast 回退进入 Conservative 的 attempt>1 重执行次数。" },
      { key: "metatrack_conservative_reexecution_share", label: "保守轨重执行占比", help: "保守轨重执行次数 ÷ 保守轨业务执行 attempt 数；不是 Block-STM 式回滚率。" },
      { key: "metatrack_fast_business_execution_sum_ms", label: "快速轨业务执行累计耗时", unit: "ms", help: "Fast worker 真实业务 attempt 的单调时钟耗时之和；并行时可大于整体墙钟时间。" },
      { key: "metatrack_fast_business_execution_mean_ms", label: "快速轨单次平均执行耗时", unit: "ms", help: "快速轨业务执行累计耗时 ÷ Fast attempt 数。" },
      { key: "metatrack_fast_business_execution_p95_ms", label: "快速轨单次执行 P95", unit: "ms", help: "Fast worker 真实 business attempt 耗时的 P95。" },
      { key: "metatrack_fast_business_execution_p99_ms", label: "快速轨单次执行 P99", unit: "ms", help: "Fast worker 真实 business attempt 耗时的 P99。" },
      { key: "metatrack_conservative_business_execution_sum_ms", label: "保守轨业务执行累计耗时", unit: "ms", help: "Conservative worker 真实业务 attempt 的单调时钟耗时之和。" },
      { key: "metatrack_conservative_business_execution_mean_ms", label: "保守轨单次平均执行耗时", unit: "ms", help: "保守轨业务执行累计耗时 ÷ Conservative attempt 数。" },
      { key: "metatrack_conservative_business_execution_p95_ms", label: "保守轨单次执行 P95", unit: "ms", help: "Conservative worker 真实 business attempt 耗时的 P95。" },
      { key: "metatrack_conservative_business_execution_p99_ms", label: "保守轨单次执行 P99", unit: "ms", help: "Conservative worker 真实 business attempt 耗时的 P99；v31 使用纳秒级单调时钟。" },
      { key: "metatrack_fast_track_sojourn_mean_ms", label: "快速轨平均全程驻留时间", unit: "ms", help: "从进入 Fast 到该 attempt 完成/回退的真实单调时钟时间，包含依赖、StateReady、排队和业务执行。" },
      { key: "metatrack_fast_track_sojourn_p95_ms", label: "快速轨全程驻留 P95", unit: "ms", help: "Fast attempt 全轨 sojourn time P95。" },
      { key: "metatrack_fast_track_sojourn_p99_ms", label: "快速轨全程驻留 P99", unit: "ms", help: "Fast attempt 全轨 sojourn time P99。" },
      { key: "metatrack_conservative_track_sojourn_mean_ms", label: "保守轨平均全程驻留时间", unit: "ms", help: "从进入 Conservative 到该 attempt 完成的真实单调时钟时间。" },
      { key: "metatrack_conservative_track_sojourn_p95_ms", label: "保守轨全程驻留 P95", unit: "ms", help: "Conservative attempt 全轨 sojourn time P95。" },
      { key: "metatrack_conservative_track_sojourn_p99_ms", label: "保守轨全程驻留 P99", unit: "ms", help: "Conservative attempt 全轨 sojourn time P99。" },
      { key: "metatrack_fast_state_wait_sum_ms", label: "快速轨 StateReady 累计等待", unit: "ms", help: "Fast 交易 StateReady 等待累计；与依赖等待可能重叠，不能与 sojourn 直接相加。" },
      { key: "metatrack_fast_dependency_wait_sum_ms", label: "快速轨依赖累计等待", unit: "ms", help: "Fast 交易等待前驱完成的累计时间。" },
      { key: "metatrack_fast_queue_wait_sum_ms", label: "快速轨就绪队列累计等待", unit: "ms", help: "Fast 交易 ready 后到 dispatch 的累计时间。" },
      { key: "metatrack_conservative_state_wait_sum_ms", label: "保守轨 StateReady 累计等待", unit: "ms", help: "Conservative 交易 StateReady 等待累计。" },
      { key: "metatrack_conservative_dependency_wait_sum_ms", label: "保守轨依赖累计等待", unit: "ms", help: "Conservative 交易等待前驱完成的累计时间。" },
      { key: "metatrack_conservative_queue_wait_sum_ms", label: "保守轨就绪队列累计等待", unit: "ms", help: "Conservative 交易 ready 后到 dispatch 的累计时间。" },
      { key: "metatrack_business_execution_cpu_sum_ms", label: "双轨真实业务执行累计耗时", unit: "ms", help: "Fast + Conservative worker business attempt 的纳秒级累计耗时；不再把整个 StateReady 包络误标成 business CPU。" },
      { key: "metatrack_business_execution_critical_path_ms", label: "双轨业务执行活跃关键路径", unit: "ms", help: "每分片 leader 按区块合并并行 business attempt 时间区间后求和，再取分片关键路径；不包含 StateReady/依赖等待。" },
      { key: "physical_remote_fetch_count", label: "远程状态读取数", help: "物理远程状态读取数。" },
      { key: "physical_remote_writeback_count", label: "远程状态写回数", help: "物理远程状态写回数。" },
      { key: "remote_operations_per_logical_tx", label: "每逻辑交易远程操作数", help: "远程物理操作数 ÷ 逻辑交易数。" },
      { key: "aggregation_reduction_ratio", label: "聚合削减比例", help: "聚合减少的物理操作比例。" },
      { key: "scheduler_idle_ratio", label: "调度器空闲比例", help: "MetaTrack 运行时调度器空闲比例。" },
    ],
  },
];

export default function V5MechanismAnalysis({ children }: { children: V5FormalChildRun[] }) {
  const methods = useMemo(() => uniqueMethods(children), [children]);
  const [selected, setSelected] = useState(methods[0]?.id ?? "");
  const active = methods.some((item) => item.id === selected) ? selected : methods[0]?.id ?? "";
  const selectedChildren = children.filter((child) => (child.method_config_id || child.method?.method_id) === active && child.status === "completed");
  const metrics = aggregateMetrics(selectedChildren);
  const matchedDefinitions = METHOD_METRICS.filter((item) => item.match(active));
  const versionedMode = String(metrics.versioned_state_ready_scheduler_mode ?? "");
  const nativeMode = String(metrics.state_ready_scheduler_mode ?? "");
  const stateReadyDefinitions = versionedMode === "per_transaction_per_key_version_frontier"
    ? VERSIONED_STATE_READY
    : nativeMode === "transaction_level_suspend_resume"
      ? NATIVE_STATE_READY
      : [];
  const projectionFrontierDefinitions = metrics.metatrack_projection_frontier_policy ? METATRACK_PROJECTION_FRONTIER_V612 : [];
  const consensusWindowDefinitions = metrics.metatrack_consensus_window_observability_available ? METATRACK_CONSENSUS_WINDOW_V656813 : [];
  const currentMetaTrack = ["metatrack_latest", "metatrack_unified", "metatrack_ab_route", "metatrack_ab_track", "metatrack_ab_cons", "metatrack_ab_state"].includes(active);
  const boundaryBatchDefinitions = currentMetaTrack && metrics.metatrack_closure_boundary_batch_policy ? METATRACK_BOUNDARY_BATCH_V621 : [];
  const asyncWritebackDefinitions = currentMetaTrack && metrics.metatrack_async_version_writeback_policy ? METATRACK_ASYNC_WRITEBACK_V640 : [];
  const incrementalRoutingDefinitions = currentMetaTrack && metrics.metatrack_incremental_routing_policy ? METATRACK_INCREMENTAL_ROUTING_V650 : [];
  const naturalWindowDefinitions = active === "metatrack_exp" && metrics.metatrack_natural_window_available ? METATRACK_NATURAL_WINDOW_V657 : [];
  const partitionInvariantDefinitions = active === "metatrack_exp" && metrics.metatrack_partition_invariant_available ? METATRACK_PARTITION_INVARIANT_V658 : [];
  const definitions = [...COMMON, ...stateReadyDefinitions, ...projectionFrontierDefinitions, ...consensusWindowDefinitions, ...boundaryBatchDefinitions, ...asyncWritebackDefinitions, ...incrementalRoutingDefinitions, ...naturalWindowDefinitions, ...partitionInvariantDefinitions, ...matchedDefinitions.flatMap((item) => item.metrics)];
  const activeName = methods.find((item) => item.id === active)?.name ?? active;
  return <section className="v5-dashboard-section" data-testid="v5-mechanism-analysis">
    <div className="v5-dashboard-heading"><div><h3>机制分析</h3><p className="muted">只显示该方法已有正式证据的指标；所有比例均在指标提取/结果层派生，不修改执行器。</p></div></div>
    <div className="v5-method-tabs">{methods.map((method) => <button key={method.id} type="button" className={active === method.id ? "active" : ""} onClick={() => setSelected(method.id)}>{method.name}</button>)}</div>
    {active ? <>
      <div className="v5-mechanism-title"><strong>{activeName}</strong><span>{selectedChildren.length} 个已完成样本</span></div>
      <div className="v5-mechanism-grid">{definitions.map((item) => <Metric key={item.key} definition={item} value={metrics[item.key]} />)}</div>
      <ExecutionBreakdown metrics={metrics} />
    </> : <p className="muted">暂无方法数据。</p>}
  </section>;
}

function Metric({ definition, value }: { definition: MetricDef; value: unknown }) {
  const numeric = number(value);
  return <div className="v5-mechanism-card"><span>{definition.label} <V5MetricHelp text={`${definition.help} 原始字段：${definition.key}`} /></span><strong>{numeric === null ? display(value) : formatMetric(numeric, definition.unit)}</strong><small>{definition.key}</small></div>;
}

function ExecutionBreakdown({ metrics }: { metrics: Record<string, unknown> }) {
  const transaction = number(metrics.transaction_execution_ms) ?? 0;
  const materialization = number(metrics.deterministic_materialization_ms) ?? 0;
  const commitment = number(metrics.state_commitment_ms) ?? 0;
  const total = number(metrics.block_execution_ms) ?? 0;
  const versionedEnvelope = number(metrics.versioned_state_ready_execution_ms) ?? 0;
  const nativeEnvelope = number(metrics.metatrack_suspend_resume_execution_ms) ?? 0;
  const porygonCritical = number(metrics.porygon_execution_critical_path_ms) ?? 0;
  const porygonBusiness = number(metrics.porygon_execution_critical_path_business_component_ms) ?? 0;
  const porygonExchange = number(metrics.porygon_execution_critical_path_exchange_component_ms) ?? 0;
  const porygonOther = number(metrics.porygon_execution_critical_path_other_component_ms) ?? 0;
  let pieces: Array<readonly [string, number]>;
  if (porygonCritical > 0) {
    pieces = [
      ["关键路径对应 Replica 的业务执行", porygonBusiness],
      ["关键路径对应 Replica 的 ESC 结果交换等待", porygonExchange],
      ["同一关键路径 Replica 的物化、承诺及其他开销", porygonOther],
    ];
  } else if (transaction > 0 || materialization > 0 || commitment > 0) {
    pieces = [
      ["交易/调度执行", transaction],
      ["确定性物化", materialization],
      ["状态承诺", commitment],
      ["其他区块执行开销", Math.max(0, total - transaction - materialization - commitment)],
    ];
  } else if (versionedEnvelope > 0) {
    const envelope = Math.min(total, versionedEnvelope);
    pieces = [["版本前沿 / StateReady 包络", envelope], ["其他区块执行开销", Math.max(0, total - envelope)]];
  } else if (nativeEnvelope > 0) {
    const envelope = Math.min(total, nativeEnvelope);
    pieces = [["双轨 / StateReady 执行包络", envelope], ["其他区块执行开销", Math.max(0, total - envelope)]];
  } else {
    pieces = [["区块执行包络", total]];
  }
  const denominator = pieces.reduce((sum, [, value]) => sum + value, 0);
  return <div className="v5-execution-breakdown"><h4>执行阶段耗时构成</h4><div className="v5-breakdown-bar">{pieces.map(([label, value]) => <span key={label} title={`${label}: ${value.toFixed(1)} ms`} style={{ flexGrow: denominator > 0 ? value : 1 }} />)}</div><div className="v5-breakdown-legend">{pieces.map(([label, value]) => <span key={label}><strong>{label}</strong> {value.toLocaleString(undefined, { maximumFractionDigits: 1 })} ms</span>)}</div></div>;
}

function uniqueMethods(children: V5FormalChildRun[]) {
  const map = new Map<string, string>();
  for (const child of children) {
    const id = child.method_config_id || child.method?.method_id || "";
    if (id) map.set(id, shortMethodName(id, child.method?.display_name ?? id));
  }
  return [...map.entries()].map(([id, name]) => ({ id, name }));
}

function aggregateMetrics(children: V5FormalChildRun[]): Record<string, unknown> {
  const keys = new Set<string>();
  const rows = children.map((child) => asRecord(child.metrics));
  for (const row of rows) Object.keys(row).forEach((key) => keys.add(key));
  const out: Record<string, unknown> = {};
  for (const key of keys) {
    const booleans = rows.map((row) => row[key]).filter((value): value is boolean => typeof value === "boolean");
    if (booleans.length) {
      out[key] = booleans.every(Boolean);
      continue;
    }
    const numeric = rows.map((row) => number(row[key])).filter((value): value is number => value !== null);
    if (numeric.length) out[key] = numeric.reduce((sum, value) => sum + value, 0) / numeric.length;
    else {
      const first = rows.map((row) => row[key]).find((value) => value !== undefined && value !== null && value !== "");
      if (first !== undefined) out[key] = first;
    }
  }
  return out;
}

function asRecord(value: unknown): Record<string, unknown> { return value && typeof value === "object" && !Array.isArray(value) ? value as Record<string, unknown> : {}; }
function number(value: unknown): number | null { if (typeof value === "boolean") return null; const parsed = Number(value); return value === null || value === undefined || value === "" || !Number.isFinite(parsed) ? null : parsed; }
function display(value: unknown): string { return value === null || value === undefined || value === "" ? "—" : typeof value === "boolean" ? (value ? "是" : "否") : String(value); }
function formatMetric(value: number, unit?: string): string { if (unit === "B") return `${value.toLocaleString(undefined, { maximumFractionDigits: 0 })} B`; if (unit === "ms") return `${value.toLocaleString(undefined, { maximumFractionDigits: 1 })} ms`; if (unit === "μs") return `${value.toLocaleString(undefined, { maximumFractionDigits: 1 })} μs`; if (Math.abs(value) > 0 && Math.abs(value) < 1) return value.toFixed(4); return value.toLocaleString(undefined, { maximumFractionDigits: 3 }); }

function shortMethodName(methodId: string, value: string): string {
  const id = methodId.toLowerCase();
  if (id === "metatrack_latest") return "Metatrack";
  if (id === "metatrack_ab_route") return "去掉共现矩阵分片";
  if (id === "metatrack_ab_track") return "去掉双轨";
  if (id === "metatrack_ab_cons") return "去掉共识聚合";
  if (id === "metatrack_ab_state") return "去掉状态预取";
  if (id === "metatrack_ab_handoff") return "历史子消融-无本地版本交接";
  if (id === "metatrack_exp") return "实验版（旧）";
  if (id === "metatrack_full_locality") return "MetaTrack（当前版）";
  if (id === "metatrack_block_stm") return "MetaTrack + Block-STM（历史）";
  if (id === "stateless_hash_block_stm") return "Stateless Block-STM";
  if (id === "metatrack_serial") return "MetaTrack（初始版）";
  if (id === "stateless_hash_serial") return "Stateless Serial";
  if (id === "hash_serial") return "Serial";
  if (id === "hash_block_stm") return "Block-STM";
  if (id === "hash_aria") return "Aria";
  if (id === "hash_groundhog") return "Groundhog";
  if (id === "hash_cg") return "CG/Nezha";
  if (id === "hash_acg") return "ACG/Nezha";
  if (id === "hash_bsx") return "BSX";
  if (id === "hash_batch_si") return "Batch-SI";
  const lower = value.toLowerCase();
  if (lower.includes("address conflict graph")) return "ACG/Nezha";
  if (lower.includes("batch-schedule-execute")) return "BSX";
  if (lower.includes("conflict graph") && !lower.includes("address")) return "CG/Nezha";
  if (lower.includes("batch-si")) return "Batch-SI";
  if (lower.includes("groundhog")) return "Groundhog";
  if (lower.includes("aria")) return "Aria";
  if (lower.includes("serial")) return "Serial";
  if (lower.includes("block-stm")) return "Block-STM";
  return value.replace(/Stateful Hash/gi, "有状态 Hash").replace(/Stateless Hash/gi, "无状态 Hash").replace(/with Block-STM backend/gi, "+ Block-STM");
}
