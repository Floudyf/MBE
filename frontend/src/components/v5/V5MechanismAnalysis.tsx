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
    match: (id) => id.includes("porygon"), title: "Porygon",
    metrics: [
      { key: "porygon_execution_shard_count", label: "ESC 数", help: "前端 topology.shards 映射得到的 Porygon Execution Sub-Committee 数。" },
      { key: "porygon_execution_wave_count", label: "执行 Wave 数", help: "Porygon 共识绑定计划实际执行的 Wave 总数。" },
      { key: "porygon_logical_state_cross_shard_ratio", label: "Porygon 逻辑跨片比例", help: "execution ESC 与签名 AccessList 状态归属共同定义的逻辑跨片比例；不是 workload source/target 跨片比例。" },
      { key: "porygon_esc_ownership_verified", label: "ESC 归属核验", help: "各 ESC replica 的本地业务执行数量是否与共识绑定 ownership 一致。" },
      { key: "porygon_business_execution_critical_path_ms", label: "业务执行关键路径", unit: "ms", help: "逐区块取 replica 业务执行最大值后跨区块求和。" },
      { key: "porygon_result_exchange_wait_critical_path_ms", label: "ESC 结果交换等待关键路径", unit: "ms", help: "逐区块取 replica ESC 结果交换等待最大值后跨区块求和。" },
      { key: "porygon_execution_critical_path_ms", label: "Porygon 执行总关键路径", unit: "ms", help: "逐区块 replica 执行关键路径最大值之和，是 Porygon 横向执行时间比较的正式字段。" },
      { key: "porygon_leader_local_transaction_execution_ms", label: "Global Leader 本地业务执行时间", unit: "ms", help: "仅保留诊断用途；不是 Porygon 全局执行墙钟时间。" },
      { key: "porygon_witness_threshold_configured", label: "Witness 配置阈值", help: "保留的 Porygon witness_threshold 配置值；当前 MBE 适配不把它解释为独立 Witness Committee quorum。" },
      { key: "porygon_witness_validation_mode", label: "Witness 验证模式", help: "当前采用所有 PBFT validator 对完整 Transaction Block / Access root 重算后再投票。" },
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
  const definitions = [...COMMON, ...stateReadyDefinitions, ...matchedDefinitions.flatMap((item) => item.metrics)];
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
function formatMetric(value: number, unit?: string): string { if (unit === "B") return `${value.toLocaleString(undefined, { maximumFractionDigits: 0 })} B`; if (unit === "ms") return `${value.toLocaleString(undefined, { maximumFractionDigits: 1 })} ms`; if (Math.abs(value) > 0 && Math.abs(value) < 1) return value.toFixed(4); return value.toLocaleString(undefined, { maximumFractionDigits: 3 }); }

function shortMethodName(methodId: string, value: string): string {
  const id = methodId.toLowerCase();
  if (id === "metatrack_block_stm") return "MetaTrack + Block-STM";
  if (id === "stateless_hash_block_stm") return "Stateless Block-STM";
  if (id === "metatrack_serial") return "MetaTrack";
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
