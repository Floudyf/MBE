// MBE_V5_RESULTS_UI_TRUTH_CN_FINAL_20260814_V5
import { useMemo, useState } from "react";
import type { V5FormalChildRun } from "../../api";
import V5MetricHelp from "./V5MetricHelp";

type MetricDef = { key: string; label: string; unit?: string; help: string };

const COMMON: MetricDef[] = [
  { key: "worker_count", label: "工作线程数", help: "该方法真实执行路径记录的 工作线程数；Serial 通常为 1。" },
  { key: "maximum_parallel_width", label: "最大并行宽度", help: "运行期/计划证据中观察到的最大同时并行交易宽度。" },
  { key: "block_execution_ms", label: "区块执行总耗时", unit: "ms", help: "Leader 区块执行墙钟时间包络累计。" },
  { key: "transaction_execution_ms", label: "交易执行耗时", unit: "ms", help: "算法交易执行阶段累计时间。" },
  { key: "deterministic_materialization_ms", label: "确定性物化耗时", unit: "ms", help: "确定性物化/应用阶段累计时间。" },
  { key: "state_commitment_ms", label: "状态承诺耗时", unit: "ms", help: "状态承诺 / 状态根更新阶段累计时间。" },
];

const METHOD_METRICS: Array<{ match: (id: string) => boolean; title: string; metrics: MetricDef[] }> = [
  {
    match: (id) => id.includes("block_stm"), title: "Block-STM",
    metrics: [
      { key: "abort_count", label: "中止事件数", help: "运行期中止事件原始计数，同一交易可能出现多次。" },
      { key: "block_stm_abort_events_per_tx", label: "每交易中止事件数", help: "中止事件数 ÷ 已提交逻辑交易数。" },
      { key: "reexecution_count", label: "重执行次数", help: "Block-STM 重执行事件计数。" },
      { key: "reexecution_events_per_tx", label: "每交易重执行次数", help: "重执行事件数 ÷ 已提交逻辑交易数。" },
      { key: "validation_failure_count", label: "验证失败次数", help: "Block-STM 验证失败事件。" },
      { key: "dependency_wait_count", label: "依赖等待次数", help: "依赖等待事件原始计数。" },
      { key: "maximum_incarnation_observed", label: "最大执行版本号", help: "运行中观察到的最大执行版本号。" },
    ],
  },
  {
    match: (id) => id === "hash_cg" || id.includes("_cg"), title: "CG",
    metrics: [
      { key: "dependency_edge_count", label: "依赖边数", help: "CG 冲突依赖图边数。" },
      { key: "dependency_edges_per_tx", label: "每交易依赖边数", help: "依赖边数 ÷ 已提交交易数。" },
      { key: "pairwise_conflict_check_count", label: "冲突检查次数", help: "CG 两两冲突检查原始计数。" },
      { key: "conflict_checks_per_tx", label: "每交易冲突检查次数", help: "两两冲突检查次数 ÷ 已提交交易数。" },
      { key: "wave_count", label: "Wave 数", help: "依赖图分层后的 Wave 总数。" },
      { key: "maximum_wave_width", label: "最大 Wave 宽度", help: "单个 Wave 的最大交易宽度。" },
    ],
  },
  {
    match: (id) => id.includes("acg"), title: "ACG / Nezha",
    metrics: [
      { key: "nezha_hs_abort_count", label: "HS 中止数", help: "Nezha/ACG HS 合法终态中止数。" },
      { key: "nezha_hs_abort_rate", label: "HS 中止率", help: "HS 中止数 ÷ 已提交逻辑交易数。" },
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
    match: (id) => id.includes("metatrack"), title: "MetaTrack",
    metrics: [
      { key: "fast_track_logical_tx_count", label: "快速轨交易数", help: "MetaTrack 快速轨逻辑交易数。" },
      { key: "conservative_track_logical_tx_count", label: "保守轨交易数", help: "MetaTrack 保守轨逻辑交易数。" },
      { key: "fast_track_ratio", label: "快速轨比例", help: "快速轨交易数 ÷ 已分类逻辑交易数。" },
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
  const definition = METHOD_METRICS.find((item) => item.match(active));
  const definitions = [...COMMON, ...(definition?.metrics ?? [])];
  return <section className="v5-dashboard-section" data-testid="v5-mechanism-analysis">
    <div className="v5-dashboard-heading"><div><h3>机制分析</h3><p className="muted">只显示该方法已有正式证据的指标；所有比例均在指标提取/结果层派生，不修改执行器。</p></div></div>
    <div className="v5-method-tabs">{methods.map((method) => <button key={method.id} type="button" className={active === method.id ? "active" : ""} onClick={() => setSelected(method.id)}>{method.name}</button>)}</div>
    {active ? <>
      <div className="v5-mechanism-title"><strong>{definition?.title ?? methods.find((item) => item.id === active)?.name ?? active}</strong><span>{selectedChildren.length} 个已完成样本</span></div>
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
  const other = Math.max(0, total - transaction - materialization - commitment);
  const pieces = [
    ["交易执行", transaction], ["确定性物化", materialization], ["状态承诺", commitment], ["其他执行开销", other],
  ] as const;
  const denominator = pieces.reduce((sum, [, value]) => sum + value, 0);
  return <div className="v5-execution-breakdown"><h4>执行阶段耗时构成</h4><div className="v5-breakdown-bar">{pieces.map(([label, value]) => <span key={label} title={`${label}: ${value.toFixed(0)} ms`} style={{ flexGrow: denominator > 0 ? value : 1 }} />)}</div><div className="v5-breakdown-legend">{pieces.map(([label, value]) => <span key={label}><strong>{label}</strong> {value.toLocaleString(undefined, { maximumFractionDigits: 0 })} ms</span>)}</div></div>;
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
function number(value: unknown): number | null { const parsed = Number(value); return value === null || value === undefined || value === "" || !Number.isFinite(parsed) ? null : parsed; }
function display(value: unknown): string { return value === null || value === undefined || value === "" ? "—" : typeof value === "boolean" ? (value ? "是" : "否") : String(value); }
function formatMetric(value: number, unit?: string): string { if (unit === "B") return `${value.toLocaleString(undefined, { maximumFractionDigits: 0 })} B`; if (unit === "ms") return `${value.toLocaleString(undefined, { maximumFractionDigits: 1 })} ms`; if (Math.abs(value) > 0 && Math.abs(value) < 1) return value.toFixed(4); return value.toLocaleString(undefined, { maximumFractionDigits: 3 }); }

function shortMethodName(methodId: string, value: string): string {
  const id = methodId.toLowerCase();
  if (id === "hash_serial") return "Serial";
  if (id === "hash_block_stm") return "Block-STM";
  if (id === "hash_aria") return "Aria";
  if (id === "hash_groundhog") return "Groundhog";
  if (id === "hash_cg") return "CG";
  if (id === "hash_acg") return "ACG/Nezha";
  if (id === "hash_bsx") return "BSX";
  if (id === "hash_batch_si") return "Batch-SI";
  const lower = value.toLowerCase();
  if (lower.includes("address conflict graph")) return "ACG/Nezha";
  if (lower.includes("batch-schedule-execute")) return "BSX";
  if (lower.includes("conflict graph") && !lower.includes("address")) return "CG";
  if (lower.includes("block-stm")) return "Block-STM";
  if (lower.includes("batch-si")) return "Batch-SI";
  if (lower.includes("groundhog")) return "Groundhog";
  if (lower.includes("aria")) return "Aria";
  if (lower.includes("serial")) return "Serial";
  return value.replace(/Stateful Hash/gi, "有状态 Hash").replace(/Stateless Hash/gi, "无状态 Hash").replace(/with Block-STM backend/gi, "+ Block-STM");
}
