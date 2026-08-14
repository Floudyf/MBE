// MBE_V5_RESULTS_UI_TRUTH_CN_FINAL_20260814_V5
import { useMemo, useState } from "react";
import type {
  V5FormalAggregate,
  V5FormalAnalysis,
  V5FormalArtifactCatalog,
  V5FormalChildRun,
  V5FormalRunGroup,
} from "../../api";
import V5AnalysisPanel from "./V5AnalysisPanel";
import V5SkewTpsChart from "./V5SkewTpsChart";
import V5ChildDetail from "./V5ChildDetail";
import V5EvidencePanel from "./V5EvidencePanel";
import V5MechanismAnalysis from "./V5MechanismAnalysis";
import V5MetricHelp from "./V5MetricHelp";
import V5ResourceNetworkPanel from "./V5ResourceNetworkPanel";

type Tab = "overview" | "performance" | "observability" | "mechanism" | "children" | "artifacts";

type Props = {
  group: V5FormalRunGroup;
  aggregate: V5FormalAggregate | null;
  children: V5FormalChildRun[];
  analysis: V5FormalAnalysis | null;
  catalog: V5FormalArtifactCatalog | null;
  selectedChild: V5FormalChildRun | null;
  selectedChildId: string;
  onSelectChild: (childId: string) => void;
  onRebuild?: () => void;
  rebuilding?: boolean;
};

const tabs: Array<{ id: Tab; label: string }> = [
  { id: "overview", label: "① 实验总览" },
  { id: "performance", label: "② 性能与稳定性" },
  { id: "observability", label: "③ 资源与通信" },
  { id: "mechanism", label: "④ 机制分析" },
  { id: "children", label: "⑤ 子实验" },
  { id: "artifacts", label: "⑥ 证据与产物" },
];

export default function V5ResultsDashboard(props: Props) {
  const [tab, setTab] = useState<Tab>("overview");
  const { group, children } = props;
  const context = useMemo(() => experimentContext(group, children), [group, children]);
  return <section className="final-card wide v5-results-dashboard" data-testid="v5-results-dashboard">
    <header className="v5-dashboard-header">
      <div>
        <p className="eyebrow">论文实验结果看板</p>
        <h2>{group.plan?.name ?? group.plan_name ?? "V5 正式实验"}</h2>
        <div className="v5-dashboard-context-line">
          <code>{group.run_group_id}</code>
          <span>{statusText(group.status)}</span>
          <span>{context.suite}</span>
          <span>{context.workload}</span>
          <span>{context.scale}</span>
          <span>{context.topology}</span>
        </div>
      </div>
      <div className="v5-dashboard-header-kpis">
        <HeaderKPI label="论文有效" value={`${context.paperEligible}/${children.length || group.total_child_runs}`} />
        <HeaderKPI label="完成" value={`${group.completed_child_runs}/${group.total_child_runs}`} />
        <HeaderKPI label="方法数" value={String(context.methodCount)} />
      </div>
    </header>
    <nav className="v5-results-tabs" aria-label="V5 结果页面分区">{tabs.map((item) => <button key={item.id} type="button" className={tab === item.id ? "active" : ""} aria-selected={tab === item.id} onClick={() => setTab(item.id)}>{item.label}</button>)}</nav>
    <div className="v5-dashboard-tab-body">
      {tab === "overview" && <Overview group={group} aggregate={props.aggregate} analysis={props.analysis} children={children} />}
      {tab === "performance" && <Performance group={group} analysis={props.analysis} children={children} />}
      {tab === "observability" && <V5ResourceNetworkPanel children={children} />}
      {tab === "mechanism" && <V5MechanismAnalysis children={children} />}
      {tab === "children" && <ChildrenPanel group={group} children={children} selectedChild={props.selectedChild} selectedChildId={props.selectedChildId} onSelectChild={props.onSelectChild} />}
      {tab === "artifacts" && <V5EvidencePanel groupId={group.run_group_id} catalog={props.catalog} children={children} onRebuild={props.onRebuild} rebuilding={props.rebuilding} />}
    </div>
  </section>;
}

function Overview({ group, aggregate, analysis, children }: { group: V5FormalRunGroup; aggregate: V5FormalAggregate | null; analysis: V5FormalAnalysis | null; children: V5FormalChildRun[] }) {
  const base = group.plan?.base_spec;
  const source = base?.workload_source;
  const workloadPlugin = base?.plugin_selections.find((item) => item.category === "workload");
  const blockPlugin = base?.plugin_selections.find((item) => item.category === "block_producer");
  const topology = base?.topology;
  const valid = children.filter((child) => child.status === "completed" && child.individual_result_valid !== false).length;
  const failed = children.filter((child) => child.status === "failed").length;
  const blocked = children.filter((child) => child.status === "blocked").length;
  const invalid = children.filter((child) => child.status === "completed" && child.individual_result_valid === false).length;
  const excluded = children.filter((child) => child.status === "completed" && child.paper_candidate === false && child.individual_result_valid !== false).length;
  const incomplete = sum(children.map((child) => numeric(metric(child, "incomplete_unique_tx_count")) ?? 0));
  const controlledTheta = singleValue(children.map((child) => numeric(child.workload_point?.target_theta ?? child.workload_point?.target_alpha ?? source?.variant_parameters?.target_theta ?? source?.target_alpha)));
  const submitted = range(children.map((child) => numeric(metric(child, "submitted_unique_tx_count"))));
  const terminal = range(children.map((child) => numeric(metric(child, "terminal_unique_tx_count"))));
  const submissionTPS = range(children.map((child) => numeric(metric(child, "observed_submission_tps"))));
  const admissionTPS = range(children.map((child) => numeric(metric(child, "observed_mempool_admission_tps"))));
  const expectedCross = percentRange(children.map((child) => numeric(metric(child, "expected_cross_shard_ratio"))));
  const actualCross = percentRange(children.map((child) => numeric(metric(child, "actual_cross_shard_ratio"))));
  const crossSemanticStatus = directComparisonStatus(group, analysis);
  return <div className="v5-dashboard-overview">
    <section className="v5-dashboard-section">
      <div className="v5-status-kpis">
        <StatusKPI label="子实验完成" value={`${group.completed_child_runs}/${group.total_child_runs}`} tone={group.completed_child_runs === group.total_child_runs ? "ok" : "warn"} />
        <StatusKPI label="论文有效" value={String(aggregate?.paper_valid_count ?? valid)} tone="ok" />
        <StatusKPI label="执行失败" value={String(failed)} tone={failed ? "bad" : "ok"} />
        <StatusKPI label="完成但无效" value={String(invalid)} tone={invalid ? "bad" : "ok"} />
        <StatusKPI label="样本比较受限" value={String(excluded + blocked)} tone={excluded + blocked ? "warn" : "ok"} />
        <StatusKPI label="跨语义比较" value={crossSemanticStatus === false ? "受限" : crossSemanticStatus === true ? "允许" : "未判定"} tone={crossSemanticStatus === true ? "ok" : "warn"} />
        <StatusKPI label="未完成交易" value={formatNumber(incomplete, 0)} tone={incomplete ? "bad" : "ok"} />
      </div>
    </section>
    <section className="v5-dashboard-section">
      <h3>有效性门禁</h3>
      <div className="table-wrap"><table className="v5-validity-table"><thead><tr><th>检查项</th><th>状态</th><th>证据</th></tr></thead><tbody>
        <GateRow label="生命周期完整" state={allTrue(children, "lifecycle_complete")} evidence={terminal === "—" ? "终态证据不可用" : `终态交易 ${terminal}`} />
        <GateRow label="状态根一致" state={allTrue(children, "state_root_consistent")} evidence="各副本一致" />
        <GateRow label="回执根一致" state={allTrue(children, "receipt_root_consistent")} evidence="各副本一致" />
        <GateRow label="执行计划摘要一致" state={allTrue(children, "plan_digest_consistent")} evidence="各副本一致" />
        <GateRow label="区块执行器一致" state={allTrue(children, "block_executor_consistent")} evidence="各副本一致" />
        <GateRow label="无静默回退" state={allTrue(children, "no_fallback")} evidence="未发生静默回退" />
        <GateRow label="产物契约完整" state={children.length > 0 && children.every((child) => child.artifact_status === "complete")} evidence={children.length ? children.map((child) => artifactStatusText(child.artifact_status)).filter((value, index, values) => values.indexOf(value) === index).join("、") : "—"} />
        <GateRow label="公平性校验" state={group.fairness_validation?.passed !== false} evidence={group.fairness_validation?.passed === false ? "未通过" : "已通过 / 未阻断"} />
        <GateRow label="状态等价性" state={group.within_semantic_cohort_state_equivalence_valid !== false && group.pairwise_logical_state_equivalent !== false} evidence="同语义组内校验" />
      </tbody></table></div>
    </section>
    <div className="v5-overview-two-column">
      <section className="v5-dashboard-section">
        <h3>实验配置</h3>
        <table className="v5-key-value-table"><tbody>
          <KV category="工作负载" label="数据集" value={source?.dataset_id ?? workloadPlugin?.plugin_id ?? "合成数据"} />
          <KV category="" label="访问模式" value={accessProfileText(source?.variant_parameters?.access_profile ?? source?.variant_parameters?.profile)} />
          <KV category="" label="受控偏斜 θ" value={controlledTheta === null ? "—" : formatNumber(controlledTheta, 3)} />
          <KV category="" label="交易数" value={base?.tx_count ?? source?.requested_tx_count} />
          <KV category="" label="随机种子" value={group.plan?.seeds.join(", ")} />
          <KV category="" label="重复次数" value={group.plan?.repeats} />
          <KV category="拓扑" label="节点数" value={topology?.nodes} />
          <KV category="" label="分片数" value={topology?.shards} />
          <KV category="" label="每分片验证节点" value={topology?.validators_per_shard} />
          <KV category="运行参数" label="区块大小" value={blockPlugin?.config.block_size} />
          <KV category="" label="出块间隔" value={blockPlugin?.config.interval_ms ? `${blockPlugin.config.interval_ms} ms` : "—"} />
          <KV category="" label="回放模式" value={replayModeText(source?.replay_mode)} />
        </tbody></table>
        <h4>工作线程实际执行配置</h4>
        <div className="v5-worker-truth">{workerTruthRows(group, children).map((item) => <span key={item.id}><strong title={item.name}>{shortMethodLabel(item.id, item.name)}</strong>{`请求 ${item.requested ?? "—"} · 实际 ${item.effective ?? "—"}`}</span>)}</div>
      </section>
      <section className="v5-dashboard-section">
        <h3>工作负载真实性</h3>
        <div className="table-wrap"><table><thead><tr><th>指标</th><th>配置值</th><th>观测值</th></tr></thead><tbody>
          <tr><td>交易数</td><td>{formatNumber(base?.tx_count ?? source?.requested_tx_count, 0)}</td><td>{submitted}</td></tr>
          <tr><td>受控偏斜 θ <V5MetricHelp text="受控构造参数，是正式实验横轴；后验诊断偏斜估计仅用于检查，不能替代该实验轴。" /></td><td>{controlledTheta === null ? "—" : formatNumber(controlledTheta, 3)}</td><td>受控实验轴</td></tr>
          <tr><td>跨分片比例</td><td>{expectedCross !== "—" ? expectedCross : formatPercent(workloadPlugin?.config.cross_shard_ratio ?? source?.variant_parameters?.cross_shard_ratio)}</td><td>{actualCross}</td></tr>
          <tr><td>提交 TPS</td><td>{source?.replay_mode === "fixed_rate" ? formatNumber(source.target_submission_tps, 2) : "最大吞吐"}</td><td>{submissionTPS}</td></tr>
          <tr><td>内存池接纳 TPS</td><td>—</td><td>{admissionTPS}</td></tr>
          <tr><td>终态交易</td><td>{formatNumber(base?.tx_count ?? source?.requested_tx_count, 0)}</td><td>{terminal}</td></tr>
        </tbody></table></div>
      </section>
    </div>
  </div>;
}

function Performance({ group, analysis, children }: { group: V5FormalRunGroup; analysis: V5FormalAnalysis | null; children: V5FormalChildRun[] }) {
  const groups = analysis?.groups ?? [];
  const sensitivity = group.plan?.suites.includes("workload_sensitivity") ?? false;
  const directComparable = directComparisonStatus(group, analysis);
  const cohorts = semanticCohorts(group, children);
  return <div className="v5-dashboard-performance">
    {directComparable !== true && <section className="notice v5-comparison-scope-banner" data-testid="v5-cross-semantic-comparison-warning"><strong>{directComparable === false ? "直接跨语义性能比较：受限" : "直接跨语义性能比较：未判定"}</strong><span>所有完成且通过有效性门禁的子实验仍可作为独立结果；只有同一执行语义组内可以直接计算性能提升、排名或加速比。跨语义组数值仅用于并列观察与机制解释。</span><ul>{cohorts.map((cohort) => <li key={cohort.id}><strong>{cohort.label}</strong>：{cohort.methods.join("、")}</li>)}</ul></section>}
    <V5AnalysisPanel analysis={analysis} />
    {sensitivity && <V5SkewTpsChart children={children} plannedThetaValues={(group.plan?.workload_points ?? []).map((point) => Number(point.target_theta)).filter((value) => Number.isFinite(value))} />}
    <div className="v5-diagnostic-chart-grid">
      <MethodMetricBars title="完成时长" rows={methodRows(children)} metric="completion_duration_ms" unit="ms" lowerBetter />
      <PipelineBars rows={methodRows(children)} />
    </div>
    <section className="v5-dashboard-section">
      <div className="v5-dashboard-heading"><div><h3>性能与稳定性总表</h3><p className="muted">每行按“方法 × 实验条件 × 随机种子”聚合重复运行；变异系数（CV）衡量同一随机种子下的运行波动。</p></div></div>
      <div className="table-wrap"><table><thead><tr><th>方法</th><th>语义组</th><th>随机种子</th><th>样本数</th><th>TPS 均值</th><th>TPS 变异系数</th><th>Student-t 95% 置信区间</th><th>P99 终态延迟</th><th>完成时长</th><th>稳定性</th></tr></thead><tbody>{groups.map((row, index) => {
        const cv = numeric(row.cv_percent_tps ?? row.cv_percent);
        return <tr key={`${String(row.method_config_id)}-${String(row.seed)}-${String(row.scan_value)}-${index}`}><td title={String(row.method_name ?? row.method_config_id ?? "—")}>{shortMethodLabel(String(row.method_config_id ?? ""), String(row.method_name ?? row.method_config_id ?? "—"))}</td><td>{semanticClassLabel(semanticClassForMethod(group, children, String(row.method_config_id ?? "")))}</td><td>{String(row.seed ?? "—")}</td><td>{String(row.sample_count ?? "—")}</td><td>{formatNumber(row.mean_tps, 2)}</td><td>{cv === null ? "—" : `${cv.toFixed(2)}%`}</td><td>{formatCI(row.ci95_low_tps, row.ci95_high_tps)}</td><td>{formatMs(row.mean_p99_ms)}</td><td>{formatMs(row.mean_completion_duration_ms)}</td><td>{cv === null ? "不可计算" : cv <= 5 ? "稳定" : "波动较高"}</td></tr>;
      })}</tbody></table></div>
    </section>
    <PipelineTable children={children} />
  </div>;
}

function MethodMetricBars({ title, rows, metric: key, unit, lowerBetter = false }: { title: string; rows: ReturnType<typeof methodRows>; metric: string; unit: string; lowerBetter?: boolean }) {
  const values = rows.map((row) => row.meanMetric(key));
  const maxValue = Math.max(1, ...values.filter((value): value is number => value !== null));
  return <section className="v5-dashboard-section v5-compact-chart"><h3>{title}</h3><p className="muted">{lowerBetter ? "越低越好" : "越高越好"} · 按已完成且有效的子实验取均值。</p><div className="v5-horizontal-bars">{rows.map((row) => { const value = row.meanMetric(key); return <div className="v5-horizontal-bar-row" key={`${title}-${row.id}`}><span>{row.name}</span><div><i style={{ width: value === null ? "0%" : `${Math.max(1.5, (value / maxValue) * 100)}%` }} /></div><strong>{value === null ? "—" : `${formatNumber(value, 2)} ${unit}`}</strong></div>; })}</div></section>;
}

function PipelineBars({ rows }: { rows: ReturnType<typeof methodRows> }) {
  const keys = ["observed_submission_tps", "observed_mempool_admission_tps", "logical_finality_tps", "end_to_end_tps"] as const;
  const labels: Record<string, string> = { observed_submission_tps: "提交", observed_mempool_admission_tps: "接纳", logical_finality_tps: "逻辑终态", end_to_end_tps: "端到端" };
  const maximum = Math.max(1, ...rows.flatMap((row) => keys.map((key) => row.meanMetric(key))).filter((value): value is number => value !== null));
  return <section className="v5-dashboard-section v5-compact-chart"><h3>流水线吞吐</h3><p className="muted">提交 → 内存池接纳 → 逻辑终态 → 端到端；统一尺度便于识别输入端波动。</p><div className="v5-pipeline-bars">{rows.map((row) => <div className="v5-pipeline-method" key={`pipeline-chart-${row.id}`}><strong>{row.name}</strong>{keys.map((key) => { const value = row.meanMetric(key); return <div className="v5-pipeline-row" key={key}><span>{labels[key]}</span><div><i style={{ width: value === null ? "0%" : `${Math.max(1.5, (value / maximum) * 100)}%` }} /></div><em>{formatNumber(value, 1)}</em></div>; })}</div>)}</div></section>;
}

function PipelineTable({ children }: { children: V5FormalChildRun[] }) {
  const rows = methodRows(children);
  return <section className="v5-dashboard-section"><h3>流水线吞吐诊断</h3><p className="muted">输入端、内存池接纳、逻辑终态与最终端到端吞吐并列，避免把输入负载波动误判为执行器性能。</p><div className="table-wrap"><table><thead><tr><th>方法</th><th>提交 TPS</th><th>内存池接纳 TPS</th><th>逻辑终态 TPS</th><th>端到端 TPS</th></tr></thead><tbody>{rows.map((row) => <tr key={`pipe-${row.id}`}><td>{row.name}</td><td>{formatNumber(row.meanMetric("observed_submission_tps"), 2)}</td><td>{formatNumber(row.meanMetric("observed_mempool_admission_tps"), 2)}</td><td>{formatNumber(row.meanMetric("logical_finality_tps"), 2)}</td><td>{formatNumber(row.meanMetric("end_to_end_tps"), 2)}</td></tr>)}</tbody></table></div></section>;
}

function ChildrenPanel({ group, children, selectedChild, selectedChildId, onSelectChild }: { group: V5FormalRunGroup; children: V5FormalChildRun[]; selectedChild: V5FormalChildRun | null; selectedChildId: string; onSelectChild: (childId: string) => void }) {
  return <section className="v5-dashboard-section" data-testid="v5-dashboard-children"><h3>子实验</h3><p className="muted">主表只保留排查重复运行所需字段；点击行后在下方查看完整子实验依据。</p><div className="table-wrap"><table><thead><tr><th>方法</th><th>随机种子</th><th>重复序号</th><th>实际工作线程</th><th>TPS</th><th>P99</th><th>平均 CPU 核</th><th>峰值 RSS</th><th>网络流量</th><th>消息/终态交易</th><th>最终确认</th><th>有效性</th></tr></thead><tbody>{children.map((child) => {
    const selected = child.child_run_id === selectedChildId;
    return <tr key={child.child_run_id} className={selected ? "selected-row" : ""} onClick={() => onSelectChild(child.child_run_id)}><td><button type="button" className="v5-child-method-button" title={child.method?.display_name ?? child.method_config_id}>{shortMethodLabel(child.method_config_id, child.method?.display_name ?? child.method_config_id)}</button></td><td>{child.seed}</td><td>{child.repeat_index + 1}</td><td>{formatNumber(effectiveWorker(group, child), 0)}</td><td>{formatNumber(metric(child, "end_to_end_tps"), 2)}</td><td>{formatMs(metric(child, "p99_finality_ms"))}</td><td>{formatNumber(metric(child, "average_cluster_cpu_cores"), 3)}</td><td>{formatBytes(metric(child, "cluster_rss_peak_bytes"))}</td><td>{formatBytes(metric(child, "delivered_network_bytes"))}</td><td>{formatNumber(metric(child, "network_messages_per_terminal_tx"), 3)}</td><td>{formatNumber(metric(child, "finalized_unique_logical_tx_count"), 0)}</td><td>{child.individual_result_valid === false ? "结果无效" : child.paper_candidate === false ? "比较受限" : child.status === "completed" ? "有效" : statusText(child.status)}</td></tr>;
  })}</tbody></table></div><div className="v5-child-detail-shell"><V5ChildDetail child={selectedChild} /></div></section>;
}

function HeaderKPI({ label, value }: { label: string; value: string }) { return <div><span>{label}</span><strong>{value}</strong></div>; }
function StatusKPI({ label, value, tone }: { label: string; value: string; tone: "ok" | "warn" | "bad" }) { return <div className={`v5-status-kpi ${tone}`}><span>{label}</span><strong>{value}</strong></div>; }
function GateRow({ label, state, evidence }: { label: string; state: boolean; evidence: string }) { return <tr><td>{label}</td><td><span className={`v5-gate-badge ${state ? "pass" : "fail"}`}>{state ? "✓ 通过" : "✕ 需检查"}</span></td><td>{evidence}</td></tr>; }
function KV({ category, label, value }: { category: string; label: string; value: unknown }) { return <tr><th>{category}</th><td>{label}</td><td>{display(value)}</td></tr>; }

function experimentContext(group: V5FormalRunGroup, children: V5FormalChildRun[]) {
  const source = group.plan?.base_spec.workload_source;
  const topology = group.plan?.base_spec.topology;
  const theta = singleValue(children.map((child) => numeric(child.workload_point?.target_theta ?? child.workload_point?.target_alpha ?? source?.variant_parameters?.target_theta ?? source?.target_alpha)));
  return {
    suite: (group.plan?.suites ?? group.suite_names ?? []).map(suiteText).join("、") || "正式实验",
    workload: [source?.dataset_id ?? sourceTypeText(source?.source_type), theta === null ? "" : `受控 θ=${theta}`].filter(Boolean).join(" · "),
    scale: `${formatNumber(group.plan?.base_spec.tx_count ?? source?.requested_tx_count, 0)} 笔交易`,
    topology: topology ? `${topology.nodes} 节点 · ${topology.shards} 分片 · 每分片 ${topology.validators_per_shard} 验证节点` : "—",
    paperEligible: Number(group.aggregate?.paper_valid_count ?? children.filter((child) => child.paper_candidate !== false && child.individual_result_valid !== false && child.status === "completed").length),
    methodCount: new Set(children.map((child) => child.method_config_id)).size,
  };
}

function methodRows(children: V5FormalChildRun[]) {
  const buckets = new Map<string, V5FormalChildRun[]>();
  for (const child of children) {
    const id = child.method_config_id || child.method?.method_id || "unknown";
    buckets.set(id, [...(buckets.get(id) ?? []), child]);
  }
  return [...buckets.entries()].map(([id, items]) => ({
    id,
    name: shortMethodLabel(id, items[0]?.method?.display_name ?? id),
    worker: formatNumber(mean(items.map((child) => numeric(metric(child, "worker_count") ?? child.topology_point?.worker_count))), 0),
    meanMetric: (key: string) => mean(items.filter((child) => child.status === "completed" && child.individual_result_valid !== false && child.paper_candidate !== false).map((child) => numeric(metric(child, key)))),
  })).sort((a, b) => a.name.localeCompare(b.name));
}

function metric(child: V5FormalChildRun, key: string): unknown {
  const metrics = asRecord(child.metrics);
  if (metrics[key] !== undefined) return metrics[key];
  const summary = asRecord(child.result?.summary);
  if (summary[key] !== undefined) return summary[key];
  const finality = asRecord(summary.finality_evidence);
  if (finality[key] !== undefined) return finality[key];
  const replay = asRecord(summary.workload_replay_summary);
  if (replay[key] !== undefined) return replay[key];
  return undefined;
}

function workerTruthRows(group: V5FormalRunGroup, children: V5FormalChildRun[]) {
  const profile = asRecord(group.formal_experiment_profile);
  const truth = asRecord(profile.worker_execution_truth);
  return methodRows(children).map((item) => {
    const row = asRecord(truth[item.id]);
    const requested = numeric(row.requested_worker_count) ?? numeric(group.plan?.worker_count);
    const effective = effectiveWorkerForMethod(row, children.filter((child) => child.method_config_id === item.id));
    return { id: item.id, name: item.name, requested: requested === null ? null : formatNumber(requested, 0), effective: effective === null ? null : formatNumber(effective, 0) };
  });
}

function effectiveWorkerForMethod(row: Record<string, unknown>, children: V5FormalChildRun[]): number | null {
  const direct = numeric(row.effective_worker_count);
  if (direct !== null) return direct;
  const counts = Array.isArray(row.effective_worker_counts) ? row.effective_worker_counts.map(numeric).filter((value): value is number => value !== null) : [];
  if (counts.length === 1) return counts[0];
  if (children[0]?.method?.plugin_overrides?.block_executor === "serial_block_executor") return 1;
  return mean(children.map((child) => numeric(metric(child, "worker_count") ?? child.topology_point?.worker_count)));
}

function effectiveWorker(group: V5FormalRunGroup, child: V5FormalChildRun): number | null {
  const profile = asRecord(group.formal_experiment_profile);
  const truth = asRecord(asRecord(profile.worker_execution_truth)[child.method_config_id]);
  const direct = numeric(truth.effective_worker_count);
  if (direct !== null) return direct;
  const counts = Array.isArray(truth.effective_worker_counts) ? truth.effective_worker_counts.map(numeric).filter((value): value is number => value !== null) : [];
  if (counts.length === 1) return counts[0];
  if (child.method?.plugin_overrides?.block_executor === "serial_block_executor") return 1;
  return numeric(metric(child, "worker_count") ?? child.topology_point?.worker_count);
}

function directComparisonStatus(group: V5FormalRunGroup, analysis: V5FormalAnalysis | null): boolean | null {
  const values = [
    group.direct_cross_semantic_performance_comparison_valid,
    group.performance_comparison_valid,
    group.fairness_validation?.direct_cross_semantic_performance_comparison_valid,
    group.fairness_validation?.performance_comparison_valid,
    group.state_equivalence_validation?.performance_comparison_valid,
    analysis?.paper_result_analysis?.performance_comparison_valid,
  ].filter((value): value is boolean => typeof value === "boolean");
  if (values.includes(false)) return false;
  if (values.includes(true)) return true;
  return null;
}

function semanticCohorts(group: V5FormalRunGroup, children: V5FormalChildRun[]) {
  const groups = new Map<string, string[]>();
  for (const child of children) {
    const id = semanticClassForChild(group, child);
    const method = shortMethodLabel(child.method_config_id, child.method?.display_name ?? child.method_config_id);
    groups.set(id, [...new Set([...(groups.get(id) ?? []), method])]);
  }
  return [...groups.entries()].map(([id, methods]) => ({ id, label: semanticClassLabel(id), methods }));
}

function semanticClassForMethod(group: V5FormalRunGroup, children: V5FormalChildRun[], methodId: string): string {
  const child = children.find((item) => item.method_config_id === methodId);
  return child ? semanticClassForChild(group, child) : "unknown";
}

function semanticClassForChild(group: V5FormalRunGroup, child: V5FormalChildRun): string {
  if (child.comparison_semantics_class) return String(child.comparison_semantics_class);
  const cohorts = group.state_equivalence_validation?.cohorts ?? [];
  for (const raw of cohorts) {
    const cohort = asRecord(raw);
    const childIds = Array.isArray(cohort.child_run_ids) ? cohort.child_run_ids.map(String) : [];
    if (childIds.includes(child.child_run_id) && cohort.comparison_semantics_class) return String(cohort.comparison_semantics_class);
  }
  return "unknown";
}

function semanticClassLabel(value: string): string {
  return ({
    stateful_local_legacy_v1: "有状态本地串行化语义",
    nezha_acg_hs_abortable_v1: "Nezha/ACG HS 可中止语义",
    bsx_deterministic_coloring_serializable_v1: "BSX 确定性图着色串行化语义",
    batch_si_common_batch_snapshot_v1: "Batch-SI 共享批快照语义",
    groundhog_typed_commutative_snapshot_v1: "Groundhog 类型化可交换快照语义",
  } as Record<string, string>)[value] ?? (value === "unknown" ? "未标注语义组" : value);
}

function suiteText(value: string): string {
  return ({ main_experiment: "主实验", comparison_experiment: "方法对比", ablation_experiment: "消融实验", workload_sensitivity: "工作负载敏感性", topology_scaling: "拓扑扩展", fault_recovery_experiment: "故障恢复" } as Record<string, string>)[value] ?? value;
}
function accessProfileText(value: unknown): string { const key = String(value ?? ""); return ({ balanced: "均衡读写", read_heavy: "读密集", "read-heavy": "读密集", write_heavy: "写密集", "write-heavy": "写密集" } as Record<string, string>)[key] ?? (key || "—"); }
function replayModeText(value: unknown): string { const key = String(value ?? ""); return ({ max_throughput: "最大吞吐", fixed_rate: "固定速率" } as Record<string, string>)[key] ?? (key || "—"); }
function artifactStatusText(value: unknown): string { const key = String(value ?? ""); return ({ complete: "完整", incomplete: "不完整", pending: "等待中", unavailable: "不可用" } as Record<string, string>)[key] ?? (key || "—"); }
function sourceTypeText(value: unknown): string { return value === "dataset" ? "真实/受控数据集" : value === "synthetic" ? "合成数据" : String(value ?? "合成数据"); }
function displayMethodName(value: string): string { return value.replace(/Stateful Hash/gi, "有状态 Hash").replace(/Stateless Hash/gi, "无状态 Hash").replace(/with Block-STM backend/gi, "+ Block-STM"); }
function shortMethodLabel(methodId: string, value: string): string {
  const id = methodId.toLowerCase();
  if (id === "hash_serial") return "Serial";
  if (id === "hash_block_stm") return "Block-STM";
  if (id === "hash_aria") return "Aria";
  if (id === "hash_groundhog") return "Groundhog";
  if (id === "hash_cg") return "CG";
  if (id === "hash_acg") return "ACG/Nezha";
  if (id === "hash_bsx") return "BSX";
  if (id === "hash_batch_si") return "Batch-SI";
  if (id.includes("metatrack") && id.includes("block_stm")) return "MetaTrack + Block-STM";
  if (id.includes("metatrack")) return "MetaTrack";
  if (id.includes("stateless") && id.includes("block_stm")) return "无状态 Block-STM";
  if (id.includes("stateless") && id.includes("serial")) return "无状态 Serial";
  const lower = value.toLowerCase();
  if (lower.includes("address conflict graph") || lower.includes("acg/nezha")) return "ACG/Nezha";
  if (lower.includes("batch-schedule-execute") || lower.includes("(bsx)")) return "BSX";
  if (lower.includes("conflict graph") && !lower.includes("address")) return "CG";
  if (lower.includes("batch-si")) return "Batch-SI";
  if (lower.includes("groundhog")) return "Groundhog";
  if (lower.includes("aria")) return "Aria";
  if (lower.includes("block-stm")) return "Block-STM";
  if (lower.includes("serial")) return "Serial";
  return displayMethodName(value);
}
function percentRange(values: Array<number | null>): string { const valid = values.filter((value): value is number => value !== null); if (!valid.length) return "—"; const low = Math.min(...valid) * 100; const high = Math.max(...valid) * 100; return Math.abs(high - low) < 1e-9 ? `${formatNumber(low, 2)}%` : `${formatNumber(low, 2)}% – ${formatNumber(high, 2)}%`; }

function allTrue(children: V5FormalChildRun[], key: string): boolean { return children.length > 0 && children.filter((child) => child.status === "completed").every((child) => metric(child, key) === true); }
function asRecord(value: unknown): Record<string, unknown> { return value && typeof value === "object" && !Array.isArray(value) ? value as Record<string, unknown> : {}; }
function numeric(value: unknown): number | null { if (value === null || value === undefined || value === "" || typeof value === "boolean") return null; const number = Number(value); return Number.isFinite(number) ? number : null; }
function mean(values: Array<number | null>): number | null { const valid = values.filter((value): value is number => value !== null); return valid.length ? valid.reduce((sum, value) => sum + value, 0) / valid.length : null; }
function sum(values: number[]): number { return values.reduce((total, value) => total + value, 0); }
function singleValue(values: Array<number | null>): number | null { const unique = [...new Set(values.filter((value): value is number => value !== null))]; return unique.length === 1 ? unique[0] : null; }
function range(values: Array<number | null>): string { const valid = values.filter((value): value is number => value !== null); if (!valid.length) return "—"; const low = Math.min(...valid); const high = Math.max(...valid); return Math.abs(high - low) < 1e-9 ? formatNumber(low, 2) : `${formatNumber(low, 2)} – ${formatNumber(high, 2)}`; }
function display(value: unknown): string { return value === null || value === undefined || value === "" ? "—" : typeof value === "number" ? formatNumber(value, 3) : String(value); }
function formatNumber(value: unknown, digits = 2): string { const number = numeric(value); return number === null ? "—" : number.toLocaleString(undefined, { maximumFractionDigits: digits }); }
function formatMs(value: unknown): string { const number = numeric(value); return number === null ? "—" : `${number.toLocaleString(undefined, { maximumFractionDigits: 2 })} ms`; }
function formatCI(low: unknown, high: unknown): string { const l = numeric(low); const h = numeric(high); return l === null || h === null ? "不可计算" : `${l.toFixed(2)} – ${h.toFixed(2)}`; }
function formatBytes(value: unknown): string { const number = numeric(value); if (number === null) return "—"; const units = ["B", "KiB", "MiB", "GiB"]; let current = number; let index = 0; while (current >= 1024 && index < units.length - 1) { current /= 1024; index += 1; } return `${current.toFixed(index === 0 ? 0 : 2)} ${units[index]}`; }
function formatPercent(value: unknown): string { const number = numeric(value); return number === null ? "—" : `${(number * 100).toFixed(2)}%`; }
function statusText(status: string): string { return ({ completed: "✓ 已完成", completed_with_failures: "⚠ 部分失败", running: "运行中", queued: "排队中", failed: "✕ 失败", cancelled: "已取消" } as Record<string, string>)[status] ?? status; }
