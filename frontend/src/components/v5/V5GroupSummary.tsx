import type { V5FormalAggregate, V5FormalChildRun, V5FormalRunGroup } from "../../api";

type Props = { group: V5FormalRunGroup; aggregate: V5FormalAggregate | null; children: V5FormalChildRun[] };

export default function V5GroupSummary({ group, aggregate, children }: Props) {
  const base = group.plan?.base_spec;
  const workload = base?.plugin_selections.find((item) => item.category === "workload");
  const topology = base?.topology;
  const failedOrBlocked = children.filter((item) => item.status === "failed" || item.status === "blocked").length;
  const completedInvalid = children.filter((item) => item.status === "completed" && item.individual_result_valid === false).length;
  const completedValid = children.filter((item) => item.status === "completed" && item.individual_result_valid !== false).length;
  const performanceComparisonValid = group.direct_cross_semantic_performance_comparison_valid ?? group.performance_comparison_valid ?? group.fairness_validation?.performance_comparison_valid;
  const stateEquivalent = group.within_semantic_cohort_state_equivalence_valid ?? group.pairwise_logical_state_equivalent ?? group.state_equivalence_validation?.within_semantic_cohort_state_equivalence_valid ?? group.state_equivalence_validation?.pairwise_logical_state_equivalent;
  return <section className="final-card wide" data-testid="v5-group-summary">
    <h2>实验组摘要</h2>
    <dl className="stage-flow-kpis">
      <Metric label="实验组" value={group.run_group_id} /><Metric label="计划名称" value={group.plan?.name ?? group.plan_name} />
      <Metric label="状态" value={group.status} /><Metric label="执行后端" value={group.execution_backend} />
      <Metric label="运行时真实性" value={group.runtime_truth} /><Metric label="子实验" value={`${group.completed_child_runs}/${group.total_child_runs}`} />
      <Metric label="执行失败或阻止" value={children.length ? failedOrBlocked : undefined} />
      <Metric label="完成但结果无效" value={children.length ? completedInvalid : undefined} />
      <Metric label="完成且结果有效" value={children.length ? completedValid : undefined} />
      <Metric label="实验类型" value={(group.plan?.suites ?? group.suite_names)?.join(", ")} /><Metric label="方法" value={group.plan?.methods.map((item) => item.display_name).join(", ") ?? group.method_names?.join(", ")} />
      <Metric label="随机种子" value={group.plan?.seeds.join(", ")} /><Metric label="重复次数" value={group.plan?.repeats} />
      <Metric label="创建时间" value={group.created_at} /><Metric label="更新时间" value={group.updated_at} />
      <Metric label="基础拓扑" value={topology ? `${topology.nodes}/${topology.shards}/${topology.validators_per_shard}` : undefined} />
      <Metric label="基础交易数量" value={base?.tx_count} /><Metric label="负载插件" value={workload?.plugin_id} />
      <Metric label="跨片交易比例" value={workload?.config.cross_shard_ratio} /><Metric label="超时间隔" value={workload?.config.timeout_every} />
    </dl>
    {performanceComparisonValid === false && <div className="notice" data-testid="v5-performance-incomparable"><strong>性能比较不可直接使用</strong><span>执行语义、公平性或跨方法最终状态等价门未通过。包含有状态与无状态方法时，请使用 Stateless Hash 与 MetaTrack 进行直接性能比较；同语义方法还必须具有相同初始状态、状态归属映射和最终全局状态。</span></div>}
    {stateEquivalent === false && <div className="notice" data-testid="v5-state-incomparable"><strong>跨方法状态不等价</strong><span>至少一个可比较方法组的初始状态、状态归属映射或最终全局状态摘要不一致，本轮性能结论已被禁止进入论文分析。</span></div>}
    <p className="muted">Local multi-process, localhost TCP, PBFT-style, signed transactions, persistent local state, and no silent fallback. This is not a production blockchain or production PBFT claim.</p>
    <div data-testid="v5-group-aggregate"><h3>论文有效结果聚合</h3><p className="muted">平均值、置信区间和图表默认只使用 paper_eligible 样本。完成但无效和兼容性阻止的结果保留为原始观察证据，不进入论文比较。</p><dl className="stage-flow-kpis">
      <Metric label="同语义论文候选数" value={aggregate?.paper_valid_count ?? aggregate?.count} /><Metric label="跨语义汇总平均 TPS" value={performanceComparisonValid ? aggregate?.mean : undefined} /><Metric label="跨语义汇总中位 TPS" value={performanceComparisonValid ? aggregate?.median : undefined} />
      <Metric label="跨语义汇总标准差" value={performanceComparisonValid ? aggregate?.std : undefined} /><Metric label="跨语义最小 / 最大" value={performanceComparisonValid && aggregate ? `${display(aggregate.min)} / ${display(aggregate.max)}` : undefined} />
      <Metric label="跨语义 95% 置信区间" value={performanceComparisonValid && aggregate ? `${display(aggregate.ci95_low)} / ${display(aggregate.ci95_high)}` : undefined} />
      <Metric label="观察到的完成数" value={aggregate?.observed_completed_count} /><Metric label="完成但无效" value={aggregate?.completed_invalid_count} />
      <Metric label="兼容性阻止" value={aggregate?.blocked_count} /><Metric label="执行失败" value={aggregate?.execution_failed_count ?? aggregate?.failed_count} /><Metric label="缺失" value={aggregate?.missing_count} />
    </dl></div>
  </section>;
}

function Metric({ label, value }: { label: string; value: unknown }) { return <div data-testid={`v5-group-${legacySlug(label) ?? slug(label)}`}><dt>{label}</dt><dd>{display(value)}</dd></div>; }
function display(value: unknown): string { return value === undefined || value === null || value === "" ? "-" : typeof value === "number" ? value.toLocaleString(undefined, { maximumFractionDigits: 3 }) : String(value); }
function slug(value: string): string { return value.toLowerCase().replace(/[^a-z0-9]+/g, "-").replace(/(^-|-$)/g, ""); }
function legacySlug(label: string): string | undefined { return ({ "实验组": "rungroup", "状态": "status", "执行后端": "backend", "子实验": "children" } as Record<string, string>)[label]; }
