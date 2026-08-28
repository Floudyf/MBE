// MBE_V5_RESULTS_UI_FINAL_CN_CLOSURE_20260814_V4
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
  const serialOracleValid = group.cross_method_serial_order_oracle_valid ?? group.state_equivalence_validation?.cross_method_serial_order_oracle_valid;
  const partialRun = children.length < group.total_child_runs || group.completed_child_runs < group.total_child_runs || failedOrBlocked > 0 || completedInvalid > 0 || group.status !== "completed";
  return <section className="final-card wide" data-testid="v5-group-summary">
    <h2>实验组摘要</h2>
    <dl className="stage-flow-kpis">
      <Metric label="实验组" value={group.run_group_id} /><Metric label="计划名称" value={group.plan?.name ?? group.plan_name} />
      <Metric label="状态" value={statusText(group.status)} /><Metric label="执行后端" value={backendText(group.execution_backend)} />
      <Metric label="运行时真实性" value={runtimeTruthText(group.runtime_truth)} /><Metric label="子实验" value={`${group.completed_child_runs}/${group.total_child_runs}`} />
      <Metric label="执行失败或阻止" value={children.length ? failedOrBlocked : undefined} />
      <Metric label="完成但结果无效" value={children.length ? completedInvalid : undefined} />
      <Metric label="完成且结果有效" value={children.length ? completedValid : undefined} />
      <Metric label="实验类型" value={(group.plan?.suites ?? group.suite_names)?.map(suiteText).join(", ")} /><Metric label="方法" value={group.plan?.methods.map((item) => item.display_name).join(", ") ?? group.method_names?.join(", ")} />
      <Metric label="随机种子" value={group.plan?.seeds.join(", ")} /><Metric label="重复次数" value={group.plan?.repeats} />
      <Metric label="创建时间" value={timestampDisplay(group.created_at)} /><Metric label="更新时间" value={timestampDisplay(group.updated_at)} />
      <Metric label="基础拓扑" value={topology ? `${topology.nodes}/${topology.shards}/${topology.validators_per_shard}` : undefined} />
      <Metric label="基础交易数量" value={base?.tx_count} /><Metric label="负载插件" value={workload?.plugin_id} />
      <Metric label="跨片交易比例" value={workload?.config.cross_shard_ratio} /><Metric label="超时间隔" value={workload?.config.timeout_every} />
    </dl>
    {partialRun && <div className="notice" data-testid="v5-partial-run-warning"><strong>部分完成 / 尚未达到论文完整结果要求</strong><span>正式矩阵尚未全部成功完成；图表会保留计划中的缺失点，但本组不能作为完整论文结果。</span></div>}
    {performanceComparisonValid === false && <div className="notice" data-testid="v5-performance-incomparable"><strong>性能比较不可直接使用</strong><span>直接跨语义性能比较受限。共同外部合同要求相同初始状态、相同逻辑工作负载，并要求每种方法的实际最终状态都等于其自身观测提交顺序的 Serial Oracle 重放结果；不同合法串行化顺序不要求最终 state digest 相同。</span></div>}
    {stateEquivalent === false && <div className="notice" data-testid="v5-state-incomparable"><strong>同语义状态等价性未通过</strong><span>至少一个相同内部执行语义的可比较方法组未通过既有状态等价性检查。</span></div>}
    {serialOracleValid === false && <div className="notice" data-testid="v5-serial-oracle-incomparable"><strong>跨方法 Serial Oracle 未通过</strong><span>至少一种方法不能由其实际提交顺序的串行重放精确复现，因此跨内部执行语义的性能比较被禁止。</span></div>}
    <p className="muted">本地多进程、localhost TCP、PBFT 风格共识消息、签名交易、持久化本地状态，并且无静默回退。本实验环境不声明为生产级区块链或生产级 PBFT 实现。</p>
    <div data-testid="v5-group-aggregate"><h3>论文有效结果聚合</h3><p className="muted">平均值、置信区间和图表默认只使用论文有效（paper_eligible）样本。完成但无效和兼容性阻止的结果保留为原始观察证据，不进入论文比较。</p><dl className="stage-flow-kpis">
      <Metric label="同语义论文候选数" value={aggregate?.paper_valid_count ?? aggregate?.count} /><Metric label="跨语义汇总平均 TPS" value={performanceComparisonValid ? aggregate?.mean : undefined} /><Metric label="跨语义汇总中位 TPS" value={performanceComparisonValid ? aggregate?.median : undefined} />
      <Metric label="跨语义汇总标准差" value={performanceComparisonValid ? aggregate?.std : undefined} /><Metric label="跨语义最小 / 最大" value={performanceComparisonValid && aggregate ? `${display(aggregate.min)} / ${display(aggregate.max)}` : undefined} />
      <Metric label="跨语义 95% 置信区间" value={performanceComparisonValid && aggregate ? `${display(aggregate.ci95_low)} / ${display(aggregate.ci95_high)}` : undefined} />
      <Metric label="观察到的完成数" value={aggregate?.observed_completed_count} /><Metric label="完成但无效" value={aggregate?.completed_invalid_count} />
      <Metric label="兼容性阻止" value={aggregate?.blocked_count} /><Metric label="执行失败" value={aggregate?.execution_failed_count ?? aggregate?.failed_count} /><Metric label="缺失" value={aggregate?.missing_count} />
    </dl></div>
  </section>;
}

function statusText(value: string): string { return ({ completed: "已完成", completed_with_failures: "完成但有失败", running: "运行中", queued: "排队中", starting: "启动中", failed: "失败", cancelled: "已取消", timed_out: "超时" } as Record<string, string>)[value] ?? value; }
function backendText(value: string): string { return ({ real_cluster: "真实集群", simulation: "仿真", preview: "预览" } as Record<string, string>)[value] ?? value; }
function runtimeTruthText(value: string): string { return ({ v5_real_cluster_candidate: "V5 真实集群候选环境", local_multi_process: "本地多进程", real_cluster: "真实集群", production: "生产环境" } as Record<string, string>)[value] ?? value; }
function suiteText(value: string): string { return ({ main_experiment: "主实验", comparison_experiment: "方法对比", ablation_experiment: "消融实验", workload_sensitivity: "工作负载敏感性", topology_scaling: "拓扑扩展", fault_recovery_experiment: "故障恢复" } as Record<string, string>)[value] ?? value; }

function timestampDisplay(value: string | undefined | null): string | undefined {
  if (!value) return undefined;
  const parsed = new Date(value);
  return Number.isNaN(parsed.getTime()) ? value : parsed.toLocaleString();
}

function Metric({ label, value }: { label: string; value: unknown }) { return <div data-testid={`v5-group-${legacySlug(label) ?? slug(label)}`}><dt>{label}</dt><dd>{display(value)}</dd></div>; }
function display(value: unknown): string { return value === undefined || value === null || value === "" ? "-" : typeof value === "number" ? value.toLocaleString(undefined, { maximumFractionDigits: 3 }) : String(value); }
function slug(value: string): string { return value.toLowerCase().replace(/[^a-z0-9]+/g, "-").replace(/(^-|-$)/g, ""); }
function legacySlug(label: string): string | undefined { return ({ "实验组": "rungroup", "状态": "status", "执行后端": "backend", "子实验": "children" } as Record<string, string>)[label]; }
