import type { V5FormalAggregate, V5FormalChildRun, V5FormalRunGroup } from "../../api";

type Props = { group: V5FormalRunGroup; aggregate: V5FormalAggregate | null; children: V5FormalChildRun[] };

export default function V5GroupSummary({ group, aggregate, children }: Props) {
  const base = group.plan?.base_spec;
  const workload = base?.plugin_selections.find((item) => item.category === "workload");
  const topology = base?.topology;
  const failedOrBlocked = children.filter((item) => item.status === "failed" || item.status === "blocked").length;
  return <section className="final-card wide" data-testid="v5-group-summary">
    <h2>实验组摘要</h2>
    <dl className="stage-flow-kpis">
      <Metric label="实验组" value={group.run_group_id} /><Metric label="计划名称" value={group.plan?.name ?? group.plan_name} />
      <Metric label="状态" value={group.status} /><Metric label="执行后端" value={group.execution_backend} />
      <Metric label="运行时真实性" value={group.runtime_truth} /><Metric label="子实验" value={`${group.completed_child_runs}/${group.total_child_runs}`} />
      <Metric label="失败或阻止的子实验" value={children.length ? failedOrBlocked : undefined} />
      <Metric label="实验类型" value={(group.plan?.suites ?? group.suite_names)?.join(", ")} /><Metric label="方法" value={group.plan?.methods.map((item) => item.display_name).join(", ") ?? group.method_names?.join(", ")} />
      <Metric label="随机种子" value={group.plan?.seeds.join(", ")} /><Metric label="重复次数" value={group.plan?.repeats} />
      <Metric label="创建时间" value={group.created_at} /><Metric label="更新时间" value={group.updated_at} />
      <Metric label="基础拓扑" value={topology ? `${topology.nodes}/${topology.shards}/${topology.validators_per_shard}` : undefined} />
      <Metric label="基础交易数量" value={base?.tx_count} /><Metric label="负载插件" value={workload?.plugin_id} />
      <Metric label="跨片交易比例" value={workload?.config.cross_shard_ratio} /><Metric label="超时间隔" value={workload?.config.timeout_every} />
    </dl>
    <p className="muted">Local multi-process, localhost TCP, PBFT-style, signed transactions, persistent local state, and no silent fallback. This is not a production blockchain or production PBFT claim.</p>
    <div data-testid="v5-group-aggregate"><h3>聚合结果</h3><dl className="stage-flow-kpis">
      <Metric label="样本数" value={aggregate?.count} /><Metric label="平均 TPS" value={aggregate?.mean} /><Metric label="中位 TPS" value={aggregate?.median} />
      <Metric label="标准差" value={aggregate?.std} /><Metric label="最小 / 最大" value={aggregate ? `${display(aggregate.min)} / ${display(aggregate.max)}` : undefined} />
      <Metric label="95% 置信区间" value={aggregate ? `${display(aggregate.ci95_low)} / ${display(aggregate.ci95_high)}` : undefined} />
      <Metric label="已完成" value={aggregate?.completed_count} /><Metric label="聚合失败" value={aggregate?.failed_count} /><Metric label="缺失" value={aggregate?.missing_count} />
    </dl></div>
  </section>;
}

function Metric({ label, value }: { label: string; value: unknown }) { return <div data-testid={`v5-group-${legacySlug(label) ?? slug(label)}`}><dt>{label}</dt><dd>{display(value)}</dd></div>; }
function display(value: unknown): string { return value === undefined || value === null || value === "" ? "-" : typeof value === "number" ? value.toLocaleString(undefined, { maximumFractionDigits: 3 }) : String(value); }
function slug(value: string): string { return value.toLowerCase().replace(/[^a-z0-9]+/g, "-").replace(/(^-|-$)/g, ""); }
function legacySlug(label: string): string | undefined { return ({ "实验组": "rungroup", "状态": "status", "执行后端": "backend", "子实验": "children" } as Record<string, string>)[label]; }
