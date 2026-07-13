import type { V5FormalAggregate, V5FormalChildRun, V5FormalRunGroup } from "../../api";

type Props = { group: V5FormalRunGroup; aggregate: V5FormalAggregate | null; children: V5FormalChildRun[] };

export default function V5GroupSummary({ group, aggregate, children }: Props) {
  const base = group.plan?.base_spec;
  const workload = base?.plugin_selections.find((item) => item.category === "workload");
  const topology = base?.topology;
  const failedOrBlocked = children.filter((item) => item.status === "failed" || item.status === "blocked").length;
  return <section className="final-card wide" data-testid="v5-group-summary">
    <h2>RunGroup Summary</h2>
    <dl className="stage-flow-kpis">
      <Metric label="RunGroup" value={group.run_group_id} /><Metric label="Plan name" value={group.plan?.name} />
      <Metric label="Status" value={group.status} /><Metric label="Backend" value={group.execution_backend} />
      <Metric label="Runtime truth" value={group.runtime_truth} /><Metric label="Children" value={`${group.completed_child_runs}/${group.total_child_runs}`} />
      <Metric label="Failed / blocked children" value={children.length ? failedOrBlocked : undefined} />
      <Metric label="Suites" value={group.plan?.suites.join(", ")} /><Metric label="Methods" value={group.plan?.methods.map((item) => item.display_name).join(", ")} />
      <Metric label="Seeds" value={group.plan?.seeds.join(", ")} /><Metric label="Repeats" value={group.plan?.repeats} />
      <Metric label="Created" value={group.created_at} /><Metric label="Updated" value={group.updated_at} />
      <Metric label="Topology" value={topology ? `${topology.nodes}/${topology.shards}/${topology.validators_per_shard}` : undefined} />
      <Metric label="Base tx count" value={base?.tx_count} /><Metric label="Workload" value={workload?.plugin_id} />
      <Metric label="Cross shard ratio" value={workload?.config.cross_shard_ratio} /><Metric label="Timeout every" value={workload?.config.timeout_every} />
    </dl>
    <p className="muted">Local multi-process, localhost TCP, PBFT-style, signed transactions, persistent local state, and no silent fallback. This is not a production blockchain or production PBFT claim.</p>
    <div data-testid="v5-group-aggregate"><h3>Aggregate</h3><dl className="stage-flow-kpis">
      <Metric label="Count" value={aggregate?.count} /><Metric label="Mean TPS" value={aggregate?.mean} /><Metric label="Median TPS" value={aggregate?.median} />
      <Metric label="Std" value={aggregate?.std} /><Metric label="Min / max" value={aggregate ? `${display(aggregate.min)} / ${display(aggregate.max)}` : undefined} />
      <Metric label="CI95" value={aggregate ? `${display(aggregate.ci95_low)} / ${display(aggregate.ci95_high)}` : undefined} />
      <Metric label="Completed" value={aggregate?.completed_count} /><Metric label="Aggregate failed" value={aggregate?.failed_count} /><Metric label="Missing" value={aggregate?.missing_count} />
    </dl></div>
  </section>;
}

function Metric({ label, value }: { label: string; value: unknown }) { return <div data-testid={`v5-group-${slug(label)}`}><dt>{label}</dt><dd>{display(value)}</dd></div>; }
function display(value: unknown): string { return value === undefined || value === null || value === "" ? "-" : typeof value === "number" ? value.toLocaleString(undefined, { maximumFractionDigits: 3 }) : String(value); }
function slug(value: string): string { return value.toLowerCase().replace(/[^a-z0-9]+/g, "-").replace(/(^-|-$)/g, ""); }
