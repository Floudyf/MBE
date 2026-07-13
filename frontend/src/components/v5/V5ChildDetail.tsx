import { useState } from "react";

import { v5RealClusterArtifactURL, type V5FinalityEvidence, type V5FormalChildRun, type V5RealClusterSummary, type V5RuntimeArtifact } from "../../api";

const preferred = ["real_cluster_summary.json", "finality_summary.json", "transaction_lifecycle.csv", "transaction_lifecycle.jsonl", "transaction_finality.csv", "latency_distribution.csv", "throughput_windows.csv", "drain_status.json", "client_receipt_log.csv", "compiled_run_plan.json", "supervisor_stdout.log", "supervisor_stderr.log"];

export default function V5ChildDetail({ child }: { child: V5FormalChildRun | null }) {
  const [query, setQuery] = useState("");
  if (!child) return <section className="final-card wide" data-testid="v5-child-detail"><p className="muted">Select a Child Run.</p></section>;
  const summary = child.result?.summary;
  const finality = summary?.finality_evidence;
  const metrics = child.metrics ?? {};
  const artifacts = (child.result?.artifacts ?? []).filter((item) => item.name.toLowerCase().includes(query.toLowerCase()));
  return <section className="final-card wide" data-testid="v5-child-detail">
    <h2>Child Run Detail</h2>
    <h3>Run Identity</h3><Grid values={[["Child", child.child_run_id], ["Run", child.result?.run_id], ["Suite", child.suite_type], ["Method", child.method.display_name], ["Seed", child.seed], ["Repeat", child.repeat_index + 1], ["Attempt", child.attempt], ["Topology", `${child.topology_point.nodes}/${child.topology_point.shards}/${child.topology_point.validators_per_shard}`], ["tx count", child.estimated_transactions], ["Status", child.status], ["Paper candidate", child.paper_candidate], ["Comparison group", child.comparison_group_id], ["Scan", `${child.scan_variable || "-"}: ${child.scan_value || "-"}`]]} />
    <h3>Performance</h3><Grid values={[["Throughput TPS", metrics.throughput_tps ?? finality?.throughput_tps], ["P50", metrics.p50_latency_ms ?? finality?.p50_finality_ms], ["P95", metrics.p95_latency_ms ?? finality?.p95_finality_ms], ["P99", metrics.p99_latency_ms ?? finality?.p99_finality_ms], ["Finalized", metrics.finalized_tx_count], ["Lifecycle complete", metrics.lifecycle_complete], ["Missing", Array.isArray(metrics.missing) ? metrics.missing.join(", ") : metrics.missing]]} />
    <div data-testid="v5-finality-summary"><h3>Finality</h3><Grid values={finalityRows(finality)} /></div>
    <div data-testid="v5-runtime-evidence"><h3>Runtime Evidence</h3><Grid values={runtimeRows(summary)} /><p className="muted">runtime_stage={value(summary?.runtime_stage)}; runtime_truth={value(summary?.runtime_truth)}; production_blockchain=false; production_pbft=false.</p></div>
    {child.error && <p className="file-error">{child.error}</p>}
    <div data-testid="v5-child-artifact-catalog"><h3>Child Runtime Artifacts</h3><label><span>Search</span><input aria-label="child artifact search" value={query} onChange={(event) => setQuery(event.target.value)} /></label><ArtifactCatalog artifacts={artifacts} /></div>
  </section>;
}

function Grid({ values }: { values: Array<[string, unknown]> }) { return <dl className="stage-flow-kpis">{values.map(([label, item]) => <div key={label} data-testid={`v5-metric-${slug(label)}`}><dt>{label}</dt><dd>{value(item)}</dd></div>)}</dl>; }
function value(item: unknown): string { return item === undefined || item === null || item === "" ? "-" : typeof item === "boolean" ? String(item) : typeof item === "number" ? item.toLocaleString(undefined, { maximumFractionDigits: 3 }) : String(item); }
function finalityRows(item: V5FinalityEvidence | undefined): Array<[string, unknown]> { return [["Submitted", item?.submitted_unique_tx_count], ["Terminal", item?.terminal_unique_tx_count], ["Incomplete", item?.incomplete_unique_tx_count], ["Finalized", item?.finalized_unique_logical_tx_count], ["Intra committed", item?.intra_shard_committed_unique_count], ["Intra terminal", item?.intra_shard_terminal_unique_count], ["Cross requested", item?.cross_shard_requested_unique_count], ["Cross target committed", item?.cross_shard_target_committed_unique_count], ["Cross finalized", item?.cross_shard_finalized_unique_count], ["Cross refunded", item?.cross_shard_refunded_unique_count], ["Cross failed", item?.cross_shard_failed_unique_count], ["Metric truth", item?.metric_truth], ["TCP send latency excluded", item?.tcp_send_latency_excluded]]; }
function runtimeRows(source: V5RealClusterSummary | undefined): Array<[string, unknown]> { return [["Ready to commit", source?.ready_to_commit], ["One node / OS process", source?.one_node_one_os_process], ["Independent TCP ports", source?.independent_tcp_ports], ["All shards active", source?.all_shards_active], ["Multiple blocks per shard", source?.per_shard_multiple_blocks], ["Real client submission", source?.real_client_submission], ["Real cross-shard network", source?.real_cross_shard_network], ["PBFT-style messages", source?.real_pbft_style_messages], ["Real signed tx", source?.real_signed_tx], ["Persistent state", source?.persistent_state], ["Plugin-driven runtime", source?.plugin_driven_runtime], ["State root consistent", source?.state_root_consistent], ["No fallback", source?.no_fallback], ["Orphan processes", source?.orphan_process_count], ["Distinct / expected processes", source ? `${value(source.distinct_process_count)} / ${value(source.expected_process_count)}` : undefined], ["Shard count", source?.shard_count], ["Shard blocks", source?.shard_blocks ? JSON.stringify(source.shard_blocks) : undefined]]; }
function ArtifactCatalog({ artifacts }: { artifacts: V5RuntimeArtifact[] }) {
  const ordered = [...artifacts].sort((a, b) => rank(a.name) - rank(b.name) || a.name.localeCompare(b.name));
  const key = ordered.filter((item) => rank(item.name) !== Number.MAX_SAFE_INTEGER);
  const other = ordered.filter((item) => rank(item.name) === Number.MAX_SAFE_INTEGER);
  return <>{key.length ? <ArtifactTable title="Key artifacts" artifacts={key} /> : null}<details open={other.length > 0}><summary>All other artifacts ({other.length})</summary>{grouped(other).map(([title, items]) => <ArtifactTable key={title} title={title} artifacts={items} />)}</details>{!ordered.length && <p className="muted">No matching runtime artifacts.</p>}</>;
}
function ArtifactTable({ title, artifacts }: { title: string; artifacts: V5RuntimeArtifact[] }) { return <section><h4>{title}</h4><div className="table-wrap"><table><thead><tr><th>Artifact</th><th>Category</th><th>Bytes</th><th>Download</th></tr></thead><tbody>{artifacts.map((item) => <tr key={item.download_url}><td>{item.name}</td><td>{item.truth_category}</td><td>{item.size_bytes}</td><td><a href={v5RealClusterArtifactURL(item.download_url)}>Download</a></td></tr>)}</tbody></table></div></section>; }
function rank(name: string): number { const index = preferred.indexOf(name); return index >= 0 ? index : Number.MAX_SAFE_INTEGER; }
function grouped(items: V5RuntimeArtifact[]): Array<[string, V5RuntimeArtifact[]]> { const labels = ["Client", "Nodes", "Logs / supervisor", "Other"]; return labels.map((label) => [label, items.filter((item) => category(item.name) === label)] as [string, V5RuntimeArtifact[]]).filter(([, current]) => current.length); }
function category(name: string): string { if (name.startsWith("client/")) return "Client"; if (name.startsWith("nodes/")) return "Nodes"; if (name.startsWith("logs/") || name.startsWith("supervisor")) return "Logs / supervisor"; return "Other"; }
function slug(value: string): string { return value.toLowerCase().replace(/[^a-z0-9]+/g, "-").replace(/(^-|-$)/g, ""); }
