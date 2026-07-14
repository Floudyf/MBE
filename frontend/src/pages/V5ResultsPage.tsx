import { useEffect, useRef, useState } from "react";

import {
  fetchV5FormalArtifactCatalog,
  fetchV5FormalChildRun,
  fetchV5FormalGroupMetrics,
  fetchV5FormalRunGroup,
  listV5FormalRunGroups,
  type V5FormalAggregate,
  type V5FormalArtifactCatalog,
  type V5FormalChildRun,
  type V5FormalRunGroup,
  type V5FormalRunGroupDetail,
} from "../api";
import V5ArtifactCatalog from "../components/v5/V5ArtifactCatalog";
import V5ChildDetail from "../components/v5/V5ChildDetail";
import V5GroupSummary from "../components/v5/V5GroupSummary";

const recentGroupKey = "mbe.v5FormalRunGroupId";

export default function V5ResultsPage({ preferredGroupId = "" }: { preferredGroupId?: string }) {
  const [groups, setGroups] = useState<V5FormalRunGroup[]>([]);
  const [detail, setDetail] = useState<V5FormalRunGroupDetail | null>(null);
  const [aggregate, setAggregate] = useState<V5FormalAggregate | null>(null);
  const [catalog, setCatalog] = useState<V5FormalArtifactCatalog | null>(null);
  const [selectedGroupId, setSelectedGroupId] = useState("");
  const [selectedChildId, setSelectedChildId] = useState("");
  const [selectedChild, setSelectedChild] = useState<V5FormalChildRun | null>(null);
  const [notice, setNotice] = useState("");
  const [error, setError] = useState("");
  const [busy, setBusy] = useState(false);
  const groupRevision = useRef(0);
  const childRevision = useRef(0);
  const selectedGroupRef = useRef("");
  const selectedChildRef = useRef("");
  const detailRef = useRef<V5FormalRunGroupDetail | null>(null);
  const timer = useRef<number | null>(null);

  useEffect(() => { void refreshGroups(); return () => stopPolling(); }, []);

  async function refreshGroups(preferredId?: string) {
    try {
      setBusy(true);
      const next = (await listV5FormalRunGroups()).sort((a, b) => String(b.created_at ?? "").localeCompare(String(a.created_at ?? "")));
      setGroups(next);
      const stored = preferredId || preferredGroupId || window.localStorage.getItem(recentGroupKey) || "";
      const choice = next.some((item) => item.run_group_id === stored) ? stored : next[0]?.run_group_id ?? "";
      const preferredMissing = Boolean(stored && !next.some((item) => item.run_group_id === stored));
      if (choice) {
        if (preferredMissing) setNotice(`Preferred RunGroup ${stored} was not found; selected the newest available record.`);
        else if (stored) setNotice("");
        await loadGroup(choice);
      } else {
        if (preferredMissing) setNotice(`Preferred RunGroup ${stored} was not found and no persisted RunGroup is available.`);
        clearSelection();
      }
    } catch (caught) { setError(message(caught)); } finally { setBusy(false); }
  }

  async function loadGroup(groupId: string, quiet = false) {
    const revision = ++groupRevision.current;
    stopPolling();
    const switched = selectedGroupRef.current !== groupId;
    selectedGroupRef.current = groupId;
    setSelectedGroupId(groupId);
    if (switched) clearChildSelection();
    try {
      const [groupDetail, groupAggregate, artifactCatalog] = await Promise.all([
        fetchV5FormalRunGroup(groupId), fetchV5FormalGroupMetrics(groupId), fetchV5FormalArtifactCatalog(groupId),
      ]);
      if (revision !== groupRevision.current) return;
      detailRef.current = groupDetail;
      setDetail(groupDetail); setAggregate(groupAggregate); setCatalog(artifactCatalog);
      setGroups((current) => current.map((item) => item.run_group_id === groupId ? { ...item, ...groupDetail.group, aggregate: groupAggregate } : item));
      const retained = selectedChildRef.current;
      const childId = retained && groupDetail.children.some((item) => item.child_run_id === retained) ? retained : groupDetail.children[0]?.child_run_id;
      if (childId) await loadChild(groupId, childId, revision);
      if (revision !== groupRevision.current) return;
      setError("");
      if (!terminal(groupDetail.group.status)) schedulePolling(groupId);
    } catch (caught) {
      if (revision !== groupRevision.current) return;
      setError(message(caught));
      if (quiet && selectedGroupRef.current === groupId && detailRef.current && !terminal(detailRef.current.group.status)) schedulePolling(groupId);
    }
  }

  async function loadChild(groupId: string, childId: string, parentRevision = groupRevision.current) {
    const revision = ++childRevision.current;
    selectedChildRef.current = childId;
    setSelectedChildId(childId);
    try {
      const child = await fetchV5FormalChildRun(groupId, childId);
      if (parentRevision === groupRevision.current && revision === childRevision.current && selectedGroupRef.current === groupId && selectedChildRef.current === childId) setSelectedChild(child);
    } catch (caught) { if (parentRevision === groupRevision.current && revision === childRevision.current) setError(message(caught)); }
  }

  function schedulePolling(groupId: string) {
    stopPolling();
    timer.current = window.setTimeout(() => void loadGroup(groupId, true), 1800);
  }
  function stopPolling() { if (timer.current !== null) { window.clearTimeout(timer.current); timer.current = null; } }
  function clearChildSelection() { childRevision.current += 1; selectedChildRef.current = ""; setSelectedChildId(""); setSelectedChild(null); }
  function clearSelection() { groupRevision.current += 1; selectedGroupRef.current = ""; setSelectedGroupId(""); detailRef.current = null; setDetail(null); setAggregate(null); setCatalog(null); clearChildSelection(); }

  const selectedGroup = detail?.group;
  return <section className="page-grid" data-testid="v5-results-page">
    <article className="final-card wide page-hero"><p className="eyebrow">V5 Formal Results</p><h2>Results and Artifacts</h2><p>Results use persisted V5 Formal RunGroup records and real runtime artifacts. No local output path is exposed as a browser download.</p>{notice && <p className="notice">{notice}</p>}{error && <p className="file-error">{error}</p>}</article>
    <article className="final-card wide" data-testid="v5-run-group-list"><div className="section-heading"><div><h2>RunGroup History</h2><p className="muted">{groups.length ? `${groups.length} persisted RunGroup(s)` : "No V5 Formal RunGroups yet."}</p></div><button type="button" onClick={() => void refreshGroups(selectedGroupId)} disabled={busy}>Refresh RunGroup</button></div>
      {groups.length ? <div className="table-wrap"><table><thead><tr><th>ID</th><th>Status</th><th>Plan</th><th>Backend</th><th>Truth</th><th>Created</th><th>Updated</th><th>Children</th><th>Failed</th><th>Suites</th><th>Methods</th></tr></thead><tbody>{groups.map((group) => <tr key={group.run_group_id} className={group.run_group_id === selectedGroupId ? "selected-row" : ""}><td><button type="button" data-testid="v5-run-group-select" onClick={() => void loadGroup(group.run_group_id)}>{group.run_group_id}</button></td><td>{group.status}</td><td>{group.plan?.name ?? "-"}</td><td>{group.execution_backend}</td><td>{group.runtime_truth}</td><td>{group.created_at ?? "-"}</td><td>{group.updated_at ?? "-"}</td><td>{group.completed_child_runs}/{group.total_child_runs}</td><td>{metric(group.aggregate?.failed_count)}</td><td>{group.plan?.suites.length ?? "-"}</td><td>{group.plan?.methods.length ?? "-"}</td></tr>)}</tbody></table></div> : <p className="muted">Start a V5 Formal RunGroup from Run Experiment to populate this history.</p>}
    </article>
    {selectedGroup && <V5GroupSummary group={selectedGroup} aggregate={aggregate} children={detail?.children ?? []} />}
    {detail && <article className="final-card wide"><h2>Child Runs</h2><div className="table-wrap"><table data-testid="v5-child-table"><thead><tr><th>Child</th><th>Suite</th><th>Method</th><th>Seed</th><th>Repeat</th><th>Topology</th><th>Tx</th><th>Status</th><th>TPS</th><th>P99</th><th>Terminal</th><th>Incomplete</th><th>Paper candidate</th></tr></thead><tbody>{detail.children.map((child) => <ChildRow key={child.child_run_id} child={child} selected={child.child_run_id === selectedChildId} onSelect={() => void loadChild(detail.group.run_group_id, child.child_run_id)} />)}</tbody></table></div></article>}
    <V5ChildDetail child={selectedChild} />
    {selectedGroup && <V5ArtifactCatalog groupId={selectedGroup.run_group_id} catalog={catalog} />}
  </section>;
}

function ChildRow({ child, selected, onSelect }: { child: V5FormalChildRun; selected: boolean; onSelect: () => void }) {
  const finality = child.result?.summary?.finality_evidence;
  return <tr className={selected ? "selected-row" : ""}><td><button type="button" onClick={onSelect}>{child.child_run_id}</button></td><td>{child.suite_type}</td><td>{child.method.display_name}</td><td>{child.seed}</td><td>{child.repeat_index + 1}</td><td>{child.topology_point.nodes}/{child.topology_point.shards}/{child.topology_point.validators_per_shard}</td><td>{child.estimated_transactions}</td><td>{child.status}{child.error ? `: ${child.error}` : ""}</td><td>{metric(child.metrics?.throughput_tps)}</td><td>{metric(child.metrics?.p99_latency_ms)}</td><td>{metric(finality?.terminal_unique_tx_count)}</td><td>{metric(finality?.incomplete_unique_tx_count)}</td><td>{metric(child.paper_candidate)}</td></tr>;
}
function metric(value: unknown): string { return value === undefined || value === null ? "-" : String(value); }
function terminal(status: string): boolean { return ["completed", "completed_with_failures", "failed", "cancelled"].includes(status); }
function message(value: unknown): string { return value instanceof Error ? value.message : String(value); }
