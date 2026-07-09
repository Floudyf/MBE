import type { ChildRunResult, ExperimentMatrixRow } from "../../api";

type StageName = "Workload" | "TxPool" | "Block" | "Consensus" | "Execution" | "Commit" | "Metrics";
type StageStatus = "idle" | "preview" | "running" | "completed" | "failed" | "blocked";

type Props = {
  childRuns?: ChildRunResult[];
  rows?: ExperimentMatrixRow[];
  summary?: Record<string, unknown> | null;
  mode?: "preview" | "dry_run" | "execute" | string;
};

const stages: StageName[] = ["Workload", "TxPool", "Block", "Consensus", "Execution", "Commit", "Metrics"];

export default function RunStageFlow({ childRuns = [], rows = [], summary = null, mode = "preview" }: Props) {
  const status = inferStatus(childRuns, rows, mode);
  const counts = inferCounts(childRuns, rows, summary);
  const dotCount = Math.max(12, Math.min(80, counts.txCount || rows.length * 8 || childRuns.length * 12 || 24));

  return (
    <section className={`run-stage-flow stage-${status}`}>
      <div className="stage-flow-head">
        <div>
          <p className="eyebrow">Stage Statistics</p>
          <h3>运行阶段进度</h3>
        </div>
        <span className={`status-badge badge-${status}`}>{statusLabel(status)}</span>
      </div>
      <p className="muted">阶段统计视图；当前版本基于运行摘要和日志计数渲染，不代表逐笔实时事件流。</p>
      <div className="stage-flow-grid">
        {stages.map((stage, index) => (
          <div key={stage} className={`stage-node stage-node-${nodeStatus(status, index)}`}>
            <span>{index + 1}</span>
            <strong>{stage}</strong>
            <small>{stageHint(stage, counts)}</small>
          </div>
        ))}
      </div>
      <div className="dots-flow" aria-label="轻量交易流转示意">
        {Array.from({ length: dotCount }).map((_, index) => (
          <i key={index} className="flow-dot" style={{ animationDelay: `${(index % 16) * 0.08}s` }} />
        ))}
      </div>
      <dl className="stage-flow-kpis">
        <div><dt>matrix rows</dt><dd>{rows.length}</dd></div>
        <div><dt>child runs</dt><dd>{childRuns.length}</dd></div>
        <div><dt>tx estimate</dt><dd>{counts.txCount || "-"}</dd></div>
        <div><dt>artifacts</dt><dd>{counts.artifacts || "-"}</dd></div>
      </dl>
    </section>
  );
}

function inferStatus(childRuns: ChildRunResult[], rows: ExperimentMatrixRow[], mode: string): StageStatus {
  if (childRuns.some((child) => child.status === "failed")) return "failed";
  if (childRuns.some((child) => child.status === "blocked") || rows.some((row) => !row.runnable)) return "blocked";
  if (childRuns.some((child) => child.status === "running")) return "running";
  if (childRuns.some((child) => child.status === "completed")) return "completed";
  if (childRuns.some((child) => child.status === "dry_run" || child.status === "preview_only") || mode === "dry_run") return "preview";
  return rows.length ? "preview" : "idle";
}

function inferCounts(childRuns: ChildRunResult[], rows: ExperimentMatrixRow[], summary: Record<string, unknown> | null) {
  const merged = {
    ...(summary || {}),
    ...(childRuns.find((child) => child.summary && Object.keys(child.summary).length)?.summary || {}),
  };
  return {
    txCount: numeric(merged.tx_count) || numeric(merged.imported_tx_count) || numeric(merged.signed_tx_verify_pass_count),
    committed: numeric(merged.committed_height) || numeric(merged.committed_block_count),
    executed: numeric(merged.executed_tx_count) || numeric(merged.success_count),
    artifacts: childRuns.reduce((total, child) => total + (child.artifacts?.length || 0), 0),
    blockedRows: rows.filter((row) => !row.runnable).length,
  };
}

function nodeStatus(status: StageStatus, index: number): StageStatus {
  if (status === "completed" || status === "failed" || status === "blocked") return status;
  if (status === "running") return index < 3 ? "completed" : index === 3 ? "running" : "idle";
  if (status === "preview") return "preview";
  return "idle";
}

function stageHint(stage: StageName, counts: ReturnType<typeof inferCounts>) {
  if (stage === "Workload") return counts.txCount ? `${counts.txCount} tx` : "condition";
  if (stage === "Execution") return counts.executed ? `${counts.executed} executed` : "summary";
  if (stage === "Commit") return counts.committed ? `height ${counts.committed}` : "commit";
  if (stage === "Metrics") return counts.artifacts ? `${counts.artifacts} artifacts` : "report";
  return "stage";
}

function numeric(value: unknown): number {
  return typeof value === "number" && Number.isFinite(value) ? value : 0;
}

function statusLabel(status: StageStatus) {
  return status === "preview" ? "preview-only" : status;
}
