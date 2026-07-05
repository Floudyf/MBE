import { useEffect, useRef, useState } from "react";
import { fetchV3FormalMetatrackRunResult, fetchV3FormalMetatrackRuns, formalArtifactsZipURL, type V3FormalMetatrackBenchmarkRunResponse, type V3FormalRunHistoryItem } from "../../api";

type Props = {
  onSelectResult: (result: V3FormalMetatrackBenchmarkRunResponse) => void;
  refreshKey?: string | number;
  autoLoadLatest?: boolean;
};

export default function FormalRunHistoryPanel({ onSelectResult, refreshKey = 0, autoLoadLatest = false }: Props) {
  const [runs, setRuns] = useState<V3FormalRunHistoryItem[]>([]);
  const [loading, setLoading] = useState(false);
  const [loadingRunId, setLoadingRunId] = useState("");
  const [loadedRunId, setLoadedRunId] = useState("");
  const [error, setError] = useState("");
  const autoLoadedRef = useRef(false);

  useEffect(() => { void loadRuns(); }, [refreshKey]);

  async function loadRuns() {
    try {
      setLoading(true);
      const nextRuns = await fetchV3FormalMetatrackRuns(20);
      setRuns(nextRuns);
      setError("");
      if (autoLoadLatest && !autoLoadedRef.current) {
        const latest = nextRuns.find((run) => run.summary_available);
        if (latest) {
          autoLoadedRef.current = true;
          void openRun(latest.run_id, true);
        }
      }
    } catch (caught) {
      setError(caught instanceof Error ? caught.message : String(caught));
    } finally {
      setLoading(false);
    }
  }

  async function openRun(runId: string, silent = false) {
    try {
      setLoadingRunId(runId);
      const result = await fetchV3FormalMetatrackRunResult(runId);
      onSelectResult(result);
      setLoadedRunId(runId);
      setError("");
    } catch (caught) {
      if (!silent) setError(caught instanceof Error ? caught.message : String(caught));
    } finally {
      setLoadingRunId("");
    }
  }

  return (
    <section className="final-card wide formal-run-history" data-testid="v3-formal-run-history">
      <div className="v3-section-head">
        <div>
          <p className="eyebrow">最近正式实验</p>
          <h3>Formal Run History</h3>
        </div>
        <button type="button" className="v3-secondary-button" onClick={loadRuns} disabled={loading}>
          {loading ? "刷新中..." : "刷新"}
        </button>
      </div>
      <p className="muted">刷新页面后，可在这里找回最近的正式性能实验结果。历史结果仍是本地 emulator 受控基准实验，不是生产链证据。</p>
      {error && <p className="file-error">{error}</p>}
      {loadedRunId && <p className="v3-inline-ok">已加载 {loadedRunId}</p>}
      {runs.length === 0 && !loading && <p className="muted">暂无历史正式实验。</p>}
      {runs.length > 0 && (
        <div className="v3-run-history-list">
          {runs.map((run) => (
            <div key={run.run_id} className="v3-run-history-row">
              <span>
                <strong>{run.run_id}</strong>
                <small>{run.created_at} / {run.experiment_type || "-"}</small>
              </span>
              <span>{run.completed_run_count}/{run.run_count} 完成 · 失败 {run.failed_run_count}</span>
              <span>tx={String(run.formal_tx_count)} · {run.runtime_evidence_mode || "-"}</span>
              <span>方法 {String(run.method_count)} / 负载 {String(run.workload_count)} / 拓扑 {String(run.topology_count)}</span>
              <span className="v3-row-actions">
                <button
                  type="button"
                  className="v3-secondary-button"
                  data-testid="v3-formal-history-open-result"
                  onClick={() => void openRun(run.run_id)}
                  disabled={!run.summary_available || loadingRunId === run.run_id}
                >
                  {loadingRunId === run.run_id ? "加载中..." : "查看结果"}
                </button>
                <a className="v3-secondary-button" data-testid="v3-formal-zip-download" href={formalArtifactsZipURL(run.run_id)} download={`formal_metatrack_results_${run.run_id}.zip`}>下载 ZIP</a>
              </span>
            </div>
          ))}
        </div>
      )}
    </section>
  );
}
