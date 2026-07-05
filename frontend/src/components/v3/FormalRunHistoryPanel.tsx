import { useEffect, useState } from "react";
import { fetchV3FormalMetatrackRunResult, fetchV3FormalMetatrackRuns, formalArtifactsZipURL, type V3FormalMetatrackBenchmarkRunResponse, type V3FormalRunHistoryItem } from "../../api";

type Props = {
  onSelectResult: (result: V3FormalMetatrackBenchmarkRunResponse) => void;
};

export default function FormalRunHistoryPanel({ onSelectResult }: Props) {
  const [runs, setRuns] = useState<V3FormalRunHistoryItem[]>([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState("");

  useEffect(() => { void loadRuns(); }, []);

  async function loadRuns() {
    try {
      setLoading(true);
      setRuns(await fetchV3FormalMetatrackRuns(20));
      setError("");
    } catch (caught) {
      setError(caught instanceof Error ? caught.message : String(caught));
    } finally {
      setLoading(false);
    }
  }

  async function openRun(runId: string) {
    try {
      const result = await fetchV3FormalMetatrackRunResult(runId);
      onSelectResult(result);
      setError("");
    } catch (caught) {
      setError(caught instanceof Error ? caught.message : String(caught));
    }
  }

  return (
    <section className="final-card wide formal-run-history">
      <div className="v3-section-head">
        <div>
          <p className="eyebrow">最近正式实验</p>
          <h3>Formal Run History</h3>
        </div>
        <button type="button" className="v3-secondary-button" onClick={loadRuns} disabled={loading}>{loading ? "刷新中..." : "刷新"}</button>
      </div>
      <p className="muted">刷新页面后，可在这里找回最近的正式性能实验结果。历史结果仍是本地 emulator 受控基准实验，不是生产链证据。</p>
      {error && <p className="file-error">{error}</p>}
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
                <button type="button" className="v3-secondary-button" onClick={() => void openRun(run.run_id)} disabled={!run.summary_available}>查看结果</button>
                <a className="v3-secondary-button" href={formalArtifactsZipURL(run.run_id)} download={`formal_metatrack_results_${run.run_id}.zip`}>下载 ZIP</a>
              </span>
            </div>
          ))}
        </div>
      )}
    </section>
  );
}
