type Props = {
  runnable: boolean;
  running: boolean;
  onRunSmoke: () => void;
};

export default function RunLevelPanel({ runnable, running, onRunSmoke }: Props) {
  return (
    <section className="final-card">
      <p className="eyebrow">运行级别</p>
      <h3>当前可运行层级</h3>
      <div className="v3-run-levels">
        <span className="v3-status-badge status-variable">Smoke 实验：可运行</span>
        <span className="v3-status-badge status-planned">Debug 实验：规划中</span>
        <span className="v3-status-badge status-planned">正式实验：规划中</span>
        <span className="v3-status-badge status-planned">压力实验：规划中</span>
      </div>
      <button type="button" disabled={!runnable || running} onClick={onRunSmoke}>
        {running ? "Smoke 实验运行中..." : "运行当前 Smoke 实验"}
      </button>
      <p className="muted">当前 Smoke 实验复用 Go-backed MetaTrack 控制实验，不启动 Fabric，不运行跨链协议。</p>
    </section>
  );
}
