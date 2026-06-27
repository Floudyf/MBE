type Props = {
  runnable: boolean;
  running: boolean;
  onRunSmoke: () => void;
};

export default function RunLevelPanel({ runnable, running, onRunSmoke }: Props) {
  return (
    <section className="final-card">
      <p className="eyebrow">Run Level</p>
      <h3>Smoke run alignment</h3>
      <div className="v3-run-levels">
        <span className="v3-status-badge status-variable">Smoke Run: enabled</span>
        <span className="v3-status-badge status-planned">Debug Run: planned</span>
        <span className="v3-status-badge status-planned">Formal Run: planned</span>
        <span className="v3-status-badge status-planned">Stress Run: planned</span>
      </div>
      <button type="button" disabled={!runnable || running} onClick={onRunSmoke}>
        {running ? "Running smoke..." : "Run existing smoke"}
      </button>
      <p className="muted">Smoke uses the existing Go-backed MetaTrack controlled run. It does not start Fabric.</p>
    </section>
  );
}
