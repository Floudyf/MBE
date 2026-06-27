import type { V3PluginMatrixRow } from "../../api";

type Props = {
  rows: V3PluginMatrixRow[];
};

export default function PluginMatrixTable({ rows }: Props) {
  const modules = Array.from(new Set(rows.flatMap((row) => Object.keys(row.module_plugins || {}))));
  const baseline = rows[0]?.module_plugins || {};

  return (
    <section className="final-card wide">
      <div className="v3-section-head">
        <div>
          <p className="eyebrow">Plugin Matrix</p>
          <h3>MetaTrack method combinations</h3>
        </div>
      </div>
      <div className="table-scroll">
        <table className="v3-plugin-table">
          <thead>
            <tr>
              <th>Method</th>
              {modules.map((module) => <th key={module}>{module}</th>)}
              <th>Tags</th>
            </tr>
          </thead>
          <tbody>
            {rows.map((row) => (
              <tr key={row.method_id}>
                <td><strong>{row.label || row.method_id}</strong><small>{row.method_id}</small></td>
                {modules.map((module) => {
                  const value = row.module_plugins?.[module] || "-";
                  const changed = baseline[module] && value !== baseline[module];
                  return <td key={module} className={changed ? "v3-plugin-changed" : ""}>{value}</td>;
                })}
                <td>
                  <span className="v3-tag-row">
                    {(row.tags || []).map((tag) => <span key={tag} className="v3-tag">{tag}</span>)}
                  </span>
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </section>
  );
}
