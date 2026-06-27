import type { V3PluginMatrixRow } from "../../api";
import { labelFor, methodLabels, moduleNames, tagLabels } from "./localization";

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
          <p className="eyebrow">插件对比矩阵</p>
          <h3>MetaTrack 四组方法组合</h3>
        </div>
      </div>
      <div className="table-scroll">
        <table className="v3-plugin-table">
          <thead>
            <tr>
              <th>方法</th>
              {modules.map((module) => <th key={module}>{labelFor(moduleNames, module)}</th>)}
              <th>标签</th>
            </tr>
          </thead>
          <tbody>
            {rows.map((row) => (
              <tr key={row.method_id}>
                <td><strong>{labelFor(methodLabels, row.method_id, row.label || row.method_id)}</strong><small>{row.method_id}</small></td>
                {modules.map((module) => {
                  const value = row.module_plugins?.[module] || "-";
                  const changed = baseline[module] && value !== baseline[module];
                  return <td key={module} className={changed ? "v3-plugin-changed" : ""}>{value}</td>;
                })}
                <td>
                  <span className="v3-tag-row">
                    {(row.tags || []).map((tag) => <span key={tag} className="v3-tag" title={tag}>{labelFor(tagLabels, tag)}</span>)}
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
