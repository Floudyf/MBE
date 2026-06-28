import type { V3PluginMatrixRow } from "../../api";
import { labelFor, methodLabels, moduleNames, tagLabels } from "./localization";

type Props = {
  rows: V3PluginMatrixRow[];
};

export default function PluginMatrixTable({ rows }: Props) {
  const modules = Array.from(new Set(rows.flatMap((row) => Object.keys(row.module_plugins || {}))));
  const baseline = rows[0]?.module_plugins || {};

  return (
    <details className="final-card wide v3-foldout">
      <summary className="v3-foldout-summary">
        <span>MetaTrack 插件矩阵</span>
        <small>展示四组消融方法在分片/路由、交易执行、状态访问、状态提交上的插件差异。</small>
      </summary>
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
    </details>
  );
}
