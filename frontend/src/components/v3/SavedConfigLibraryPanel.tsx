import type { V3SavedConfig } from "../../api";
import SavedConfigPicker from "./SavedConfigPicker";

type Props = {
  configs: V3SavedConfig[];
  loading?: boolean;
  error?: string;
  onRefresh: () => void;
  onLoad: (config: V3SavedConfig) => void;
  onCopy: (config: V3SavedConfig) => void;
  onDelete: (config: V3SavedConfig) => void;
};

export default function SavedConfigLibraryPanel({ configs, loading = false, error = "", onRefresh, onLoad, onCopy, onDelete }: Props) {
  return (
    <section className="final-card saved-config-library-panel">
      <div className="v3-section-head">
        <div>
          <p className="eyebrow">配置库</p>
          <h3>加载已保存方案 / 负载 / 拓扑</h3>
        </div>
        <button type="button" className="v3-secondary-button" onClick={onRefresh} disabled={loading}>{loading ? "刷新中..." : "刷新"}</button>
      </div>
      {error && <p className="file-error">{error}</p>}
      <SavedConfigPicker title="已保存完整方案" kind="method" configs={configs} onLoad={onLoad} onCopy={onCopy} onDelete={onDelete} />
      <SavedConfigPicker title="已保存负载" kind="workload" configs={configs} onLoad={onLoad} onCopy={onCopy} onDelete={onDelete} />
      <SavedConfigPicker title="已保存拓扑" kind="topology" configs={configs} onLoad={onLoad} onCopy={onCopy} onDelete={onDelete} />
    </section>
  );
}
