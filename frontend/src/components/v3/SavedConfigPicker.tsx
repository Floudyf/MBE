import type { V3SavedConfig, V3SavedConfigKind } from "../../api";

type Props = {
  title: string;
  kind?: V3SavedConfigKind;
  configs: V3SavedConfig[];
  onLoad: (config: V3SavedConfig) => void;
  onCopy: (config: V3SavedConfig) => void;
  onDelete: (config: V3SavedConfig) => void;
};

export default function SavedConfigPicker({ title, kind, configs, onLoad, onCopy, onDelete }: Props) {
  const visible = kind ? configs.filter((config) => config.config_kind === kind) : configs;
  return (
    <details className="v3-foldout" open={!kind}>
      <summary className="v3-foldout-summary">{title}<small>{visible.length} 个</small></summary>
      <div className="v3-template-list">
        {visible.length === 0 && <p className="muted">暂无已保存配置。</p>}
        {visible.map((config) => (
          <div key={config.config_id} className="v3-template-row saved-config-row">
            <span>
              <strong>{config.name}</strong>
              <small>{config.config_kind} / {config.validation_status} / {config.updated_at}</small>
              {config.description && <small>{config.description}</small>}
            </span>
            <span className="v3-saved-config-actions">
              <button type="button" className="v3-secondary-button" onClick={() => onLoad(config)}>加载</button>
              <button type="button" className="v3-secondary-button" onClick={() => onCopy(config)}>复制</button>
              <button type="button" className="v3-secondary-button danger" onClick={() => onDelete(config)}>删除</button>
            </span>
          </div>
        ))}
      </div>
    </details>
  );
}
