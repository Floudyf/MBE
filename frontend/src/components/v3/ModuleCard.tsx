import type { V3ComposerModule } from "../../api";
import { labelFor, moduleNames, statusLabels, tagLabels } from "./localization";

type Props = {
  module: V3ComposerModule;
  selected: boolean;
  onSelect: (module: V3ComposerModule) => void;
};

export default function ModuleCard({ module, selected, onSelect }: Props) {
  const plugin = module.plugin && module.plugin !== "none" ? module.plugin : "无";

  return (
    <button
      type="button"
      className={`v3-module-card v3-module-${module.status}${selected ? " selected" : ""}`}
      onClick={() => onSelect(module)}
      title={`${module.display_name || module.module_id} / ${module.module_id} / ${plugin}`}
    >
      <span className="v3-module-position">{module.position}</span>
      <strong>{labelFor(moduleNames, module.module_id, module.display_name)}</strong>
      <small title={module.module_id}>{module.module_id}</small>
      <span className="v3-plugin-id" title={plugin}>插件：{plugin}</span>
      <span className={`v3-status-badge status-${module.status}`}>{labelFor(statusLabels, module.status)}</span>
      <span className="v3-tag-row">
        {(module.tags || []).map((tag) => (
          <span key={tag} className="v3-tag" title={tag}>{labelFor(tagLabels, tag)}</span>
        ))}
      </span>
    </button>
  );
}
