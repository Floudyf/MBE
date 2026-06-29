import type { V3ComposerModule } from "../../api";
import { statusLabels } from "./composerCatalog";
import { labelFor, moduleNames, tagLabels } from "./localization";

type Props = {
  module: V3ComposerModule;
  selected: boolean;
  onSelect: (module: V3ComposerModule) => void;
};

export default function ModuleCard({ module, selected, onSelect }: Props) {
  const plugin = module.plugin && module.plugin !== "none" ? module.plugin : "无";

  const status = (module.status in statusLabels ? module.status : "fixed") as keyof typeof statusLabels;
  const supportHint = module.status === "planned" ? "planned" : module.status === "variable" || module.status === "fixed" || module.status === "default" ? "configured runnable; runtime support depends on backend validation" : String(module.status);

  return (
    <button
      type="button"
      className={`v3-module-card v3-module-${status}${selected ? " selected" : ""}`}
      onClick={() => onSelect(module)}
      title={`${module.display_name || module.module_id} / ${module.module_id} / ${plugin} / ${supportHint}`}
    >
      <span className="v3-module-position">{module.position}</span>
      <strong>{labelFor(moduleNames, module.module_id, module.display_name)}</strong>
      <small title={module.module_id}>{module.module_id}</small>
      <span className="v3-plugin-id" title={plugin}>插件：{plugin}</span>
      <span className={`v3-status-badge status-${status}`}>{statusLabels[status]}</span>
      <small title={supportHint}>{supportHint}</small>
      <span className="v3-tag-row">
        {(module.tags || []).map((tag) => (
          <span key={tag} className="v3-tag" title={tag}>{labelFor(tagLabels, tag)}</span>
        ))}
      </span>
    </button>
  );
}
