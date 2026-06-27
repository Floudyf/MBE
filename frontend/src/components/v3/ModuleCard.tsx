import type { V3ComposerModule } from "../../api";

type Props = {
  module: V3ComposerModule;
  selected: boolean;
  onSelect: (module: V3ComposerModule) => void;
};

export default function ModuleCard({ module, selected, onSelect }: Props) {
  return (
    <button
      type="button"
      className={`v3-module-card v3-module-${module.status}${selected ? " selected" : ""}`}
      onClick={() => onSelect(module)}
    >
      <span className="v3-module-position">{module.position}</span>
      <strong>{module.display_name}</strong>
      <small>{module.plugin || "none"}</small>
      <span className={`v3-status-badge status-${module.status}`}>{module.status}</span>
      <span className="v3-tag-row">
        {(module.tags || []).map((tag) => (
          <span key={tag} className="v3-tag">{tag}</span>
        ))}
      </span>
    </button>
  );
}
