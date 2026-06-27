import type { V3ComposerModule } from "../../api";

type Props = {
  module?: V3ComposerModule | null;
};

function ListLine({ label, items }: { label: string; items?: string[] }) {
  return (
    <div>
      <dt>{label}</dt>
      <dd>{items && items.length ? items.join(", ") : "none"}</dd>
    </div>
  );
}

export default function ModuleDetailPanel({ module }: Props) {
  if (!module) {
    return (
      <aside className="v3-detail-panel">
        <p className="eyebrow">Module Detail</p>
        <h3>Select a module</h3>
        <p>Select a module to inspect its plugin, status, metrics, and artifacts.</p>
      </aside>
    );
  }

  return (
    <aside className="v3-detail-panel">
      <p className="eyebrow">Module Detail</p>
      <h3>{module.display_name}</h3>
      <dl className="v3-detail-list">
        <div><dt>Current Plugin</dt><dd>{module.plugin || "none"}</dd></div>
        <div><dt>Status</dt><dd>{module.status}</dd></div>
        <div><dt>Role</dt><dd>{module.role || "environment"}</dd></div>
        <ListLine label="Tags" items={module.tags} />
        <ListLine label="Allowed Plugins" items={module.allowed_plugins} />
        <ListLine label="Metrics" items={module.metrics} />
        <ListLine label="Artifacts" items={module.artifacts} />
      </dl>
    </aside>
  );
}
