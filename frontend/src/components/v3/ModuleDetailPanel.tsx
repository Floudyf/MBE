import type { V3ComposerModule } from "../../api";
import { labelFor, moduleNames, roleLabels, statusLabels, tagLabels } from "./localization";

type Props = {
  module?: V3ComposerModule | null;
};

function ListLine({ label, items }: { label: string; items?: string[] }) {
  return (
    <div>
      <dt>{label}</dt>
      <dd>{items && items.length ? items.join(", ") : "无"}</dd>
    </div>
  );
}

function TagLine({ items }: { items?: string[] }) {
  return (
    <div>
      <dt>标签</dt>
      <dd>{items && items.length ? items.map((item) => labelFor(tagLabels, item)).join(", ") : "无"}</dd>
    </div>
  );
}

export default function ModuleDetailPanel({ module }: Props) {
  if (!module) {
    return (
      <aside className="v3-detail-panel">
        <p className="eyebrow">模块详情</p>
        <h3>请选择模块</h3>
        <p>请选择一个模块，查看它的插件、状态、指标与产物。</p>
      </aside>
    );
  }

  return (
    <aside className="v3-detail-panel">
      <p className="eyebrow">模块详情</p>
      <h3>{labelFor(moduleNames, module.module_id, module.display_name)}</h3>
      <p className="v3-sub-id">{module.module_id}</p>
      <dl className="v3-detail-list">
        <div><dt>当前插件</dt><dd>{module.plugin && module.plugin !== "none" ? module.plugin : "无"}</dd></div>
        <div><dt>状态</dt><dd>{labelFor(statusLabels, module.status)}</dd></div>
        <div><dt>角色</dt><dd>{labelFor(roleLabels, module.role || "environment")}</dd></div>
        <TagLine items={module.tags} />
        <ListLine label="可选插件" items={module.allowed_plugins} />
        <ListLine label="相关指标" items={module.metrics} />
        <ListLine label="相关产物" items={module.artifacts} />
      </dl>
    </aside>
  );
}
