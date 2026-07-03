import { type CSSProperties, useMemo, useState } from "react";

import type { V3ComposerModule, V3ComposerPreview } from "../../api";
import ModuleCard from "./ModuleCard";
import ModuleDetailPanel from "./ModuleDetailPanel";
import { type ComposerDraft, moduleView, updateDraftModule } from "./composerDraft";

type Props = {
  preview: V3ComposerPreview;
  draft: ComposerDraft;
  onDraftChange: (draft: ComposerDraft) => void;
  variableModule?: string;
  variableModules?: string[];
  lockedModules?: Record<string, string>;
};

type SnakeSlot = {
  moduleId: string;
  row: number;
  column: number;
  edge?: "right" | "left" | "down";
};

const snakeSlots: SnakeSlot[] = [
  { moduleId: "Workload", row: 1, column: 1, edge: "right" },
  { moduleId: "TxPool", row: 1, column: 2, edge: "right" },
  { moduleId: "BlockProducer", row: 1, column: 3, edge: "right" },
  { moduleId: "Consensus", row: 1, column: 4, edge: "down" },
  { moduleId: "StateAccess", row: 2, column: 1, edge: "down" },
  { moduleId: "Execution", row: 2, column: 2, edge: "left" },
  { moduleId: "Routing", row: 2, column: 3, edge: "left" },
  { moduleId: "CommitteeEpoch", row: 2, column: 4, edge: "left" },
  { moduleId: "StateStorage", row: 3, column: 1, edge: "right" },
  { moduleId: "Commit", row: 3, column: 2, edge: "right" },
  { moduleId: "MetricsReport", row: 3, column: 3 },
];

const stageGroups = [
  { title: "输入阶段", modules: "负载生成 / 交易池 / 区块生成" },
  { title: "排序阶段", modules: "共识排序 / 委员会" },
  { title: "执行阶段", modules: "分片路由 / 交易执行 / 状态访问" },
  { title: "提交阶段", modules: "状态存储 / 状态提交 / 指标报告" },
];

export default function SingleChainComposer({ preview, draft, onDraftChange, variableModule = "", variableModules = [], lockedModules = {} }: Props) {
  const modules = preview.modules || [];
  const [selectedId, setSelectedId] = useState(modules[0]?.module_id || "");
  const modulesById = useMemo(
    () => new Map(modules.map((module) => [module.module_id, module])),
    [modules],
  );
  const snakeModules = useMemo(
    () => snakeSlots
      .map((slot) => ({ slot, module: modulesById.get(slot.moduleId) }))
      .filter((item): item is { slot: SnakeSlot; module: V3ComposerModule } => Boolean(item.module)),
    [modulesById],
  );
  const selected = useMemo(
    () => modules.find((module) => module.module_id === selectedId) || modules[0],
    [modules, selectedId],
  );

  function selectModule(module: V3ComposerModule) {
    setSelectedId(module.module_id);
  }

  function updateSelected(moduleId: string, patch: Parameters<typeof updateDraftModule>[2]) {
    onDraftChange(updateDraftModule(draft, moduleId, patch));
  }

  return (
    <section className="v3-composer-layout v3-workbench">
      <div className="v3-chain-band">
        <div className="flow-stage-legend" aria-label="实验流程阶段说明">
          {stageGroups.map((group) => (
            <div key={group.title}>
              <strong>{group.title}</strong>
              <small>{group.modules}</small>
            </div>
          ))}
        </div>
        <div className="v3-flow-grid" aria-label="单链模块化流程图">
          {snakeModules.map(({ module, slot }) => (
            <div
              key={module.module_id}
              className="v3-chain-node"
              style={{ gridColumn: slot.column, gridRow: slot.row } as CSSProperties}
            >
              <ModuleCard
                module={moduleView(module, draft)}
                selected={selected?.module_id === module.module_id}
                onSelect={selectModule}
                templateRole={module.module_id === variableModule || variableModules.includes(module.module_id) ? "variable" : lockedModules[module.module_id] ? "locked" : undefined}
              />
              {slot.edge && (
                <span className={`v3-edge v3-edge-${slot.edge}`} aria-hidden="true">
                  {slot.edge === "left" ? "←" : slot.edge === "down" ? "↓" : "→"}
                </span>
              )}
            </div>
          ))}
        </div>
      </div>
      <ModuleDetailPanel
        module={selected}
        draft={draft}
        onDraftModuleChange={updateSelected}
        variableModule={variableModule}
        lockedModules={lockedModules}
      />
    </section>
  );
}
