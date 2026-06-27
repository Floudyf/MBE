import { type CSSProperties, useMemo, useState } from "react";

import type { V3ComposerModule, V3ComposerPreview } from "../../api";
import ModuleCard from "./ModuleCard";
import ModuleDetailPanel from "./ModuleDetailPanel";
import { moduleShortNames } from "./localization";

type Props = {
  preview: V3ComposerPreview;
};

export default function SingleChainComposer({ preview }: Props) {
  const modules = preview.modules || [];
  const [selectedId, setSelectedId] = useState(modules[0]?.module_id || "");
  const selected = useMemo(
    () => modules.find((module) => module.module_id === selectedId) || modules[0],
    [modules, selectedId],
  );

  function selectModule(module: V3ComposerModule) {
    setSelectedId(module.module_id);
  }

  return (
    <section className="v3-composer-layout">
      <div className="v3-chain-band">
        <div className="v3-flow-grid" aria-label="单链模块化流程图">
          {modules.map((module, index) => (
            <div key={module.module_id} className="v3-chain-node" style={{ "--flow-index": index } as CSSProperties}>
              <ModuleCard module={module} selected={selected?.module_id === module.module_id} onSelect={selectModule} />
              {index < modules.length - 1 && (
                <span className="v3-edge" aria-hidden="true">
                  <span>流向</span>
                  <b>{moduleShortNames[modules[index + 1]?.module_id] || modules[index + 1]?.module_id}</b>
                </span>
              )}
            </div>
          ))}
        </div>
      </div>
      <ModuleDetailPanel module={selected} />
    </section>
  );
}
