import { useMemo, useState } from "react";

import type { V3ComposerModule, V3ComposerPreview } from "../../api";
import ModuleCard from "./ModuleCard";
import ModuleDetailPanel from "./ModuleDetailPanel";

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
        <div className="v3-chain-scroll" aria-label="Single-chain module graph">
          {modules.map((module, index) => (
            <div key={module.module_id} className="v3-chain-node">
              <ModuleCard module={module} selected={selected?.module_id === module.module_id} onSelect={selectModule} />
              {index < modules.length - 1 && <span className="v3-edge" aria-hidden="true">-&gt;</span>}
            </div>
          ))}
        </div>
      </div>
      <ModuleDetailPanel module={selected} />
    </section>
  );
}
