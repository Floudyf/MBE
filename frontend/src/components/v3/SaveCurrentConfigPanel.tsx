import { useState } from "react";
import type { V3SavedConfigKind } from "../../api";

type Props = {
  disabled?: boolean;
  validationStatus: "unknown" | "valid" | "runnable" | "blocked";
  onSave: (kind: V3SavedConfigKind, name: string, description: string, tags: string[]) => void;
};

export default function SaveCurrentConfigPanel({ disabled = false, validationStatus, onSave }: Props) {
  const [name, setName] = useState("");
  const [description, setDescription] = useState("");
  const [tags, setTags] = useState("metatrack, local_emulator");

  function save(kind: V3SavedConfigKind) {
    if (!name.trim()) return;
    onSave(kind, name.trim(), description.trim(), tags.split(",").map((tag) => tag.trim()).filter(Boolean));
  }

  return (
    <section className="final-card saved-config-save-panel">
      <p className="eyebrow">保存当前配置</p>
      <h3>命名后复用为正式实验方案</h3>
      <p className="muted">工作流状态进入保存阶段前，请先完成后端校验；校验未通过仍可保存草稿，但会标记为 blocked。</p>
      <div className="topology-field-grid">
        <label className="field-card">
          <span>配置名称</span>
          <input value={name} onChange={(event) => setName(event.target.value)} placeholder="例如 MetaTrack full + Relay + Merkle" />
          <small>必填</small>
        </label>
        <label className="field-card">
          <span>描述</span>
          <input value={description} onChange={(event) => setDescription(event.target.value)} placeholder="可选" />
          <small>description</small>
        </label>
        <label className="field-card">
          <span>标签</span>
          <input value={tags} onChange={(event) => setTags(event.target.value)} />
          <small>逗号分隔</small>
        </label>
      </div>
      <div className="v3-run-buttons">
        <button type="button" disabled={disabled || !name.trim()} onClick={() => save("method")}>保存当前完整方案</button>
        <button type="button" className="v3-secondary-button" disabled={disabled || !name.trim()} onClick={() => save("workload")}>保存当前负载配置</button>
        <button type="button" className="v3-secondary-button" disabled={disabled || !name.trim()} onClick={() => save("topology")}>保存当前拓扑配置</button>
      </div>
      <p className="muted">当前保存 validation_status = {validationStatus}</p>
    </section>
  );
}
