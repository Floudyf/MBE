import type { V3TemplateSummary } from "../../api";
import { labelFor, profileLabels, templateLabels } from "./localization";

type Props = {
  templates: V3TemplateSummary[];
  selectedProfile: string;
  onProfileChange: (profileId: string) => void;
};

const profileOptions = [
  { id: "metatrack_go_backed_ablation_smoke", label: "MetaTrack Go-backed 消融 Smoke" },
  { id: "single_chain_role_separation_smoke", label: "单链角色拆分 Smoke" },
  { id: "single_chain_composer_preview", label: "单链 Composer 预览" },
];

export default function ExperimentTemplateSelector({ templates, selectedProfile, onProfileChange }: Props) {
  return (
    <section className="final-card">
      <p className="eyebrow">实验模板</p>
      <label>
        实验配置
        <select value={selectedProfile} onChange={(event) => onProfileChange(event.target.value)}>
          {profileOptions.map((item) => <option key={item.id} value={item.id}>{item.label}</option>)}
        </select>
      </label>
      <div className="v3-template-list">
        {templates.map((template) => (
          <div key={template.template_id} className="v3-template-row">
            <span><strong>{labelFor(templateLabels, template.template_id)}</strong><small>{template.template_id}</small></span>
            <span className={`v3-status-badge status-${template.runnable ? "variable" : "planned"}`}>
              {template.runnable ? "可运行" : template.preview_only ? "仅预览" : "规划中"}
            </span>
          </div>
        ))}
      </div>
      <p className="muted">当前选择：{labelFor(profileLabels, selectedProfile)}</p>
    </section>
  );
}
