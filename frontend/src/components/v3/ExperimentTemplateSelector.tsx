import type { V3TemplateSummary } from "../../api";

type Props = {
  templates: V3TemplateSummary[];
  selectedProfile: string;
  onProfileChange: (profileId: string) => void;
};

const profileOptions = [
  { id: "metatrack_go_backed_ablation_smoke", label: "MetaTrack Go-backed ablation smoke" },
  { id: "single_chain_role_separation_smoke", label: "Role separation smoke" },
  { id: "single_chain_composer_preview", label: "Composer preview only" },
];

export default function ExperimentTemplateSelector({ templates, selectedProfile, onProfileChange }: Props) {
  return (
    <section className="final-card">
      <p className="eyebrow">Experiment Template</p>
      <label>
        Experiment Profile
        <select value={selectedProfile} onChange={(event) => onProfileChange(event.target.value)}>
          {profileOptions.map((item) => <option key={item.id} value={item.id}>{item.label}</option>)}
        </select>
      </label>
      <div className="v3-template-list">
        {templates.map((template) => (
          <div key={template.template_id} className="v3-template-row">
            <strong>{template.template_id}</strong>
            <span className={`v3-status-badge status-${template.runnable ? "variable" : "planned"}`}>
              {template.runnable ? "Runnable" : template.preview_only ? "Preview Only" : "Planned"}
            </span>
          </div>
        ))}
      </div>
    </section>
  );
}
