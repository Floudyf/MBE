import type { V3FairnessScope } from "../../api";

type Props = {
  scope: V3FairnessScope;
  valid?: boolean;
  warnings?: string[];
};

function ModulePills({ label, items, status }: { label: string; items?: string[]; status: string }) {
  return (
    <div className="v3-scope-row">
      <dt>{label}</dt>
      <dd>
        {(items && items.length ? items : ["none"]).map((item) => (
          <span key={item} className={`v3-status-badge status-${status}`}>{item}</span>
        ))}
      </dd>
    </div>
  );
}

export default function FairnessScopePanel({ scope, valid, warnings = [] }: Props) {
  return (
    <section className="final-card">
      <p className="eyebrow">Fairness Scope</p>
      <h3>{valid === false ? "Fairness check needs attention" : "Fairness guard passed"}</h3>
      <p>Only variable modules may differ across methods.</p>
      <dl className="v3-scope-list">
        <ModulePills label="Variable Modules" items={scope.variable_modules} status="variable" />
        <ModulePills label="Fixed Modules" items={scope.fixed_modules} status="fixed" />
        <ModulePills label="Disabled Modules" items={scope.disabled_modules} status="disabled" />
        <ModulePills label="Planned Modules" items={scope.planned_modules} status="planned" />
        <ModulePills label="Output Modules" items={scope.output_modules} status="output" />
      </dl>
      <ul className="v3-check-list">
        <li>Fixed modules must match: {String(Boolean(scope.fixed_modules_must_match))}</li>
        <li>Planned modules not runnable: {String(Boolean(scope.planned_modules_not_runnable))}</li>
        <li>Same workload and seed: {String(Boolean(scope.same_workload && scope.same_seed))}</li>
      </ul>
      {warnings.length > 0 && <p className="muted">{warnings[0]}</p>}
    </section>
  );
}
