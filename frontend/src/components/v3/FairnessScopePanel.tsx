import type { V3FairnessScope } from "../../api";
import { labelFor, moduleNames, yesNo } from "./localization";

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
        {(items && items.length ? items : ["无"]).map((item) => (
          <span key={item} className={`v3-status-badge status-${status}`}>{labelFor(moduleNames, item, item)}</span>
        ))}
      </dd>
    </div>
  );
}

export default function FairnessScopePanel({ scope, valid, warnings = [] }: Props) {
  return (
    <section className="final-card">
      <p className="eyebrow">公平性约束</p>
      <h3>{valid === false ? "公平性检查需要处理" : "公平性检查通过"}</h3>
      <p>不同方法之间只允许“实验变量模块”发生变化，其余模块保持一致。</p>
      <dl className="v3-scope-list">
        <ModulePills label="实验变量模块" items={scope.variable_modules} status="variable" />
        <ModulePills label="固定环境模块" items={scope.fixed_modules} status="fixed" />
        <ModulePills label="已关闭模块" items={scope.disabled_modules} status="disabled" />
        <ModulePills label="规划中模块" items={scope.planned_modules} status="planned" />
        <ModulePills label="输出模块" items={scope.output_modules} status="output" />
      </dl>
      <ul className="v3-check-list">
        <li>固定模块必须保持一致：{yesNo(scope.fixed_modules_must_match)}</li>
        <li>规划中模块不可运行：{yesNo(scope.planned_modules_not_runnable)}</li>
        <li>使用相同负载和随机种子：{yesNo(scope.same_workload && scope.same_seed)}</li>
      </ul>
      {warnings.length > 0 && <p className="muted">{warnings[0]}</p>}
    </section>
  );
}
