import type { V3FairnessScope } from "../../api";
import type { ComposerDraft } from "./composerDraft";
import { labelFor, moduleNames, yesNo } from "./localization";

type Props = {
  scope: V3FairnessScope;
  valid?: boolean;
  warnings?: string[];
  draft?: ComposerDraft | null;
};

function moduleList(items?: string[]): string {
  return items && items.length ? items.map((item) => labelFor(moduleNames, item, item)).join("、") : "无";
}

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

export default function FairnessScopePanel({ scope, valid, warnings = [], draft }: Props) {
  const summaryMessages = draft?.validationMessages.slice(0, 2) || [];

  return (
    <section className="final-card v3-draft-summary-card">
      <p className="eyebrow">Draft 校验摘要</p>
      <h3>{draft?.hasValidationErrors ? "当前 Draft 需要调整" : "当前 Draft 可预览"}</h3>
      <ul className="v3-check-list compact">
        {summaryMessages.map((message) => <li key={message}>{message}</li>)}
        <li>自定义 Draft 运行后续支持。</li>
      </ul>
      {draft && (
        <dl className="v3-compact-scope">
          <div><dt>实验变量模块</dt><dd>{moduleList(draft.variableModules)}</dd></div>
          <div><dt>固定 / 默认模块</dt><dd>{moduleList(draft.fixedModules)}</dd></div>
        </dl>
      )}
      {warnings.length > 0 && <p className="muted">{warnings[0]}</p>}

      <details className="v3-foldout">
        <summary className="v3-foldout-summary">模板公平性约束</summary>
        <dl className="v3-scope-list">
          <ModulePills label="模板实验变量模块" items={scope.variable_modules} status="variable" />
          <ModulePills label="模板固定环境模块" items={scope.fixed_modules} status="fixed" />
          <ModulePills label="模板已关闭模块" items={scope.disabled_modules} status="disabled" />
          <ModulePills label="模板规划中模块" items={scope.planned_modules} status="planned" />
          <ModulePills label="模板输出模块" items={scope.output_modules} status="output" />
        </dl>
        <ul className="v3-check-list">
          <li>后端公平性检查：{valid === false ? "需要处理" : "通过"}</li>
          <li>固定模块必须保持一致：{yesNo(scope.fixed_modules_must_match)}</li>
          <li>规划中模块不可运行：{yesNo(scope.planned_modules_not_runnable)}</li>
          <li>使用相同负载和随机种子：{yesNo(scope.same_workload && scope.same_seed)}</li>
        </ul>
      </details>

      {draft && (
        <details className="v3-foldout">
          <summary className="v3-foldout-summary">当前 Draft Scope 详情</summary>
          <dl className="v3-scope-list">
            <ModulePills label="Draft 实验变量模块" items={draft.variableModules} status="variable" />
            <ModulePills label="Draft 固定 / 默认模块" items={draft.fixedModules} status="fixed" />
            <ModulePills label="Draft 已关闭模块" items={draft.disabledModules} status="disabled" />
            <ModulePills label="Draft 规划中模块" items={draft.plannedModules} status="planned" />
            <ModulePills label="Draft 输出模块" items={draft.outputModules} status="output" />
          </dl>
        </details>
      )}
    </section>
  );
}
