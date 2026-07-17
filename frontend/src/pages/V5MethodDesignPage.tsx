import { useEffect, useMemo, useRef, useState } from "react";

import { createV3SavedConfig, fetchV5PluginCatalog, listV3SavedConfigs, validateV5ExperimentSpec, type V3SavedConfig, type V5CompatibilityResult, type V5PluginManifest, type V5PluginSelection } from "../api";
import { V5_METHOD_PROFILE_SCHEMA_VERSION, buildV5MethodValidationSpec, parseSavedV5Method, v5MethodSelectionsFromCatalog } from "../v5MethodProfile";

type Props = { onOpenRun: (configId: string) => void; onDirtyChange?: (dirty: boolean) => void; saveRequestToken?: number; onSaveRequestHandled?: (token: number) => void };
type View = "flow" | "category" | "parameters" | "dependencies";
type ModuleGroup = { id: string; title: string; description: string; tone: string; categories: string[]; support?: boolean };

const roles = ["main", "baseline", "ablation", "custom"] as const;
const roleLabels: Record<string, string> = { main: "完整方法", baseline: "基线方法", ablation: "消融方法", custom: "自定义方法" };
const tabs: Array<{ id: View; label: string }> = [
  { id: "flow", label: "流程配置" },
  { id: "category", label: "组件清单" },
  { id: "parameters", label: "默认参数" },
  { id: "dependencies", label: "兼容性与来源" },
];
const groups: ModuleGroup[] = [
  { id: "ingress", title: "交易入口", description: "定义交易接收、校验与进入系统的入口行为。", tone: "green", categories: ["transaction_admission", "txpool", "block_producer"] },
  { id: "sharding", title: "分片控制", description: "定义分片、路由与跨片协同的控制逻辑。", tone: "blue", categories: ["sharding", "routing", "cross_shard"] },
  { id: "ordering", title: "排序与共识", description: "定义交易预排序、共识确认与区块最终性。", tone: "amber", categories: ["scheduler", "consensus"] },
  { id: "execution", title: "执行与状态", description: "交易分类、区块执行、状态访问、存储和提交。", tone: "purple", categories: ["execution", "block_executor", "state_access", "state_storage", "commit"] },
  { id: "environment", title: "运行环境", description: "定义参与节点、网络拓扑与运行支撑能力。", tone: "cyan", categories: ["network"], support: true },
  { id: "evidence", title: "结果证据", description: "定义区块执行证据与可验证性。", tone: "slate", categories: ["metrics", "observability"] },
];
const categoryLabels: Record<string, string> = {
  transaction_admission: "交易准入",
  txpool: "交易池",
  block_producer: "区块组装",
  sharding: "状态分片",
  routing: "交易放置 / 路由",
  cross_shard: "跨片协调",
  scheduler: "区块前排序",
  consensus: "共识与最终性",
  execution: "执行策略",
  block_executor: "区块执行引擎",
  state_access: "状态访问",
  state_storage: "状态存储",
  commit: "提交策略",
  network: "节点网络",
  metrics: "指标采集",
  observability: "运行观测",
};
const categoryResponsibilities: Record<string, string> = {
  transaction_admission: "校验交易签名、随机数与准入条件",
  txpool: "缓存和组织等待处理的交易",
  block_producer: "按照数量或时间组装交易区块",
  sharding: "定义状态和账户的分片映射",
  routing: "决定交易的目标执行分片",
  cross_shard: "协调跨分片交易的执行与完成",
  scheduler: "确定交易进入区块或执行阶段前的顺序",
  consensus: "对区块提议达成共识并确认最终性",
  execution: "决定交易执行轨道或执行策略",
  block_executor: "执行区块内交易并生成执行结果",
  state_access: "定义执行期间如何读取状态",
  state_storage: "负责状态持久化与状态根维护",
  commit: "决定执行结果如何提交和聚合",
  network: "负责节点进程之间的消息通信",
  metrics: "采集实验性能和运行指标",
  observability: "生成日志、追踪和运行证据",
};

export default function V5MethodDesignPage({ onOpenRun, onDirtyChange, saveRequestToken = 0, onSaveRequestHandled }: Props) {
  const [catalog, setCatalog] = useState<V5PluginManifest[]>([]);
  const [selections, setSelections] = useState<V5PluginSelection[]>([]);
  const [saved, setSaved] = useState<V3SavedConfig[]>([]);
  const [name, setName] = useState("");
  const [description, setDescription] = useState("");
  const [role, setRole] = useState("main");
  const [compatibility, setCompatibility] = useState<V5CompatibilityResult | null>(null);
  const [savedConfigId, setSavedConfigId] = useState("");
  const [validatedSnapshot, setValidatedSnapshot] = useState("");
  const [message, setMessage] = useState("");
  const [error, setError] = useState("");
  const [busy, setBusy] = useState(false);
  const [activeView, setActiveView] = useState<View>("flow");
  const [focusedCategory, setFocusedCategory] = useState("");
  const [showAllCapabilities, setShowAllCapabilities] = useState(false);
  const [showAllSavedMethods, setShowAllSavedMethods] = useState(false);
  const revision = useRef(0);
  const lastHandledSaveRequestToken = useRef(0);
  const initialSnapshot = useRef("");
  const snapshot = useMemo(() => JSON.stringify(selections.map((item) => [item.category, item.plugin_id]).sort()), [selections]);
  const validCurrent = Boolean(name.trim() && compatibility?.valid && validatedSnapshot === snapshot);
  const designSnapshot = useMemo(() => JSON.stringify({ name, description, role, selections: selections.map((item) => [item.category, item.plugin_id]) }), [name, description, role, selections]);
  const defaultSelections = useMemo(() => v5MethodSelectionsFromCatalog(catalog), [catalog]);
  const currentPlugins = useMemo<ModuleState[]>(() => selections.flatMap((selection) => {
    const plugin = pluginFor(catalog, selection);
    if (!plugin) return [];
    return [{ selection, plugin, defaultPlugin: defaultPluginFor(defaultSelections, catalog, selection.category) }];
  }), [catalog, defaultSelections, selections]);
  const summary = useMemo(() => buildSummary(currentPlugins), [currentPlugins]);
  const focused = (focusedCategory ? currentPlugins.find((item) => item.selection.category === focusedCategory) : undefined) ?? currentPlugins.find((item) => item.selection.category === "block_executor") ?? currentPlugins[0];

  useEffect(() => { void load(); }, []);
  useEffect(() => { if (!initialSnapshot.current && selections.length) initialSnapshot.current = designSnapshot; onDirtyChange?.(Boolean(initialSnapshot.current && initialSnapshot.current !== designSnapshot)); }, [designSnapshot, onDirtyChange, selections.length]);
  useEffect(() => { if (saveRequestToken > 0 && saveRequestToken !== lastHandledSaveRequestToken.current) { lastHandledSaveRequestToken.current = saveRequestToken; onSaveRequestHandled?.(saveRequestToken); void save(true); } }, [saveRequestToken, onSaveRequestHandled]);

  async function load() {
    const [catalogResult, savedResult] = await Promise.allSettled([fetchV5PluginCatalog("real_cluster"), listV3SavedConfigs("method")]);
    if (catalogResult.status === "fulfilled") {
      setCatalog(catalogResult.value);
      const initial = v5MethodSelectionsFromCatalog(catalogResult.value);
      setSelections(initial);
    } else {
      setError(text(catalogResult.reason));
    }
    if (savedResult.status === "fulfilled") setSaved(savedResult.value);
    else setMessage("已保存方法暂时无法读取；仍可设计新的目录方法。");
  }

  function change(category: string, pluginId: string) {
    const plugin = catalog.find((item) => item.category === category && item.plugin_id === pluginId);
    if (!plugin) { setError(`Plugin ${pluginId} is not available for ${category}.`); return; }
    revision.current += 1;
    setSelections((current) => current.map((item) => item.category === category ? { ...item, plugin_id: pluginId, config: { ...plugin.default_config } } : item));
    setFocusedCategory(category);
    setCompatibility(null);
    setValidatedSnapshot("");
    setMessage("");
    setError("");
  }

  async function validate() {
    const current = snapshot;
    const requestRevision = revision.current;
    setBusy(true);
    try {
      const result = await validateV5ExperimentSpec(buildV5MethodValidationSpec(catalog, selections));
      if (requestRevision !== revision.current || current !== snapshot) return;
      setCompatibility(result);
      setValidatedSnapshot(current);
      setError("");
    } catch (caught) {
      if (requestRevision === revision.current) setError(text(caught));
    } finally {
      setBusy(false);
    }
  }

  async function save(continueToRun: boolean) {
    if (!validCurrent) { setError("请先完成方法名称并验证当前插件组合。"); return; }
    setBusy(true);
    try {
      const profile = await createV3SavedConfig({ config_kind: "method", name: name.trim(), description, owner_label: "local_user", tags: ["v5", "real_cluster", role], validation_status: "runnable", last_validation: compatibility ?? undefined, payload: { schema_version: V5_METHOD_PROFILE_SCHEMA_VERSION, plugin_selections: selections, plugin_parameters: selections, compatibility_snapshot: compatibility, source_composer_draft: { source: "v5_method_designer", role } } });
      setSaved((current) => [profile, ...current]);
      setSavedConfigId(profile.config_id);
      initialSnapshot.current = designSnapshot;
      onDirtyChange?.(false);
      setMessage(`已保存 ${profile.config_id}。`);
      setError("");
      if (continueToRun) onOpenRun(profile.config_id);
    } catch (caught) {
      setError(text(caught));
    } finally {
      setBusy(false);
    }
  }

  const methods = saved.map((item) => ({ saved: item, method: parseSavedV5Method(item, catalog) })).filter((item): item is { saved: V3SavedConfig; method: NonNullable<ReturnType<typeof parseSavedV5Method>> } => item.method !== null);

  return <section className="v5-method-page" data-testid="v5-method-design-page">
    <header className="final-card wide v5-method-hero">
      <div>
        <h2>实验方法设计</h2>
        <p>配置论文方法、基线方法和消融方法，并保存为可复用的方法配置。</p>
      </div>
      {error && <p className="file-error">{error}</p>}
      {message && <p className="notice">{message}</p>}
    </header>

    <div className="v5-method-layout">
      <main className="v5-method-workspace">
        <article className="final-card v5-method-basic">
          <div className="section-heading"><div><h3>方法基本信息</h3></div><span className="v5-method-role-pill">{roleLabels[role] ?? role}</span></div>
          <div className="form-grid">
            <label><span>方法名称</span><input data-testid="v5-method-name" value={name} onChange={(event) => setName(event.target.value)} /></label>
            <label><span>方法说明</span><input data-testid="v5-method-description" value={description} onChange={(event) => setDescription(event.target.value)} /></label>
            <label><span>方法角色</span><select data-testid="v5-method-role" value={role} onChange={(event) => setRole(event.target.value)}>{roles.map((item) => <option key={item} value={item}>{roleLabels[item]}</option>)}</select></label>
          </div>
        </article>

        <article className="final-card v5-method-designer">
          <div className="section-heading">
            <div><h3>方法设计工作区</h3></div>
            <p className="muted">负载与故障注入属于实验工况，不属于方法变量。</p>
          </div>
          <div className="v5-method-tabs" role="tablist" aria-label="方法设计视图">
            {tabs.map((tab) => <button key={tab.id} type="button" role="tab" aria-selected={activeView === tab.id} className={activeView === tab.id ? "active" : ""} onClick={() => setActiveView(tab.id)}>{tab.label}</button>)}
          </div>
          {activeView === "flow" && <MethodFlowView states={currentPlugins} catalog={catalog} onFocus={setFocusedCategory} onChange={change} />}
          {activeView === "category" && <MethodCategoryView states={currentPlugins} catalog={catalog} onFocus={setFocusedCategory} onChange={change} />}
          {activeView === "parameters" && <MethodParameterOverview states={currentPlugins} />}
          {activeView === "dependencies" && <MethodDependencyView states={currentPlugins} catalog={catalog} />}
          {focused && <FocusedPluginDetails state={focused} />}
        </article>

      </main>

      <MethodSummarySidebar
        name={name}
        description={description}
        role={role}
        summary={summary}
        compatibility={compatibility}
        savedConfigId={savedConfigId}
        busy={busy}
        catalogReady={Boolean(catalog.length)}
        validCurrent={validCurrent}
        showAllCapabilities={showAllCapabilities}
        onShowAllCapabilities={() => setShowAllCapabilities((value) => !value)}
        onValidate={() => void validate()}
        onSave={() => void save(false)}
        onSaveAndRun={() => void save(true)}
      />
    </div>
    <SavedMethodLibrary methods={methods} defaultSelections={defaultSelections} catalog={catalog} showAll={showAllSavedMethods} onToggleShowAll={() => setShowAllSavedMethods((value) => !value)} onOpenRun={onOpenRun} />
  </section>;
}

type ModuleState = { selection: V5PluginSelection; plugin: V5PluginManifest; defaultPlugin?: V5PluginManifest };
type MethodSummary = ReturnType<typeof buildSummary>;

function MethodFlowView({ states, catalog, onFocus, onChange }: { states: ModuleState[]; catalog: V5PluginManifest[]; onFocus: (category: string) => void; onChange: (category: string, pluginId: string) => void }) {
  return <div className="v5-flow-shell">
    <div className="v5-flow-main">{groups.map((group, index) => <div key={group.id} className="v5-flow-step-wrap">
      <MethodGroup group={group} states={states} catalog={catalog} onFocus={onFocus} onChange={onChange} />
      {index < groups.length - 1 && <span className="v5-flow-arrow" aria-hidden="true">↓</span>}
    </div>)}</div>
  </div>;
}

function MethodCategoryView(props: { states: ModuleState[]; catalog: V5PluginManifest[]; onFocus: (category: string) => void; onChange: (category: string, pluginId: string) => void }) {
  return <div className="v5-category-view">
    <p className="notice">流程配置和组件清单共享同一份方法配置，任意一处修改都会同步。</p>
    {groups.map((group) => <CategoryGroupTable key={group.id} group={group} {...props} />)}
  </div>;
}

function MethodGroup({ group, states, catalog, onFocus, onChange, variant = "flow" }: { group: ModuleGroup; states: ModuleState[]; catalog: V5PluginManifest[]; onFocus: (category: string) => void; onChange: (category: string, pluginId: string) => void; variant?: "flow" | "list" }) {
  const groupStates = group.categories.map((category) => states.find((item) => item.selection.category === category)).filter((item): item is ModuleState => Boolean(item));
  return <section className={`v5-method-group tone-${group.tone} ${group.support ? "support" : ""} ${variant === "list" ? "list" : ""}`} data-testid={`v5-method-group-${group.id}`}>
    <header><div><h4>{group.title}</h4><p>{group.description}</p></div><span className="v5-status-chip neutral">{groupStates.length} 个模块</span></header>
    <div className="v5-module-grid">{groupStates.map((state) => <MethodModuleCard key={state.selection.category} state={state} choices={catalog.filter((item) => item.category === state.selection.category)} onFocus={onFocus} onChange={onChange} />)}</div>
  </section>;
}

function MethodModuleCard({ state, choices, onFocus, onChange }: { state: ModuleState; choices: V5PluginManifest[]; onFocus: (category: string) => void; onChange: (category: string, pluginId: string) => void }) {
  const category = state.selection.category;
  const isOverride = state.defaultPlugin?.plugin_id !== state.selection.plugin_id;
  const status = moduleStatus(state);
  return <article className={`v5-module-card ${isOverride ? "override" : ""}`} data-testid={`v5-method-category-${category}`} tabIndex={0} onClick={() => onFocus(category)} onFocus={() => onFocus(category)}>
    <div className="v5-module-card-head">
      <div className="v5-module-title"><span aria-hidden="true">{moduleIcon(category)}</span><h5>{label(category)}</h5></div>
      <span className={`v5-status-chip ${status.tone}`}>{status.label}</span>
    </div>
    <code className="plugin-code">{category}</code>
    <p>{categoryResponsibilities[category] || state.plugin.description_zh || state.plugin.description}</p>
    <label onClick={(event) => event.stopPropagation()}><span>{state.plugin.plugin_id}</span><select value={state.selection.plugin_id} onChange={(event) => onChange(category, event.target.value)}>{choices.map((item) => <option key={item.plugin_id} value={item.plugin_id}>{item.display_name_zh || item.display_name}</option>)}</select></label>
  </article>;
}

function CategoryGroupTable({ group, states, catalog, onFocus, onChange }: { group: ModuleGroup; states: ModuleState[]; catalog: V5PluginManifest[]; onFocus: (category: string) => void; onChange: (category: string, pluginId: string) => void }) {
  const groupStates = group.categories.map((category) => states.find((item) => item.selection.category === category)).filter((item): item is ModuleState => Boolean(item));
  return <section className={`v5-category-group tone-${group.tone}`} data-testid={`v5-method-category-group-${group.id}`}>
    <header><div><h4>{group.title}</h4><p>{groupStates.length} 个模块</p></div></header>
    <div className="v5-category-table" role="table" aria-label={`${group.title} 插件分类`}>
      <div className="v5-category-row header" role="row"><span>模块</span><span>ID</span><span>当前插件选择</span><span>说明</span><span>状态</span></div>
      {groupStates.map((state) => {
        const status = moduleStatus(state);
        return <button key={state.selection.category} type="button" className="v5-category-row" role="row" onClick={() => onFocus(state.selection.category)}>
          <strong>{label(state.selection.category)}</strong>
          <code>{state.selection.category}</code>
          <label onClick={(event) => event.stopPropagation()}><select value={state.selection.plugin_id} onChange={(event) => onChange(state.selection.category, event.target.value)}>{catalog.filter((item) => item.category === state.selection.category).map((item) => <option key={item.plugin_id} value={item.plugin_id}>{item.display_name_zh || item.display_name}</option>)}</select><small>{state.plugin.plugin_id}</small></label>
          <span>{categoryResponsibilities[state.selection.category] || state.plugin.description_zh || state.plugin.description}</span>
          <span className={`v5-status-chip ${status.tone}`}>{status.label}</span>
        </button>;
      })}
    </div>
  </section>;
}

function MethodParameterOverview({ states }: { states: ModuleState[] }) {
  const configurable = states.filter((item) => Object.keys(item.plugin.default_config).length > 0);
  const empty = states.filter((item) => Object.keys(item.plugin.default_config).length === 0);
  return <div className="v5-parameter-view">
    <p className="notice">当前版本保存插件选择，并展示目录中的默认参数。方法独立参数尚未进入 Formal Matrix。</p>
    <h4>有默认参数的组件：{configurable.length} 个，共 {configurable.reduce((count, item) => count + Object.keys(item.plugin.default_config).length, 0)} 个参数</h4>
    {groups.map((group) => {
      const groupStates = group.categories.map((category) => configurable.find((item) => item.selection.category === category)).filter((item): item is ModuleState => Boolean(item));
      if (!groupStates.length) return null;
      return <section key={group.id} className="v5-parameter-group">
      <h4>{group.title}</h4>
      <div className="v5-parameter-list">{groupStates.map((state) => <article key={state.selection.category} className="v5-parameter-card">
        <header><strong>{label(state.selection.category)}</strong><code>{state.plugin.plugin_id}</code></header>
        <ParameterList plugin={state.plugin} />
      </article>)}</div>
    </section>;
    })}
    <details className="v5-empty-parameters"><summary>无参数组件（{empty.length}）</summary><div className="v5-compact-list">{empty.map((state) => <span key={state.selection.category}>{label(state.selection.category)} <code>{state.plugin.plugin_id}</code></span>)}</div></details>
  </div>;
}

function ParameterList({ plugin }: { plugin: V5PluginManifest }) {
  const entries = Object.entries(plugin.default_config);
  if (!entries.length) return <p className="muted">无可配置参数</p>;
  const schema = plugin.config_schema.properties ?? {};
  return <dl className="v5-parameter-kv">{entries.map(([key, value]) => <div key={key}><dt>{schema[key]?.title || key}{schema[key]?.readOnly ? <span>只读</span> : null}</dt><dd>{formatValue(value)}</dd></div>)}</dl>;
}

function MethodDependencyView({ states, catalog }: { states: ModuleState[]; catalog: V5PluginManifest[] }) {
  const [filter, setFilter] = useState<"all" | "attention" | "override">("all");
  const rows = states.filter((state) => {
    if (filter === "override") return state.defaultPlugin?.plugin_id !== state.selection.plugin_id;
    if (filter === "attention") {
      const checks = state.plugin.requirements.map((requirement) => requirementSatisfied(requirement, states, catalog));
      return state.plugin.conflicts.length > 0 || checks.some((item) => !item) || state.plugin.requirements.length > 0;
    }
    return true;
  });
  return <div className="v5-dependency-view">
    <header className="v5-dependency-intro"><h4>兼容性与来源</h4><p>查看当前插件组合的依赖、冲突、能力、来源与真实性边界。最终运行资格以后端兼容性校验结果为准。</p></header>
    <div className="v5-filter-row" aria-label="兼容性筛选">
      <button type="button" className={filter === "all" ? "active" : ""} onClick={() => setFilter("all")}>全部组件</button>
      <button type="button" className={filter === "attention" ? "active" : ""} onClick={() => setFilter("attention")}>有依赖或待验证</button>
      <button type="button" className={filter === "override" ? "active" : ""} onClick={() => setFilter("override")}>仅方法覆盖</button>
    </div>
    <div className="v5-dependency-matrix" role="table" aria-label="模块依赖与兼容性矩阵">
      <div className="v5-dependency-row header" role="row"><span>模块</span><span>当前插件</span><span>依赖与冲突</span><span>核心能力</span><span>状态</span></div>
      {rows.map((state) => <DependencyRow key={state.selection.category} state={state} states={states} catalog={catalog} />)}
    </div>
    {!rows.length && <p className="muted">当前筛选下没有组件。</p>}
    <p className="notice">前端判断仅用于预览；最终运行资格以后端兼容性校验结果为准。</p>
  </div>;
}

function DependencyRow({ state, states, catalog }: { state: ModuleState; states: ModuleState[]; catalog: V5PluginManifest[] }) {
  const checks = state.plugin.requirements.map((requirement) => ({ requirement, satisfied: requirementSatisfied(requirement, states, catalog) }));
  const status = state.plugin.conflicts.length ? { label: "有冲突", tone: "warning" } : checks.length ? checks.every((item) => item.satisfied) ? { label: "已满足", tone: "ok" } : { label: "未满足", tone: "warning" } : { label: "无依赖", tone: "neutral" };
  return <details className="v5-dependency-entry">
    <summary className="v5-dependency-row" role="row">
      <span><strong>{label(state.selection.category)}</strong><code>{state.selection.category}</code></span>
      <span><strong>{state.plugin.display_name_zh || state.plugin.display_name}</strong><code>{state.plugin.plugin_id}</code></span>
      <span><small>依赖</small><CompactTags values={state.plugin.requirements} checks={checks} empty="无" /><small>冲突</small><CompactTags values={state.plugin.conflicts} empty="无" /></span>
      <CompactTags values={state.plugin.capabilities} empty="未提供" limit={3} />
      <span className={`v5-status-chip ${status.tone}`}>{status.label}</span>
    </summary>
    <dl className="v5-dependency-details">
      <div><dt>requirements</dt><dd><CompactTags values={state.plugin.requirements} checks={checks} empty="无" limit={99} /></dd></div>
      <div><dt>conflicts</dt><dd><CompactTags values={state.plugin.conflicts} empty="无" limit={99} /></dd></div>
      <div><dt>capabilities</dt><dd><CompactTags values={state.plugin.capabilities} empty="未提供" limit={99} /></dd></div>
      <div><dt>truth_boundary</dt><dd><code>{state.plugin.truth_boundary || "未声明"}</code></dd></div>
      <div><dt>source</dt><dd>{sourceSummary(state.plugin.source)}</dd></div>
      <div><dt>implementation_status</dt><dd>{state.plugin.implementation_status || "未声明"}</dd></div>
      <div><dt>supported_backends</dt><dd>{state.plugin.supported_backends.join(", ") || "未声明"}</dd></div>
      <div><dt>version</dt><dd>{state.plugin.version || "未声明"}</dd></div>
    </dl>
  </details>;
}

function CompactTags({ values, checks, empty, limit = 2 }: { values: string[]; checks?: Array<{ requirement: string; satisfied: boolean }>; empty: string; limit?: number }) {
  if (!values.length) return <span className="muted">{empty}</span>;
  const visible = values.slice(0, limit);
  return <span className="v5-compact-tags">{visible.map((value) => <code key={value} className={checks?.find((item) => item.requirement === value)?.satisfied === false ? "unmet" : ""}>{value}</code>)}{values.length > visible.length && <em>+{values.length - visible.length}</em>}</span>;
}

function FocusedPluginDetails({ state }: { state: ModuleState }) {
  return <details className="v5-focused-plugin" data-testid={`v5-method-plugin-${state.plugin.plugin_id}`}>
    <summary><span>当前聚焦：{label(state.selection.category)} · {state.plugin.display_name_zh || state.plugin.display_name}</span><strong>查看详情</strong></summary>
    <div><p className="eyebrow">当前聚焦模块</p><h4>{label(state.selection.category)} <code>{state.selection.category}</code></h4></div>
    <p><strong>{state.plugin.display_name_zh || state.plugin.display_name}</strong> <code>{state.plugin.plugin_id}</code></p>
    <p>{state.plugin.description_zh || state.plugin.description || "未声明"}</p>
    <div className="v5-detail-grid">
      <span>状态：{state.plugin.implementation_status || "未声明"}</span>
      <span>版本：{state.plugin.version || "未声明"}</span>
      <span>后端：{state.plugin.supported_backends.join(", ") || "未声明"}</span>
      <span>truth：{state.plugin.truth_boundary || "未声明"}</span>
      <span>source：{sourceSummary(state.plugin.source)}</span>
    </div>
    <MetaSection title="requirements" values={state.plugin.requirements} />
    <MetaSection title="conflicts" values={state.plugin.conflicts} />
    <MetaSection title="capabilities" values={state.plugin.capabilities} />
    <details><summary>查看默认参数</summary><ParameterList plugin={state.plugin} /></details>
  </details>;
}

function MetaSection({ title, values }: { title: string; values: string[] }) {
  return <p className="v5-meta-section"><strong>{title}</strong><CompactTags values={values} empty="未声明" limit={99} /></p>;
}

function MethodSummarySidebar({ name, description, role, summary, compatibility, savedConfigId, busy, catalogReady, validCurrent, showAllCapabilities, onShowAllCapabilities, onValidate, onSave, onSaveAndRun }: { name: string; description: string; role: string; summary: MethodSummary; compatibility: V5CompatibilityResult | null; savedConfigId: string; busy: boolean; catalogReady: boolean; validCurrent: boolean; showAllCapabilities: boolean; onShowAllCapabilities: () => void; onValidate: () => void; onSave: () => void; onSaveAndRun: () => void }) {
  const shownCapabilities = showAllCapabilities ? summary.capabilities : summary.capabilities.slice(0, 5);
  const shownOverrides = summary.overrides.slice(0, 5);
  return <aside className="final-card v5-method-summary" data-testid="v5-method-summary-sidebar">
    <section><p className="eyebrow">方法摘要</p><h3>{name.trim() || "未命名方法"}</h3><p>{roleLabels[role] ?? role}</p><p>{description.trim() || "未填写方法说明"}</p></section>
    <section><h4>模块覆盖</h4><dl className="v5-summary-metrics"><div><dt>总模块</dt><dd>{summary.total}</dd></div><div><dt>目录默认</dt><dd>{summary.defaults}</dd></div><div><dt>方法覆盖</dt><dd data-testid="v5-summary-overrides-count">{summary.overrides.length}</dd></div><div><dt>有依赖</dt><dd>{summary.dependencyCount}</dd></div><div><dt>有冲突</dt><dd>{summary.conflictCount}</dd></div></dl></section>
    <section><h4>方法改变了什么</h4>{summary.overrides.length ? <ul className="v5-summary-list">{shownOverrides.map((item) => <li key={item.category}><strong>{label(item.category)}</strong><span>{item.from} → {item.to}</span></li>)}</ul> : <p className="muted">当前方法仍使用全部目录默认组件。</p>}{summary.overrides.length > shownOverrides.length && <p className="muted">另有 {summary.overrides.length - shownOverrides.length} 个覆盖模块。</p>}</section>
    <section><h4>核心能力</h4>{shownCapabilities.length ? <div className="v5-chip-list">{shownCapabilities.map((item) => <span key={item}>{item}</span>)}</div> : <p className="muted">未提供能力标签。</p>}{summary.capabilities.length > 5 && <button type="button" className="ghost-button" onClick={onShowAllCapabilities}>{showAllCapabilities ? "收起" : `查看全部 ${summary.capabilities.length} 项`}</button>}</section>
    <section><h4>参数摘要</h4><p>{summary.pluginsWithConfig} 个插件存在非空目录默认参数，共 {summary.parameterKeyCount} 个参数键。</p></section>
    <section><h4>依赖与兼容性</h4><p>前端依赖：{summary.dependenciesSatisfied ? "全部满足或无依赖" : "存在未满足项"}</p><div data-testid="v5-method-compatibility"><strong>{compatibility ? String(compatibility.valid) : "尚未验证"}</strong>{compatibility?.blockers.map((item) => <p className="file-error" key={item}>{item}</p>)}{compatibility?.warnings.map((item) => <p className="muted" key={item}>{item}</p>)}</div></section>
    <section className="v5-summary-actions"><div className="button-row"><button data-testid="v5-method-validate" type="button" onClick={onValidate} disabled={busy || !catalogReady}>验证方法兼容性</button><button data-testid="v5-method-save" type="button" onClick={onSave} disabled={busy || !validCurrent}>保存方法</button><button data-testid="v5-method-save-and-run" type="button" onClick={onSaveAndRun} disabled={busy || !validCurrent}>保存并进入运行实验</button></div>{savedConfigId && <p data-testid="v5-saved-method-id">{savedConfigId}</p>}</section>
  </aside>;
}

function SavedMethodLibrary({ methods, defaultSelections, catalog, showAll, onToggleShowAll, onOpenRun }: { methods: Array<{ saved: V3SavedConfig; method: NonNullable<ReturnType<typeof parseSavedV5Method>> }>; defaultSelections: V5PluginSelection[]; catalog: V5PluginManifest[]; showAll: boolean; onToggleShowAll: () => void; onOpenRun: (configId: string) => void }) {
  const visibleMethods = showAll ? methods : methods.slice(0, 5);
  return <article className="final-card v5-saved-method-library" data-testid="v5-saved-method-library">
    <div className="section-heading"><div><h3>已保存的 V5 方法</h3></div>{methods.length > 5 && <button type="button" className="ghost-button" onClick={onToggleShowAll}>{showAll ? "收起" : "查看全部方法"}</button>}</div>
    {methods.length ? <div className="v5-saved-method-grid">{visibleMethods.map(({ saved, method }) => {
      const overrideCount = Object.entries(method.plugin_overrides).filter(([category, pluginId]) => defaultSelections.find((item) => item.category === category)?.plugin_id !== pluginId).length;
      return <article key={saved.config_id} className="v5-saved-method-card">
        <h4>{saved.name}</h4>
        <code title={saved.config_id}>{saved.config_id}</code>
        <p>{saved.validation_status} · {roleLabels[method.role ?? "custom"] ?? method.role}</p>
        <p>{overrideCount} 个覆盖模块</p>
        <button type="button" onClick={() => onOpenRun(saved.config_id)} disabled={!catalog.length}>用于运行实验</button>
      </article>;
    })}</div> : <p className="muted">历史 V3 方法仍可在高级功能 → V3 Composer（历史兼容）中使用。</p>}
  </article>;
}

function pluginFor(catalog: V5PluginManifest[], selection: V5PluginSelection) { return catalog.find((item) => item.category === selection.category && item.plugin_id === selection.plugin_id); }
function defaultPluginFor(defaults: V5PluginSelection[], catalog: V5PluginManifest[], category: string) { const selection = defaults.find((item) => item.category === category); return selection ? pluginFor(catalog, selection) : undefined; }
function label(category: string): string { return categoryLabels[category] ?? category; }
function text(value: unknown): string { return value instanceof Error ? value.message : String(value); }
function formatValue(value: unknown): string { return value === null || value === undefined || value === "" ? "未提供" : typeof value === "object" ? JSON.stringify(value) : String(value); }
function sourceSummary(source: V5PluginManifest["source"]): string {
  if (!source || !Object.keys(source).length) return "未声明来源";
  return Object.entries(source).map(([key, value]) => `${key}: ${formatValue(value)}`).join("; ");
}
function moduleIcon(category: string): string {
  if (category === "block_executor") return "◈";
  if (category === "scheduler") return "⌁";
  if (category === "execution") return "◇";
  if (category === "network") return "⌘";
  if (category === "metrics" || category === "observability") return "◎";
  return "○";
}
function moduleStatus(state: ModuleState): { label: string; tone: string } {
  const isOverride = state.defaultPlugin?.plugin_id !== state.selection.plugin_id;
  if (state.plugin.implementation_status && !["implemented", "available"].includes(state.plugin.implementation_status)) return { label: "实现中", tone: "warning" };
  if (state.plugin.conflicts.length > 0) return { label: "有冲突", tone: "warning" };
  if (isOverride) return { label: "方法覆盖", tone: "override" };
  if (state.plugin.requirements.length > 0) return { label: "有依赖", tone: "dependency" };
  return { label: "目录默认", tone: "neutral" };
}

function buildSummary(states: ModuleState[]) {
  const overrides = states.filter((item) => item.defaultPlugin?.plugin_id !== item.selection.plugin_id).map((item) => ({ category: item.selection.category, from: item.defaultPlugin?.display_name_zh || item.defaultPlugin?.display_name || "未提供", to: item.plugin.display_name_zh || item.plugin.display_name }));
  const capabilities = Array.from(new Set(states.flatMap((item) => item.plugin.capabilities))).sort();
  const dependencyChecks = states.flatMap((state) => state.plugin.requirements.map((requirement) => requirementSatisfied(requirement, states, [])));
  return {
    total: states.length,
    defaults: states.length - overrides.length,
    overrides,
    dependencyCount: states.filter((item) => item.plugin.requirements.length > 0).length,
    conflictCount: states.filter((item) => item.plugin.conflicts.length > 0).length,
    capabilities,
    pluginsWithConfig: states.filter((item) => Object.keys(item.plugin.default_config).length > 0).length,
    parameterKeyCount: states.reduce((count, item) => count + Object.keys(item.plugin.default_config).length, 0),
    dependenciesSatisfied: dependencyChecks.every(Boolean),
  };
}

function requirementSatisfied(requirement: string, states: ModuleState[], catalog: V5PluginManifest[]): boolean {
  const [category, pluginId] = requirement.split(":");
  if (!category || !pluginId) return false;
  const selected = states.find((item) => item.selection.category === category);
  if (selected) return selected.selection.plugin_id === pluginId;
  const fallback = catalog.find((item) => item.category === category && item.plugin_id === pluginId);
  return Boolean(fallback && (category === "workload" || category === "fault_injection"));
}
