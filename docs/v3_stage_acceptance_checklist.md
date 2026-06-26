# V3 Stage Acceptance Checklist

## V3.3 Gate Update

V3.3 now absorbs the earlier V3.2b / V3.2.5 Go-backed parity stage.

- Gate A: Go-backed minimal runtime parity.
- Gate B: Go-backed MetaTrack plugin combinations and fair ablation.
- V3.3 must not implement Fabric validation, frontend integration, dual-chain runtime, MetaFlow, AFS, or FDA.

## Current V3 Acceptance Scope

Current V3-final acceptance covers:

- V3.0 Planning Scaffold.
- V3.1 Profile Layer.
- V3.2 Minimal Single-chain Modular Runtime.
- V3.2b / V3.2.5 Go-backed Minimal Runtime / Go parity, planned after V3.2.
- V3.3 MetaTrack Plugin Evaluation.
- V3.4 Fabric-backed Validation for MetaTrack.
- V3-final Frontend Integration and Acceptance.

Deferred / future roadmap:

- V3.5 Minimal Dual-chain Runtime.
- V3.6 MetaFlow Protocol Plugin and AFS/FDA.

V3.5 and V3.6 may remain documented as future roadmap stages, but they are not current V3-final acceptance gates. MetaFlow preview profiles and MetaFlow plugin profiles remain planned / not runnable.

## V3.0 Planning Scaffold

- 阶段目标：建立 V3 skill、docs、边界、阶段路线、profile 定义、公平 baseline 政策。
- 允许修改文件类型：V3 skill and Markdown docs only。
- 必须实现内容：V3 skill exists; all V3 docs exist; V3 positioning, stages, non-goals, profiles, fairness, acceptance checklist are clear。
- 必须输出 artifacts：documentation only。
- 必须运行测试：`git diff --check`, `git status --short`, pre-commit cached checks。
- 禁止事项：no business code, no config files, no tests, no runtime implementation。
- 通过条件：V3 skill exists; all V3 docs exist; V3 positioning is clear; V3 stages are clear; V3 non-goals are clear; Fair baseline policy exists; no business code changed; no V3 implementation started。
- 失败条件：any V3 code implementation, unclear non-goals, planned capability described as runnable。
- 下一阶段入口条件：V3.0 committed and not pushed; user explicitly starts V3.1。

## V3.1 Profile Layer

- 阶段目标：ChainProfile / PluginProfile / ExperimentProfile schema / loader / validator / preview。
- 允许修改文件类型：profile schema/config docs, backend profile loader/validator, tests, minimal API preview。
- 必须实现内容：schema or loader exists; preview works; planned/runnable guard works。
- 必须输出 artifacts：profile preview result, validation result。
- 必须运行测试：Python tests, V0 sanity, diff checks; frontend build if UI touched。
- 禁止事项：no runtime implementation beyond profile layer。
- 通过条件：ChainProfile / PluginProfile / ExperimentProfile schema or loader exists; Preview works; Invalid planned/runnable guard works; No runtime implementation beyond profile layer。
- 失败条件：runtime starts early; planned backend runnable。
- 下一阶段入口条件：profile layer stable and validated。

## V3.2 Minimal Single-chain Modular Runtime

- 阶段目标：minimal single-chain modular runtime。
- 允许修改文件类型：runtime services/modules, configs for V3.2, tests, docs。
- 必须实现内容：NodeRuntime, TxPool, BlockProducer, simple ConsensusPlugin, ExecutionSchedulerPlugin, StateAccessPlugin, CommitPlugin, MetricsCollector。
- 必须输出 artifacts：block_log, tx_results, state_commit_log, summary, report, used profiles。
- 必须运行测试：Python/backend tests, Go tests if Go touched, V0 sanity, frontend build if UI touched。
- 禁止事项：no MetaTrack full evaluation, no Fabric validation, no dual-chain runtime。
- 通过条件：Minimal single-chain runtime can run a small workload; outputs block_log, tx_results, state_commit_log, summary, report; at least one simple consensus plugin and one normal commit plugin work。
- 失败条件：no reproducible artifacts; no profile capture; uses wall-clock sleep for virtual latency。
- 下一阶段入口条件：single-chain runtime validated。

## V3.3 MetaTrack Plugin Evaluation

- 阶段目标：MetaTrack plugin combinations and fair comparison。
- 允许修改文件类型：MetaTrack plugins, evaluation runner, tests, docs。
- 必须实现内容：baseline_hash_only, co_access_only, co_access_dual_track, full_MetaTrack。
- 必须输出 artifacts：metatrack_summary, metatrack_latency, mechanism metrics, block_log, tx_results, report。
- 必须运行测试：fairness tests, runtime tests, V0 sanity, full modified-surface validation。
- 禁止事项：no Fabric peer patch, no MetaFlow, no unfair workload differences。
- 通过条件：MetaTrack plugin combinations can run on same chain profile; baseline and full MetaTrack use identical workload and seed; mechanism metrics output。
- 失败条件：different workload/seed/profile for proposed method; smoke result claimed as final evidence。
- 下一阶段入口条件：MetaTrack runtime comparison stable。

## V3.4 Fabric-backed Validation for MetaTrack

- 阶段目标：Fabric observation, calibration, and small-scale validation。
- 允许修改文件类型：Fabric validation adapter/runner, docs, tests, safety gates。
- 必须实现内容：read or generate authorized Fabric observation, calibration report, validation truth labels。
- 必须输出 artifacts：fabric_validation_summary, fabric_tx_results, fabric_commit_latency, fabric_block_log, fabric_validation_report。
- 必须运行测试：adapter tests, safety tests, backend tests, V0 sanity; Fabric commands only when explicitly allowed。
- 禁止事项：no Fabric peer internal patch; no normal web auto-start without explicit stage gate。
- 通过条件：Fabric validation can read or generate real Fabric observation; Calibration report produced; Validation truth label is correct; Does not claim Fabric peer modification。
- 失败条件：web silently starts Fabric; Fabric trace replay described as live execution。
- 下一阶段入口条件：validation path bounded and documented。

## V3.5 Minimal Dual-chain Runtime

- 阶段目标：minimal source/target chain runtime。
- 允许修改文件类型：dual-chain runtime modules, profiles, tests, docs。
- 必须实现内容：two chain runtimes, source lock, target mint, finality wait, complete/refund representation。
- 必须输出 artifacts：dual-chain block logs, protocol event logs, pending/finality metrics, summary/report。
- 必须运行测试：dual-chain runtime tests, fairness guard tests, modified-surface validation。
- 禁止事项：no MetaFlow AFS/FDA, no production bridge。
- 通过条件：Two chain runtime exists; Source/target chain profiles work; Source lock, target mint, finality wait, source complete/refund path is represented。
- 失败条件：source/target profile mismatch not detected; production bridge claims。
- 下一阶段入口条件：minimal dual-chain path stable。

## V3.6 MetaFlow Protocol Plugin and AFS/FDA

- 阶段目标：MetaFlow and baselines on V3 dual-chain runtime。
- 允许修改文件类型：CrossChainProtocolPlugin implementations, control logic, tests, docs。
- 必须实现内容：MetaFlow basic, MetaFlow AFS/FDA, baselines, B/D/T, three scenarios。
- 必须输出 artifacts：metaflow_summary, metaflow_events, protocol_results, control_decisions, comparison report。
- 必须运行测试：protocol tests, control decision tests, fairness tests, V0 sanity, full modified-surface validation。
- 禁止事项：no production bridge/security claims; real threshold signatures optional future work。
- 通过条件：MetaFlow basic and baselines run; AFS/FDA control decisions are output; Three scenarios run; MetaFlow report generated。
- 失败条件：MetaFlow uses different profiles than baselines; no control_decisions output。
- 下一阶段入口条件：MetaFlow comparison validated。

## V3-final Frontend Integration and Acceptance

- 阶段目标：frontend integration and acceptance report。
- 允许修改文件类型：frontend, docs, acceptance tests, minimal backend contract if needed。
- 必须实现内容：MetaTrack-oriented runtime pages, Fabric validation page, run history/artifacts, boundaries, developer mode. MetaFlow remains planned / deferred and must not be presented as runnable in current V3-final。
- 必须输出 artifacts：V3 acceptance report and downloadable run artifacts。
- 必须运行测试：frontend build, backend/API contract tests, V0 sanity, diff checks。
- 禁止事项：no new runtime mechanisms; no new protocols。
- 通过条件：Frontend pages expose current-scope MetaTrack/Fabric V3 experiments; MetaFlow preview is clearly planned / deferred; Artifacts downloadable; System boundary page clearly explains truth labels and non-goals; V3 acceptance report exists。
- 失败条件：UI claims production bridge/live public-chain; planned capabilities runnable。
- 下一阶段入口条件：manual acceptance complete。
