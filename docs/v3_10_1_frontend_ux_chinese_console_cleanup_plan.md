# V3.10.1 Frontend UX and Chinese Console Cleanup Plan

## 1. Goal

V3.10.1 is not a new on-chain mechanism stage. Its goal is to turn the V3.10 experiment capability into a Chinese, concise, demonstrable, and explainable experiment console.

V3.10.1 = Frontend UX and Chinese Console Cleanup.

V3.10.1 is not V3.11 CrossShard Protocol Hardening, not a Relay MVP, not a Go runtime mechanism change, not a backend experiment semantic change, not a complex UI framework migration, and not a paper-grade visualization system.

V3.10 completed benchmark templates, baselines, sweep MVP, reproducibility manifest, and benchmark report artifacts. V3.10.1 only reorganizes how existing capabilities are presented in the frontend. V3.11 remains planned and has not started.

## 2. Current Frontend Problems

- Navigation exposes too many V1/V2/V3 entries at once.
- Brand wording still looks like an older V2 experiment platform.
- Chinese and English are mixed in primary UI text.
- Long boundary paragraphs make the console feel like a debug page.
- Smoke terminology can confuse Chinese users.
- Runnable, preview, and planned plugins are shown together without enough hierarchy.
- Experiment flow is not obvious at a glance.
- Runs do not show enough frontend-side phase feedback.
- Results expose many raw keys before showing core metrics and chart previews.

## 3. V3.10.1 Scope

V3.10.1 implements:

- simplified left navigation
- default entry into the V3 experiment console
- V3.10 / MBE brand wording
- Chinese replacement for Smoke wording in primary UI
- primary page Chinese localization
- HelpTip progressive explanations
- shorter top-level descriptions with details or tips
- RuntimeTopologyPanel localization and grouping
- V3ComposerPage section cleanup
- module flow stage grouping
- runnable-first plugin selection cleanup
- frontend run progress feedback
- core metric cards and lightweight chart preview
- collapsed raw metrics by default
- clearer artifact and result areas
- unified, simpler CSS styling
- README / execution_plan / skill closure wording
- validation and commit

## 4. Non-goals

V3.10.1 does not implement V3.11, Relay MVP, complete Relay/Broker/2PC, Go runtime mechanism changes, V3.10 benchmark semantic changes, backend experiment semantic changes, new main-flow modules, main transaction-flow changes, deletion of V1/V2 features, paper-grade benchmark claims, production network claims, BlockEmulator backend claims, or a new large UI framework.

## 5. Navigation Simplification

The main sidebar should show only:

- 实验控制台
- 运行记录
- 产物下载
- 系统边界
- 高级功能

The default page is the V3 experiment console. Historical V1/V2/developer pages remain available under Advanced features; they are not deleted.

## 6. Chinese Localization Rule

Primary UI labels should be Chinese. English IDs may remain as small text, title attributes, HelpTip content, or developer details.

Examples:

- `shard_count` -> 分片数量
- `validators_per_shard` -> 每片验证节点数
- `network_adapter` -> 网络通信方式
- `cross_shard_protocol` -> 跨片协议
- `state_backend` -> 状态存储后端
- `benchmark_template` -> 实验模板
- `baseline_profile` -> 对照基线
- `repeat_count` -> 重复次数

## 7. HelpTip / Progressive Disclosure Rule

Long explanations should move into HelpTip or details blocks. HelpTip uses a small question-mark control, is keyboard accessible, and does not require a third-party UI library.

HelpTip should be used for experiment template, baseline, network adapter, cross-shard protocol, state backend, repeat count, quick verification, truth boundary, state proof / witness, and Benchmark.

## 8. Experiment Flow Layout

The main flow remains:

```text
Workload -> TxPool -> BlockProducer -> ConsensusRuntime -> CommitteeEpoch -> Routing/Sharding -> Execution -> StateAccess -> StateStorage -> Commit -> MetricsReport
```

The UI may add stage grouping labels:

- 输入阶段
- 排序阶段
- 执行阶段
- 提交阶段

These labels are presentation only and are not runtime modules.

## 9. Module Selection Cleanup

Plugin selection should prioritize runnable and meaningful preview options. Planned plugins should be folded under 规划中插件. Duplicate aliases should not dominate the primary selection list.

CrossShardProtocol remains a Routing/Sharding sub-capability. StateProof and Witness remain under StateAccess / StateStorage / Commit. Benchmark remains in experiment control / result layer.

## 10. Run Progress Feedback

The frontend may show known phases for configuration draft trial runs and controlled comparison trial runs. It must not pretend to receive real backend live progress.

Draft phases:

1. 校验配置
2. 生成实验配置
3. 提交运行
4. 等待本地 runtime
5. 读取 summary
6. 收集产物
7. 渲染结果

Controlled phases:

1. 提交受控对照
2. 执行预设组合
3. 聚合 summary
4. 收集产物
5. 渲染结果

## 11. Result Chart Preview

Results should first show core metric cards and lightweight CSS/SVG charts:

- latency bars: `avg_latency_ms`, `p95_latency_ms`, `p99_latency_ms`
- key counts: `cross_shard_tx_count`, `state_proof_verified_count`, `witness_verified_count`, `benchmark_run_count`
- success/failure mini chart

Raw metrics should be collapsed by default.

## 12. Visual Style Rule

The console should be simpler, more symmetric, and calmer: narrower sidebar, consistent card radius and spacing, 2- or 3-column configuration grids, concise result cards, lightweight chart areas, and black/white/gray with restrained blue accents.

## 13. Relationship with V3.10 and V3.11

V3.10.1 follows V3.10 benchmark hardening and precedes V3.11 cross-shard hardening. It keeps V3.10 runtime semantics, benchmark truth, cross-shard skeleton semantics, and state authenticity semantics unchanged.

V3.11 has not started.

## 14. Truth Boundary

V3.10.1 can claim:

- Chinese V3 experiment console
- simplified navigation
- progressive HelpTip explanations
- clearer module flow and selection display
- frontend run progress feedback
- lightweight result chart preview
- visual cleanup

V3.10.1 cannot claim:

- runtime semantics changed
- new cross-shard protocol capability
- new benchmark capability beyond V3.10
- paper-grade benchmark evidence
- production UI or production deployment
- production network
- BlockEmulator backend

Runtime truth: `frontend_ux_cleanup_no_runtime_semantics_change`.

## 15. Acceptance Criteria

V3.10.1 is complete when the V3 console is the default entry; navigation is simplified; brand wording reflects MBE V3.10; primary V3 UI is Chinese; HelpTip exists and is used; long explanations are folded or moved into tips; RuntimeTopologyPanel is grouped and localized; module flow has stage grouping; planned plugins are folded; run progress feedback is visible; result panel shows metric cards and charts before raw metrics; artifacts use Chinese missing text; README, execution plan, and skill are updated; validation passes; and a commit is created without pushing.

## 16. Next Stage

V3.11 CrossShard Protocol Hardening.
