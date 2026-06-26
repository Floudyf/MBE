# V3 Fair Baseline Policy

## 1. 为什么需要 fair baseline policy

V3 的目标是让 MetaTrack 和 MetaFlow 在同一研究链环境下与 baseline 公平比较。若 proposed method 和 baseline 使用不同 workload、seed、chain profile、submit rate、hardware profile 或 calibration profile，实验结果将无法说明机制本身的贡献。

Fair baseline policy 是 V3 的强制规则，不是建议。

## 2. 通用公平原则

所有实验必须保存完整 used profiles，并满足：

- 同一 workload。
- 同一 seed。
- 同一 tx_count。
- 同一 ChainProfile。
- 同一 hardware profile。
- 同一 submit rate。
- 同一 block config。
- 同一 consensus config。
- 同一 network profile。
- 同一 calibration profile if used。
- 只允许声明的插件或控制策略不同。

任何差异都必须在 `used_experiment_profile` 和 report 中说明。

## 3. MetaTrack 公平原则

MetaTrack comparisons must use identical workload, seed, ChainProfile, hardware profile, block config, consensus config, submit rate, and calibration profile.

MetaTrack 只允许以下插件类别不同：

```text
ShardingPlugin
ExecutionSchedulerPlugin
StateAccessPlugin
CommitPlugin
```

允许比较组合：

- `baseline_hash_only`
- `co_access_only`
- `co_access_dual_track`
- `full_MetaTrack`

不允许通过改变 trace、key distribution、block interval、finality、worker count 或 calibration 参数来给 full_MetaTrack 制造优势。

## 4. MetaFlow 公平原则

MetaFlow comparisons must use identical source ChainProfile, target ChainProfile, workload arrival sequence, finality profile, timeout baseline, hardware profile, and network profile.

MetaFlow 只允许以下内容不同：

```text
CrossChainProtocolPlugin
control policy
B / D / T adaptation logic
```

允许比较：

- `lock_mint_serial`
- `lock_mint_pipeline`
- `fixed_window_baseline`
- `committee_bridge_basic`
- `metaflow_basic`
- `metaflow_afs_fda`

不允许为 MetaFlow 使用更快目标链、更宽 timeout、更优 workload arrival sequence 或不同硬件。

## 5. Fabric validation 公平原则

Fabric validation 是 calibration and small-scale real-chain validation。它不能成为只给 proposed method 使用的特权。

若 modular runtime 结果使用 Fabric calibration profile，则所有 baseline 必须使用同一 calibration profile。Fabric validation report 必须说明：

- Fabric trace 来源。
- Fabric 是否真实运行。
- web/API 是否启动 Fabric。
- calibration profile 是否同时用于 proposed method and baselines。
- 哪些指标来自 Fabric observation，哪些来自 modular runtime。

## 6. 不允许的实验做法

禁止：

- 不得为 proposed method 和 baseline 使用不同 workload。
- 不得使用不同 seed。
- 不得使用不同 tx_count。
- 不得使用不同 chain profile。
- 不得使用不同硬件。
- 不得使用不同 submit rate。
- 不得在 proposed method 上使用 calibration profile，而 baseline 不使用。
- 不得用 tiny smoke 结果作为论文最终结论。
- 不得把 `local_virtual` 结果写成 real-chain result。
- 不得把 Fabric trace replay 写成 Fabric live execution。
- 不得把 `committee_bridge_basic` 写成真实委员会桥。
- 不得把 planned capability 写成 implemented/runnable。

## 7. 必须保存的 used_config / used_profile

每个 V3 run 必须保存：

- `used_chain_profile.yaml/json`
- `used_plugin_profile.yaml/json`
- `used_experiment_profile.yaml/json`
- workload profile or embedded workload config。
- calibration profile if used。
- `runtime.log`
- `summary.csv/json`
- `report.md`

MetaTrack 必须额外保存 plugin mechanism metrics。MetaFlow 必须额外保存 protocol events and control decisions。Fabric validation 必须额外保存 Fabric observation artifacts。

## 8. 论文写作中允许和不允许的表述

允许：

- "The V3 modular runtime evaluates MetaTrack plugins under identical ChainProfile and workload settings."
- "Fabric-backed validation is used for observation and calibration."
- "Synthetic replay and modular runtime results are labeled separately from Fabric validation."
- "Smoke-level results validate the pipeline but are not final performance evidence."

不允许：

- "V3 is a production blockchain."
- "V3 implements a production cross-chain bridge."
- "Fabric peer internally runs MetaTrack."
- "Local virtual replay is real-chain execution."
- "Tiny smoke traces prove final performance."
- "Committee baseline is a real threshold-signature bridge."

## 9. V3.3.2 Template-driven Fairness Scope

V3.3.2 extends fairness checks from plugin-class rules to template module rules:

- Only `variable_modules` may differ across methods.
- `fixed_modules` must match across all methods.
- `disabled_modules` must not be enabled.
- `planned_modules` must not be runnable.
- `output_modules` must not become experiment variables.
- Workload, seed, tx count, ChainProfile, submit rate, block config, consensus config, network profile, and calibration profile must remain shared.

Example error semantics:

```text
Consensus is fixed by template metatrack_ablation and cannot differ across methods.
CommitteeEpoch is planned in current stage and cannot be runnable.
Routing is disabled by template consensus_only and cannot be enabled.
```

These checks are for experiment organization and reproducibility. They do not implement frontend UI, Fabric validation, MetaFlow, dual-chain runtime, PBFT/HotStuff, committee lifecycle, dynamic resharding, or state migration.
