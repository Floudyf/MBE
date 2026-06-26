# V3 ExperimentProfile

## 1. ExperimentProfile 目的

ExperimentProfile 描述一次 V3 实验如何组织：使用哪些 ChainProfile、PluginProfile、WorkloadProfile、CalibrationProfile，如何执行 fairness checks，输出哪些 artifacts，以及哪些能力是 runnable/planned。

## 2. 与 ChainProfile 的关系

ChainProfile 定义链环境。ExperimentProfile 引用一个或多个 ChainProfile，并声明哪些参数必须固定。

MetaTrack 通常使用一个 `chain_profile`。MetaFlow 使用 `source_chain_profile` 和 `target_chain_profile`。

## 3. 与 PluginProfile 的关系

PluginProfile 定义可替换模块。ExperimentProfile 选择 baseline/plugin combination，并声明 only-plugin-diff fairness policy。

## 4. 与 WorkloadProfile 的关系

WorkloadProfile 定义 tx_count、seed、arrival sequence、hotspot/skew、access list、aggregation candidates。Fair baseline 必须共享同一 workload 和 seed。

## 5. 与 CalibrationProfile 的关系

CalibrationProfile 记录 Fabric-backed observation 或 replay-vs-observed parameter suggestion。若 proposed method 使用 calibration profile，baseline 也必须使用同一 calibration profile。

## 6. 单链 MetaTrack 实验 profile

MetaTrack ExperimentProfile 应描述：

- experiment id/stage/type。
- truth label。
- one ChainProfile。
- workload。
- baselines。
- fairness rules。
- expected artifacts。

示例：

```yaml
experiment:
  experiment_id: v3_metatrack_ablation_hotspot
  stage: v3.3
  type: metatrack_plugin_ablation
  truth_label: modular_runtime_calibrated

chain_profile: chain_x_default

workload:
  source: synthetic_or_chain_backed
  tx_count: 50000
  seed: 42
  hotspot_ratio: 0.2
  skew: 1.2
  access_list_enabled: true
  aggregation_candidates_enabled: true

baselines:
  - baseline_hash_only
  - co_access_only
  - co_access_dual_track
  - full_MetaTrack

fairness:
  same_workload: true
  same_seed: true
  same_chain_profile: true
  same_block_config: true
  same_consensus_config: true
  only_plugin_diff: true

outputs:
  - metatrack_summary.csv
  - metatrack_latency.csv
  - metatrack_mechanism_metrics.csv
  - report.md
```

## 7. 双链 MetaFlow 实验 profile

MetaFlow ExperimentProfile 应描述：

- source/target chain profiles。
- scenarios。
- protocols。
- B/D/T and AFS/FDA control。
- fairness rules。
- artifacts。

示例：

```yaml
experiment:
  experiment_id: v3_metaflow_three_scenarios
  stage: v3.6
  type: metaflow_protocol_comparison
  truth_label: modular_runtime_calibrated

source_chain_profile: chain_x_fast
target_chain_profile: chain_y_slow

scenarios:
  - steady_load
  - source_burst
  - slow_target_finality

protocols:
  - lock_mint_serial
  - lock_mint_pipeline
  - fixed_window_baseline
  - committee_bridge_basic
  - metaflow_basic
  - metaflow_afs_fda

metaflow:
  B:
    initial: 100
    min: 20
    max: 500
  D:
    initial: 4
    min: 1
    max: 16
  T:
    static_timeout_ms: 30000
  afs_enabled: true
  fda_enabled: true

outputs:
  - metaflow_summary.csv
  - metaflow_events.csv
  - control_decisions.csv
  - metaflow_vs_baselines_report.md
```

## 8. Sweep profile

Sweep profile expands a set of ExperimentProfiles while preserving fairness. It may vary one declared factor such as workload skew, target finality, window size, or protocol. It must record case_id, varied parameter, fixed parameters, and truth label.

## 9. Artifacts

Every ExperimentProfile run must save:

- `used_chain_profile.yaml/json`
- `used_plugin_profile.yaml/json`
- `used_experiment_profile.yaml/json`
- `runtime.log`
- `summary.csv/json`
- `report.md`

Stage-specific artifacts are added by MetaTrack, MetaFlow, and Fabric validation stages.

## 10. Fairness checks

ExperimentProfile validator must check:

- same workload where required。
- same seed。
- same chain profile。
- same block/consensus/network config。
- same calibration profile if used。
- only allowed plugin classes differ。
- planned configs are not runnable。

These checks begin in V3.1 profile layer and become mandatory for V3.3 and V3.6 experiments.

## 11. V3.3.2 ExperimentTemplate and Composer Fields

V3.3.2 ExperimentProfiles may include:

```yaml
experiment_template: metatrack_ablation

composer:
  enabled: true
  view: single_chain
  module_graph_id: single_chain_default

variable_modules:
  - Routing
  - Execution
  - StateAccess
  - Commit

fixed_modules:
  - Workload
  - TxPool
  - BlockProducer
  - Consensus
  - StateStorage

planned_modules:
  - CommitteeEpoch
```

These fields are metadata for ComposerPreview and fairness checking. They do not start a frontend, Fabric, MetaFlow, or Go runtime by themselves.

The single-chain composer order is:

```text
Workload -> TxPool -> BlockProducer -> Consensus -> CommitteeEpoch -> Routing -> Execution -> StateAccess -> StateStorage -> Commit -> MetricsReport
```

Module status is limited to `fixed`, `variable`, `disabled`, `planned`, and `output`.
