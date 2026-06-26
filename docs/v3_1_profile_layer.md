# V3.1 Profile Layer

## 1. Goal

V3.1 adds the profile layer for the future modular plugin chain work. It defines how V3 chain, plugin, and experiment profiles are declared, loaded, validated, and previewed.

V3.1 is not a runtime stage. It does not run a node, build a block, execute a consensus plugin, execute a sharding plugin, run MetaTrack V3, run MetaFlow, start Fabric, or generate runtime artifacts.

## 2. What This Stage Adds

V3.1 adds:

- `ChainProfile`: stable chain configuration input.
- `PluginProfile`: stable plugin combination declaration.
- `ExperimentProfile`: stable experiment organization input.
- YAML profile directories under `configs/v3/`.
- Loader, validator, and preview services.
- Planned/runnable/invalid guards.
- Basic fair baseline validation for MetaTrack and MetaFlow profile previews.

V3.1 does not add:

- `NodeRuntime`
- `TxPool`
- `BlockProducer`
- executable consensus/sharding/execution/state/commit plugins
- executable cross-chain protocol plugins
- MetaTrack V3 execution
- MetaFlow execution
- Fabric validation execution
- Docker/Fabric/network.sh automation
- `block_log.csv`, `tx_results.csv`, or `state_commit_log.csv`

## 3. Profile Directory Layout

```text
configs/v3/
  chains/
    chain_x_default.yaml
    chain_x_fast.yaml
    chain_y_slow.yaml
    fabric_validation_planned.yaml
  plugins/
    metatrack_plugin_profiles.yaml
    metaflow_plugin_profiles.yaml
  experiments/
    metatrack_ablation_profile_preview.yaml
    metaflow_dual_chain_profile_preview.yaml
    fabric_validation_profile_preview.yaml
```

These files are profile inputs. They are not experiment results.

## 4. ChainProfile

`ChainProfile` describes a future chain configuration. It includes sections for deployment, node identity, block cutting, consensus, finality, transaction pool, sharding, execution, state, commit, network, cross-chain settings, application model, fault safety, metrics, and capability.

The validator rejects result-like fields inside ChainProfile, including:

```text
max_tps
stable_tps
peak_tps
throughput_tps
p99_latency_ms
avg_latency_ms
aggregation_ratio
```

These values belong to experiment outputs, not fixed chain profile inputs.

Current V3.1 examples are all planned:

- `chain_x_default`: future single-chain modular research chain profile.
- `chain_x_fast`: future fast source-chain profile.
- `chain_y_slow`: future slower target-chain profile.
- `fabric_validation_planned`: future Fabric-backed validation profile.

## 5. PluginProfile

`PluginProfile` declares plugin combinations. It does not implement plugin execution.

MetaTrack profile combinations:

- `baseline_hash_only`
- `co_access_only`
- `co_access_dual_track`
- `full_MetaTrack`

Allowed MetaTrack plugin classes:

```text
ShardingPlugin
ExecutionSchedulerPlugin
StateAccessPlugin
CommitPlugin
```

MetaFlow profile combinations:

- `serial_baseline`
- `pipeline_baseline`
- `fixed_window_baseline`
- `committee_baseline`
- `metaflow_basic`
- `metaflow_afs_fda`

Allowed MetaFlow plugin classes:

```text
CrossChainProtocolPlugin
```

`committee_bridge_basic` is only a research baseline model. It must not be described as a production bridge. MetaFlow profiles remain planned in V3.1.

## 6. ExperimentProfile

`ExperimentProfile` describes how a future V3 experiment should be organized. It records chain profile references, plugin profile references, workload controls, calibration policy, fairness rules, expected output names, and capability status.

Current examples:

- `metatrack_ablation_profile_preview`
- `metaflow_dual_chain_profile_preview`
- `fabric_validation_profile_preview`

These are preview profiles. They do not run experiments in V3.1.

## 7. Status Semantics

V3.1 uses these statuses:

```text
runnable
planned
invalid
```

`runnable` means the current stage can actually execute the profile. V3.1 does not currently mark modular chain runtime, MetaTrack V3, MetaFlow, Fabric validation, Fabric live backend, EVM live backend, or public-chain live execution as runnable.

`planned` means the profile structure is valid but depends on a later stage.

`invalid` means the profile is structurally invalid, references unknown profiles, violates fair baseline policy, has contradictory truth/backend status, or declares a future planned ability as runnable.

In V3.1, the following remain planned:

- V3.2 modular research chain runtime
- V3.3 MetaTrack plugin evaluation
- V3.4 Fabric-backed validation
- V3.5 dual-chain modular runtime
- V3.6 MetaFlow protocol plugin
- V3.6 AFS/FDA
- `fabric_live_planned`
- `evm_live_planned`
- public-chain live execution

## 8. Data Truth And Backend Guards

Allowed truth labels include:

```text
synthetic_replay
existing_trace_replay
fabric_chain_backed_trace_replay
fabric_live_validation
modular_runtime
modular_runtime_calibrated
public_chain_imported_trace_semantic_unknown
planned_cross_chain_replay
```

Allowed backend/runtime types include:

```text
local_virtual
trace_replay
modular_research_chain
fabric_validation
fabric_live_planned
evm_live_planned
```

`modular_research_chain` and `fabric_validation` are planned in V3.1. `fabric_live_planned` and `evm_live_planned` are not implemented.

## 9. Fair Baseline Checks

MetaTrack fairness requires the same workload, seed, transaction count, chain profile, hardware profile, submit rate, block config, consensus config, network profile, and calibration profile when calibration is used. Only these plugin classes may vary:

```text
ShardingPlugin
ExecutionSchedulerPlugin
StateAccessPlugin
CommitPlugin
```

MetaFlow fairness requires the same source/target chain profiles, workload arrival sequence, finality profile, timeout baseline, hardware profile, and network profile. Only these dimensions may vary:

```text
CrossChainProtocolPlugin
control_policy
B/D/T adaptation logic
```

The validator rejects profiles that give the proposed method a faster target chain, a different source/target profile, a different arrival sequence, a wider timeout baseline, or a different hardware/network profile.

## 10. Services

`backend/app/services/v3_profile_loader.py`:

- discovers `configs/v3/chains/`
- discovers `configs/v3/plugins/`
- discovers `configs/v3/experiments/`
- loads YAML profiles
- indexes profile IDs
- rejects duplicate IDs
- does not execute runtime code

`backend/app/services/v3_profile_validator.py`:

- checks required fields and sections
- validates enum-like values
- validates references
- enforces planned/runnable/invalid guards
- rejects ChainProfile result-like fields
- checks MetaTrack and MetaFlow fair baseline policy
- does not start jobs, Fabric, Docker, network.sh, or Go executor

`backend/app/services/v3_profile_preview.py`:

- combines loader and validator output
- returns profile preview payloads
- includes referenced profiles, plugin summary, fairness summary, blocking reasons, warnings, and expected outputs
- explicitly reports that it creates no run ID, writes no runtime artifacts, starts no Fabric/Docker, and calls no Go executor

## 11. Preview Result Example

```json
{
  "profile_id": "metatrack_ablation_profile_preview",
  "profile_type": "experiment_profile",
  "declared_stage": "v3.1",
  "status": "planned",
  "valid": true,
  "runnable": false,
  "backend_type": "modular_research_chain",
  "truth_label": "modular_runtime_calibrated",
  "referenced_profiles": {},
  "plugin_summary": [],
  "fairness_summary": {},
  "blocking_reasons": [
    "requires V3.2 minimal single-chain modular runtime",
    "requires V3.3 MetaTrack plugin evaluation"
  ],
  "warnings": [
    "requires v3.3; V3.1 only supports profile preview and validation"
  ],
  "expected_outputs": [
    "metatrack_summary.csv",
    "metatrack_latency.csv",
    "metatrack_mechanism_metrics.csv",
    "report.md"
  ]
}
```

## 12. Validation Commands

```powershell
$env:PYTHONPATH = (Get-Location).Path
python -m pytest tests -q
python -m pytest backend/tests -q
python scripts/v0_sanity.py
git diff --check
git status --short
```

Frontend build and Go tests are not required for V3.1 unless those areas are changed.

## 13. V3.2 Entry Conditions

V3.2 should start only after V3.1 profiles can be loaded, validated, and previewed. V3.2 may then introduce the minimal modular research chain runtime, but that work is explicitly outside V3.1.
