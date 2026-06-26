# V3.3.2 Single-chain Modular Composer

## Scope

V3.3.2 adds the single-chain modular Composer Profile and ExperimentTemplate layer. Its goal is to make MBE experiments describable, composable, reproducible, and fair-checkable before frontend integration.

V3.3.2 is not a frontend UI stage, not Fabric validation, not MetaFlow, not dual-chain, not PBFT / HotStuff, not runnable committee lifecycle, and not runnable dynamic resharding or state migration.

## Module Graph

The single-chain composer uses this fixed logical order:

```text
Workload
  -> TxPool
  -> BlockProducer
  -> Consensus
  -> CommitteeEpoch
  -> Routing
  -> Execution
  -> StateAccess
  -> StateStorage
  -> Commit
  -> MetricsReport
```

Each module status must be one of:

```text
fixed
variable
disabled
planned
output
```

The status vocabulary is intentionally small so later frontend views can show the chain as a clear block-by-block experiment composition.

## ExperimentTemplate

Templates live under:

```text
configs/v3/templates/
```

V3.3.2 adds:

- `consensus_only`
- `sharding_only`
- `execution_scheduler_only`
- `state_access_only`
- `commit_only`
- `metatrack_ablation`
- `committee_lifecycle_planned`

`metatrack_ablation` is the current runnable template. `committee_lifecycle_planned` is preview-only and must not make committee lifecycle runnable.

## Plugin Matrix

The MetaTrack plugin matrix maps each method to module-level plugins:

```text
baseline_hash_only:
  Routing = hash_sharding
  Execution = serial_execution
  StateAccess = direct_fetch
  Commit = normal_commit

co_access_only:
  Routing = co_access_sharding
  Execution = serial_execution
  StateAccess = direct_fetch
  Commit = normal_commit

co_access_dual_track:
  Routing = co_access_sharding
  Execution = dual_track_execution
  StateAccess = access_list_prefetch
  Commit = normal_commit

full_MetaTrack:
  Routing = co_access_sharding
  Execution = dual_track_execution
  StateAccess = access_list_prefetch
  Commit = hot_update_aggregation_commit
```

This matrix is for composition, preview, and fairness checking. It does not add a new runtime mechanism.

## Composer Preview

`backend/app/services/v3_composer_preview.py` produces:

- `composer_preview`
- `experiment_template`
- `module_graph`
- `plugin_matrix`
- `fairness_scope`

The preview never starts Go, Fabric, Docker, network.sh, frontend, MetaFlow, or dual-chain services.

## Fairness Scope

Template-driven fairness requires:

- Only `variable_modules` may differ across methods.
- `fixed_modules` must match.
- `disabled_modules` must not be enabled.
- `planned_modules` must not be runnable.
- `output_modules` must not become experiment variables.
- Workload, seed, tx count, ChainProfile, submit rate, block config, consensus config, network profile, and calibration profile must remain shared.

Smoke or preview outputs are not final paper-scale evidence.
