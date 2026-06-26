# V3 ExperimentTemplate

## Purpose

`ExperimentTemplate` describes which modules an experiment studies and which modules stay fixed, disabled, planned, or output-only. It lets different research directions reuse the same single-chain runtime without changing unrelated modules.

## Required Fields

```yaml
template_id: metatrack_ablation
stage: V3.3.2
chain_mode: single_chain
runnable: true
preview_only: false
description: ...

module_order:
  - Workload
  - TxPool
  - BlockProducer
  - Consensus
  - CommitteeEpoch
  - Routing
  - Execution
  - StateAccess
  - StateStorage
  - Commit
  - MetricsReport

variable_modules: []
fixed_modules: []
disabled_modules: []
planned_modules: []
output_modules: []

allowed_plugins: {}

fairness:
  only_variable_modules_may_differ: true
  fixed_modules_must_match: true
  planned_modules_not_runnable: true
```

## Status Rules

Module status is limited to:

```text
fixed
variable
disabled
planned
output
```

A module cannot appear in more than one status list. Planned modules can be shown in preview but cannot become runnable. Output modules collect metrics and reports; they are not experiment variables.

## Current Templates

- `metatrack_ablation`: runnable template for the four MetaTrack combinations.
- `consensus_only`: preview-only until additional consensus plugins are implemented.
- `sharding_only`: routing-focused template; dynamic resharding remains planned.
- `execution_scheduler_only`: scheduler-focused template; Block-STM-like plugins remain planned.
- `state_access_only`: state access template; cache/witness prefetch remain planned.
- `commit_only`: commit template; batch commit remains planned.
- `committee_lifecycle_planned`: preview-only committee / epoch lifecycle template.

## Service Contract

`backend/app/services/v3_experiment_templates.py` loads, normalizes, validates, lists, and fetches templates from `configs/v3/templates/*.yaml`.

The service is metadata-only. It does not run experiments, start Fabric, invoke Go, or build frontend UI.
