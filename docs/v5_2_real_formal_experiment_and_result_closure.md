# V5.2 Real Formal Experiment and Result Closure

## 1. Stage Definition

V5.2 uses the V5.1 real plugin-driven multi-process multi-shard runtime to execute complete experiment matrices and close unified results, reproducibility artifacts, and paper data. It absorbs the old planned formal-runner dispatch and result-center integration work.

## 2. Current Foundation

Current foundations:

- V3 formal MetaTrack benchmark runner already builds matrices, runs Go V3 logical runtime, aggregates metrics, writes confidence intervals, chart data, reports, and ZIP artifacts.
- Run Experiment already expands saved method templates, workloads, topology, seeds, and repeats into rows.
- Results and Artifacts page exists, but it primarily uses V2 run registry and separate artifact paths.
- V4 realism run artifacts are accessible through separate `/api/v4/realism/*` endpoints.

Current gaps:

- Main/comparison/ablation/workload sensitivity/topology scaling rows are preview-only in Run Experiment.
- No persistent unified Run Group / Child Run model spans V3, V4, and V5.
- No real-cluster scheduler exists.
- Result Center metrics are not yet driven by plugin manifest metadata.
- No Paper Candidate gate exists for V5 real-cluster evidence.

## 3. Execution Modes

Preview:

- validates configuration, compatibility, matrix expansion, and resource estimate;
- does not execute runtime;
- never produces paper evidence.

Simulation:

- uses V3 logical runtime;
- supports quick screening, debugging, and inexpensive matrix pruning;
- must not be automatically marked as formal paper result.

Real Cluster:

- uses V5.1 real multi-process runtime;
- each child run starts a new clean cluster;
- no automatic fallback to simulation or V4 smoke;
- only this mode can produce paper-candidate formal results.

## 4. Formal Experiment Types

V5.2 must support:

- Main Experiment
- Comparison Experiment
- Ablation Experiment
- Workload Sensitivity
- Topology Scaling
- Fault / Recovery Experiment

Each experiment type must derive rows from the same `ExperimentSpec` and `CompiledRunPlan` path, not from separate hardcoded runners.

## 5. Run Group And Child Run

Run Group persistence:

```text
RunGroup
|-- child run
|-- method
|-- workload
|-- topology
|-- seed
|-- repeat
|-- scan point
|-- execution backend
`-- status
```

Each `real_cluster` child run:

```text
create clean directory
-> compile ExperimentSpec
-> start real nodes
-> run workload
-> collect results
-> stop all nodes
-> check orphan processes
-> persist result
```

The scheduler must support cancellation and failure cleanup. A failed child run must preserve logs, process manifests, cleanup attempts, and orphan-process checks.

## 6. Unified Run Registry

The result registry must unify:

- V3 simulation runs;
- V4 historical realism smoke;
- V5 real-cluster runs;
- run groups;
- child runs.

Every record must include:

- `execution_backend`
- `runtime_truth`
- `workload_truth`
- `method_config_id`
- plugin snapshot
- nodes
- shards
- validators_per_shard
- `tx_count`
- seed
- repeat
- status
- `paper_candidate`

## 7. Result Center

The Result Center must display run groups and child runs as first-class objects. It must not remain a V2-only run registry view plus separate V4 pages.

Metrics must not all be hardcoded. Plugin manifests should expose:

- metric key
- type
- unit
- aggregation
- visualization
- description

The frontend can then generate summary cards, table columns, time series, ratio visualizations, and artifact/log entries from metadata.

## 8. Statistical And Paper Artifacts

Formal outputs must include at least:

- `raw_summary.csv`
- `aggregate_summary.csv`
- `confidence_interval.csv`
- `comparison_summary.csv`
- `ablation_summary.csv`
- `sensitivity_summary.csv`
- `scaling_summary.csv`
- `paper_figure_data.csv`
- `paper_table_data.csv`
- `reproducibility_manifest.json`
- `experiment_report.md`
- `artifacts.zip`

V5.2 may keep V3 artifact names for compatibility through adapters, but new V5 real-cluster outputs should use the unified names above.

## 9. Paper Candidate Gate

Paper Candidate requires:

- `execution_backend = real_cluster`;
- all nodes started as independent OS processes;
- process count matches topology;
- all selected plugins support `real_cluster`;
- real client submit path;
- real TCP;
- each shard completes consensus;
- persistence completes;
- state roots are consistent;
- artifacts are complete;
- no automatic downgrade;
- no orphan processes.

Simulation, preview, and V4 smoke may be useful references but cannot be automatically marked as Paper Candidate.

## 10. Final Acceptance

Minimum final V5.2 acceptance:

- 16 nodes;
- 4 shards;
- 4 validators per shard;
- MetaTrack Full;
- Hash Serial Baseline;
- No Aggregation;
- 10000+ transactions;
- multiple seeds;
- multiple repeats.

The run must generate real method comparison, confidence intervals, run history, and complete reproducibility ZIP from real-cluster child runs.

## 11. Non-Goals

V5.2 does not introduce another template store. It does not redefine V5.1 runtime semantics. It does not promote simulation results to paper evidence. It does not claim production blockchain security or performance superiority without controlled artifacts.

## 12. Completion Standard

V5.2 is complete when formal run groups can execute on V5.1 `real_cluster`, persist child runs, show unified metrics and artifacts, pass the Paper Candidate gate, and export paper-ready tables, figures, reports, and reproducibility bundles.
