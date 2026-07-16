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

## 3. Mandatory Entry Gates

V5.2 formal run groups and matrices must not start until both gates below have
machine-readable acceptance evidence. These gates are implementation work for
V5.2, not claims about the V5.1 baseline.

### Gate A: Behavioral Plugin Boundary

V5.1 provides manifests, a catalog, and factory registration, but V5.2 must
make the runtime behavior boundary explicit before formal comparisons rely on
it. All 17 plugin categories use category-specific runtime interfaces. The
node runtime, client, and supervisor select behavior through instantiated
plugins and factory registration rather than algorithm-name branches in their
main execution paths. At minimum, routing, execution, and commit must produce
different observable behavior for registered test plugins and for the four
published method compositions. Manifests and registry registration may name a
plugin; runtime flow must not use those names as behavior switches.

### Gate B: Finality Metric Truth

Formal metrics must be derived from the real transaction lifecycle, not from
TCP send latency or generated summary rows. Every real-cluster child run must
write raw lifecycle events and derive finality, throughput, failure, and
cross-shard outcomes from durable runtime events. The lifecycle records
logical and physical transaction identity, submission, admission, proposal,
quorum commit, durable commit, target commit, source finalization, refund or
failure, shard, node, block, and timestamp. The finality acceptance script
must fail when a lifecycle is incomplete, duplicated incorrectly, or lacks
durable evidence.

Implementation checkpoint: Gate A is closed by
`scripts/v5_2_plugin_behavior_gate.py`: registered test factories prove
category behavior, the V5 runtime calls instantiated routing, execution, and
commit plugins, the four-method real-cluster regression passes, and the core
ID-branch audit passes. Gate B is covered by
`scripts/v5_2_finality_metric_acceptance.py`, which runs an 8-node / 2-shard
cluster and verifies raw lifecycle, unique logical-transaction finality,
cross-shard finalization/refund, `no_fallback`, and zero orphan processes.
Gate B is closed for the implemented metric contract by the same script and the
backend/Go regression suite. This does not make every future paper matrix row a
completed result.

## 4. Execution Modes

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

## 5. Formal Experiment Types

V5.2 must support:

- Main Experiment
- Comparison Experiment
- Ablation Experiment
- Workload Sensitivity
- Topology Scaling
- Fault / Recovery Experiment

Each experiment type must derive rows from the same `ExperimentSpec` and `CompiledRunPlan` path, not from separate hardcoded runners.

## 6. Run Group And Child Run

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

## 7. Unified Run Registry

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

## 8. Result Center

The Result Center must display run groups and child runs as first-class objects. It must not remain a V2-only run registry view plus separate V4 pages.

Metrics must not all be hardcoded. Plugin manifests should expose:

- metric key
- type
- unit
- aggregation
- visualization
- description

The frontend can then generate summary cards, table columns, time series, ratio visualizations, and artifact/log entries from metadata.

## 9. Statistical And Paper Artifacts

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

## 10. Paper Candidate Gate

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

## 11. Correctness, Performance, And Long-Running Matrix

The software correctness gate is separate from a performance target. A child
passes correctness only when `terminal=submitted`, `incomplete=0`,
`completion_reason=drain_quiescent`, all validators are height-aligned, common
height block/state/receipt roots agree, all queues are empty, `no_fallback=true`,
and `orphan_process_count=0`. Actual throughput and finality percentiles are
reported as measurements; V5.2 sets no TPS pass threshold and does not claim a
production-performance PBFT implementation.

The completed single-child acceptance used:

- 16 nodes;
- 4 shards;
- 4 validators per shard;
- MetaTrack Full;
- Hash Serial Baseline;
- No Aggregation;
- 10000+ transactions;
- one MetaTrack Full method;
- seed 71;
- 10000 signed transactions;
- 16 independent node processes;
- 16 nodes / 4 shards / 4 validators per shard.

Observed evidence is stored in the acceptance run artifacts: 10000 submitted,
10000 terminal, `drain_quiescent`, 16 processes, 251 blocks per shard,
`real_cross_shard_network=true`, `state_root_consistent=true`,
`no_fallback=true`, and zero orphan processes. The run measured 84.6009 TPS,
P50 finality 28419 ms, P95 98987 ms, and P99 109563 ms. These are observed
local-runtime measurements, not required performance thresholds.

The batch software acceptance uses 2 methods x 2 seeds x 2 repeats = 8 real
Child Runs and verifies sequential scheduling, persistence, reload, aggregate
statistics, CI, cancellation/retry paths, and ZIP artifacts.

The formal paper matrix is compiled and persisted as 3 methods x 2 seeds x 2
repeats = 12 rows, each configured for 10000 transactions, with fairness and
resource estimates. This round intentionally does not execute all twelve
long-running real clusters. Rows not executed remain queued/unexecuted and are
not marked completed or Paper Candidate. The same Scheduler can resume that
matrix later; this is a workload boundary, not a relaxation of the single-child
correctness gate.

Completed RunGroups generate real method comparison, confidence intervals, run
history, and reproducibility ZIPs from real-cluster child runs. An unexecuted
formal matrix only produces a plan/matrix persistence record.

## 12. Non-Goals

V5.2 does not introduce another template store. It does not redefine V5.1 runtime semantics. It does not promote simulation results to paper evidence. It does not claim production blockchain security or performance superiority without controlled artifacts.

The internal block-execution foundation work inside V5.2 adds a generic
`block_executor` extension point and `serial_block_executor` baseline. This is
not a new public V5 version and does not implement any external parallel
execution paper mechanism. Formal methods may vary the block executor only after
the selected implementation is represented in the plugin catalog, compiled plan,
runtime snapshot, results, and artifacts.

## 13. Completion Standard

V5.2 software closure is complete when formal run groups can execute on V5.1
`real_cluster`, persist child runs and attempts, show unified metrics and
artifacts, enforce fairness and the Paper Candidate gate, and export paper
tables, figures, reports, and reproducibility bundles. A future paper claim
still requires every included child to be independently completed and to pass
the per-child gate. Decentraland dataset integration is not part of this stage.
