# V3.10 Benchmark / Experiment Template Hardening Plan

## 1. Goal

V3.10 moves MBE from a function-oriented platform toward an experiment-oriented platform. The V3.5-V3.9 foundations for node topology, NetworkAdapter, ConsensusRuntime, CrossShardProtocol skeleton, and State Authenticity Layer MVP can now be exercised through standard benchmark templates, baselines, sweeps, multi-seed repeatability, automatic aggregation, and reproducibility manifests.

V3.10 = Benchmark / Experiment Template Hardening.

V3.10 is not a new low-level mechanism stage, not complete Relay / Broker / 2PC, not production PBFT / HotStuff / Raft, not Ethereum-compatible MPT, not Fabric/EVM live backend, not BlockEmulator backend, and not final paper-scale benchmark evidence.

V3.5 completed node topology, launcher preview, and node process preview. V3.6 completed NetworkAdapter and typed message runtime. V3.7 completed configurable ConsensusRuntime and PBFT preview over NetworkAdapter. V3.8 completed CrossShardProtocol skeleton and relay_preview artifacts. V3.9 completed State Authenticity Layer MVP. V3.10 builds benchmark templates, baselines, sweeps, repeatability, manifests, and benchmark reports on top of those boundaries.

## 2. V3.10 Scope

V3.10 implements:

- benchmark template catalog
- baseline profile catalog
- sweep runner MVP
- multi-seed repeatability MVP
- reproducibility manifest
- benchmark run index
- benchmark summary aggregation
- baseline comparison output
- benchmark report
- artifact allowlist / history / download integration
- minimal frontend display
- V3.10 closure

## 3. Non-goals

V3.10 does not implement complete Relay, complete Broker, complete 2PC, real atomic cross-shard commit, production PBFT / HotStuff / Raft, Ethereum-compatible MPT, full stateless execution, Fabric/EVM live backend, BlockEmulator backend, real large-scale distributed benchmarks, paper-grade benchmark evidence, or performance superiority over BlockEmulator.

## 4. Benchmark Template Catalog

Required templates:

- `metatrack_hotspot_template`
- `pbft_network_template`
- `cross_shard_relay_preview_template`
- `state_authenticity_template`
- `full_stack_v3_template`

Each template records `template_id`, `description`, `workload_profile`, `topology`, `consensus_runtime`, `network_adapter`, `cross_shard_protocol`, `state_backend`, `baseline_candidates`, `sweep_parameters`, `required_artifacts`, and `truth_boundary`.

Templates are controlled experiment configurations. They are not paper conclusions and do not automatically prove performance advantages.

## 5. Baseline Profiles

Required baselines:

- `baseline_simple_chain`
- `baseline_hash_sharding`
- `baseline_no_prefetch`
- `baseline_no_cross_shard_protocol`
- `baseline_memory_kv`
- `baseline_no_state_authenticity`

Each baseline records `baseline_id`, `description`, `disabled_features`, `enabled_features`, `comparison_target`, and `truth_boundary`.

V3.10 establishes runnable baseline structure and outputs only. It does not claim final performance superiority.

## 6. Sweep Runner

The sweep runner MVP supports:

- `tx_count`
- `shard_count`
- `hotspot_ratio`
- `hot_key_count`
- `submit_rate`
- `network_adapter`
- `consensus_runtime`
- `cross_shard_protocol`
- `state_backend`
- `seed`
- `repeat_count`

It can generate a benchmark plan from `template_id`, `baseline_id`, sweep parameters, seed, and repeat count; record run summaries; write `sweep_summary.csv`, `sweep_summary.json`, `benchmark_run_index.csv`, and `baseline_comparison.csv`.

V3.10 sweeps are local controlled benchmark MVP outputs, not large-scale distributed experiments.

## 7. Multi-seed Repeatability

V3.10 supports `repeat_count` and seed base plus repeat index. Each repeat records `seed`, `repeat_index`, `template_id`, and `baseline_id`.

Repeatability outputs include:

- `benchmark_run_index.csv`
- `aggregate_summary.csv`
- `sweep_summary.csv`

Aggregation distinguishes `mean`, `min`, and `max`; later stages may add `std`.

## 8. Reproducibility Manifest

`reproducibility_manifest.json` includes:

- `benchmark_id`
- `template_id`
- `baseline_id`
- `run_id`
- `repeat_index`
- `seed`
- `git_commit`
- `python_version`
- `go_version`
- `node_version`
- `os`
- `created_at`
- `used_chain_profile`
- `used_plugin_profile`
- `used_experiment_profile`
- `artifact_list`
- `runtime_truth`
- `current_stage`
- `latest_runtime_stage`

The manifest is an experiment reproduction index, not performance proof. Unknown version values must be written as `unknown`, not fabricated.

## 9. Benchmark Report

`benchmark_report.md` includes benchmark title, `template_id`, `baseline_id`, sweep parameters, `repeat_count`, run count, core metrics table reference, artifact list reference, truth boundary, reproducibility manifest reference, and limitations.

The V3.10 benchmark report is an automated local experiment report, not a final paper experiment chapter.

## 10. Artifacts

V3.10 artifacts:

- `benchmark_template_catalog.json`
- `baseline_profile_catalog.json`
- `benchmark_plan.json`
- `benchmark_run_index.csv`
- `sweep_matrix.csv`
- `sweep_summary.csv`
- `sweep_summary.json`
- `aggregate_summary.csv`
- `baseline_comparison.csv`
- `reproducibility_manifest.json`
- `benchmark_report.md`
- `benchmark_summary.json`

`paper_grade_benchmark` must be false in benchmark summaries.

## 11. Summary Metrics

Required summary metrics:

- `benchmark_template_selected`
- `baseline_profile_selected`
- `benchmark_run_count`
- `sweep_parameter_count`
- `repeat_count`
- `benchmark_artifact_count`
- `baseline_comparison_count`
- `reproducibility_manifest_available`
- `benchmark_report_available`
- `paper_grade_benchmark`

Optional reserved metrics:

- `benchmark_success_count`
- `benchmark_failed_count`
- `benchmark_mean_tps`
- `benchmark_mean_p95_latency_ms`
- `benchmark_mean_p99_latency_ms`

## 12. Frontend Layout Rule

V3.10 must not add a Benchmark main-flow card.

The main flow remains:

```text
Workload -> TxPool -> BlockProducer -> ConsensusRuntime -> CommitteeEpoch -> Routing/Sharding -> Execution -> StateAccess -> StateStorage -> Commit -> MetricsReport
```

Benchmark belongs to the experiment control layer / result layer.

Allowed frontend changes are limited to benchmark template and baseline selection in existing configuration, Benchmark summary in the result panel, Benchmark artifacts in ArtifactGroups, and legacy missing handling for old runs.

## 13. Relationship with Previous Stages

V3.10 reuses V3.5 topology / launcher / node process preview, V3.6 NetworkAdapter / typed message runtime, V3.7 ConsensusRuntime / PBFT preview, V3.8 CrossShardProtocol skeleton, and V3.9 State Authenticity Layer MVP.

V3.10 does not modify V3.6 NetworkAdapter semantics, V3.7 PBFT preview semantics, V3.8 CrossShardProtocol skeleton semantics, V3.9 State Authenticity semantics, or the main-flow card layout.

## 14. Truth Boundary

V3.10 can claim:

- benchmark template catalog MVP
- baseline profile catalog MVP
- local controlled sweep runner MVP
- multi-seed repeatability MVP
- reproducibility manifest
- benchmark report
- benchmark artifacts
- frontend minimal benchmark summary

V3.10 cannot claim:

- paper-grade benchmark evidence
- large-scale distributed benchmark
- performance superiority over BlockEmulator
- complete Relay / Broker / 2PC
- production PBFT / HotStuff / Raft
- Ethereum-compatible MPT
- Fabric/EVM live backend
- BlockEmulator backend
- production sharding system

Runtime truth: `benchmark_template_hardening_not_paper_grade_benchmark`.

## 15. Acceptance Criteria

V3.10 is complete when benchmark template catalog and baseline profile catalog are available; at least three templates and three baselines are selectable or generated; the sweep runner MVP creates `sweep_matrix.csv`; repeatability records `repeat_index` and `seed`; `reproducibility_manifest.json`, `benchmark_run_index.csv`, `sweep_summary.csv/json`, `baseline_comparison.csv`, `benchmark_report.md`, and `benchmark_summary.json` are generated; summary metrics are visible; frontend shows Benchmark summary and artifacts; no Benchmark main-flow card is added; README, execution plan, and skill are updated; and validation passes.

## 16. Next Stage

V3.11 CrossShard Protocol Hardening.
