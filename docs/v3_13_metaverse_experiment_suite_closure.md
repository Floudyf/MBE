# V3.13 Metaverse Experiment Suite Closure

## 1. V3.13 Goal

V3.13 closes the metaverse experiment suite stage. It moves MBE from a generic local modular chain/runtime emulator toward a metaverse-oriented controlled experiment platform with workload catalog, scenario templates, baseline matrix, multi-seed/sweep scaffolding, and paper table/figure data export.

## 2. Relationship To V3.12

V3.12 closed runtime realism with `local_multi_process` dry_run/smoke, lifecycle artifacts, shard/committee/epoch logs, and light reconfiguration plans. V3.13 does not replace that runtime. It adds metaverse workload semantics and benchmark/export artifacts above the existing Go-backed runtime path.

## 3. Metaverse Workload Suite

The V3.13 suite supports these controlled synthetic scenarios:

- `asset_transfer`
- `avatar_update`
- `scene_hotspot`
- `item_transfer`
- `cross_scene_migration`
- `onchain_offchain_confirmation`
- `cross_metaverse_transfer`
- `mixed_metaverse`

Generated workload metadata is deterministic by `seed`. It is not collected from Roblox, Decentraland, Sandbox, or any real platform.

## 4. Scenario Templates

The benchmark template catalog now includes:

- `metaverse_mixed_template`
- `metaverse_asset_transfer_template`
- `metaverse_cross_scene_template`
- `metaverse_cross_metaverse_template`

These templates organize local controlled experiments. They do not imply production bridge, live Fabric/EVM backend, or BlockEmulator execution.

## 5. Config Fields

V3.13 adds fields for enabling the suite, selecting scenario, sizing user/asset/item/avatar/scene/metaverse spaces, choosing `tx_count` and `seed`, setting hotspot/cross-scene/cross-shard/burst/read-write/skew ratios, enabling offchain confirmation/cross-metaverse metadata, and enabling benchmark/paper export scaffolds.

Validation enforces bounded counts, ratios in `[0, 1]`, scenario enum membership, and small bounded sweep lists.

## 6. Offchain And Cross-Metaverse Paths

`onchain_offchain_confirmation` writes deterministic `offchain_confirmation_log.csv` with configured delay and failure ratio. It does not call a real external service.

`cross_metaverse_transfer` writes `cross_metaverse_transfer_log.csv` with source/target metaverse IDs and Relay MVP compatibility metadata. It is not a production bridge.

## 7. Baseline Matrix

`baseline_matrix.csv` lists implemented/local candidates such as `single_chain`, `hash_sharding`, `relay_preview`, `relay_mvp`, `state_auth_disabled`, `prefetch_disabled`, and `local_multi_process_dry_run`.

External/planned baselines such as Broker, 2PC, Monoxide, BlockEmulator, Fabric, EVM, Porygon, and Block-STM are marked `planned_external`, `runnable=false`, with reason `not_implemented_in_mbe_v3_13`.

## 8. Multi-Seed / Sweep

When enabled, V3.13 writes `multi_seed_summary.csv` and `benchmark_suite_summary.json` from deterministic combinations of seeds, shard counts, cross-shard ratios, and hotspot ratios. This is a controlled local sweep scaffold, not large-scale distributed benchmarking.

## 9. Paper Export

When `paper_export_enabled=true`, V3.13 writes:

- `paper_table_latency.csv`
- `paper_table_throughput.csv`
- `paper_table_cross_shard.csv`
- `paper_table_offchain_confirmation.csv`
- `paper_figure_data.csv`
- `paper_export_manifest.json`

These are paper-data scaffolds. They do not prove paper-grade performance conclusions.

## 10. Artifacts

Metaverse artifacts:

- `metaverse_workload_catalog.json`
- `metaverse_workload_config.json`
- `metaverse_trace_meta.json`
- `scenario_summary.csv`
- `hotspot_distribution.csv`
- `cross_scene_transfer_log.csv`
- `offchain_confirmation_log.csv`
- `cross_metaverse_transfer_log.csv`
- `metaverse_experiment_summary.json`

Benchmark/export artifacts:

- `baseline_matrix.csv`
- `multi_seed_summary.csv`
- `benchmark_suite_summary.json`
- paper export CSV/manifest files listed above

## 11. Summary Metrics

V3.13 adds metrics for metaverse suite enablement, scenario, tx/user/asset/item/avatar/scene/metaverse counts, hotspot/cross-scene/cross-shard ratios, cross-scene/cross-shard/burst/offchain/cross-metaverse counts, baseline matrix, seed count, paper export availability, and:

```text
metaverse_experiment_truth = controlled_metaverse_workload_not_real_platform_trace
```

## 12. Frontend Changes

The V3 composer keeps the same navigation and main flow. Runtime topology adds a Chinese “元宇宙实验套件” configuration group, result panels show metaverse/sweep/paper metrics, and ArtifactGroups adds Metaverse Workload, Benchmark Matrix, and Paper Export groups.

## 13. Truth Boundary

V3.13 implements a controlled metaverse-oriented experiment suite for local emulator experiments.

It is not real metaverse platform trace collection.
It is not multi-server deployment.
It is not a production cluster.
It is not production PBFT / HotStuff / Raft.
It is not BlockEmulator backend.
It is not Fabric/EVM live backend.
It does not prove paper-grade performance.

中文口径：V3.13 实现的是受控合成的元宇宙实验套件，用于本地 emulator 的场景化 workload、baseline matrix、multi-seed/sweep 和 paper export scaffold。它不是真实平台 trace，不是生产级集群，也不是论文级性能结论。

## 14. Validation Commands

```powershell
git diff --check

cd frontend
npm.cmd run build
cd ..

cd executor
go test ./...
cd ..

$env:PYTHONPATH = (Get-Location).Path
python -m pytest backend/tests -q
python -m pytest tests -q
python scripts/v0_sanity.py
```
