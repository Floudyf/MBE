# V3 Maintenance: MetaTrack Formal Benchmark Console

## Goal

This maintenance patch adds a MetaTrack formal controlled benchmark console after V3-final closure. It keeps Draft Smoke as a quick validation path and adds a separate formal benchmark path for controlled local emulator experiments.

## Quick Validation vs Formal Benchmark

Draft Smoke validates one current composer draft with bounded runtime settings. It checks configuration, Go runtime wiring, summaries, and artifacts. It does not run the full `metaverse_tx_count` workload and must not be used as paper-final evidence.

The formal MetaTrack benchmark uses explicit numeric parameters: per-run transaction count, seed list, selected baselines, and one scan variable per experiment type. It writes formal result tables for plotting and review, while preserving the V3-final truth boundary.

## Formal Parameters

- `formal_tx_count`: 1,000 to 1,000,000 transactions per run. Default 10,000.
- `seed_base`, `seed_count`, `seed_list`: deterministic multi-seed control. Default seed list is derived from 42 and 5 seeds.
- `baseline_ids`: controlled MetaTrack baseline registry.
- `hotspot_ratio_points`: default `0.0, 0.2, 0.4, 0.6, 0.8`.
- `cross_shard_ratio_points`: default `0.0, 0.2, 0.4, 0.6`.
- `shard_count_points`: default `1, 2, 4, 8`.
- `zipf_alpha`: saved to the formal config for workload adapter extension.
- `runtime_evidence_mode`: `logical_single_process` for main performance experiments, or `local_multi_process_validation` only for prototype realism validation.

The console does not use vague scale presets. Every formal run count and transaction count is derived from explicit numbers.

## Experiment Types

- `ablation`: scans MetaTrack baseline/plugin combinations.
- `hotspot_sensitivity`: scans only `hotspot_ratio`.
- `cross_shard_sensitivity`: scans only `cross_shard_ratio`.
- `shard_scalability`: scans only `shard_count`.
- `control_overhead`: scans mechanism combinations and reports overhead metrics when the runtime emits them.
- `workload_comparison`: scans workload scenarios such as `scene_hotspot`, `cross_scene_migration`, and `mixed_metaverse`.

The default rule is single-variable scanning. The formal console does not perform a full-factorial sweep by default.

Saved methods, workloads, and topologies from the V3 saved config library can be selected as formal benchmark inputs. Legacy built-in baseline IDs remain supported for compatibility.

## Resource Protection

Backend guards reject oversized designs:

- `max_run_count = 200`
- `max_total_tx_count = 20,000,000`
- `max_seed_count = 10`
- `max_tx_count_per_run = 1,000,000`
- `max_scan_points = 20`

Preview returns `is_runnable=false` with clear errors when the matrix exceeds limits. Run requests are rejected instead of silently changing user parameters.

## Outputs

Formal benchmark runs write to `.cache/v3_metatrack_formal_runs/<run_id>/` and generate:

- `formal_benchmark_config.json`
- `formal_matrix_preview.json`
- `formal_run_matrix.csv`
- `formal_run_index.csv`
- `formal_run_manifest.json`
- `formal_progress.json`
- `formal_failed_runs.csv`
- `formal_child_artifact_index.csv`
- `formal_chart_preview.json`
- `formal_metric_extraction_report.csv`
- `formal_metric_extraction_report.json`
- `formal_missing_metrics.csv`
- `formal_raw_summary.csv`
- `formal_aggregate_summary.csv`
- `formal_workload_comparison.csv`
- `formal_latency_summary.csv`
- `formal_throughput_summary.csv`
- `formal_overhead_summary.csv`
- `formal_confidence_interval.csv`
- `formal_paper_figure_data.csv`
- `formal_reproducibility_manifest.json`
- `formal_benchmark_report.md`
- `summary.json`

The frontend also exposes a one-click ZIP endpoint for the formal result root. The ZIP includes root-level formal files and key child-run allowlisted artifacts when present. Missing child files do not fail ZIP generation.

## Result Dashboard

Completed formal runs now include a result dashboard:

- Summary cards for evidence level, run count, seed list, method/workload/topology counts, and failed child runs.
- Chart preview for throughput, average latency, P95 latency, and P99 latency when those metrics exist.
- Data file explanations for `formal_paper_figure_data.csv`, `formal_workload_comparison.csv`, `formal_aggregate_summary.csv`, `formal_raw_summary.csv`, `formal_child_artifact_index.csv`, `formal_reproducibility_manifest.json`, and `formal_chart_preview.json`.
- Separate preview and download links for individual CSV/JSON/MD files.
- ZIP download for the full formal result package.

`formal_chart_preview.json` is derived from aggregate rows and paper figure rows. It does not fabricate missing metrics. The chart preview is a convenience view for trend inspection; the CSV files remain the authoritative plotting source.

## Formal Run History

The console can list recent formal runs and reload a previous result after page refresh. History reads local run metadata plus `summary.json` and `formal_chart_preview.json` when available. Runs without a summary are listed without breaking the page.

Missing runtime metrics are left empty and listed in the report. The runner does not fabricate overhead, latency, or cross-shard fields.

The formal metric extraction layer also records where each metric came from. It can read runtime summary fields, JSON/CSV summaries, MetaTrack mechanism metrics, and latency files. When summary files do not expose latency metrics, it computes average/P95/P99 from successful `latency_ms` rows. When throughput is absent but a success count and positive elapsed duration exist, it derives `throughput_tps`.

`formal_aggregate_summary.csv` keeps unavailable metrics with `metric_available=false` for diagnostics. `formal_workload_comparison.csv` filters those unavailable rows so the workload comparison table is not dominated by empty metrics.

## Paper Candidate Rule

Formal results default to `controlled_benchmark`. A run becomes `paper_candidate` only when:

1. `formal_tx_count >= 10000`
2. `seed_count >= 3`
3. baselines include `baseline_hash_serial` and `metatrack_full`
4. no preview/planned plugins are used
5. aggregate statistics and figure data are generated
6. `run_count > 0`
7. all child runs completed

Even a paper candidate is still local emulator evidence, not a final paper conclusion by itself.

## Frontend Changes

The V3 composer page is organized as:

1. Current configuration summary
2. Plugin combination presets
3. Runtime topology and workload
4. Module flow
5. Paper experiment design
6. Run and results
7. Artifact downloads

Runtime topology is folded into core runtime settings, node topology details, metaverse workload details, fault/observability/reproducibility settings, and legacy benchmark compatibility settings. Artifact groups are folded by purpose, with formal benchmark core results and paper figure data shown first.

## Truth Boundary

V3 maintenance adds a local controlled benchmark console for MetaTrack. It does not start V4. It does not connect Fabric/EVM live backends, does not use BlockEmulator backend, does not implement production PBFT/HotStuff/Raft, and does not provide production multi-server deployment.

`local_multi_process_validation` is only a local prototype realism validation mode. Main performance experiments should use `logical_single_process` to keep control variables stable.

## Validation Commands

```powershell
git diff --check

cd frontend
npm.cmd run build
cd ..

$env:PYTHONPATH = (Get-Location).Path
py -3.12 -m pytest backend/tests -q
py -3.12 -m pytest tests -q
py -3.12 scripts/v0_sanity.py
```
