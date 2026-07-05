# V3 Maintenance: Result Dashboard And Usability

## Why Close The Result Loop First

The formal MetaTrack benchmark already writes CSV/JSON artifacts, but a console workflow is incomplete if users must inspect run directories by hand. This maintenance patch closes the loop after a formal run:

```text
run -> dashboard -> chart preview -> file explanation -> preview/download -> ZIP -> history reload
```

This is still V3 maintenance after V3-final. It does not start V4.

## Dashboard Structure

The formal result panel shows:

- Summary cards: run ID, evidence level, transaction count, seed list, run count, method/workload/topology counts, completion count, and failed child count.
- Result chart preview: throughput TPS, average latency, P95 latency, and P99 latency when those metrics exist.
- Automatic interpretation: short “本次结果显示” notes only when the data has exactly two series and complete numeric values.
- Data file guide: each major formal file is labeled by purpose.
- ZIP download: one-click package for the formal root and selected child-run allowlisted artifacts.
- ArtifactGroups: grouped preview/download links for all discovered artifacts.

## Chart Preview And CSV Data Source

`formal_chart_preview.json` is generated from `formal_aggregate_summary.csv` and `formal_paper_figure_data.csv`. It is a lightweight frontend data source, not a replacement for the CSV tables.

The chart preview does not fabricate missing metrics. If the runtime does not emit a metric, the corresponding chart says the metric has no preview data.

## ZIP Contents

`GET /api/v3/composer/formal-metatrack/<run_id>/artifacts.zip` returns:

- Core formal files such as `summary.json`, `formal_benchmark_config.json`, `formal_matrix_preview.json`, `formal_run_manifest.json`, `formal_progress.json`, `formal_run_matrix.csv`, `formal_run_index.csv`, `formal_failed_runs.csv`, `formal_child_artifact_index.csv`, `formal_raw_summary.csv`, `formal_aggregate_summary.csv`, `formal_workload_comparison.csv`, and `formal_chart_preview.json`.
- Paper files such as `formal_latency_summary.csv`, `formal_throughput_summary.csv`, `formal_overhead_summary.csv`, `formal_confidence_interval.csv`, and `formal_paper_figure_data.csv`.
- Reproducibility files such as `formal_reproducibility_manifest.json` and `formal_benchmark_report.md`.
- Selected child-run allowlisted files under `child_runs/run_<index>/`.

The ZIP builder only includes files inside the run directory and only includes allowlisted artifact names. Missing child files are skipped.

## Formal Run History

The history API lists recent formal runs and allows the frontend to reload a previous result after page refresh. It reads local metadata, `summary.json`, `formal_matrix_preview.json`, and `formal_chart_preview.json` when available.

History is a local convenience feature. It is not a production result database.

## Ratio Slider Safety

Ratio fields use a slider plus number input. The UI accepts decimal input such as `0.8`, and also treats `80` as `0.8` to avoid the common 80/0.8 mistake. Values are clamped to `0..1`.

This applies to hotspot, cross-shard, cross-scene, read/write, burst, skew, offchain failure, message drop, and target congestion ratio fields.

## Recommended First MetaTrack Run

Use the “最真实链路确认” preset first:

- `experiment_type = workload_comparison`
- `formal_tx_count = 1000`
- `seed_count = 1`
- `runtime_evidence_mode = local_multi_process_validation`
- workload scenarios: `scene_hotspot`, `cross_scene_migration`, `mixed_metaverse`

After the link is confirmed, use a logical single-process formal run for main performance comparisons to keep control variables stable.

## Truth Boundary

V3 maintenance keeps the V3-final boundary:

- `local_multi_process` is local multi-process prototype validation, not multi-server deployment.
- `localhost_tcp_preview` is local TCP preview, not production networking.
- `relay_mvp` is local observable Relay MVP, not production atomic commit.
- `merkle_trie_mvp` is state authenticity MVP, not Ethereum MPT.
- `existing_trace_preview` is preview-only and does not enter the default formal benchmark path.
- Chart previews show trends; they do not prove paper-final conclusions.
