# V2.2 Job and Artifact Manager

## Goal

V2.2 upgrades the V1-final-plus `latest` custom run into file-system-backed run history. Each run receives a unique `run_id`, its own artifact directory, persisted metadata, and safe download endpoints.

This is a synchronous job record wrapper. It does not introduce Celery, Redis, a distributed task queue, multi-user permissions, or a background worker.

## Run ID Rule

Run IDs use this readable form:

```text
v2run_YYYYMMDD_HHMMSS_ab12cd
```

The suffix prevents collisions. A run ID must not contain path separators and is only used as a directory name under `.cache/v2_jobs/`.

## Metadata Schema

Each run persists `.cache/v2_jobs/{run_id}/metadata.json` with:

```text
run_id
created_at
updated_at
stage
source
experiment_name
status
status_message
output_dir
data_truth_label
summary_available
report_available
artifact_count
```

Status values are:

```text
created
running
completed
failed
cancelled
```

## Artifact Allowlist

Only these files are downloadable through the V2.2 artifact API:

```text
config.yaml
used_config.yaml
used_config.json
trace_meta.json
summary.csv
latency.csv
runtime.log
report.md
sweep_summary.csv
sweep_summary.json
```

Artifact paths are resolved inside the selected run directory. Filenames with `/`, `\`, `.`, or `..` are rejected. Missing allowlisted files return a missing-artifact response instead of allowing arbitrary path reads.

## Latest Compatibility

`POST /api/v1/custom-run` now creates a V2.2 run directory under `.cache/v2_jobs/{run_id}/`. After the run completes, artifacts are mirrored back to `.cache/v1_custom_runs/latest/` so existing V1-final-plus endpoints remain compatible:

```text
GET /api/v1/custom-run/latest/summary
GET /api/v1/custom-run/latest/files
GET /api/v1/custom-run/latest/files/{filename}
```

The old response fields remain present, and the response now includes the real `run_id` and `latest_compat_dir`.

## Non-goals

V2.2 does not implement trace source expansion, a multi-chain trace schema, dual-chain replay, cross-chain protocols, MetaFlow, committee bridge, Pending Pool, Docker/Fabric startup, `network.sh` automation, or a V2.7 run-history UI.

## Relationship to Later Stages

V2.3 may add more trace sources that can be recorded with the same metadata. V2.4/V2.5/V2.6 may later produce different artifacts under the same run directory model. Those stages must still respect planned/runnable validation and must not execute planned topologies before their substrate exists.
