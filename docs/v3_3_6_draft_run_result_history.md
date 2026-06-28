# V3.3.6 Draft Run Result and History

## Scope

V3.3.6 turns the V3.3.5b Draft Smoke path into a demonstrable and traceable local loop:

```text
configure Draft -> backend validate -> run Draft Smoke -> inspect result -> revisit history
```

This history is a local `.cache` record. It is not a database and is not a formal paper result store.

## Directory Layout

Each Draft Smoke run writes to:

```text
.cache/v3_draft_runs/<run_id>/
```

Typical files:

```text
composer_draft.json
normalized_draft.json
draft_validation.json
generated_experiment_profile.json
generated_experiment_profile.yaml
generated_plugin_profile.json
generated_plugin_profile.yaml
summary.csv
summary.json
latency.csv
runtime.log
block_log.csv
tx_results.csv
state_commit_log.csv
used_chain_profile.yaml
used_plugin_profile.yaml
used_experiment_profile.yaml
```

Missing files are reported as `missing_files`; missing optional files do not crash the history API.

## List API

`GET /api/v3/composer/draft-runs?limit=20`

Returns recent Draft Smoke runs from `.cache/v3_draft_runs/`.

Each item includes:

- `run_id`
- `created_at`
- `template_id`
- `run_mode`
- `is_valid`
- `is_runnable`
- `selected_plugins`
- module scope lists
- `artifact_count`
- `artifact_groups`
- `summary_preview`
- `missing_files`

The API ignores `latest` and rejects unsafe run identifiers.

## Detail API

`GET /api/v3/composer/draft-runs/{run_id}`

Returns:

- raw `composer_draft`
- `normalized_draft`
- backend `validation`
- generated experiment profile
- generated plugin profile
- artifact groups
- downloadable artifacts
- summary preview
- missing files

This lets a user re-check exactly which draft, normalized plugin selection, validation result, and temporary profile produced a run.

## Frontend UX

The Composer page now shows:

- a compact current Draft Smoke result card after a run succeeds;
- actual plugin selection used by the run;
- summary preview fields when present;
- current run artifacts;
- a collapsible "Recent Draft Smoke runs" panel.

The history panel can refresh recent local runs and open a run detail. The same artifact component is reused for current and historical artifacts.

## Boundary

Draft Smoke history is only for local debugging, demos, and configuration traceability. It must not be called:

- a formal experiment database;
- paper-grade evidence;
- Fabric-backed validation;
- MetaFlow evaluation;
- dual-chain runtime output.
