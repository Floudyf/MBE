# V3.3.5 Composer Draft Validation and Smoke

## Scope

V3.3.5 closes the first interactive single-chain Composer Draft loop.

- V3.3.5a added the frontend Composer Draft UI.
- V3.3.5b added backend `validate-draft` and `run-draft-smoke`.
- The scope is still the single-chain modular research chain.
- This is not Fabric-backed, not MetaFlow, not dual-chain, and not a formal paper experiment run.

## V3.3.5a Frontend Draft UI

The V3 Composer page lets the user select a single-chain module, choose a plugin, and mark the module as one of:

- `default`
- `fixed`
- `variable`
- `disabled`
- `planned`
- `output`

The frontend maintains a local `ComposerDraft` for immediate feedback. This local check is a usability hint only.

## V3.3.5b Backend Authority

The backend is the authority for runtime eligibility.

`POST /api/v3/composer/validate-draft`:

- accepts a frontend draft payload;
- checks it against a server-side catalog;
- normalizes module status and plugin selections;
- checks required modules, output modules, planned plugins, preview-only plugins, and template fairness;
- returns `is_valid`, `is_runnable`, normalized draft, warnings, and errors;
- does not start the runtime and does not write `.cache`.

`POST /api/v3/composer/run-draft-smoke`:

- calls the same backend validator first;
- refuses to run if `is_runnable` is false;
- generates a temporary single Draft Smoke profile;
- calls the existing V3 Go-backed single-chain runtime once;
- returns artifacts for that single Draft Smoke run.

## Local Validation vs Backend Validation

Frontend local validation:

- immediate UI feedback;
- helps explain module choices;
- must not be treated as authoritative.

Backend validation:

- authoritative for running;
- does not trust frontend `runnable`, `planned`, `preview_only`, or plugin ownership flags;
- recomputes capability from server-side catalog and runtime support.

## Built-in Smoke vs Draft Smoke

Built-in smoke:

- endpoint: `POST /api/v3/composer/run-smoke`;
- runs the built-in MetaTrack Go-backed ablation smoke;
- expands the fixed comparison set used by that smoke path.

Draft Smoke:

- endpoint: `POST /api/v3/composer/run-draft-smoke`;
- runs only the current Composer Draft single configuration;
- does not auto-expand the MetaTrack ablation matrix;
- is for local debugging, demo, and configuration tracing.

## Plugin Boundary

Runnable examples:

- Routing: `hash_sharding`, `co_access_sharding`
- Execution: `serial_execution`, `dual_track_execution`
- StateAccess: `direct_fetch`, `access_list_prefetch`
- Commit: `normal_commit`, `hot_update_aggregation_commit`

Preview-only examples:

- Workload: `existing_trace`, `saved_workload`
- CommitteeEpoch: `fixed_epoch_placeholder`

Planned examples:

- `fabric_observation_trace`
- `pbft_model`, `hotstuff_model`, `raft_model`
- `committee_lifecycle_planned`
- `dynamic_resharding`
- `block_stm_like`

Planned plugins may be displayed but must not enter runtime execution.

## Artifacts

Draft Smoke artifacts are written only under:

```text
.cache/v3_draft_runs/<run_id>/
```

The generated profiles remain temporary artifacts. They must not be written into `configs/` and must not be committed to Git.
