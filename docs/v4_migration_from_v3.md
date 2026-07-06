# V4 Migration From V3

## 1. Migration Principle

V4 does not delete V3. V4 adds a new real runtime path and gradually integrates it with existing backend/frontend controls after standalone validation.

```text
V3-final remains runnable.
V4 is additive first.
Integration comes after standalone runtime evidence.
```

## 2. Keep From V3

Keep and reuse where appropriate:

- frontend shell and experiment console patterns;
- saved config workflow;
- formal benchmark matrix concepts;
- workload catalog and metaverse scenario templates;
- artifact grouping and download logic;
- reproducibility manifest concepts;
- topology profile ideas;
- metrics naming where compatible;
- MetaTrack mechanism concepts.

## 3. Do Not Reuse As Real Runtime

Do not treat these V3 components as V4 real runtime implementations:

- `local_multi_process` smoke/dry-run;
- `localhost_tcp_preview`;
- `blockemulator_aligned_pbft_preview`;
- `pbft_light_model`;
- `relay_mvp` deterministic summary path;
- `state_authenticity_mvp` as full durable state.

They may serve as references, baselines, or compatibility adapters.

## 4. V4 Additive Path

Recommended path:

```text
executor/realism/          # new V4 runtime libraries
executor/cmd/mbe-node/     # new node process
executor/cmd/mbe-client/   # new client submitter
executor/cmd/mbe-supervisor/# new supervisor/launcher
configs/v4/                # new V4 runtime configs
backend/app/services/v4_*  # later integration
frontend/src/components/v4/# later integration
```

## 5. Backend Migration

Backend integration should happen after standalone V4.1 or V4.2 works.

Do not create frontend buttons before the runtime has a validated smoke path.

Potential API names:

```text
/api/v4/realism/preview
/api/v4/realism/run-smoke
/api/v4/realism/runs/{run_id}
/api/v4/realism/artifacts/{run_id}/{filename}
```

## 6. Frontend Migration

Frontend should clearly separate:

```text
V3 Light Mode
V4 Realism Mode
```

V4 Realism Mode must display:

- runtime truth label;
- implemented stage;
- non-goals;
- node count / shard count;
- real network status;
- consensus status;
- state consistency;
- cross-shard state-machine status;
- artifacts.

## 7. Artifact Compatibility

V4 artifacts should use explicit `v4_` prefixes where needed to avoid confusion with V3 preview artifacts.

V4 should not overwrite V3 run directories. Use a separate cache/run root such as:

```text
.cache/v4_realism_runs/
```

## 8. Regression Rule

After each V4 code round:

- V3 smoke/basic validation must still pass where feasible;
- V4 standalone test must pass;
- docs must state what changed;
- generated artifacts must not be committed.
