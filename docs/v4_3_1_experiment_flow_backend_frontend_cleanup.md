# V4.3.1 Experiment Flow + Backend/Frontend Clarity Cleanup

## A. Current Problems

- The frontend currently exposes "experiment console" and "V4 Realism Mode" as peer-level entries, so users cannot easily tell which one is the formal experiment entry.
- The experiment console already acts as the plan/profile configuration surface.
- V4 Realism Mode already acts as the real-node validation and real-workload execution surface.
- `backend/app/main.py` currently mixes V1, V2, V3, and V4 routes, which will become harder to maintain over time.
- The V4 runner is clear, but V4 routes are still embedded in `main.py`.
- The V2/V3 run registry and V4 realism runs are still relatively separate. This round does not force a unified registry, but the later direction should be explicit.

## B. Correct Experiment Flow

### 1. Select Plan

The first step is to choose mechanism, runtime, topology defaults, cross-shard settings, and fault profile. The result should be saved as a Profile or Plan.

This is not the same thing as an experiment result. A saved plan is reusable input for validation, real workload execution, and later comparison.

### 2. Small Real-node Validation

Use the V4.3 runtime for a small validation before larger runs.

Recommended default:

```text
nodes=8
shards=2
tx_count=20
blockemulator_tx_limit=20
fault_profile=mixed_light
enable_cross_shard=true
enable_faults=true
```

Pass conditions:

```text
ready_to_commit=true
state_root_mismatch_count=0
real_p2p=true
pbft_style_consensus=true
```

### 3. Real Workload Run

Use filtered workloads after the small real-node validation passes.

Supported workload families for this flow:

```text
small_test
real_skew_low
real_skew_medium
real_skew_high
extreme_hotspot
blockemulator_selectedTxs_subset
```

Workload size may be `small`, `medium`, or `large`. Early runs should start with small real workloads; they do not need to jump directly to 300K transactions.

### 4. Batch Comparison

Run fixed-plan or multi-plan comparisons by switching workload and skew level.

Expected outputs:

```text
TPS
P95/P99 latency
cross-shard count
state root mismatch count
fault events
artifacts
```

### 5. View Results And Artifacts

Result review should expose:

```text
core summary
network log
PBFT log
block commit
state root
receipt
tx index
xshard
fault
bridge artifacts
```

## C. Backend Target Architecture

Target layering:

```text
backend/app/
  main.py                 app assembly only
  api/                    FastAPI routers
  models/                 Pydantic schemas
  services/               business logic
  core/                   paths/settings/errors
```

Long-term shared concepts:

```text
Profile
Topology
Workload
Run
Artifact
```

The later registry direction is to make V2/V3 run records and V4 realism runs visible through a coherent run/artifact model while preserving existing paths and historical behavior.

## D. Allowed Changes In This Round

- Documentation.
- Skill updates.
- Frontend navigation labels.
- Frontend flow guidance.
- V4 `RealismModePanel` presentation.
- Backend V4 route extraction.
- Backend core paths.
- App title.

## E. Forbidden Changes In This Round

- Go runtime changes.
- `executor/realism/*` changes.
- V4.3 command semantics changes.
- `/api/v4/realism/*` path changes.
- V3/V2/V1 behavior changes.
- Changing non-claim truth fields to true:
  - `production_pbft`
  - `full_byzantine_security`
  - `production_blockchain`
  - `production_atomic_commit`
  - `full_blockemulator_compatibility`

