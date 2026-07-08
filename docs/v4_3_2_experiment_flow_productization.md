# V4.3.2 Experiment Flow Productization

## Scope

V4.3.2 is not a new runtime. It does not change the Go runtime, supervisor command semantics, or the existing `/api/v4/realism/*` execution API.

This round productizes the experiment flow by adding a backend abstraction layer for selecting and previewing runnable experiment combinations before the user enters V4 realism execution.

## Goal

The goal is to turn the frontend flow:

```text
Profile -> real-node validation -> real workload run
```

into a shared frontend/backend data model:

```text
Profile + Topology + Workload + runtime -> RunPlan preview -> V4 realism request
```

The V4 realism API remains the actual run entry. The experiment-flow layer prepares and validates run parameters; it does not replace real execution.

## Concepts

### Profile

An experiment Profile is a mechanism or method combination. It names what is being tested, the target runtime family, default topology, default workload, mechanism tags, and whether it is currently runnable.

Examples:

- V4.3 realism default profile.
- MetaTrack V3 mechanism profile.
- Baseline Hash comparison profile.

### Topology

Topology describes the local node and shard shape used by the run plan.

Fields include node count, shard count, validators per shard, runtime mode, description, and runnable state.

### Workload

Workload describes data source, scale, skew level, default transaction count, BlockEmulator import limit, and whether a CSV path is required.

Workloads whose real dataset is not attached yet must be marked `planned=true` and must not appear runnable.

### RunPlan

A RunPlan is a preview of:

```text
Profile + Topology + Workload + runtime
```

It produces a V4-compatible recommended request, warnings, runnable status, and next-step guidance. It does not execute the runtime and does not replace `/api/v4/realism/smoke`.

## Frontend Use

The frontend should fetch catalog and recommendation data from the backend:

- `GET /api/experiment-flow/profiles`
- `GET /api/experiment-flow/topologies`
- `GET /api/experiment-flow/workloads`
- `GET /api/experiment-flow/recommended-run`
- `POST /api/experiment-flow/preview-run-plan`

The V3 selection page may store the selected Profile, Topology, and Workload as a local current RunPlan selection. The V4 realism page should read that selection, ask the backend for a RunPlan preview, and apply `recommended_v4_request` to the existing V4 request form.

Major recommended experiment parameters should come from `recommended-run` or `preview-run-plan`, not hardcoded frontend constants.

## Execution Boundary

The V4 realism API remains the actual runtime entry:

```text
/api/v4/realism/status
/api/v4/realism/smoke
/api/v4/realism/runs/{run_id}/summary
/api/v4/realism/runs/{run_id}/artifacts
/api/v4/realism/runs/{run_id}/artifacts/{filename:path}
```

V4.3.2 does not change these paths or response compatibility.

## Non-goals

- Do not unify the V2/V3 run registry and V4 realism runs in this round.
- Do not modify `executor/`.
- Do not modify the Go runtime.
- Do not change `go run ./cmd/mbe-supervisor` command semantics.
- Do not turn non-claims such as production PBFT, full Byzantine security, production blockchain, production atomic commit, or full BlockEmulator compatibility into true claims.

