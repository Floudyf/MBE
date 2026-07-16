# V5 Migration, Compatibility, And Truth Boundary

## 1. Purpose

This document defines how V3, V4, and V5 coexist. V5 must grow from the real codebase without deleting useful history, duplicating template stores, or overclaiming current runtime behavior.

## 2. Path Relationship

| Path | Role in V5 | Truth boundary |
| --- | --- | --- |
| V3 final logical/formal runtime | `simulation` backend | local modular emulator / logical runtime |
| V4 realism smoke | historical realism validation and regression | research-grade smoke evidence, not final real cluster |
| V5 real cluster | future formal backend | independent-OS-process local multi-shard runtime after V5.1 acceptance |

## 3. Current System To V5 Mapping

Current `V3SavedConfig` remains the saved template store. V5 must not create a parallel method-template store.

Current 11-module Composer maps into V5 plugin categories through an adapter:

| Current module | V5 category |
| --- | --- |
| Workload | workload |
| TxPool | transaction_admission, txpool |
| BlockProducer | block_producer |
| Consensus | consensus |
| CommitteeEpoch | scheduler |
| Routing | sharding, routing, cross_shard |
| Execution | execution, scheduler |
| StateAccess | state_access |
| StateStorage | state_storage |
| Commit | commit |
| MetricsReport | metrics, observability |

The adapter must preserve user templates, validation status, tags, payload, last validation, and last smoke run id.

## 4. API Migration

Do not remove existing APIs at the start of V5:

- `/api/v3/*` remains available for V3 Composer, saved configs, quick validation, and formal logical-runtime runs.
- `/api/v4/realism/*` remains available for V4 smoke regression and historical artifact lookup.
- `/api/experiment-flow/*` can become a facade over V5 catalog/spec/compile flows, but compatibility must be deliberate.

New V5 APIs should expose catalog, compatibility, ExperimentSpec, compile, run group, child run, scheduler, and result-center concepts. Old APIs can be wrapped and later deprecated after UI migration and regression coverage.

## 5. Artifact Compatibility

V3 formal artifacts remain valid under simulation truth labels. V4 realism artifacts remain valid under V4 smoke regression labels. V5 real-cluster artifacts must include process manifests, plugin snapshots, node logs, network logs, consensus logs, block/state/receipt/tx-index artifacts, xshard evidence, metrics, cleanup results, and paper outputs.

V5 Result Center should index old artifacts through adapters rather than moving or rewriting historical outputs.

## 6. Truth Labels

Use explicit labels:

```text
v3_final_light_runtime_baseline
v3_formal_simulation_logical_runtime
v4_realism_smoke_regression
v5_preview_only
v5_simulation_logical_runtime
v5_real_cluster_candidate
v5_paper_candidate_real_cluster
```

Do not claim:

- V5 is implemented before V5.1 code and acceptance exist;
- `real_cluster` exists before independent process evidence exists;
- current V4 smoke is independent OS multi-process;
- current V3 formal runtime is V5 real cluster;
- all current plugins already load in V4 real-node runtime;
- all formal suites are executable from Run Experiment today;
- production PBFT, Byzantine security, production blockchain, Fabric/EVM live backend, or performance superiority.

## 7. Preview / Simulation / Real Cluster Boundary

Preview validates configuration and produces no runtime evidence.

Simulation uses V3 logical runtime and can guide experiment design. It cannot become paper evidence automatically.

Real Cluster is the future V5 backend. It must start independent node processes, run real workload clients, produce real network/consensus/state artifacts, and clean up processes. Startup failure is a failed real-cluster run, not a reason to fall back.

## 8. Windows Local Resource Boundary

Windows is the primary development environment. V5 should use normal child processes and TCP ports. It must avoid Go `.so` plugins, Linux-only process assumptions, and hidden external services unless explicitly introduced and documented.

The supervisor must estimate CPU, memory, ports, disk, and process count before a run. Scale acceptance must prove cleanup and no orphan processes.

## 9. Regression Rules

Implementation rounds must preserve:

- current Go tests;
- backend tests;
- frontend build;
- V3 formal regression;
- V4 realism regression.

Docs-only rounds run `git diff --check` and status checks. Code rounds add the relevant backend, frontend, Go, and sanity validations.
# V5.1 Implementation Status

V3 remains the `simulation` backend and V4 realism smoke remains a historical regression path. V5.1 adds, rather than renames, the `real_cluster` backend: a local one-logical-node/one-OS-process runtime with real localhost TCP client submission. A failed V5 run is recorded as failed and is never downgraded to V3 simulation or V4 smoke.

V5 method configurations continue to use `V3SavedConfig(config_kind="method")`; no `V5SavedConfig` store exists. Legacy payloads are adapted at compile time, retaining unmapped legacy choices as explicit blockers for `real_cluster` rather than mutating or deleting user templates.

The V5.1 truth boundary is `v5_real_cluster_candidate`: it has independent process, port, persistence, state-root, cross-shard, and artifact evidence, but does not claim production PBFT, Byzantine security, production atomicity, cloud/multi-server deployment, or V5.2 paper-candidate closure.

# V5.2 Internal Block Execution Extension

The block-execution foundation is an internal V5.2 extension. It introduces a
`block_executor` plugin category so block state execution can be selected,
compiled, instantiated, and audited independently from the existing `execution`
classification plugin and `scheduler` ordering plugin. The first executor,
`serial_block_executor`, is locked to the legacy MBE realism serial execution
engine and carries `legacy_faithful_reference_baseline`.

This extension is a compatibility-preserving migration point. Historical method
profiles that lack `block_executor` must be migrated to `serial_block_executor`
explicitly in the compiled plan. Runtime must not guess silently and must not
fall back to legacy execution after a selected block executor fails.
