---
name: metaverse-chainlab-v1
description: Use when planning or implementing the V1 paper-experiment stage of the modular metaverse blockchain experiment platform. It preserves V0 closure while enforcing topology-first, single-chain-first V1 delivery and prevents premature V2/V3 work.
---

# Metaverse Chainlab V1 Skill

## 1. Purpose

V0 has completed the platform skeleton closure. V1 is the paper-experiment-ready stage: it builds a topology-first experiment wizard, a single-chain sharded-execution prototype, paper-mechanism plugins, baseline comparisons, small-scale chain-backed trace validation, and report outputs.

V1 is not a complete lab-wide general platform, not a production multi-chain/cross-chain platform, and not the MetaFlow stage.

## 2. Source of Truth Priority

Use this priority order for V1 work:

```text
AGENTS.md
  > .agents/skills/metaverse-chainlab-v1/SKILL.md
  > docs/v1_implementation_plan.md
  > docs/platform_plan_full.md
  > .agents/skills/metaverse-chainlab-v0/SKILL.md
  > docs/v0_implementation_plan.md
```

For an explicitly authorized V1 task, this skill takes precedence over the V0 skill where they conflict. Preserve all V0 anti-regression rules: do not break V0, keep its tests passing, use virtual time instead of simulated wall-clock waits, and never commit generated run artifacts.

## 3. Current Version State

### V0

- Complete and runnable as the default single-chain experiment closure.
- Frontend, FastAPI backend, synthetic workload, streaming trace, Go replay, metrics, downloads, and CI sanity are closed.

### V1

- V1.1 is in progress: topology-first wizard and declarative configuration framework.
- The only runnable V1 configuration is `v1_baseline_hash_serial`.
- `blockstm_like`, `calvin_like`, `porygon_like`, ours, ablation, and Fabric configurations are planned until their owning V1 phase is implemented.

## 4. Topology-first Experiment Flow

The platform must configure experiments in this order, rather than starting with a flat template or algorithm list:

1. **Experiment scope**: `single_chain`, `dual_chain`, `multi_chain`, or `cross_chain_protocol`.
   - `single_chain` is available in V1.
   - `dual_chain`, `multi_chain`, and `cross_chain_protocol` are planned in V1 and implemented in V2.
2. **Chain topology**: chain count, chain name, backend, shard count, block interval, finality, throughput, and chain links.
3. **Per-chain components**: consensus, consensus sharding, state sharding, execution sharding, routing, cross-shard protocol, execution, commit, clock, network model, and metrics.
4. **Workload source**: synthetic workload, existing trace replay, chain-backed workload, or hybrid workload.
5. **Workload parameters**: transaction count and mix, Zipf, hot keys, cross-shard/cross-chain ratios, conflict and commutative ratios, arrival/burst rate, and seed.
6. **Multi-chain/cross-chain configuration**: source/target chain, protocol, finality wait, committee, thresholds, timeout, pending pool, and window policy. Show only for applicable scopes.
7. **Experiment suite / strategy group**: baseline, routing, execution, commit, ablation, MetaTrack, or MetaFlow comparison.
8. **Composer Preview**: complete defaults, validate compatibility, label `runnable` / `planned` / `invalid`, and generate `config.yaml`.
9. **Run mode**: trace-only, replay-only, full-pipeline, sweep, or report-only.
10. **Results**: summary, latency, throughput, remote wait, rollback, aggregation, cross-chain, pending-pool, figures, and report.

Only implemented scopes and components may be scheduled. Planned capabilities may be shown but never run.

## 5. V1 Scope

V1 may implement, one phase at a time:

- V1.1 topology-first experiment wizard and declarative configuration framework.
- V1.2 MBE sharded-execution prototype enhancement.
- V1.3 workload and trace enhancement.
- V1.4 minimal single-chain Fabric chain-backed trace validation.
- V1.5 co-access / MetaTrack routing.
- V1.6 dual-track execution.
- V1.7 hot-update aggregation.
- V1.8 baselines, sweeps, reports, figures, and tables.
- Small V1 CI sanity and paper-experiment artifacts.

Keep each task within exactly one V1 phase. Do not implement a later phase merely because a declaration or planned configuration exists.

## 6. V1 Out of Scope

Do not implement in V1:

- Formal dual-chain or multi-chain execution.
- Formal cross-chain protocol experiments or cross-chain multi-channel support.
- MetaFlow, committee bridge, or Pending Pool.
- Multi-server Fabric or multi-organization/multi-peer deployment.
- Grafana dashboard or a complete Prometheus monitoring system.
- Multi-user permissions or distributed deployment.
- Parquet/Arrow, full EVM, complex drag-and-drop topology, or a lab plugin marketplace.

These belong to V2 or V3.

## 7. V1/V2/V3 Boundary

### V1

- `single_chain` is the primary runnable object.
- `dual_chain`, `multi_chain`, and `cross_chain_protocol` are planned display/configuration structures only.
- Fabric chain-backed trace is a small-scale, single-chain authenticity check only.
- Do not formally implement cross-chain protocol experiments.

### V2

- Implement dual-chain, multi-chain, heterogeneous-chain configuration, and cross-chain protocols.
- Implement committee bridge, Pending Pool, MetaFlow, cross-chain workload, and cross-chain metrics such as `cross_chain.csv` and `pending_pool.csv`.
- Add topology, cross-chain protocol, and pending-pool panels.

### V3

- Multi-server real deployment, multi-organization Fabric, real multi-chain cross-chain deployment, object archival, and long-lived lab infrastructure.

## 8. V1.1 Rules

- The frontend must present an experiment-scope/topology-first wizard as the primary V1.1 abstraction.
- Do not use a flat template-list-plus-configuration-list display as the primary V1.1 experience.
- Do not describe a template or suite as runnable; use only `previewable` or `planned`.
- Use `runnable` only for a concrete experiment configuration.
- `v1_baseline_hash_serial` is the only runnable configuration in V1.1.
- Planned configurations must have no run button.
- Do not show dual-chain, multi-chain, cross-chain protocol, or Fabric as currently runnable.

## 9. V1 Development Workflow

- Work on one phase only: V1.1 through V1.8.
- State the active phase and whether the work crosses into V2/V3 in every final report.
- Do not automatically run `git commit`, `git push`, or upload artifacts.
- Do not commit `experiments/runs/`, `frontend/dist/`, `frontend/node_modules/`, `tsconfig.tsbuildinfo`, caches, `.venv/`, large traces, or runtime logs.

## 10. Required Validation

After code changes, run at least:

```text
python -m pytest backend/tests -q
python -m pytest tests -q
python scripts/v0_sanity.py
cd executor && go test ./...
cd frontend && npm run build
git diff --check
git status --short
```

For Markdown-only changes, run at least:

```text
git diff --check
git status --short
git diff --stat
```

Never report a command as passing when the local environment prevented it from running.

## 11. Final Report Format

Every V1 report must include:

1. Active V1 phase.
2. Modified files.
3. Whether V0 remains intact.
4. Whether the change crossed into V2/V3.
5. Any changed planned/runnable semantics.
6. Validation commands and results.
7. `git diff --check` result.
8. `git status --short`.
9. Whether a git commit was executed.
10. Recommended next phase or next step.
