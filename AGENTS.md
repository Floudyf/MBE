# AGENTS.md

## Project

This repository implements a modular metaverse blockchain experiment platform.

The current implementation target is V0: platform skeleton closure.

V0 means:
- Basic frontend.
- FastAPI backend.
- Experiment composer.
- Default plugin package.
- Synthetic workload.
- MockChain.
- trace.jsonl.gz pipeline.
- Go executor.
- Virtual clock.
- Basic metrics.
- CI sanity test.

Do not implement V1/V2/V3 features unless explicitly requested.

## Runtime and language versions

V0 must use these versions unless the user explicitly approves a change:

- Python: 3.12.x
- Go: 1.24.x
- Node.js: 22 LTS
- React: 18.x
- TypeScript: 5.x
- FastAPI: 0.115.x
- Uvicorn: 0.30.x

Keep dependency manifests and local development tooling aligned with these
constraints when the corresponding V0 module is introduced.

## V0 default plugins

Use only these default plugins in V0:

- chain_backend: mockchain
- workload: asset_hotspot
- trace: jsonl_gzip
- consensus_protocol: simple_ordering
- consensus_sharding: single_group
- state_sharding: hash_state_sharding
- execution_sharding: hash_execution_sharding
- routing: hash_routing
- cross_shard_protocol: local_only
- cross_chain_protocol: disabled
- execution: serial_execution
- commit: normal_commit
- clock: virtual_clock
- metrics: basic_metrics
- composer: default_composer
- frontend_template: default_single_chain_experiment

## V0 forbidden scope

Do not implement these in V0:

- PBFT
- HotStuff
- DAG consensus
- Fabric network
- EVM contracts
- co-access routing
- dual-track execution
- hot update aggregation
- Block-STM-like baseline
- Calvin-like baseline
- MetaFlow
- committee bridge
- Pending Pool
- Grafana dashboards
- multi-user permission system
- distributed deployment

Only create placeholders or interfaces for future V1/V2 modules when needed.

## Timing rules

In executor replay mode:
- Never use time.Sleep to simulate network latency, remote fetch latency, execution delay, commit delay, or finality delay.
- Use virtual time and event timestamps.
- Separate virtual latency from wall-clock runtime.

## Trace rules

Formal V0 experiments use trace.jsonl.gz.
Small readable examples may use trace.jsonl.
Use streaming read/write.
Do not load the entire trace file into memory.

## Coding style

- Prefer small, testable modules.
- Keep interfaces stable.
- Use config-driven plugin loading.
- Every public config field must be documented.
- Every plugin must have a plugin.yaml declaration.
- Update README or docs when changing config, schema, or plugin behavior.

## Required V0 outputs

A successful V0 experiment must output:

- config.yaml
- trace.jsonl.gz
- trace_meta.json
- summary.csv
- latency.csv
- runtime.log

## V0 acceptance criteria

The user can:
1. Open the basic frontend.
2. Create a default single-chain experiment.
3. Preview the default component composition.
4. Start the experiment.
5. See runtime logs.
6. Wait for executor replay to complete.
7. View TPS, average latency, P95, P99, success count, and failure count.
8. Download config.yaml, summary.csv, latency.csv, and runtime.log.

## Test command

When modifying executor, trace, plugin, or composer logic, run the V0 sanity test before finishing.
