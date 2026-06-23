---
name: metaverse-chainlab-v0
description: Use this skill when implementing the V0 platform skeleton for the modular metaverse blockchain experiment platform. It restricts scope to the default V0 pipeline and prevents premature implementation of V1/V2 features.
---

# Metaverse ChainLab V0 Skill

## Goal

Implement the V0 platform skeleton closure.

The V0 pipeline is:

Frontend experiment creation
-> Backend config generation
-> Experiment Composer default component completion
-> Synthetic workload generation
-> trace.jsonl.gz writing
-> Go executor replay
-> virtual clock latency accounting
-> metrics output
-> frontend results display

## Required V0 modules

- frontend: Experiments, Composer Preview, Run Console, Results
- backend: FastAPI experiment APIs, composer APIs, process runner, log streaming
- workload: asset_hotspot generator
- trace: gzip JSONL writer and reader
- chain_backend: mockchain
- executor: simple_ordering, single_group, hash_state_sharding, hash_execution_sharding, hash_routing, local_only, serial_execution, normal_commit, virtual_clock, basic_metrics
- configs: v0_default_asset_hotspot.yaml
- tests: V0 sanity test

## Default plugin package

Use exactly these default plugins:

- mockchain
- asset_hotspot
- simple_ordering
- single_group
- hash_state_sharding
- hash_execution_sharding
- hash_routing
- local_only
- disabled_cross_chain
- serial_execution
- normal_commit
- virtual_clock
- basic_metrics
- default_composer

## Implementation rules

1. Do not implement advanced algorithms.
2. Create stable interfaces for future plugins.
3. Use virtual time in replay mode.
4. Use trace.jsonl.gz for formal traces.
5. Keep frontend minimal and functional.
6. Keep black/white/gray UI styling only.
7. Add or update tests when creating core logic.
8. Use Go 1.26.1 for the V0 executor.

## V0 config example

The default config should support:

- tx_count
- zipf_theta
- hot_key_ratio
- cross_shard_ratio
- shard_count
- block_size
- block_interval_ms
- finality_delay_ms
- clock_mode
- output_dir

## Done condition

The task is done only when the V0 pipeline can run from config to summary.csv.
