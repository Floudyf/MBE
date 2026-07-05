# V3 Maintenance: Saved Config Workflow

## Why Add A Saved Config Library

The V3 console is no longer only a one-shot experiment form. The maintenance workflow is:

```text
configure -> validate -> Draft Smoke -> save and name -> reuse -> compare combinations -> formal run
```

Saved configs make repeated MetaTrack experiments usable by a research group: a validated method, workload, or topology can be named once and reused later as a formal benchmark baseline or main方案.

## Config Object Kinds

Saved configs are stored as JSON files under `.cache/v3_saved_configs/` with safe IDs of the form `v3cfg_<timestamp>_<short_hash>`.

- `module`: a single module choice such as `Routing=metatrack_coaccess_routing`.
- `workload`: workload source and parameters such as `scene_hotspot` or synthetic hotspot settings.
- `topology`: runtime topology such as local TCP preview, Relay MVP, Merkle Trie MVP, shard count, and committee/epoch settings.
- `method`: the full 11-module composer draft plus topology, workload source, validation state, and last smoke run ID.
- `formal_plan`: a formal experiment plan containing methods, workloads, topologies, seeds, transaction counts, and scan settings.

Every config carries `truth_boundary = local_emulator_config_not_production_chain`.

## Recommended Workflow

1. Configure workload and topology.
2. Configure the 11 module cards.
3. Validate the current draft.
4. Run Draft Smoke for quick verification.
5. Save the full method, workload, or topology with a name.
6. Use saved methods/workloads/topologies in the formal MetaTrack benchmark panel.
7. Preview the matrix before running.
8. Run the controlled formal benchmark and inspect manifest/progress/failed run index artifacts.

Draft Smoke remains a quick validation path. It is not paper-final evidence.

## Workload Comparison

Formal benchmarks now support `workload_comparison`.

Default scenarios:

- `scene_hotspot`
- `cross_scene_migration`
- `mixed_metaverse`

The matrix uses:

```text
method_count x workload_scenario_count x topology_count x seed_count
```

Each child run writes `workload_source = metaverse` and sets `metaverse_scenario` to the current scenario. `formal_workload_comparison.csv`, `formal_run_matrix.csv`, `formal_raw_summary.csv`, `formal_aggregate_summary.csv`, and `formal_paper_figure_data.csv` include workload fields.

## Inheriting Topology And Workload Details

Formal profiles inherit the user draft or saved topology details, including local multi-process mode, local TCP preview, Relay MVP, Merkle Trie MVP, committee/epoch settings, shard counts, metaverse workload fields, offchain confirmation fields, fault settings, observability, reproducibility, and paper mapping flags.

Scan variables may override one field:

- `hotspot_sensitivity`: overrides `hotspot_ratio`
- `cross_shard_sensitivity`: overrides `cross_shard_ratio`
- `shard_scalability`: overrides `shard_count`
- `workload_comparison`: overrides `metaverse_scenario`

Other user settings should remain visible in `generated_experiment_profile.json/yaml`.

## Run Diagnostics

Formal runs now write:

- `formal_run_manifest.json`
- `formal_progress.json`
- `formal_failed_runs.csv`
- `formal_child_artifact_index.csv`

The child artifact index records child output directories and whether key child artifacts exist.

## Truth Boundary

This is still V3 maintenance after V3-final closure. It does not start V4. It does not connect Fabric/EVM live backend, does not use BlockEmulator backend, does not implement production PBFT/HotStuff/Raft, and does not provide production multi-server deployment.

`local_multi_process` remains local-machine validation only. `localhost_tcp_preview` remains a local typed-message path preview. `relay_mvp` and `merkle_trie_mvp` are local MVP artifacts, not production protocols. Existing trace input remains preview-only and is not admitted into the default formal benchmark path.
