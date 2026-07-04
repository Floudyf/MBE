# V3-final Artifact Catalog

## Core Groups

- Core Runtime: `summary.json`, `summary.csv`, `runtime.log`, `block_log.csv`, `tx_results.csv`.
- Network / PBFT Preview: typed message, TCP preview, consensus network, and PBFT preview artifacts.
- Relay MVP: source lock, relay certificate, proof verification, target commit, timeout/refund, and Relay MVP summary artifacts.
- State Authenticity: state root, proof, witness, verification, and authenticity summary artifacts.
- Local Multi-process Runtime: address table, manifest, process logs, lifecycle logs, network message logs, process status, and local summary artifacts.
- Committee / Epoch: shard assignment, committee assignment, epoch log, reconfiguration plan, reshard log, and reconfiguration summary artifacts.
- Metaverse Workload: workload config, trace metadata, scenario summary, hotspot, cross-scene, offchain, cross-metaverse, and metaverse summary artifacts.
- Benchmark Matrix: benchmark catalog, baseline catalog, plan, sweep, aggregate, baseline comparison, report, and summary artifacts.
- Fault Injection: `fault_injection_config.json`, `fault_injection_log.csv`, `node_failure_log.csv`, `node_recovery_log.csv`, `network_fault_log.csv`, `target_congestion_log.csv`, `relay_fault_observation_log.csv`, `fault_injection_summary.json`.
- Observability: `observability_summary.json`, `observability_timeline.csv`, `component_health_summary.csv`, `runtime_component_status.json`.
- Reproducibility: `final_artifact_catalog.json`, `final_artifact_catalog.md`, `v3_final_reproducibility_manifest.json`, `v3_reproducibility_guide.md`, `v3_experiment_manual.md`, `v3_paper_experiment_mapping.md`, `v3_final_summary.json`.

## Truth Boundary

Catalog entries explain local artifact purpose and truth boundary. They do not transform local emulator outputs into production monitoring, production consensus evidence, or paper-grade performance conclusions.
