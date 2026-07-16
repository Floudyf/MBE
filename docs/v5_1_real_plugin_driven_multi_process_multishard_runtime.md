# V5.1 Real Plugin-Driven Multi-Process Multi-Shard Runtime

## 1. Stage Definition

V5.1 builds a runtime where a unified `ExperimentSpec` drives frontend plugin configuration, backend validation and compilation, and real local multi-process multi-shard execution. It is not V5.2 formal experiment closure and it must not fabricate paper results.

## 2. Current Foundation

Current foundations:

- The 11-module Composer exists in `backend/app/services/v3_composer_catalog.py` and `frontend/src/components/experiment/MethodPipelineWorkbench.tsx`.
- Saved method templates already use `V3SavedConfig`.
- Experiment-flow can load saved methods and expand run matrix rows.
- V4 realism packages provide signed tx, mempool, TCP, PBFT-style messages, deterministic execution, state/storage persistence, xshard state machine, and faults.
- `mbe-node --run-mode server` exists.

Current gaps:

- Multiple plugin/config systems coexist.
- V4 realism request does not contain full plugin selection.
- Go runtime has concrete packages but no full all-module plugin factory registry.
- `mbe-supervisor` is not a real cluster orchestrator.
- V4.3 smoke is not one independent OS process per node and not continuous multi-shard execution across all shards.

## 3. Plugin Categories

V5.1 uses these categories:

- workload
- transaction_admission
- txpool
- sharding
- routing
- block_producer
- consensus
- network
- execution
- scheduler
- state_access
- state_storage
- cross_shard
- commit
- fault_injection
- metrics
- observability

The later V5.2 internal block-execution foundation adds an additional
`block_executor` category. It does not replace the existing `execution` category:
`execution` remains the MetaTrack-style transaction classification plugin, while
`block_executor` owns block state execution and receipt generation.

Current 11-module compatibility mapping:

| Current Composer module | V5 category mapping |
| --- | --- |
| Workload | workload |
| TxPool | transaction_admission + txpool |
| BlockProducer | block_producer |
| Consensus | consensus |
| CommitteeEpoch | scheduler |
| Routing | sharding + routing + cross_shard where legacy values embed cross-shard policy |
| Execution | execution + scheduler |
| StateAccess | state_access |
| StateStorage | state_storage |
| Commit | commit |
| MetricsReport | metrics + observability |

This mapping must preserve old templates. Do not rewrite saved method records in place.

## 4. Plugin Manifest Shape

Every plugin manifest must include at least:

- `plugin_id`
- `category`
- `version`
- `display_name`
- `description`
- `implementation_status`
- `supported_backends`: `preview`, `simulation`, `real_cluster`
- `config_schema`
- `default_config`
- `capabilities`
- `requirements`
- `conflicts`
- `metrics`
- `runtime_factory` or `runtime_adapter`
- `truth_boundary`

Adding HotStuff, Raft, Tendermint-like consensus, new sharding/routing, execution, scheduler, state storage, cross-shard protocol, commit policy, workload, fault model, or metrics plugin must follow the same flow:

```text
implement runtime interface
-> register factory
-> add plugin manifest
-> add tests
```

After registration, the plugin should appear in Catalog API, frontend selection, schema form, compatibility validation, saved template support, compiler output, real-cluster loading, and result metric discovery without rewriting core pages or supervisor control flow.

## 5. Frontend Architecture

The frontend must render plugins generically:

```text
Plugin Catalog API
-> Generic Plugin Category Panel
-> Generic Plugin Selector
-> Generic Schema Form Renderer
-> Compatibility Feedback
-> Saved Template / ExperimentSpec
```

Suggested component responsibilities:

- `PluginCategoryPanel`: category-level status, selected plugin, dependency/conflict summary.
- `PluginSelector`: plugin choice list with implementation and backend badges.
- `PluginConfigForm`: schema-driven parameter form.
- `PluginCompatibilityPanel`: validation errors and warnings.
- `PluginCapabilityBadge`: capabilities exposed by the manifest.
- `PluginBackendSupportBadge`: `preview`, `simulation`, `real_cluster` support.
- `PluginMetricPreview`: metric keys and visualization hints.

Core frontend code must avoid large `if (pluginId === "...")` branches. Optional UI extensions are allowed only as enhancements. A plugin must still be configurable through the generic schema form if no UI extension exists.

Before a formal Real Cluster run, every selected plugin must support `real_cluster`.

## 6. Backend Layers

V5.1 must avoid placing all logic into one service file.

Required layers:

1. Plugin Manifest Store: metadata, schema, capabilities, requirements, conflicts, metrics.
2. Plugin Catalog Service: frontend catalog, filtering by category/backend/status, version and truth metadata.
3. Compatibility Engine: parameter validity, dependencies, conflicts, topology/consensus constraints, workload capability, storage proof capability, cross-shard capability, backend support.
4. `ExperimentSpec`: user experiment definition, plugin choices/params, workload, topology, backend, seed, `tx_count`, fault policy, metrics.
5. Experiment Compiler: immutable `CompiledRunPlan`, plugin version/param snapshot, node configs, address/port table, shard/validator/leader assignment, workload plan, fault plan, expected artifacts, resource estimate.
6. Cluster Orchestrator: process lifecycle only; no MetaTrack/PBFT/hash-routing method logic embedded as name checks.
7. Runtime Adapter / Factory Registry: maps compiled plugin choices to Go runtime factories by category and `plugin_id`.

`V3SavedConfig`, existing Composer drafts, and existing method template data must be reused through adapters.

## 7. Go Runtime Plugin Architecture

Windows is the main environment, so V5.1 must not rely on Go `.so` dynamic plugins. Use:

```text
Interface + Factory Registry + Plugin Manifest + Runtime Configuration
```

Target interfaces:

- `WorkloadPlugin`
- `AdmissionPlugin`
- `TxPoolPlugin`
- `ShardingPlugin`
- `RoutingPlugin`
- `BlockProducerPlugin`
- `ConsensusPlugin`
- `NetworkPlugin`
- `ExecutionPlugin`
- `SchedulerPlugin`
- `StateAccessPlugin`
- `StateStoragePlugin`
- `CrossShardPlugin`
- `CommitPlugin`
- `FaultPlugin`
- `MetricsPlugin`
- `ObservabilityPlugin`

Startup flow:

```text
node_config
-> plugin profile
-> registry lookup
-> factory instantiate
-> parameter validation
-> dependency injection
-> runtime start
```

Initialization plugins set up node-local services. Transaction-path plugins cover admission, sharding/routing, txpool, execution, state access, and commit. Block-path plugins cover block production, consensus, execution, state storage, and metrics. Cross-shard plugins participate in SourceLock, RelayCertificate, TargetVerify, TargetCommit, SourceFinalize, Timeout, Refund, and Abort. Shutdown plugins flush metrics, logs, state, receipts, tx index, process manifests, and plugin snapshots.

Every run artifact must include plugin versions and parameter snapshots.

## 8. Real Multi-Process Definition

Formal `real_cluster` requires:

- one logical node equals one independent OS process;
- independent PID;
- independent TCP port;
- independent mempool;
- independent consensus state;
- independent data directory;
- independent state/block/receipt/tx-index;
- independent logs.

The supervisor must:

1. estimate resources;
2. allocate ports;
3. compile node configs;
4. start N `mbe-node` child processes;
5. save process manifest and PIDs;
6. wait for node health;
7. wait for peer network readiness;
8. wait for shard committee readiness;
9. start real `mbe-client`;
10. monitor nodes and workload;
11. execute fault policy;
12. wait for termination conditions;
13. gracefully stop nodes;
14. force-reap abnormal child processes;
15. check orphan processes;
16. collect and validate artifacts.

Formal `real_cluster` must not replace independent nodes with same-process runtime objects. It must not fall back to V4 smoke or simulation after startup failure.

## 9. Continuous Multi-Shard Runtime

V5.1 must not be "single shard commits once plus separate cross-shard smoke".

Target runtime:

- every shard has its own committee;
- every shard has leader and validators;
- every shard has independent mempool;
- every shard continuously produces blocks;
- every shard continuously runs consensus rounds;
- every shard has independent block height;
- every shard has independent state root;
- multiple shards run at the same time.

Unified transaction path:

```text
mbe-client
-> signed transaction
-> ingress node
-> admission
-> sharding/routing
-> shard mempool
-> block producer
-> consensus
-> execution
-> commit
-> state storage
-> receipt
```

Cross-shard path inside the same runtime:

```text
SourceLock
-> RelayCertificate
-> TargetVerify
-> TargetCommit
-> SourceFinalize
```

Failure path:

```text
Timeout
-> Refund / Abort
```

Cross-shard certificates and state messages must travel over the real node network and pass through shard consensus/commit paths.

## 10. Acceptance

Basic acceptance:

- 8 nodes, 2 shards, 4 validators per shard, 100 signed transactions.
- Evidence: 8 PIDs, 8 ports, 8 data directories, both shards running, multiple blocks per shard, real network messages, PBFT-style messages, consistent state roots, receipts and tx index, normal process exit, no orphans.

Plugin-difference acceptance:

- MetaTrack Full
- Hash Serial Baseline
- No Aggregation
- Routing Only

Evidence must show different compiled plugin snapshots, different node loading logs, different routing decisions, different execution paths/tracks, different commit aggregation behavior, and not only method-name changes.

Continuous multi-shard acceptance:

- every shard commits multiple blocks;
- intra-shard and cross-shard transactions coexist;
- block heights keep increasing.

Cross-shard acceptance:

- SourceLock;
- TargetCommit;
- SourceFinalize;
- at least one Timeout / Refund;
- relay certificate transported by real network.

Scale acceptance:

- 16 nodes, 4 shards, 4 validators per shard, 1000+ transactions.
- Proves topology is not hardcoded, processes are stable, no residual processes remain, and all shards actually run.

Extensibility acceptance:

- one test consensus plugin and one test routing plugin enter catalog/config/compile/runtime by registering manifest and factory;
- no frontend core page rewrite;
- no supervisor main-flow rewrite;
- no `ExperimentSpec` main-structure rewrite;
- no compiler main-flow rewrite.

## 11. Non-Goals

V5.1 does not produce final paper comparison results. It does not claim production PBFT, production Byzantine security, Ethereum-compatible MPT, Fabric/EVM live backend, cloud deployment, public-chain compatibility, or performance superiority over BlockEmulator.

## 12. Completion Standard

V5.1 is complete when `real_cluster` satisfies the acceptance criteria with reproducible process, network, block, state, xshard, plugin-snapshot, and cleanup evidence.

## 13. Implementation Evidence

V5.1 is implemented as a local research runtime. The backend exposes `/api/v5/plugins`, `/api/v5/experiment-spec`, and `/api/v5/real-cluster`; the frontend renders V5 categories and schema fields from the catalog rather than plugin-id-specific pages. Method templates remain in `V3SavedConfig` payloads and are adapted at compile time.

The Go supervisor builds temporary node/client binaries under the ignored run directory, allocates ports, records PID/process manifests, starts one `mbe-node --run-mode v5-server` per logical node, waits for readiness, runs a real TCP signed client, waits for clean node shutdown, and writes a `real_cluster_summary.json`. Nodes persist blocks, state, receipts, and tx index files and emit `plugin_snapshot.json`, `plugin_load_log.json`, `routing_decision_log.csv`, `execution_log.csv`, and `commit_log.csv` from runtime decisions.

Verified acceptance commands on Windows:

```powershell
$env:PYTHONPATH = (Get-Location).Path
python scripts/v5_1_real_cluster_acceptance.py --include-16
python scripts/v5_1_plugin_difference_acceptance.py
```

The scale acceptance passed for 8 nodes / 2 shards / 4 validators / 100 signed transactions and 16 nodes / 4 shards / 4 validators / 1000 signed transactions. Both reports recorded distinct PIDs and TCP ports, multiple blocks per shard, consistent roots inside each shard, real client submission, cross-shard success plus Timeout/Refund, `no_fallback=true`, and `orphan_process_count=0`.

The four-method acceptance passed with a common seed/topology/workload: MetaTrack Full used MetaTrack routing + dual-track + aggregation; Hash Serial Baseline used hash + serial + normal commit; No Aggregation retained MetaTrack routing + dual-track with normal commit; Routing Only retained MetaTrack routing with serial + normal commit. The verifier reads node/client artifacts and returns nonzero unless routing assignments, execution tracks, aggregation groups/physical updates, plugin loading, receipts, tx index, state-root consistency, no fallback, and cleanup all satisfy their checks.

This is still not production PBFT, production Byzantine security, production atomic cross-shard commit, a production blockchain, or a V5.2 formal result center.
