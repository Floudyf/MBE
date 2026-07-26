# V5 Core Hardening Stage Plan

Status: implementation stage for V5.2 internal hardening. This is not V5.3,
V5.2.1, V6, or a new workload/algorithm stage.

## Stage Boundary

This round hardens the existing V5 real-cluster runtime around persistence,
MetaTrack mechanism evidence, Block-STM execution boundaries, and runtime plugin
wiring. It must preserve the Decentraland canonical workload contract and use
the already integrated dataset path when running real workload acceptance:

- `dataset_id`: `dcl_sales_polygon_271868`
- workload plugin: `canonical_trace_replay`
- variant: `contract_zipf`

The round does not add workloads, CG, ACG, BSX, Batch-SI, V6 features, or
production-chain claims. MetaTrack and Block-STM remain independent methods.
`MetaTrack + Block-STM` is a compatibility/combined run configuration only.

## Source Audit Table

| category | current plugin | real runtime call site | current gap | required change |
| --- | --- | --- | --- | --- |
| transaction_admission | `signature_nonce_admission` | client-side signing and node gossip/relay admission | implemented: node ingress now calls `Admission.Admit` before mempool admission | extend event aggregation for rejected/admitted counts |
| txpool | `fifo_per_node_mempool` | `TxPoolPlugin.CreatePool` creates the real mempool; runtime calls the resulting pool operations | implemented: creation and capacity are plugin-owned; operations remain the real mempool object | optional future adapter can hide every method call behind the interface |
| sharding | `deterministic_state_key_sharding` | canonical replay, client planning, routing, runtime remote fetch/writeback | implemented: home-shard ownership flows through one injected `ShardingPlugin` instance | keep remaining direct `stableKey` use limited to default plugin and deterministic tie-breakers |
| routing | `hash_routing_baseline`, `metatrack_coaccess_routing` | client batch planning and runtime proposal | implemented: routing inputs carry the sharding resolver and batch plans record `sharding_plugin_id` | include resolver config/version in richer artifacts if non-default sharding plugins are added |
| block_producer | `time_or_count_block_producer` | runtime proposal loop calls `ShouldProduce` and `BuildCandidate`; loop interval comes from plugin | implemented for block size/interval/readiness/candidate construction | richer timed production policy remains future work |
| consensus | `pbft_style_consensus` | quorum threshold and proposal timeout path | implemented: quorum, timeout, proposal policy, and vote policy are plugin-owned metadata/behavior | still research-grade PBFT-style, not production PBFT |
| network | `localhost_tcp_typed_network` | `NetworkPlugin.CreateTransport` creates the real TCP transport | implemented: runtime no longer directly constructs transport | transport operations are still the concrete p2p transport object |
| execution | `serial_execution_baseline`, `dual_track_execution` | scheduler classification | implemented: batch classifier records dependency reasons and allows independent ordinary writes in Fast Track | complete consolidated metric reporting |
| scheduler | `fifo_serial_scheduler`, `fast_first_scheduler` | proposal ordering and scheduler artifacts | implemented: deterministic ready/blocked/wakeup queues | extend SCC/cycle evidence and report aggregation |
| block_executor | `serial_block_executor`, `block_stm_block_executor` | `NodeRuntime.commitOnce` | implemented: correctness/performance oracle split and validated-write materialization without hidden full-block replay | complete continuously interleaved OCC worker loop and sampled oracle |
| state_access | `direct_state_access` | MetaTrack remote fetch and remote delta apply helpers call `StateAccessPlugin.BuildFetchRequest` / `BuildDeltaApplyRequest` | implemented: request construction and witness/delta metadata are plugin-owned; transport remains runtime/network-owned | full local read adapter and richer access metrics remain future hardening |
| state_storage | `persistent_local_state_store` | `StateStoragePlugin.Open`, `Snapshot`, `Root`, `ApplyBatch`, `Checkpoint`, `Rollback`, `PersistDelta`, and `SnapshotIfDue` wrap StateDB/BlockStore commit paths | implemented: creation/recovery, WAL append, deterministic apply, checkpoint, rollback, root, and snapshot cadence are plugin-owned entry points | full close/lifecycle adapter remains future hardening |
| cross_shard | `relay_certificate_protocol` | runtime relay/finalize handlers call `SourceLock`, `TargetCommit`, `HandleFinalize`, `TimeoutRefund`, `BuildRelay`, and `BuildFinalize` | implemented: protocol event/message entry points are plugin-owned while consensus delivery remains runtime-owned | recovery and richer timeout state are still current runtime code |
| commit | `normal_commit`, `commutative_hot_update_aggregation` | `Commit.DecideCommit` | implemented: safe commutative updates produce reduced physical deltas with witnesses | expand report-level physical-write-reduction summaries |
| fault_injection | `network_delay_drop` | `FaultPlugin.Policy` configures transport fault policy | implemented: artifact records selected plugin and applied policy | advanced fault plans remain bounded to current p2p policy |
| metrics | `runtime_core_metrics` | RuntimeEvent stream consumed into `runtime_metrics.json` | implemented: plugin consumes events and counts core event metrics | report-level aggregation still required |
| observability | `node_network_consensus_observer` | RuntimeEvent stream written to `runtime_events.csv` | implemented: `Observe` returns trace rows and is no longer empty | frontend surfacing remains future work |

## Implementation Order

1. Persistence: batch receipt/index writes, WAL, snapshot cadence, recovery, runtime roots, and persistence metrics.
2. Sharding ownership: remove duplicated home-shard calculations from client, runtime, and MetaTrack routing paths.
3. MetaTrack: batch classification, deterministic dependency queues, cost-aware placement, real aggregation, and mechanism metrics.
4. Block-STM: correctness/performance mode split, no hidden serial oracle in performance, no full-block business reexecution, incarnation limits, and business execution invocation evidence.
5. Plugin wiring: admission, txpool, producer, consensus, network, state access/storage, cross-shard, fault, metrics, observability.
6. Method templates/frontend: Baseline, MetaTrack, Block-STM, and compatibility Combined wording.
7. Validation: Go, race in WSL2 when toolchain permits, backend, frontend, Playwright, and real-cluster 100/1K/10K.
