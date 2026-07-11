# V5.1 Implementation And Acceptance Report

## Scope

V5.1 implements the local `real_cluster` runtime only. It does not implement V5.2 run groups, formal result closure, paper figures, or a production blockchain.

## Implemented Path

```text
Plugin Catalog -> generic React schema form -> ExperimentSpec -> compatibility validation
-> CompiledRunPlan -> Go supervisor -> independent mbe-node processes
-> real TCP mbe-client -> PBFT-style commit -> persistent artifacts
```

The catalog covers workload, transaction admission, txpool, sharding, routing, block producer, consensus, network, execution, scheduler, state access/storage, cross-shard, commit, fault injection, metrics, and observability. `V3SavedConfig` remains the only saved method-template store.

## Acceptance Results

- Backend: `tests` 24 passed; `backend/tests` 385 passed.
- Go: `go test ./...` passed.
- Frontend: `npm.cmd run build` passed; Playwright 9/9 passed, including the V5 catalog page.
- V0 regression: `scripts/v0_sanity.py` passed.
- 8-node: 2 shards, 4 validators per shard, 100 signed transactions passed.
- 16-node: 4 shards, 4 validators per shard, 1000 signed transactions passed.
- Both scale runs reported unique PIDs/ports/data directories, multiple committed blocks per shard, in-shard state-root consistency, real TCP client submission, cross-shard success plus refund evidence, `no_fallback=true`, and `orphan_process_count=0`.

## Four-Method Evidence

`scripts/v5_1_plugin_difference_acceptance.py` passed with a shared 8-node/2-shard topology and seed 53. It runs `metatrack_full`, `hash_serial_baseline`, `no_aggregation`, and `routing_only` in independent real clusters. It consumes runtime-generated `plugin_snapshot.json`, `plugin_load_log.json`, `routing_decision_log.csv`, `execution_log.csv`, `commit_log.csv`, receipts, tx index, and cluster summaries.

- MetaTrack Full: MetaTrack co-access routing, dual-track execution, aggregation commit.
- Hash Serial Baseline: hash routing, serial execution, normal commit.
- No Aggregation: MetaTrack co-access routing, dual-track execution, normal commit.
- Routing Only: MetaTrack co-access routing, serial execution, normal commit.

The verifier rejects a run unless routing assignments differ between hash and MetaTrack, dual-track fast counts differ from serial runs, aggregation groups and physical/logical update counts differ from normal commit, and all correctness/cleanup conditions pass.

## Truth Boundary

`v5_real_cluster_candidate` is a local research runtime with independent local OS processes. It is not production PBFT, full Byzantine security, a production blockchain, production atomic cross-shard commit, multi-server deployment, or V5.2 result/paper closure.
