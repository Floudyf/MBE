# V4 BlockEmulator Gap And Surpass Plan

## 1. Purpose

BlockEmulator is the realism baseline for MBE V4. V4 should not copy BlockEmulator code, but it should study its runtime design and exceed its realism for MBE's research goals.

## 2. BlockEmulator Baseline

BlockEmulator's public documentation describes it as a blockchain testbed for verifying new protocols and mechanisms. It implements core blockchain functions such as transaction pool, block packaging, consensus protocols, and on-chain transaction storage, and supports PBFT as well as cross-shard mechanisms such as Relay and BrokerChain.

Its repository layout includes modules for broker, chain, PBFT consensus, message, networks, partition, shard, storage, and supervisor.

Its PBFT implementation uses node-level PBFT state, request pools, prepare/commit confirmation maps, TCP listener, chain database, and view-change maps.

## 3. MBE V3 Gap

MBE V3-final currently provides:

- V3 light runtime;
- local process smoke/dry-run;
- localhost TCP typed-message preview;
- PBFT preview;
- Relay MVP;
- State Authenticity MVP;
- observability and reproducibility artifacts.

Gap:

```text
MBE V3 = preview/MVP/local evidence
BlockEmulator = real emulator node/shard/PBFT/storage baseline
```

Therefore V4 must focus on lower runtime realism, not additional frontend controls.

## 4. Alignment Targets

V4 must align with or exceed these BlockEmulator-style capabilities:

| Capability | V4 target |
| --- | --- |
| Transaction pool | independent per-node mempool with admission logs |
| Block packaging | leader packs real block from mempool |
| Node runtime | long-running node process with address table |
| Network | real local P2P send/receive with fault injection |
| PBFT | real PrePrepare/Prepare/Commit messages and ViewChange |
| Chain storage | block db and commit log |
| State storage | persistent state db and state root |
| Cross-shard | SourceLock / RelayCert / TargetCommit / Finalize / Refund |
| Historical trace | BlockEmulator/Ethereum trace import bridge |
| Experiment logs | richer runtime evidence and frontend result display |

## 5. Surpass Targets

V4 should surpass BlockEmulator in these MBE-specific dimensions:

1. Metaverse workload catalog and scenario templates.
2. MetaTrack-aware state/routing mechanism integration.
3. Signed transaction admission evidence.
4. Rich per-node mempool metrics.
5. Network delay/drop/bandwidth fault injection.
6. PBFT quorum and view-change latency breakdown.
7. State root consistency reports across honest nodes.
8. State proof / witness MVP for stateless sharding experiments.
9. Node recovery evidence.
10. Frontend Realism Mode with artifact explanations.
11. Formal experiment matrix and reproducibility bundles.
12. Import/export bridge for direct BlockEmulator comparisons.

## 6. What Not To Claim Too Early

Do not claim:

- production PBFT;
- complete Byzantine security;
- Ethereum-compatible MPT before full implementation;
- public-chain compatible execution semantics;
- cloud-scale distributed deployment;
- paper-final superiority over BlockEmulator.

## 7. Direct Comparison Plan

After V4.2, add a comparison bridge that can:

- import BlockEmulator selected transactions;
- map BlockEmulator config to MBE V4 config where possible;
- run MBE V4 with comparable shard/node/block settings;
- export MBE results in a table compatible with BlockEmulator metrics;
- compare throughput, confirmation latency, mempool waiting, network message count, consensus latency, commit latency, and cross-shard finality latency.

## 8. Success Definition

MBE can be described as a realism candidate beyond BlockEmulator only when:

```text
real signed tx
+ per-node mempool
+ real P2P
+ PBFT-style real message runtime
+ block commit
+ persistent state and receipts
+ cross-shard state machine
+ recovery/fault evidence
+ BlockEmulator comparison bridge
```

are implemented and validated.

## 9. V4.2 Verified Alignment

The current V4.2 implementation matches BlockEmulator's core runtime realism targets in the research-emulator sense: real signed transaction admission, independent per-node mempools, localhost TCP node messaging, PBFT-style real PrePrepare / Prepare / Commit messages, quorum block commit, durable block artifacts, and runtime logs.

V4.2 extends that baseline with deterministic execution, persistent state root generation, receipt and tx-index storage, state-root consistency reports across honest nodes, recovery evidence, fault-injection logs, frontend Realism Mode, and a BlockEmulator bridge MVP for comparison-oriented artifacts.

Truth boundary:

- `blockemulator_bridge_mvp=true`
- `full_blockemulator_compatibility=false`
- `research_grade_real_emulator=true`
- `production_blockchain=false`
- `production_pbft=false`
- `full_byzantine_security=false`

MBE V4 can be described as matching BlockEmulator-style core runtime realism and extending it with state root, receipt/tx index, cross-shard state machine, recovery/fault injection, frontend Realism Mode, and metaverse-oriented experiment control. It must not be described as fully replacing BlockEmulator in every scenario without controlled comparison experiments.

## 10. V4.3 Closure Evidence

V4.3 strengthens the BlockEmulator-surpass evidence chain:

- signed transaction authenticity now includes sender/public-key binding;
- cross-shard relay certificates are transmitted over real localhost TCP P2P;
- P2P fault injection can actually delay/drop messages and logs those events;
- BlockEmulator selectedTxs-style CSV input is converted into verifiable MBE signed transaction JSONL;
- backend/frontend V4.3 controls expose nodes, shards, tx count, cross-shard, faults, fault profile, and bridge parameters.

The allowed claim is that MBE V4.3 matches BlockEmulator core emulator realism and surpasses it in evidence chain, frontend-controlled realism, state/receipt/tx-index observability, real cross-shard network commit MVP, real P2P fault injection, and BlockEmulator trace-to-signed-tx bridge.

Non-claims remain:

- `production_pbft=false`
- `full_byzantine_security=false`
- `production_blockchain=false`
- `production_atomic_commit=false`
- `full_blockemulator_compatibility=false`
