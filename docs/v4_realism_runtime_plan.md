# V4 Realism Runtime Plan

## 1. Purpose

V4 opens the MBE Realism Runtime direction. Its purpose is to upgrade MBE from the V3-final local light-runtime and experiment-console baseline into a real multi-node sharded-chain emulator.

V3-final is preserved. V4 does not delete V3. V4 adds a new lower runtime layer that can eventually become the default backend for realism-focused experiments.

## 2. Current Baseline

V3-final provides:

- frontend experiment console;
- formal benchmark workflow;
- saved configuration workflow;
- metaverse workload suite;
- local modular runtime;
- local multi-process smoke/dry-run capability;
- NetworkAdapter typed-message preview;
- BlockEmulator-aligned PBFT preview;
- Relay MVP;
- State Authenticity MVP;
- deterministic fault-observation MVP;
- reproducibility artifacts.

V3-final remains valuable, but it is not a real multi-node sharded-chain emulator. Its truth boundary remains: local emulator prototype, not production PBFT, not production networking, not production state durability, not BlockEmulator replacement.

## 3. Why V4 Is Needed

To exceed BlockEmulator-style realism, MBE must replace preview/MVP layers with real runtime layers:

```text
experiment input -> real signed transactions
logical process preview -> long-running node processes
network preview -> real local P2P message loop
PBFT preview -> PBFT-style real message consensus
summary state -> persistent state/block/receipt commit
relay MVP summary -> cross-shard state machine with real state transitions
```

## 4. V4 Target

The final V4 target is:

```text
real trace / live client
-> signed tx
-> node submit
-> per-node mempool
-> shard router
-> P2P gossip
-> block proposer
-> PBFT-style consensus
-> deterministic execution
-> persistent state/block/receipt commit
-> cross-shard SourceLock / RelayCert / TargetCommit / Finalize / Refund
-> metrics / receipts / proofs / frontend result
```

## 5. Four-round Route

### Round 1: V4 docs and skill reset

Only documentation and skill updates. No runtime code.

Outputs:

- updated README status wording;
- updated global `AGENTS.md`;
- V3 skill frozen notice;
- new V4 skill;
- V4 roadmap and acceptance docs.

### Round 2: V4.0 Real Node Foundation

Target:

```text
real trace / client -> signed tx -> node submit -> per-node mempool
```

Main outputs:

- `mbe-node` skeleton;
- `mbe-client` skeleton;
- signed transaction model;
- account/nonce model;
- per-node mempool;
- admission logs;
- signed tx import logs.

### Round 3: V4.1 Network / Consensus / Commit

Target:

```text
mempool -> P2P -> block proposer -> PBFT-style consensus -> committed block
```

Main outputs:

- real local P2P message loop;
- peer table;
- message codec/router;
- tx gossip;
- block proposer;
- PBFT-style PrePrepare / Prepare / Commit;
- basic ViewChange / NewView;
- block commit logs.

### Round 4: V4.2 State / Cross-shard / Recovery

Target:

```text
committed block -> execution -> state db -> cross-shard state machine -> recovery/faults -> frontend Realism Mode
```

Main outputs:

- deterministic executor;
- state db, block db, receipt db, tx index;
- state roots and consistency checks;
- proof/witness MVP;
- SourceLock / RelayCert / TargetCommit / Finalize / Refund state machine;
- node recovery;
- fault injection;
- BlockEmulator trace/config/result bridge;
- backend/frontend Realism Mode integration.

## 6. Directory Plan

Recommended new structure:

```text
executor/
  v3runtime/
  cmd/
    mbe-node/
    mbe-client/
    mbe-supervisor/
  realism/
    tx/
    account/
    mempool/
    p2p/
    node/
    router/
    block/
    consensus/
      pbft/
      hotstuff/
    execution/
    state/
    storage/
    xshard/
    metrics/
    faults/
    recovery/
    config/
```

`executor/v3runtime/` remains the stable baseline. `executor/realism/` is the new V4 path.

## 7. Integration Strategy

V4 must be additive first:

1. New V4 directories and commands.
2. V4 standalone smoke tests.
3. Backend API integration after the standalone runtime works.
4. Frontend Realism Mode after backend integration.
5. V3 remains runnable throughout.

## 8. Truth Boundary

Before V4.0 code exists, V4 is planning only.

Before V4.1, MBE must not claim real P2P or real PBFT.

Before V4.2, MBE must not claim persistent state commit or real cross-shard commit.

Before direct comparison artifacts exist, MBE must not claim measured superiority over BlockEmulator.

## 9. Current Implementation Status

V4.0, V4.1, and V4.2 are implemented and verified in the current repository.

Verified V4.2 chain segment:

```text
signed tx
-> per-node mempool
-> real localhost TCP P2P
-> PBFT-style quorum commit
-> deterministic execution
-> persistent state root
-> durable block/receipt/tx-index commit
-> cross-shard state machine
-> recovery/fault evidence
-> backend API and frontend Realism Mode
```

Current runtime truth:

```text
v4_real_state_cross_shard_recovery
```

Allowed description:

```text
MBE V4 provides a research-grade real multi-node sharded blockchain emulator.
```

Non-claims remain in force: V4 is not a production blockchain, not production PBFT, not full Byzantine security, not an industrial-grade chain, and not complete Ethereum/Fabric compatibility.
