# V4 Architecture And Transaction Lifecycle

## 1. Architecture Goal

V4 turns MBE into a real multi-node sharded-chain emulator. The architecture is organized around the transaction lifecycle, not around frontend preview artifacts.

## 2. Final Transaction Lifecycle

```text
RealTrace / LiveClient
  -> SignedTransaction
  -> NodeSubmit
  -> PerNodeMempool
  -> ShardRouter
  -> P2P Tx Gossip
  -> BlockProposer
  -> PBFT-style Consensus
  -> DeterministicExecutor
  -> StateAccess / StateProof / Witness
  -> DurableCommit
  -> CrossShardStateMachine
  -> Receipt / Proof / Metrics / FrontendResult
```

## 3. Module Responsibilities

### 3.1 Client / Trace Input

Responsibilities:

- convert synthetic, historical, or live input into signed transactions;
- preserve original trace metadata;
- assign sender, receiver, nonce, value, state keys, timestamp, payload, and signature;
- report invalid or incomplete imported transactions.

### 3.2 Signed Transaction

A V4 transaction must include at least:

```text
tx_id
sender
receiver
nonce
value
state_keys
payload
signature
timestamp
source_kind
```

### 3.3 Node Submit

The node submit path validates:

- format;
- transaction id;
- signature;
- duplicate tx;
- nonce ordering;
- basic account/state precheck when available.

### 3.4 Per-node Mempool

Each node owns an independent mempool. There is no single global queue.

Mempool responsibilities:

- admission validation;
- deduplication;
- nonce ordering;
- capacity control;
- TTL expiration;
- priority selection;
- mempool wait time metrics;
- gossip status.

### 3.5 Shard Router

The router decides:

- destination shard;
- cross-shard classification;
- state key placement;
- whether MetaTrack co-access routing applies;
- whether a cross-shard state machine is required.

### 3.6 P2P Network

The P2P layer provides:

- long-running node listeners;
- peer table;
- typed message codec;
- tx gossip;
- block broadcast;
- consensus message broadcast;
- cross-shard message transport;
- delay/drop/bandwidth fault injection.

### 3.7 Block Proposer

The shard leader selects transactions from its local shard mempool and builds blocks:

```text
block_height
previous_hash
tx_root
state_root_before
state_root_after
receipt_root
proposer_id
shard_id
timestamp
tx_list
signature
```

### 3.8 PBFT-style Consensus

V4 consensus should first implement a PBFT-style real message runtime:

```text
PrePrepare
Prepare
Commit
ViewChange
NewView
```

The first real implementation must produce prepare/commit quorum from real node messages, not generated summary rows.

### 3.9 Deterministic Execution

After consensus, all honest nodes execute the same committed block and produce matching state roots.

Execution responsibilities:

- account/state transition;
- failed transaction handling;
- receipt generation;
- execution latency metrics;
- deterministic state root.

### 3.10 Durable Commit

Commit writes:

```text
block db
state db
receipt db
tx index
event log
consensus certificate
```

### 3.11 Cross-shard State Machine

Cross-shard transactions follow:

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
-> Refund
-> Abort
```

## 4. Runtime Evidence

Each module must produce evidence that its behavior is real:

- node logs from the owning node;
- network logs from actual send/receive;
- consensus votes from received messages;
- state roots from state transitions;
- commit logs from database writes;
- cross-shard logs from state-machine transitions.

## 5. Compatibility With V3

V3 modules remain as baseline and comparison paths. V4 may reuse V3 profile concepts, but V4 should not depend on V3 preview semantics to claim real runtime behavior.
