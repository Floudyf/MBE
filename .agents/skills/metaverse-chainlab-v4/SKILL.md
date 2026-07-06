# metaverse-chainlab-v4

## 0. Purpose

This skill governs V4 work for MBE: MBE Realism Runtime.

V4 upgrades MBE from a V3-final local light-runtime / experiment-console baseline into a real multi-node sharded-chain emulator. The goal is to meet and exceed BlockEmulator-style runtime realism while preserving MBE's strengths: metaverse workloads, MetaTrack-aware routing, formal experiment control, frontend usability, artifact organization, and reproducibility.

V4 must be built deliberately. Planning comes before implementation. Do not jump into code without a stage document and acceptance checklist.

## 1. Starting Rule

Every V4 round must start with:

```powershell
cd F:\Metaverse_Blockchain_Env
git status --short
```

If the worktree is dirty, stop and report unless the user explicitly identifies the existing changes as expected and authorizes continuing on top of them.

Every V4 round must also inspect:

- `README.md`
- `AGENTS.md`
- `.agents/skills/metaverse-chainlab-v3/SKILL.md`
- `.agents/skills/metaverse-chainlab-v4/SKILL.md`
- `docs/v4_realism_runtime_plan.md`
- the current stage doc, such as `docs/v4_0_real_node_foundation.md`

## 2. Stable Baseline

V3-final remains the stable baseline. V3 is not deleted or rewritten.

```text
V3-final = light runtime / experiment console / reproducibility baseline
V4 = real node / real network / real consensus / real state / real cross-shard runtime
```

V4 may reuse V3 configuration, frontend, metrics, artifact grouping, workload catalog, and profile concepts, but V4 must not mutate preview/MVP behavior into overclaimed real runtime behavior.

## 3. Four-round V4 Plan

V4 is developed in four large bounded rounds.

### Round 1: V4 docs and skill reset

Allowed:

- update README status wording;
- replace stale `AGENTS.md` with current global rules;
- freeze V3 skill as maintenance baseline;
- add V4 skill;
- add V4 roadmap, architecture, stage, gap, migration, and acceptance docs.

Forbidden:

- backend code changes;
- frontend code changes;
- Go runtime changes;
- new executable V4 paths;
- claiming V4 is runnable.

### Round 2: V4.0 Real Node Foundation

Target chain segment:

```text
real trace / client -> signed tx -> node submit -> per-node mempool
```

Allowed:

- `executor/cmd/mbe-node/`
- `executor/cmd/mbe-client/`
- `executor/cmd/mbe-supervisor/` skeleton if needed
- `executor/realism/tx/`
- `executor/realism/account/`
- `executor/realism/mempool/`
- `executor/realism/node/`
- `executor/realism/config/`
- `executor/realism/metrics/`
- signed transaction model;
- account/nonce model;
- duplicate transaction check;
- signature verification;
- per-node mempool with capacity, TTL, priority, and wait metrics;
- real trace import as signed tx input;
- node mempool logs.

Forbidden in V4.0:

- PBFT implementation;
- real P2P gossip;
- block proposer;
- state db commit;
- cross-shard protocol;
- frontend Realism Mode;
- production blockchain claims.

### Round 3: V4.1 Network / Consensus / Commit

Target chain segment:

```text
node mempool -> real P2P -> block proposal -> PBFT-style consensus -> committed block
```

Allowed:

- long-running node server;
- real TCP or gRPC local P2P message loop;
- peer address table;
- message codec and router;
- transaction gossip;
- block proposal broadcast;
- shard leader block proposer;
- PBFT-style PrePrepare / Prepare / Commit;
- basic ViewChange / NewView;
- 2f+1 quorum checks;
- block db MVP;
- consensus certificate MVP;
- network, consensus, quorum, and block commit logs.

Forbidden in V4.1:

- production PBFT claims;
- full Byzantine security claims;
- full checkpoint/stable-log PBFT unless explicitly scoped;
- full MPT;
- cross-shard commit;
- large-scale cloud deployment;
- paper-final performance claims.

### Round 4: V4.2 State / Cross-shard / Recovery

Target chain segment:

```text
committed block -> deterministic execution -> persistent state -> cross-shard state machine -> recovery/fault validation -> frontend Realism Mode
```

Allowed:

- deterministic execution;
- account/state transition;
- persistent block db / state db / receipt db / tx index;
- state root generation;
- Merkle proof / witness MVP;
- state root consistency checks across honest nodes;
- node crash/restart recovery;
- network delay/drop/bandwidth fault injection;
- cross-shard SourceLock / RelayCertificate / TargetVerify / TargetCommit / SourceFinalize;
- Timeout / Refund / Abort paths;
- Relay / Broker / MetaTrack-aware routing comparison hooks;
- BlockEmulator trace/config/result bridge;
- backend `/api/v4/realism/*`;
- frontend Realism Mode.

Forbidden in V4.2 unless explicitly reopened:

- claiming Ethereum-compatible MPT without full encoding/path/RLP tests;
- claiming production bridge security;
- claiming cloud-scale deployment;
- claiming paper-final superiority without controlled comparisons.

## 4. V4 Target Architecture

Recommended new directories:

```text
executor/
  v3runtime/              # keep as stable light-runtime baseline
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

Do not move V3 code unless the user explicitly asks for refactoring. V4 should be additive first, then integration can follow after validation.

## 5. Target Transaction Lifecycle

V4 final target:

```text
real trace / live client
-> signed transaction
-> node RPC submit
-> per-node mempool admission
-> shard routing
-> P2P tx gossip
-> shard leader block proposal
-> PBFT-style PrePrepare / Prepare / Commit / ViewChange
-> deterministic execution
-> state db / state root / receipt
-> durable block/state/receipt/tx-index commit
-> cross-shard SourceLock / RelayCertificate / TargetCommit / SourceFinalize or Refund
-> metrics / proof / receipt / frontend result
```

Every V4 stage must clearly state which segment of this lifecycle becomes real and which segments remain planned.

## 6. BlockEmulator Alignment And Surpass Principle

BlockEmulator is the realism baseline. V4 should study its concepts but not copy its code.

Minimum alignment targets:

- transaction pool;
- block packaging;
- PBFT-style consensus;
- TCP/gRPC node communication;
- node/shard/supervisor roles;
- chain/block storage;
- cross-shard Relay/Broker-style mechanisms;
- historical transaction replay;
- experiment logs and metrics.

Surpass targets:

- signed transaction and admission evidence;
- per-node mempool observability;
- stronger network fault injection;
- richer PBFT quorum and view-change breakdown;
- deterministic state-root consistency reports;
- state proof / witness MVP;
- node recovery evidence;
- MetaTrack-aware routing and metaverse workload support;
- frontend Realism Mode and reproducibility bundles;
- BlockEmulator trace/config/result bridge for direct comparison.

## 7. Consensus Naming Rules

Use careful names:

```text
V3 pbft_light_model / pbft_preview = preview/light model only
V4 PBFT-style real message runtime = real node messages and quorum commit, not production PBFT
V4 PBFT-conformant emulator = only after authentication, view-change proof, checkpoint/stable log, recovery, and Byzantine tests are implemented
```

Do not claim production PBFT unless production-grade PBFT semantics, authentication, recovery, and adversarial tests are implemented and documented.

## 8. Truth Labels

Use V4 truth labels consistently:

```text
v4_planning_only
v4_real_node_foundation
v4_real_p2p_consensus_commit
v4_real_state_cross_shard_recovery
v4_realism_runtime_candidate
```

A capability becomes runnable only when a test or smoke command exercises the real code path.

## 9. Artifact Rules

V4 artifacts must be generated by real runtime behavior, not fabricated preview rows.

Expected V4.0 artifacts:

```text
signed_tx_import_log.csv
node_mempool_log.csv
node_admission_log.csv
nonce_validation_log.csv
signature_validation_log.csv
v4_node_foundation_summary.json
```

Expected V4.1 artifacts:

```text
v4_network_send_log.csv
v4_network_receive_log.csv
v4_message_trace.csv
v4_block_proposal_log.csv
v4_pbft_message_log.csv
v4_quorum_log.csv
v4_view_change_log.csv
v4_block_commit_log.csv
v4_consensus_summary.json
```

Expected V4.2 artifacts:

```text
v4_execution_log.csv
v4_state_update_log.csv
v4_state_root_log.csv
v4_receipt_log.csv
v4_state_consistency_log.csv
v4_recovery_log.csv
v4_cross_shard_state_machine_log.csv
v4_relay_certificate_log.csv
v4_refund_abort_log.csv
v4_realism_summary.json
```

## 10. Validation Commands

Docs/config-only:

```powershell
git diff --check
git status --short
```

Go modified:

```powershell
cd executor
go test ./...
cd ..
```

Python/backend modified:

```powershell
$env:PYTHONPATH = (Get-Location).Path
python -m pytest tests -q
python -m pytest backend/tests -q
python scripts/v0_sanity.py
```

Frontend modified:

```powershell
cd frontend
npm.cmd run build
cd ..
```

If validation cannot be completed, report the blocker and do not claim success.

## 11. Final Report Format

Every V4 final report must include:

```text
1. 本轮阶段
2. 实现内容
3. 新增/修改文件
4. 未实现内容
5. 阶段边界检查
6. 与 BlockEmulator 对齐/超越点
7. Data truth / backend truth 影响
8. Artifacts / outputs
9. 兼容性
10. 测试与验证结果
11. git status
12. commit hash
13. 是否 push：必须说明未 push，除非用户明确要求 push
```

## 12. Strict Truthfulness

Do not claim V4 exists before the first V4 code stage is implemented.

Do not claim real P2P before long-running nodes send and receive real messages.

Do not claim real PBFT before real nodes exchange PrePrepare / Prepare / Commit messages and quorum results come from those messages.

Do not claim persistent state before block/state/receipt storage is written and recovered.

Do not claim cross-shard commit before SourceLock / TargetCommit / Finalize or Refund change real state.

Do not claim superiority over BlockEmulator before direct comparison artifacts exist.
