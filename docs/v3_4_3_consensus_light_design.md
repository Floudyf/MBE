# V3.4.3 Consensus-light Design and BlockEmulator Reference Check

## 1. Stage Positioning

V3.4.3 introduces consensus-light models in the local Go-backed modular runtime. It comes after V3.4.1 FIFO TxPool hardening and V3.4.2 BlockProducer hardening, and it remains before V3.5 Fabric-backed validation.

V3.4.3 keeps `simple_leader` as the default and may add `poa_light` and `pbft_light_model`. It is not paper-scale experimentation, not Fabric live execution, and not completion of a BlockEmulator-like full emulator.

V3.4.3 是轻量共识模型阶段，不是真实网络 PBFT 阶段。`pbft_light_model` 只模拟 PBFT 的阶段、quorum 和消息数量，用于可观测 consensus latency / message count / finality metrics。

## 2. Current MBE Consensus State

Current MBE V3 consensus is represented as a local `simple_leader` model in `executor/v3runtime/runtime.go`.

- `PluginProfile.ConsensusPlugin` defaults to `simple_leader`.
- `requireSupportedPlugins` requires `ConsensusPlugin=simple_leader`.
- `Run` assigns a logical proposer from `chain.NodeIDPrefix` and `chain.ValidatorCount`.
- `ordered_time_ms` is currently `block.CutTimeMS + 1`.
- `finalized_time_ms` is currently `ordered_time_ms + 1`.
- `block_log.csv` already includes `consensus_plugin`, `ordered_time_ms`, `finalized_time_ms`, `consensus_domain_id`, and `validator_count`.
- Summary output does not yet include consensus latency, quorum, message count, round, view, finalized block count, or failed block count fields.
- `backend/app/services/v3_composer_draft_validator.py` fixed runtime requirements still require `Consensus=simple_leader`.
- `backend/app/services/v3_composer_catalog.py` marks PBFT / HotStuff / Raft style consensus options as planned, while `simple_leader` is runnable.
- The V3 Composer frontend shows module status and boundary badges, but does not yet show consensus latency, message count, view-change count, or `consensus_log.csv` availability.

If `poa_light` or `pbft_light_model` is added in V3.4.3b, the code stage must update the Go runtime plugin guard, draft validator, catalog status, artifact/history handling, summary fields, and frontend result/module/artifact displays together.

## 3. Why Consensus-light Before Real PBFT

The current runtime is deterministic, local, and virtual-time based. TxPool and BlockProducer are now observable, but consensus still behaves as a fixed timestamp offset after block production.

A full PBFT implementation would require real node lifecycle, message transport, view-change safety, fault handling, and persistent consensus state. Implementing that before consensus observability would blur the boundary between a local modular research runtime and a multi-node network emulator.

Consensus-light adds a smaller, truthful step: expose stage, quorum, message count, and finality metrics without claiming real PBFT safety.

## 4. BlockEmulator PBFT Reference Summary

BlockEmulator was fetched as a read-only reference into `F:\_external\block-emulator`. It was not copied into MBE and must not be vendored or committed.

The following files were read:

- `F:\_external\block-emulator\consensus_shard\pbft_all\pbft.go`
- `F:\_external\block-emulator\consensus_shard\pbft_all\messageHandle.go`
- `F:\_external\block-emulator\consensus_shard\pbft_all\view_change.go`
- `F:\_external\block-emulator\consensus_shard\pbft_all\pbftMod_interface.go`
- `F:\_external\block-emulator\consensus_shard\pbft_all\toolFuncs.go`
- `F:\_external\block-emulator\params\global_config.go`

BlockEmulator's PBFT implementation is a real networked implementation shape: `PbftConsensusNode` stores shard/node identity, blockchain and LevelDB/MPT handles, global chain config, node table, node count, malicious count, view state, sequence id, request pool, prepare and commit confirmation maps, stage, logs, TCP listener, and handler interfaces.

The message flow is implemented through `Propose`, `handlePrePrepare`, `handlePrepare`, `handleCommit`, old-message sync handlers, and view-change handlers. It uses `net.Listen`, `networks.Broadcast`, `networks.TcpDial`, goroutines, `time.Sleep`, and a chain / MPT / relay stack. Those implementation mechanisms are intentionally outside V3.4.3.

## 5. What to Borrow from BlockEmulator

Borrow the PBFT-observability concepts, not the network implementation:

- Node fields: shard id, node id, validator / node count, malicious count `f`, view, sequence id, request pool, prepare confirm map, commit confirm map, stage, and log.
- Stage flow: Propose, PrePrepare, Prepare, Commit, Reply / Finalized.
- Quorum logic: `f = (N - 1) / 3`, prepare quorum `2f + 1`, and commit quorum `2f + 1`.
- Message types for accounting: PrePrepare, Prepare, Commit, ViewChange, NewView, RequestOldMessage, and SendOldMessage.
- Metrics: consensus round, view id, sequence id, leader id, prepare message count, commit message count, total message count, quorum size, finalized flag, consensus latency, and view change count.

## 6. What Not to Borrow Yet

V3.4.3 must not borrow runtime mechanisms that would turn the stage into a real network consensus implementation:

- TCP listener.
- `networks.Broadcast`.
- Goroutine-based message handler.
- `time.Sleep`.
- LevelDB / MPT / BlockChain coupling.
- CLPA / Broker / Relay logic outside the consensus module.
- Real view-change safety.
- Old request sync.
- Multi-process node lifecycle.

## 7. V3.4.3 Consensus Plugin Plan

Allowed future V3.4.3b plugins:

- `simple_leader`: default local leader ordering model.
- `poa_light`: lightweight authority confirmation model.
- `pbft_light_model`: PBFT stage and message-count model; not real PBFT.

`simple_leader` must remain the default. `poa_light` and `pbft_light_model` may become runtime-supported only after their runtime semantics, artifacts, summary fields, validator/catalog entries, and frontend displays are implemented together.

## 8. PBFT-light Model Semantics

`pbft_light_model` should follow PrePrepare / Prepare / Commit / Finalized stage semantics and `2f + 1` quorum accounting, but it must not implement real TCP networking, goroutine-based node message handling, view-change safety, real fault tolerance, or production PBFT.

Suggested deterministic local semantics:

- For each produced block, create a consensus record with `view_id`, `sequence_id`, and `leader_id`.
- Compute `fault_tolerance_f = (validator_count - 1) / 3`.
- Compute `prepare_quorum = 2f + 1` and `commit_quorum = 2f + 1`.
- Count PrePrepare, Prepare, and Commit messages as virtual accounting fields.
- Derive `consensus_ordered_time_ms` and `consensus_finalized_time_ms` from virtual time, not wall-clock sleep.
- Mark finalized only if the virtual quorum constraints are satisfied.
- Keep view change count at 0 unless a future explicit light model introduces a truthful, deterministic failure scenario.

## 9. Proposed consensus_log.csv

V3.4.3b may add `consensus_log.csv` as a local modular runtime artifact, not a Fabric artifact and not paper-grade evidence by itself.

Suggested fields:

```text
block_height
block_hash
consensus_plugin
round_id
view_id
sequence_id
leader_id
validator_count
fault_tolerance_f
prepare_quorum
commit_quorum
preprepare_msg_count
prepare_msg_count
commit_msg_count
total_message_count
consensus_start_time_ms
consensus_ordered_time_ms
consensus_finalized_time_ms
consensus_latency_ms
finalized
view_change_count
reason
```

## 10. Proposed Summary Metrics

Suggested summary fields:

```text
consensus_latency_ms
avg_consensus_latency_ms
p95_consensus_latency_ms
consensus_message_count
avg_consensus_message_count
consensus_round_count
view_change_count
finalized_block_count
failed_block_count
```

These metrics should be derived from consensus-light records, not fixed constants.

## 11. Frontend Alignment Plan

The V3.4.3b code stage must align frontend display when consensus summary fields or `consensus_log.csv` are added.

Target files:

- `frontend/src/components/v3/DraftRunResultPanel.tsx`
- `frontend/src/components/v3/DraftRunHistoryPanel.tsx`
- `frontend/src/components/v3/ModuleDetailPanel.tsx`
- `frontend/src/components/v3/ModuleCard.tsx`
- `frontend/src/pages/V3ComposerPage.tsx`
- `frontend/src/components/v3/ArtifactGroups.tsx`

The frontend should display:

- consensus plugin.
- consensus latency.
- consensus message count.
- finalized block count.
- view change count.
- `consensus_log.csv` availability.

It must continue to state the boundary: non-Fabric, non-MetaFlow, non-real-PBFT, non-HotStuff / Raft, and non-multi-node network.

## 12. Non-goals

V3.4.3 forbids:

- real PBFT.
- production PBFT.
- HotStuff.
- Raft.
- real TCP networking.
- `net.Listen` / `TcpDial` / `networks.Broadcast`.
- goroutine-based node message handling.
- real view-change safety.
- real Byzantine fault injection.
- Fabric / EVM live backend.
- Fabric Docker / `network.sh`.
- MetaFlow.
- dual-chain runtime.
- cross-chain bridge.
- relay / broker / 2PC.
- dynamic resharding.
- committee lifecycle.
- state migration.
- state root / persistent KV / snapshot.
- multi-process / multi-machine networking.
- paper-ready sweep.
- full dashboard.

## 13. Acceptance Criteria for V3.4.3b

V3.4.3b code implementation should be accepted only if:

- `simple_leader` remains the default and remains compatible with existing Draft Smoke.
- `poa_light` and / or `pbft_light_model` are truthfully labeled as light models.
- PBFT-light records PrePrepare / Prepare / Commit / Finalized stage accounting.
- Quorum fields follow `f = (N - 1) / 3` and `2f + 1`.
- `consensus_log.csv` is emitted when consensus-light logging is implemented.
- Summary fields derive from consensus records.
- Draft Smoke result and history views tolerate old runs missing consensus fields and `consensus_log.csv`.
- Frontend module details do not imply real PBFT, HotStuff, Raft, Fabric, or multi-node networking.
- Existing TxPool and BlockProducer artifacts remain intact.

## 14. Validation Commands

For this V3.4.3a docs / skill stage:

```powershell
git diff --check
git status --short
```

For the later V3.4.3b code implementation stage:

```powershell
cd executor
go test ./...
cd ..
python -m pytest backend/tests -q
python -m pytest tests -q
python scripts/v0_sanity.py
cd frontend
npm.cmd run build
cd ..
git diff --check
git status --short
```

## 15. Truthfulness Rules

- Do not call `pbft_light_model` real PBFT.
- Do not claim production-grade BFT safety.
- Do not claim V3.4.3 provides real multi-node or network consensus.
- Do not claim HotStuff or Raft before actual implementation.
- Do not label `consensus_log.csv` as Fabric evidence.
- Do not present Draft Smoke consensus-light metrics as paper-grade evidence by themselves.
