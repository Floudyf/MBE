# V3.4.1 Runtime Plugin Hardening: FIFO TxPool

## 1. Stage Positioning

V3.4.1 is a runtime hardening stage. It comes after the V3.3 Composer Draft Smoke path and before Fabric-backed validation.

V3.4.1 is not a paper-scale experiment. It is not Fabric live execution. It is not the completion of a BlockEmulator-like full emulator. Its purpose is narrower: make the FIFO TxPool a real, observable runtime object in the local Go-backed modular research chain runtime.

## 2. Why Runtime Hardening Before Fabric Validation

The current V3 runtime can run Draft Smoke, but TxPool is still config-only / catalog-only. `queue_wait_ms` is fixed to 0. BlockProducer still uses `cutBlocks(txs, chain)` style logical slicing.

Fabric validation should calibrate a stable runtime. It should not calibrate a logical replay that lacks queue behavior and cannot explain transaction admission, waiting, selection, and rejection. Therefore TxPool and BlockProducer runtime hardening must come before Fabric-backed validation.

## 3. Implementation Scope

V3.4.1 may implement or connect:

- FIFO TxPool struct.
- `Admit`.
- `SelectForBlock`.
- dedup.
- capacity.
- reject/backpressure.
- queue wait.
- `txpool_log.csv`.
- summary TxPool metrics.
- artifact allowlist.
- Draft Smoke result display for TxPool metrics.
- frontend alignment for TxPool summary fields, artifact grouping, history detail, and module truthfulness.

## 4. Non-goals

V3.4.1 must not implement:

- PBFT / HotStuff / Raft.
- Fabric / EVM live backend.
- Fabric Docker / `network.sh`.
- MetaFlow.
- dual-chain runtime.
- cross-chain bridge.
- relay / broker / 2PC.
- dynamic resharding.
- committee lifecycle.
- state migration.
- state root / persistent KV.
- multi-process / multi-machine networking.
- paper-grade experiment claim.

## 5. Target File Boundary

The later V3.4.1 implementation should be constrained to:

- `executor/v3runtime/runtime.go`
- `executor/v3runtime/runtime_test.go`
- `backend/app/services/artifact_manager.py`
- `backend/app/services/v3_draft_run_history.py`
- `frontend/src/api.ts`
- `frontend/src/components/v3/ArtifactGroups.tsx`
- `frontend/src/components/v3/DraftRunResultPanel.tsx`
- `frontend/src/components/v3/DraftRunHistoryPanel.tsx`
- `frontend/src/components/v3/ModuleDetailPanel.tsx`
- `frontend/src/components/v3/ModuleCard.tsx`
- `frontend/src/pages/V3ComposerPage.tsx`

This documentation and skill realignment round does not modify those files.

## 6. FIFO TxPool Minimal Semantics

Minimal FIFO TxPool semantics:

- `Admit(tx, nowMS)`: attempts to admit one transaction at virtual time `nowMS`.
- `SelectForBlock(maxTx, cutTimeMS, blockHeight)`: selects up to `maxTx` transactions in FIFO order for a block at virtual time `cutTimeMS`.
- `dedup_enabled`: when true, duplicate transaction IDs are rejected or ignored according to the configured policy.
- `max_pool_size`: capacity limit for queued transactions.
- `backpressure_policy=reject`: full-pool admission rejects the transaction and records a reason.
- `txpool_peak_size`: maximum observed queued transaction count.
- `queue_wait_ms`: selected transaction wait time from admission to block selection.
- `rejected_count`: total rejected transactions.
- `reason`: machine-readable reason for reject or notable pool event.

## 7. BlockProducer and TxPool Interaction

Old flow:

```text
generateWorkload -> cutBlocks(txs, chain) -> execute
```

New flow:

```text
generateWorkload -> TxPool.Admit -> BlockProducer tick/count trigger -> TxPool.SelectForBlock -> block -> execute
```

The runtime must keep virtual time. It must not use `time.Sleep` to simulate queueing, block production, network latency, execution delay, commit delay, or finality delay.

Blocks should no longer be directly generated from a transaction slice. `block_log.csv` and `txpool_log.csv` should explain each other: block `tx_count` should correspond to TxPool select events for the same block height.

## 8. Artifacts and Metrics

New artifact:

```text
txpool_log.csv
```

Suggested fields:

- `event_time_ms`
- `event_type`
- `tx_id`
- `block_height`
- `pool_size_before`
- `pool_size_after`
- `admitted_count`
- `selected_count`
- `rejected_count`
- `queue_wait_ms`
- `reason`

Suggested summary metrics:

- `queue_wait_ms`
- `txpool_admitted_count`
- `txpool_rejected_count`
- `txpool_peak_size`
- `txpool_avg_wait_ms`
- `txpool_p95_wait_ms`

`txpool_log.csv` is a local modular runtime / Draft Smoke artifact. It is not paper-grade final evidence by itself.

## 9. Frontend Alignment Scope

V3.4.1 implementation must include frontend alignment. This documentation / skill-only phase only writes the rules and does not modify frontend code.

Frontend alignment targets:

1. `frontend/src/api.ts`
   - Add or remain compatible with TxPool summary fields.
   - Suggested fields: `txpool_admitted_count`, `txpool_rejected_count`, `txpool_peak_size`, `txpool_avg_wait_ms`, `txpool_p95_wait_ms`, and `queue_wait_ms`.

2. `frontend/src/components/v3/DraftRunResultPanel.tsx`
   - Draft Smoke result panel must show TxPool metrics, not only TPS / latency / plugin summary.
   - At minimum show average queue wait, peak pool size, admitted count, rejected count, and TxPool artifact availability.

3. `frontend/src/components/v3/ArtifactGroups.tsx`
   - `txpool_log.csv` must appear in artifact download groups.
   - Suggested group names: Runtime queue logs, TxPool logs, or Chain runtime logs.
   - Do not label `txpool_log.csv` as a Fabric artifact or paper-grade result.

4. `frontend/src/components/v3/DraftRunHistoryPanel.tsx`
   - Historical Draft Smoke runs that include `txpool_log.csv` should display or link it.
   - Older runs without `txpool_log.csv` should be treated as missing legacy artifacts, not frontend errors.

5. `frontend/src/components/v3/ModuleDetailPanel.tsx`
   - TxPool details must distinguish `fifo_pool` as runtime-realized after V3.4.1 implementation.
   - `priority_pool`, `hotspot_aware_pool`, and `fee_based_pool` remain planned.
   - The UI must not imply those planned pools are real runnable runtime implementations.

6. `frontend/src/components/v3/ModuleCard.tsx`
   - Module cards should continue showing status and should later distinguish configured runnable, runtime-supported, preview-only, and planned.
   - A plugin appearing in a selector must not be interpreted as proof that runtime behavior exists.

7. `frontend/src/pages/V3ComposerPage.tsx`
   - Page boundary wording must preserve or strengthen: single-chain, Go Runtime, Smoke experiment, non-Fabric, non-MetaFlow, non-PBFT / HotStuff / Raft, and non-multi-node network.
   - After V3.4.1 it may add TxPool runtime hardening, FIFO pool only, and local modular runtime.

Frontend non-goals:

- No full real-time dashboard.
- No WebSocket live monitor.
- No drag-and-drop freeform composer.
- No multi-user permission system.
- No formal result database.
- No Fabric live status console.
- No MetaFlow frontend.
- No dual-chain frontend.
- No cross-shard relay / broker / 2PC frontend.
- Do not present Draft Smoke as paper-grade result.

Frontend acceptance after V3.4.1 code implementation:

- Draft Smoke result panel shows TxPool summary metrics.
- Artifact area can download `txpool_log.csv`.
- History can show runs with `txpool_log.csv`; older runs without it do not crash.
- TxPool module detail explains FIFO pool as the hardening target and other TxPool plugins as planned.
- Page still clearly says non-Fabric, non-MetaFlow, non-PBFT, and non-multi-node network.
- `npm.cmd run build` passes.

## 10. Fairness Rules

V3.4.1 only makes FIFO TxPool real. It should not immediately open TxPool multi-plugin comparison.

For later TxPool single-module tests, only `TxPoolPlugin` may vary. Workload, seed, submit rate, BlockProducer, Consensus, Routing, Execution, StateAccess, StateStorage, Commit, and Metrics must remain fixed.

No experiment may create advantage through workload, seed, submit rate, block config, network profile, hardware profile, frontend display filtering, or hidden runtime setting differences.

## 11. Acceptance Criteria

V3.4.1 is acceptable when:

- V3 Draft Smoke can run.
- Go tests pass.
- Output directory contains `txpool_log.csv`.
- `summary.csv/json` has `queue_wait_ms` derived from TxPool wait time and no longer hardcoded to 0.
- `block_log.csv` block `tx_count` corresponds to `txpool_log.csv` select events.
- Draft Smoke history can display or download `txpool_log.csv`.
- Draft Smoke result panel displays TxPool summary metrics.
- TxPool module detail keeps planned TxPool plugins visibly planned.
- Existing MetaTrack four combinations still run.
- No Fabric / Docker / `network.sh` / MetaFlow / dual-chain / PBFT is introduced.

## 12. Validation Commands

Documentation / skill-only phase:

```powershell
git diff --check
git status --short
```

V3.4.1 code implementation phase:

```powershell
python -m pytest backend/tests -q
python -m pytest tests -q
cd executor
go test ./...
cd ..
cd frontend
npm.cmd run build
cd ..
python scripts/v0_sanity.py
git diff --check
git status --short
```

## 13. Next Route Toward BlockEmulator-like Emulator

Recommended route:

- V3.4.1 FIFO TxPool.
- V3.4.2 BlockProducer.
- V3.4.3 Consensus-light.
- V3.4.4 Single-module templates.
- V3.4.9 MetaTrack ablation templates.
- V3.4.10 controlled smoke runner.
- V3.4.11 stage/version/frontend/docs/skill closure.
- V3.5 node-level emulator skeleton.
- Later work: multi-process runtime, network model, relay/broker, PBFT-style consensus, state root, persistent KV, state snapshot, and stronger emulator semantics.

Each V3.4.x runtime hardening substage must include corresponding frontend alignment. When runtime adds a new artifact or summary metric, frontend artifact grouping, summary preview, and history detail must align in the same implementation stage. Runtime must not produce a new artifact that the frontend cannot download or explain.
## V3.4.11 Closure Update

Current stage is V3.4.11 closure. V3.4.1 remains the FIFO TxPool runtime hardening stage; the latest runtime capability is V3.4.10 controlled smoke runner, and V3.4.11 only aligns stage/version/frontend/docs/skill wording and validation. It does not add new TxPool, consensus, network, Fabric/EVM, or cross-shard runtime behavior.
