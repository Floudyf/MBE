# V3.4 Runtime Self-check

Date: 2026-06-29

Scope: read-only self-check for current MBE repository state before V3.4 Runtime Plugin Hardening. No business code, config, test, frontend, backend, commit, or push changes were made by this report.

## 1. Baseline Command Results

| Command | Result | Notes |
|---|---:|---|
| `git status --short` | pass | Empty output before report generation. Working tree had no tracked changes at that point. |
| `C:\Users\飛\AppData\Local\Programs\Python\Python312\python.exe -m pytest backend/tests -q` | pass | 240 passed, 1 FastAPI/Starlette deprecation warning. Initial sandbox attempt failed with access denied to the external Python path; rerun with permission succeeded. |
| `C:\Users\飛\AppData\Local\Programs\Python\Python312\python.exe -m pytest tests -q` | pass | 24 passed. Initial sandbox attempt failed with access denied to the external Python path; rerun with permission succeeded. |
| `cd executor && go test ./...` | pass | Passed after rerun with permission. Initial sandbox attempt failed because Go could not write `C:\Users\飛\AppData\Local\go-build`. Packages: `cmd/replay` no tests; `commit`, `core`, `execution_sharding`, `routing`, `state_sharding`, `v3runtime` passed. |
| `cd frontend && npm.cmd run build` | pass | Passed after rerun with permission. Initial sandbox attempt reached Vite output and failed with `EPERM: operation not permitted, mkdir 'frontend\dist\assets'`; permission rerun built successfully. |
| `C:\Users\飛\AppData\Local\Programs\Python\Python312\python.exe scripts/v0_sanity.py` | pass | Generated V0 asset_hotspot trace, replayed 10000 transactions, and passed V0 workload/trace/replay/metrics sanity. |

## 2. Current Runtime Truth

Current MBE is V3.3 single-chain modular Composer plus Go-backed MetaTrack Draft Smoke. It can validate a single-chain Composer Draft, generate a temporary experiment/plugin profile, run the Go `v3-runtime`, display current and historical Draft Smoke results, and download allowlisted artifacts. It is still a local logical runtime and local replay platform, not a multi-process, multi-node, networked sharded blockchain emulator.

The strongest evidence is:

- `docs/v3_3_7_boundary_and_skill_closure.md` explicitly defines V3.3 as single-chain modular research chain composer plus Go-backed MetaTrack smoke.
- `docs/v3_3_go_backed_metatrack_evaluation.md` says Go V3 mode does not start Fabric, Docker, network, frontend, public-chain clients, or dual-chain services.
- `executor/v3runtime/runtime.go` generates workload in-process, cuts blocks with `cutBlocks`, logs `simple_leader`, uses a Go map as state, and sets `QueueWaitMS = 0`.
- `backend/app/services/chain_backend.py` has runnable `local_virtual` and `trace_replay`; `fabric_live` and `evm_live` are planned unsupported backends.

## 3. Module Self-check Table

| 模块 | 当前代码位置 | 当前状态 | 是否真实运行 | 已有输出指标/artifacts | 当前限制 | V3.4 下一步建议 |
|---|---|---|---|---|---|---|
| Workload | `executor/v3runtime/runtime.go`; `backend/app/services/v3_composer_draft_runner.py`; `configs/v3/experiments/*` | runnable | 是，但是真实含义是内置 synthetic workload 生成，不是外部 workload 插件 runtime | `used_experiment_profile.*`, `tx_results.csv`, summary latency/TPS | `existing_trace`/`saved_workload` 在 Composer 中是 preview-only；没有 workload 插件接口或 streaming trace ingestion 接到 Go V3 runtime | 把 workload 保持固定，先不要作为 V3.4.1 变量；后续补 trace-backed workload adapter |
| TxPool | `configs/v3/chains/single_chain_research_default.yaml`; `backend/app/services/v3_composer_catalog.py`; `executor/v3runtime/runtime.go` | config-only | 否。Go runtime 中没有真实池对象；`AdmitTimeMS = SubmitTimeMS`，`queue_wait_ms = 0` | summary 有 `queue_wait_ms` 字段，但固定为 0；无 `txpool_log.csv` | FIFO pool 是 validator/catalog/profile 约束，不是可观测队列行为；无 capacity、dedup、backpressure、admission log | V3.4.1 第一优先级：实现真实 FIFO TxPool、admit/select/drop、`txpool_log.csv`、非零 queue wait |
| BlockProducer | `configs/v3/chains/single_chain_research_default.yaml`; `executor/v3runtime/runtime.go` | partially-runnable | 部分。当前是 `cutBlocks(txs, chain)` 批量切块，不是节点事件循环从 TxPool 拉交易出块 | `block_log.csv` 有 block height/id/proposer/cut/ordered/finalized/tx_count | 没有独立 BlockProducer 组件；没有空块策略运行、定时 tick、mempool selection、节点生命周期 | V3.4.1 让 BlockProducer 从 TxPool `SelectForBlock` 取交易，并记录 block input queue stats |
| Consensus | `backend/app/services/v3_composer_catalog.py`; `backend/app/services/v3_composer_draft_validator.py`; `executor/v3runtime/runtime.go` | partially-runnable | 部分。只有 `simple_leader` 的本地排序/时间戳模型；PBFT/HotStuff/Raft 没有实现 | `block_log.csv` 有 `consensus_plugin`, ordered/finalized time, validator count | 没有真实消息、view/term、quorum、fault model；PBFT/HotStuff/Raft 均为 planned | 保持 simple_leader 固定；V3.4 不先碰 BFT，先把 TxPool/BlockProducer harden |
| Committee / Epoch | `configs/v3/chains/single_chain_research_default.yaml`; `backend/app/services/v3_composer_catalog.py`; `backend/app/services/v3_composer_draft_validator.py` | preview-only | 否。默认 disabled；fixed epoch 是 placeholder/preview | Draft validation 会列出 disabled/planned/preview 状态；无 committee runtime artifact | 无 epoch lifecycle、committee rotation、membership change、reconfiguration log | 保持 disabled；等单链 runtime observable 后再引入 `epoch_log.csv` |
| Routing / Sharding | `executor/v3runtime/runtime.go`; `configs/v3/plugins/metatrack_plugin_profiles.yaml`; `backend/app/services/v3_composer_catalog.py` | runnable | 是，但只是真实运行在 execution-side routing 层 | `tx_results.csv` 有 execution/state unit 字段；summary 有 cross state unit、remote fetch、load balance；`state_commit_log.csv` 有 placement/routing | `co_access_sharding` 不迁移 persistent state placement；没有真实跨片 relay/broker/2PC；`dynamic_resharding` planned | V3.4.1 不扩跨片协议；只确保 TxPool/BlockProducer 指标公平，不改变 routing 语义 |
| Execution | `executor/v3runtime/runtime.go`; `configs/v3/plugins/metatrack_plugin_profiles.yaml` | runnable | 是，本地逻辑执行。`serial_execution` 和 `dual_track_execution` 影响 track/metrics | `tx_results.csv`, summary `fast_track_count`, `conservative_track_count`, `conflict_count` | 不是真并行 executor；没有 worker scheduling、dependency graph、abort/retry；Block-STM-like planned | 先保持现状；新增 TxPool 后校验 execution start time 从 block finality/queue 输出一致 |
| StateAccess | `executor/v3runtime/runtime.go`; `configs/v3/plugins/metatrack_plugin_profiles.yaml` | runnable | 是，本地逻辑模型。`direct_fetch` 计 remote fetch；`access_list_prefetch` 把 remote fetch 归零 | summary `remote_fetch_count`, `remote_state_fetch_count`, `state_locality_ratio`; `tx_results.csv` remote fields | 没有真实远程 fetch、cache、witness、prefetch IO；latency cost 没有真实累加到 execution timeline | 后续让 remote fetch cost 影响 execution latency；V3.4.1 可暂不做 |
| StateStorage | `configs/v3/chains/single_chain_research_default.yaml`; `executor/v3runtime/runtime.go` | partially-runnable | 部分。运行时是内存 Go map；`memory_kv`/`hash_state_storage` 主要体现为 placement policy 和 unit id | `state_commit_log.csv` 记录 old/delta/new、storage unit、remote commit | 无 state root、持久化 KV、snapshot、Merkle proof、crash recovery；`memory_kv` 在 validator 中会归一化为 `hash_state_storage` | 先不作为 V3.4.1 首批；后续补 `state_snapshot.json` 或 persistent KV adapter |
| Commit | `executor/v3runtime/runtime.go`; `configs/v3/plugins/metatrack_plugin_profiles.yaml` | runnable | 是，本地逻辑提交。`normal_commit` 写 map；`hot_update_aggregation_commit` 影响聚合指标 | `state_commit_log.csv`; summary `aggregated_update_count`, `aggregation_ratio`, `block_commit_latency_ms` | `StateCommit.CommitPlugin` 当前硬写 `normal_commit`，即使 profile 是 hot aggregation；无 batch/2PC/commit barrier | 修正 commit artifact 插件标识；后续再做真实 batch/aggregation behavior |
| MetricsReport | `executor/v3runtime/runtime.go`; `backend/app/services/v3_go_runtime_runner.py`; `backend/app/services/artifact_manager.py` | runnable | 是，作为输出汇总和 allowlisted artifacts | `summary.csv/json`, `report.md`, `runtime.log`, `block_log.csv`, `tx_results.csv`, `state_commit_log.csv`, MetaTrack aggregate CSV/JSON | `queue_wait_ms` 固定 0；缺 TxPool 指标；metrics 主要来自逻辑模型，不是 runtime probes | V3.4.1 扩展 `summary.csv/json` 加 txpool 指标，并新增 `txpool_log.csv` 到 allowlist |
| V2 dual-chain replay | `backend/app/services/dual_chain_replay.py`; `backend/app/services/chain_backend.py` | runnable | 是，但只是真实 local virtual-time/trace replay，不是真实链执行 | `dual_chain_summary.csv/json`, `stage_metrics.csv`, `runtime.log`, `report.md` | runtime log 明确 `mode=local virtual-time replay only`; 不执行 cross-chain protocol；不启动 Fabric/Docker | 保持为 V2 replay baseline；不要混入 V3.4 TxPool 范围 |
| V2 cross-chain protocol replay | `backend/app/services/protocol_replay.py`; `backend/app/services/cross_chain_protocols.py`; `backend/app/services/chain_backend.py` | runnable | 是，但协议和 backend 都是本地模型 | `protocol_summary.csv/json`, `protocol_results.csv`, `protocol_events.csv`, `runtime.log`, `report.md` | 不是 production bridge；无真实 committee signatures、MintCert、RefundCert、FinalityProof；backend 仍 local/trace | 保持边界；V3.4.1 不扩 cross-chain |
| ChainBackend / Fabric / EVM live backend | `backend/app/services/chain_backend.py`; `configs/v3/chains/fabric_validation_planned.yaml`; `configs/v3/experiments/fabric_validation_profile_preview.yaml` | planned | 否。`fabric_live`/`evm_live` 创建 UnsupportedLiveBackend 并抛 NotImplementedError | capability API 能显示 planned；无 live artifacts | 不支持 submit、finality observation、event listener、replay、virtual time；不会启动 Fabric/EVM | 继续 planned；只有 TxPool hardening 完成后再进入 chain-backed validation |
| V3 frontend Composer | `frontend/src/pages/V3ComposerPage.tsx`; `frontend/src/components/v3/*`; `frontend/src/api.ts` | runnable | 是，前端可加载 preview、编辑 Draft、调用 validate/run smoke、显示当前结果和历史 | `DraftRunResultPanel`, `DraftRunHistoryPanel`, `ArtifactGroups` 展示 summary/artifacts/history | UX 已有边界说明，但 TxPool 指标还没有专门展示；模块状态可能让用户误以为 TxPool/BlockProducer 是真实插件 | V3.4.1 加 TxPool 指标展示和 artifact group |
| Backend validate-draft / run-draft-smoke | `backend/app/main.py`; `backend/app/services/v3_composer_draft_validator.py`; `backend/app/services/v3_composer_draft_runner.py`; `backend/app/services/v3_go_runtime_runner.py` | runnable | 是。validate 归一化/约束 Draft；run-draft-smoke 调 Go runtime | Draft inputs、generated profiles、runtime artifacts、history metadata | 只支持 `metatrack_ablation` Draft Smoke；固定 TxPool/BlockProducer/Consensus/Metrics；run root 是 local `.cache` | V3.4.1 扩 validator allowlist 与 generated profile 参数以支持真实 TxPool 指标 |
| Artifact / run history | `backend/app/services/artifact_manager.py`; `backend/app/services/job_manager.py`; `backend/app/services/v3_draft_run_history.py`; `frontend/src/components/v3/DraftRunHistoryPanel.tsx` | runnable | 是，本地 run history 和 allowlisted download | V2 jobs, V3 draft runs, artifact allowlist, historical summary preview | 本地 `.cache` history，不是正式结果数据库；allowlist 还不含 `txpool_log.csv` | V3.4.1 增加 `txpool_log.csv` allowlist、history grouping、frontend link |
| Tests and build status | `backend/tests`; `tests`; `executor/v3runtime/runtime_test.go`; `frontend` | runnable | 是，当前 baseline pass | pytest/go/frontend build/V0 sanity 均通过，部分需权限访问用户目录/cache | Go/Python/build 首次 sandbox 运行会因外部 cache/path 权限失败；测试还没有覆盖真实 TxPool 行为 | V3.4.1 增加 Go TxPool tests、backend artifact allowlist tests、frontend build verification |

## 4. Explicit Judgements

1. TxPool is not a real transaction pool yet. It is a catalog/profile/config item plus direct batch processing. Evidence: `TxResult.AdmitTimeMS` equals `SubmitTimeMS`; summary `QueueWaitMS` is fixed to 0; no `txpool_log.csv`.

2. BlockProducer is not a real node-driven producer yet. It is `cutBlocks(txs, chain)`, which chunks generated transactions by `max_tx_per_block` and calculates cut time from block interval and last submit time.

3. Consensus only has the local `simple_leader` model. PBFT, HotStuff, Raft, and committee consensus are planned catalog entries, not runtime implementations.

4. There is no real multi-process, multi-node, or network communication in the current V3 runtime. The chain profile says `single_process_logical_nodes`; Go V3 runtime does not start Fabric, Docker, network scripts, public-chain clients, or dual-chain services.

5. Routing/Sharding is execution-side routing. It can alter `execution_shard_id` and mechanism metrics, but persistent state placement remains hash-based. There is no real cross-shard relay, broker, 2PC, state migration, or dynamic resharding.

6. StateStorage is memory map / memory_kv style. There is no state root, persistent KV, snapshot, Merkle proof, or crash recovery. The runtime does write `state_commit_log.csv`, but state itself is in-memory.

7. V2 dual-chain and cross-chain protocol paths are local virtual-time replay/model execution. They do not submit to real chains. Fabric/EVM live execution is not present.

8. Fabric/EVM live backends are planned/unsupported. `create_backend` returns `UnsupportedLiveBackend` for `fabric_live` and `evm_live`; submit/finality methods raise `NotImplementedError`.

9. V3 Composer can validate Drafts, run Draft Smoke, show current result, show local history, and download artifacts. This is local Draft Smoke history, not a formal result database.

10. Best V3.4.1 first real-hardening objects are TxPool and BlockProducer, because they are currently the thinnest "runnable-looking" modules and can be hardened without changing cross-chain, BFT, Fabric, or state-storage scope.

## 5. Main Gaps Toward a BlockEmulator-like Platform

Priority gaps:

1. No real TxPool: no admission queue, dedup behavior, backpressure, pool occupancy, drop reason, or queue wait metric.
2. No independent BlockProducer: block production is offline chunking, not event/tick-driven selection from a pool.
3. No node runtime lifecycle: logical node IDs are labels, not running nodes with process/thread/mailbox/network boundaries.
4. No real network model in execution path: fixed delay config exists, but no message passing, bandwidth, jitter, loss, or per-message logs drive consensus/execution.
5. Consensus is a timestamped simple leader model only; no PBFT/HotStuff/Raft state machines, quorum messages, view changes, or fault injection.
6. Routing is execution-side only; no true cross-shard transaction protocol, relay, broker, 2PC, locking, abort, or shard-to-shard message artifact.
7. StateStorage lacks state root, persistent KV, snapshots, proof/witness artifacts, and crash/restart semantics.
8. StateAccess does not add real fetch latency to the execution timeline and has no cache/prefetch IO behavior.
9. Commit metrics are partly synthetic; `CommitPlugin` in `state_commit_log.csv` is currently hardcoded as `normal_commit`, so hot aggregation is not fully reflected at row level.
10. Artifact schema lacks TxPool/BlockProducer-specific logs, making fairness and bottleneck inspection incomplete.
11. V2/V3 live chain backends are planned only; Fabric/EVM calibration does not mean the V3 runtime is chain-backed.
12. Frontend can run smoke and download artifacts, but it does not yet surface TxPool queue depth/wait/drop metrics or clearly separate "configured runnable" from "runtime-realized behavior" per module.

## 6. Recommended V3.4.1 Minimal Closed Loop

Start with: real FIFO TxPool plus BlockProducer selection and TxPool artifacts.

Core file scope should stay within six files:

1. `executor/v3runtime/runtime.go`
   - Add a small `TxPool` struct with `Admit`, `SelectForBlock`, dedup, capacity, and queue wait accounting.
   - Replace direct `cutBlocks(txs, chain)` with pool admission plus block producer selection by virtual time/count.
   - Set `AdmitTimeMS` from pool admission and compute non-zero `queue_wait_ms`.
   - Write `txpool_log.csv`.

2. `executor/v3runtime/runtime_test.go`
   - Add tests for FIFO order, bounded queue wait, block selection from pool, and presence/header of `txpool_log.csv`.

3. `backend/app/services/artifact_manager.py`
   - Add `txpool_log.csv` to `ARTIFACT_ALLOWLIST`.

4. `backend/app/services/v3_draft_run_history.py`
   - Add `txpool_log.csv` to artifact grouping and missing/summary handling if needed.

5. `frontend/src/components/v3/ArtifactGroups.tsx`
   - Add TxPool artifact group or include `txpool_log.csv` under Chain runtime logs.

6. `frontend/src/components/v3/DraftRunResultPanel.tsx`
   - Show `queue_wait_ms` and, if added to summary, pool admit/drop/peak depth metrics.

Suggested first acceptance criteria:

- `go test ./...` passes.
- V3 Draft Smoke writes `txpool_log.csv`.
- `summary.csv/json` has real `queue_wait_ms` derived from transaction wait time, no longer hardcoded to 0.
- `block_log.csv` blocks are produced from TxPool selection, not direct transaction slicing.
- Frontend Draft Smoke result/history can download `txpool_log.csv`.
- Existing V0 sanity, backend tests, top-level tests, and frontend build still pass.

## 7. Final Conclusion

### A. Current Real Stage

当前是 V3.3 单链模块化 Composer + Go-backed MetaTrack Smoke，本地逻辑 runtime，可运行单配置 Draft Smoke，但还不是多节点分片链 emulator。

### B. Distance to BlockEmulator-like Platform

The largest gap is not the Composer UI; it is runtime realism. The platform has good draft validation, artifact discipline, and local logical evaluation, but it still needs observable TxPool, producer, node/network, consensus, state, and cross-shard behavior before it can be honestly used as a modular blockchain emulator/testbed.

### C. V3.4.1 Recommendation

Implement the smallest real runtime hardening loop: FIFO TxPool + `txpool_log.csv` + non-zero queue wait + BlockProducer selecting from TxPool + frontend artifact/metric display. This gives the group an immediately inspectable module variable boundary without opening the much larger PBFT/Fabric/cross-shard scope.
