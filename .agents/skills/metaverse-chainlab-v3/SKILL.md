# metaverse-chainlab-v3

## 0. Current V3 Scope Realignment

As of V3-final, current_stage is V3-final Fault, Observability, and Reproducibility Closure, latest_runtime_stage is V3-final closure with deterministic fault injection MVP, observability summary, final artifact catalog, reproducibility guide, experiment manual, and paper experiment mapping, runtime_truth is v3_final_emulator_closure_not_production_system, and next_stage is V3 maintenance only; do not start V4 unless explicitly requested. V3.4.11 closure, V3.5.4 closure, V3.6 closure, V3.7 closure, V3.8 closure, V3.9 closure, V3.10 closure, V3.10.1 closure, V3.11 closure, V3.12 closure, V3.13 closure, and V3-final closure are complete after this round. Do not continue adding V3.5, V3.6, V3.7, V3.8, V3.9, V3.10, V3.10.1, V3.11, V3.12, V3.13, or V3-final features after closure.

When taking over the project, first check `git status --short`, latest commit, README Current Status, `docs/v3_5_node_topology_and_local_launcher_plan.md`, `docs/v3_5_1_logical_node_topology_runtime.md`, `docs/v3_5_2_local_multi_process_launcher_preview.md`, `docs/v3_5_3_local_node_process_runtime.md`, `docs/v3_5_4_v3_5_closure.md`, `docs/v3_6_network_adapter_and_tcp_message_runtime_plan.md`, `docs/v3_7_consensus_runtime_and_blockemulator_aligned_pbft_plan.md`, `docs/v3_8_cross_shard_protocol_skeleton_plan.md`, `docs/v3_9_state_authenticity_layer_mvp_plan.md`, `docs/v3_10_benchmark_experiment_template_hardening_plan.md`, `docs/v3_10_1_frontend_ux_chinese_console_cleanup_plan.md`, `docs/v3_11_cross_shard_protocol_closure.md`, `docs/v3_12_runtime_realism_closure.md`, `docs/v3_13_metaverse_experiment_suite_closure.md`, final V3 closure docs, the frontend Runtime Topology / NetworkAdapter / Node Process Preview / PBFT preview / Cross-shard / Relay MVP / State Authenticity / Benchmark / Runtime Realism / Metaverse Experiment / Fault / Observability / Reproducibility summary panels, backend V3 composer topology validation, Go logical node / launcher preview / node process preview / local multi-process runtime / committee epoch / network adapter / consensus-light-over-network / PBFT-preview / cross-shard / Relay MVP / state authenticity / benchmark hardening tests, and controlled smoke tests. V3.6, V3.7, V3.8, V3.9, V3.10, V3.10.1, V3.11, V3.12, V3.13, and V3-final are closed after this round. Do not enter V4 unless the user explicitly asks for it. Do not implement production PBFT, HotStuff/Raft, Fabric/EVM live backend, BlockEmulator backend, complete Broker/2PC/Monoxide, production atomic cross-shard commit, Byzantine-secure relay, Ethereum-compatible MPT, production database durability, full stateless execution, full cross-shard proof protocol, large-scale distributed benchmark, performance superiority claims, or paper-grade benchmark claims.

Roadmap after V3.5 closure:

- V3.6.1 is implemented as configurable `NetworkAdapter` plus localhost TCP typed message runtime preview. It keeps `in_memory_message_bus` and `localhost_tcp_preview` as selectable adapter concepts.
- V3.6.2 is implemented as consensus-light proposal/vote preview over NetworkAdapter plus V3.6 closure.
- V3.7.1 is implemented as configurable `ConsensusRuntimePlugin` plus `blockemulator_aligned_pbft_preview` state machine artifacts.
- V3.7.2 is implemented as PBFT preview over the selected NetworkAdapter typed message path plus V3.7 closure. It does not implement production PBFT and must not hardcode PBFT as the only consensus.
- V3.8 is implemented as CrossShardProtocol skeleton and closure. It keeps CrossShardProtocol under Routing/Sharding, supports `none` and runnable `relay_preview`, keeps `broker_preview` and `two_phase_commit_preview` planned-only, and does not implement atomic cross-shard commit.
- V3.9 is implemented as State Authenticity Layer MVP and closure. It supports persistent state backend MVP, `merkle_trie_mvp`, deterministic state roots, proof generation, proof verification, and stateless witness artifacts under StateAccess / StateStorage / Commit.
- V3.10 is implemented as Benchmark / Experiment Template Hardening Closure. It supports benchmark_template_catalog, baseline_profile_catalog, local_controlled_sweep_runner, multi_seed_repeatability, reproducibility_manifest, benchmark_report, and benchmark_artifacts. Do not continue adding V3.10 features after closure.
- V3.10.1 is implemented as Frontend UX and Chinese Console Cleanup Closure. It supports frontend_navigation_cleanup, chinese_console_localization, help_tip_progressive_disclosure, module_selection_cleanup, run_progress_feedback, lightweight_result_chart_preview, and visual_style_cleanup. Do not continue adding V3.10.1 features after closure.
- V3.11 is implemented as CrossShard Protocol Closure. It upgrades `relay_preview` into runnable `relay_mvp` with SourceLock, RelayCertificate, proof/certificate verification records, target commit, source finalization, and deterministic timeout/refund/abort artifacts. It is not production atomic commit, complete Broker/2PC/Monoxide, Byzantine-secure relay, a production cross-chain bridge, BlockEmulator backend, or paper-grade benchmark evidence.
- V3.12 is implemented as Runtime Realism Closure. It hardens the V3.5 launcher/node process previews into a runnable, observable, cleanable local multi-process runtime MVP with shard/committee/epoch artifacts. It is not multi-server deployment, not a production cluster, not production PBFT/HotStuff/Raft, not BlockEmulator backend, not Fabric/EVM live backend, and not paper-grade performance evidence.
- V3.13 is implemented as Metaverse Experiment Suite Closure. It adds a controlled metaverse workload catalog, scenario templates, synthetic trace metadata, offchain confirmation events, cross-metaverse transfer metadata, baseline matrix, multi-seed sweep, and paper table/figure export artifacts. It is not real metaverse platform integration, not production workload crawling, not Fabric/EVM live backend, not BlockEmulator backend, and not paper-grade performance evidence by itself.
- V3-final is implemented as Fault, Observability, and Reproducibility Closure. It closes V3 with frontend/backend stage alignment, deterministic fault injection MVP, local observability summary, final artifact catalog, reproducibility guide, experiment manual, and paper experiment mapping. It is not production fault tolerance, not production monitoring, not a Byzantine adversary model, not a production blockchain system, not BlockEmulator backend, not Fabric/EVM live backend, and not paper-grade performance evidence.
- V3 maintenance may add bounded console, artifact, validation, and documentation improvements when explicitly requested. The MetaTrack formal benchmark console is maintenance scope: it separates Draft Smoke from controlled formal benchmark runs, uses explicit numeric parameters, previews matrices, enforces resource guards, exports formal_* CSV/JSON, and preserves V3-final truth boundaries. It is not V4, not a new research mechanism, not Fabric/EVM live backend, not BlockEmulator backend, not production multi-server deployment, and not paper-final proof by itself.
- V3 maintenance may add a saved configuration workflow when explicitly requested. Saved configs are local JSON files under `.cache/v3_saved_configs/` for `module`, `workload`, `topology`, `method`, and `formal_plan` objects. They support configure -> validate -> Draft Smoke -> save -> reuse -> formal run. They are local emulator configs, not production-chain deployment manifests.
- In docs-only planning rounds, do not implement V3.6/V3.7 code, schemas, tests, configs, frontend, backend, or Go runtime.
- Do not claim production networking or real PBFT from V3.6 NetworkAdapter / consensus-light preview. Do not claim production PBFT even after V3.7 preview.

The main transaction flow wording must remain:

```text
Workload -> TxPool -> BlockProducer -> ConsensusRuntime -> CommitteeEpoch -> Routing/Sharding -> Execution -> StateAccess -> StateStorage -> Commit -> MetricsReport
```

RuntimeTopology / NodeProcessRuntime / NetworkAdapter belong to the runtime support layer and must not be inserted into the transaction main flow.

CrossShardProtocol belongs under Routing/Sharding as a sub-capability. V3.8 UI rules:

- no new CrossShardProtocol main-flow card
- CrossShardProtocol is Routing/Sharding sub-capability
- no frontend layout refactor
- no left navigation change

V3.8 forbids:

- no full Relay
- no full Broker
- no full 2PC
- no atomic cross-shard commit
- no cross-shard proof
- no rollback/timeout recovery
- no Fabric/EVM live
- no BlockEmulator cross-shard backend
- no paper-grade benchmark claim

StateProof and Witness belong under StateAccess / StateStorage / Commit as sub-capabilities. V3.9 UI rules:

- Do not add StateProof or Witness as new main-flow cards.
- StateProof and Witness belong under StateAccess / StateStorage / Commit.
- Do not refactor the V3 Composer page.
- Do not change the left navigation.

V3.9 implemented scope:

- persistent_state_backend
- merkle_trie_mvp
- deterministic state_root
- proof generation
- proof verification
- stateless witness artifacts

V3.9 truth boundary:

- Do not claim Ethereum-compatible MPT unless Ethereum hexary Patricia trie, nibble path, RLP, and extension/branch/leaf node encoding are fully implemented and tested.
- Do not claim production database durability.
- Do not claim full stateless execution.
- Do not claim full cross-shard proof protocol.
- Do not claim paper-grade benchmark evidence.

Benchmark belongs to the experiment control layer / result layer. V3.10 UI rules:

- Do not add Benchmark as a new main-flow card.
- Benchmark belongs to the experiment control layer / result layer.
- Do not refactor the V3 Composer page.
- Do not change the left navigation.

V3.10 implemented scope:

- benchmark_template_catalog
- baseline_profile_catalog
- local_controlled_sweep_runner
- multi_seed_repeatability
- reproducibility_manifest
- benchmark_report
- benchmark_artifacts

V3.10 truth boundary:

- Do not claim paper-grade benchmark evidence.
- Do not claim large-scale distributed benchmark.
- Do not claim performance superiority over BlockEmulator.
- Do not claim production network.
- Do not claim BlockEmulator backend.
- Do not claim Fabric/EVM live backend.

V3.10.1 truth boundary:

- Do not claim runtime semantics changed.
- Do not claim new cross-shard protocol capability.
- Do not claim new benchmark capability beyond V3.10.
- Do not claim paper-grade benchmark.
- Do not claim production UI or production deployment.

## Compressed Remaining V3 Roadmap After V3.10.1

Current closed stage: V3-final.

Next implementation stage: V3 maintenance only; do not start V4 unless explicitly requested.

Compressed route:

1. V3.11 CrossShard Protocol Closure. Complete.
2. V3.12 Runtime Realism Closure. Complete.
3. V3.13 Metaverse Experiment Suite Closure. Complete.
4. V3-final Fault, Observability, and Reproducibility Closure. Complete.

Rules:

- Do not continue V3.12, V3.13, or V3-final after closure or skip into V4 unless the user explicitly asks.
- Current topology settings are logical topology until `local_multi_process` is implemented.
- V3.5 already introduced launcher preview and node process preview.
- V3.12 hardens V3.5 previews into a runnable local multi-process runtime.
- Do not overclaim preview features as production implementations.

## V3-final Fault, Observability, and Reproducibility Closure

Current implementation stage:
V3-final Fault, Observability, and Reproducibility Closure

Allowed in V3-final:

- frontend/backend stage metadata alignment
- final stage/truth/next-stage cleanup
- deterministic fault injection MVP
- node failure / recovery event model
- network delay / drop event model
- target shard congestion event model
- Relay MVP fault observation using proof_fail / timeout / target_reject semantics
- observability summary
- component health summary
- final artifact catalog
- reproducibility guide
- experiment manual
- paper experiment mapping
- final README / docs / roadmap closure
- final tests for V3 closure

Forbidden in V3-final:

- V3.14
- V4 unless explicitly requested by the user
- new workload suite
- new consensus protocol
- production PBFT / HotStuff / Raft
- production Byzantine adversary model
- multi-server deployment
- production cluster
- Fabric/EVM live backend
- BlockEmulator backend
- real metaverse platform integration
- production cross-chain bridge
- complete Broker / 2PC / Monoxide
- paper-grade performance conclusion
- performance superiority claim
- large UI refactor
- new left navigation
- 3D visualization

Important boundary:

- V3-final closes V3.
- V3-final makes MBE reproducible and observable as a local emulator prototype.
- It is not a production blockchain system.
- It is not a full replacement for BlockEmulator.
- It is not a production monitoring system.
- It is not paper-grade performance evidence by itself.
- After V3-final, do not continue adding features unless the user explicitly starts V4 or a maintenance patch.

## V3 Maintenance: MetaTrack Formal Benchmark Console

Current implementation stage:
V3 maintenance after V3-final closure.

Allowed in this maintenance scope:

- frontend console information architecture cleanup
- separate Draft Smoke quick validation from formal controlled benchmark runs
- MetaTrack formal benchmark design panel
- explicit numeric formal parameters: transaction count, seed count/list, scan points, and baseline IDs
- experiment matrix preview
- resource guards for run count, total transaction count, seed count, transaction count per run, and scan point count
- formal baseline registry using existing runnable plugins only
- single-variable scans for ablation, hotspot sensitivity, cross-shard sensitivity, shard scalability, and control overhead
- multi-seed aggregation with mean/std/min/max/count/ci95
- formal_* CSV/JSON artifacts for paper figure/table preparation
- saved config library for module/workload/topology/method/formal_plan reuse
- workload_comparison formal experiment type
- formal run manifest, progress, failed run index, and child artifact index
- formal result dashboard, chart preview, data-file explanation, preview/download split, ZIP export, and Formal Run History
- formal metric extraction and aggregation repairs for child-run summaries, latency CSVs, missing metric diagnostics, and chart preview data
- formal runtime compatibility diagnostics, including generated Go profile normalization when a saved method uses `MetricsReport=metatrack_metrics`
- Playwright E2E validation that starts the local backend/frontend, opens the V3 console, clicks Formal Run History, and runs a minimal formal workload-comparison workflow
- frontend failure diagnostics that surface `failure_summary.top_errors` instead of forcing users to inspect `formal_failed_runs.csv` manually
- slider/chip usability controls for ratios, workload comparison scenarios, and recommended local emulator presets
- paper_candidate eligibility labels with reasons
- docs, tests, and artifact downloads for the maintenance console

Forbidden in this maintenance scope:

- V4 unless explicitly requested by the user
- new research mechanism
- Fabric/EVM live backend
- BlockEmulator backend
- production multi-server deployment
- production PBFT / HotStuff / Raft
- treating `local_multi_process` as production nodes
- treating `localhost_tcp_preview` as production networking
- including preview/planned plugins in formal benchmark runs
- using vague scale presets for formal benchmark size
- claiming controlled benchmark outputs are paper-final proof
- treating saved configs as production deployment manifests
- admitting `existing_trace_preview` into the default formal benchmark path
- claiming chart previews prove paper-final results

Important boundary:

- Draft Smoke remains a bounded quick validation path.
- Formal MetaTrack benchmark runs are local controlled emulator experiments.
- `logical_single_process` is the default evidence mode for main performance comparisons.
- `local_multi_process_validation` is only a prototype realism validation mode and is affected by local machine scheduling.
- `experiment_evidence_level` must distinguish `quick_validation`, `controlled_benchmark`, and `paper_candidate`.
- A `paper_candidate` label is a readiness label for paper data review, not a claim of final paper-grade evidence.
- Saved method configs must preserve the full 11-module draft, topology, workload source, validation state, and last smoke run ID.
- Formal benchmark profiles must inherit user topology/workload details unless a single scan variable intentionally overrides them.
- Formal result downloads must preserve artifact allowlists and run-directory path boundaries.
- `formal_chart_preview.json` must be derived from aggregate or figure rows and must not fabricate missing metrics.
- Formal metric extraction must not fill missing values with zero. It may derive latency from successful `latency_ms` rows and throughput from explicit success count plus positive elapsed time only.
- Formal Run History is a local convenience view over `.cache/v3_metatrack_formal_runs/`, not a production result database.
- Generated formal Go profiles may normalize metrics reporting to `basic_metrics` for current Go runtime compatibility, but saved configs must remain unchanged.
- Playwright E2E is a maintenance acceptance tool for the local console; it is not a production monitoring system or hosted test service.

## V3.13 Metaverse Experiment Suite Closure

Current implementation stage:
V3.13 Metaverse Experiment Suite Closure

Allowed in V3.13:

- metaverse workload catalog
- metaverse scenario templates
- virtual asset transfer workload
- avatar state update workload
- scene hotspot workload
- item / equipment transfer workload
- cross-scene migration workload
- on-chain + off-chain confirmation workload
- cross-metaverse transfer MVP workload
- mixed metaverse workload
- baseline matrix for implemented/local capabilities
- multi-seed and parameter sweep MVP
- paper table / figure data export
- frontend metaverse experiment configuration and summary
- docs and tests for V3.13

Forbidden in V3.13:

- V3-final fault injection
- node failure / recovery injection
- message delay/drop injection
- production workload dataset claims
- real metaverse platform crawling
- Fabric/EVM live backend
- BlockEmulator backend
- production chain deployment
- production cross-chain bridge
- production PBFT/HotStuff/Raft
- fake runnable Broker/2PC/Monoxide baselines
- paper-grade performance conclusion
- new navigation refactor
- 3D metaverse visualization

Important boundary:

- V3.13 is a controlled metaverse-oriented experiment suite.
- It generates deterministic synthetic/metaverse-style workloads and benchmark artifacts.
- It is not real Roblox/Decentraland/Sandbox platform integration.
- It is not a production data collection system.
- It does not prove paper-grade performance by itself.

## V3.12 Runtime Realism Closure

Current implementation stage:
V3.12 Runtime Realism Closure

Allowed in V3.12:

- local_multi_process runtime mode
- managed local process launcher MVP
- node process lifecycle artifacts
- localhost TCP / NetworkAdapter process path
- shard model
- committee model
- epoch model
- light reconfiguration plan
- frontend process / shard / committee / epoch summary
- docs and tests for V3.12

Forbidden in V3.12:

- V3.13 metaverse workload suite
- paper benchmark export
- Fabric/EVM live backend
- BlockEmulator backend
- multi-server deployment
- production PBFT / HotStuff / Raft
- production cluster
- performance superiority claims

Important boundary:

- V3.5 already introduced launcher preview and node process preview.
- V3.12 hardens those previews into a runnable, observable, cleanable local multi-process runtime.
- Current logical topology remains valid.
- local_multi_process is local-machine only.
- It is not production networking and not multi-machine deployment.

As of the V3.4 runtime self-check, current V3 acceptance is realigned to:

- `V3.0 Planning Scaffold`: complete.
- `V3.1 Profile Layer`: complete.
- `V3.2 Minimal Single-chain Modular Runtime`: historical Python backend reference semantics.
- `V3.2b` / `V3.2.5 Go-backed Minimal Runtime / Go parity`: absorbed into V3.3.
- `V3.3 MetaTrack Plugin Evaluation`.
- `V3.3.1 Research-chain Role Separation`.
- `V3.3.2 Single-chain Modular Composer Profile / Experiment Templates`.
- `V3.3.3 Single-chain Composer Frontend MVP`.
- `V3.3.4 Composer Chinese Localization and Snake Layout Polish`.
- `V3.3.5a Interactive Single-chain Composer Draft UI`.
- `V3.3.5b Backend Draft Validation and Draft Smoke Run`.
- `V3.3.6 Draft Run Result UX and History Management`.
- `V3.3.7 Boundary, Documentation, and Skill Closure`.
- `V3.4.0 Runtime Self-check and Scope Realignment`.
- `V3.4.1 Runtime Plugin Hardening: FIFO TxPool`.
- `V3.4.2 Runtime Plugin Hardening: BlockProducer`.
- `V3.4.3 Runtime Plugin Hardening: Consensus-light`.
- `V3.4.4 Single-module Experiment Templates`.
- `V3.4.9 MetaTrack Ablation Templates`.
- `V3.4.10 Controlled Smoke Runner`.
- `V3.4.11 Stage / Version / Frontend / Docs / Skill Closure`.
- `V3.5 Node-level Emulator Skeleton`.
- `V3.5.1 Logical Node Topology Runtime`.
- `V3.5.2 Local Multi-process Launcher Preview`.
- `V3.5.3 Local Node Process Runtime`.
- `V3.5.4 V3.5 Closure`.
- `V3.6 NetworkAdapter and TCP Typed Message Runtime`.
- `V3.6.1 NetworkAdapter + localhost TCP + typed messages`.
- `V3.7 ConsensusRuntime and BlockEmulator-aligned PBFT Preview`.
- `V3.8 CrossShardProtocol Skeleton Closure`.
- `V3.9 State Authenticity Layer MVP Closure`.
- `V3.10 Benchmark / Experiment Template Hardening Closure`.
- `V3.10.1 Frontend UX and Chinese Console Cleanup Closure`.
- `V3.11 CrossShard Protocol Closure`.
- `V3.12 Runtime Realism Closure`.
- `V3.13 Metaverse Experiment Suite Closure`.
- `V3-final Fault, Observability, and Reproducibility Closure`.
- `V3 maintenance only unless V4 is explicitly requested`.

V3.4 series goal: harden critical foundation modules in the V3 Go-backed modular research chain runtime into observable runtime behavior, add controlled smoke comparison, and close stage/version/frontend/docs/skill wording before V3.5. V3.4 remains a local modular research chain runtime. It is not Fabric live execution.

Every V3.4.x runtime hardening substage must include corresponding frontend alignment. When runtime adds an artifact, summary metric, or module truth boundary, frontend artifact grouping, result summary, history detail, and module detail must align in the same implementation stage. Runtime must not output a new artifact that the frontend cannot download or explain.

V3.5, V3.6, V3.7, V3.8, V3.9, V3.10, V3.10.1, V3.11, V3.12, V3.13, and V3-final are closed. Do not continue adding features after V3-final unless the user explicitly asks for a maintenance patch or starts V4.

Deferred / future scope:

- Minimal dual-chain runtime.
- MetaFlow Protocol Plugin and AFS/FDA.
- Multi-machine network emulator behavior.

Existing MetaFlow profiles remain planned preview profiles and must not become runnable unless a later user request explicitly reopens that scope.

## 1. Scope

This skill governs V3 work for MBE.

V3 builds a MetaTrack-oriented modular plugin chain runtime and is currently closed through V3-final Fault, Observability, and Reproducibility Closure. Fabric/EVM live validation is deferred unless explicitly reopened. V3 reuses V2 experiment management, artifacts, sweeps, reports, calibration, and frontend shell. MetaFlow dual-chain protocol evaluation is retained only as planned preview / future roadmap material in the current V3 scope.

V3 positioning:

```text
V3 = Modular Plugin Chain Runtime with node topology, NetworkAdapter, ConsensusRuntime, CrossShardProtocol skeleton, State Authenticity MVP, benchmark hardening, controlled metaverse workload suite, deterministic fault injection MVP, observability summary, and reproducibility bundle
V3 = 面向 MetaTrack 的模块化插件链实验平台；V3-final 已完成故障、观测与复现收口；Fabric/EVM live validation 和 MetaFlow 保留为 planned preview / future roadmap。
```

Fabric/EVM live backend work is not part of V3.5.4. V3.5 closes local topology, launcher preview, and node process preview only, not an automatic Fabric/EVM live implementation.

## 2. V3 Non-goals

V3 is not:

- Not a production blockchain.
- Not a production cross-chain bridge.
- Not a public-chain deployment platform.
- Not a Fabric peer internal patch.
- Not a replacement for Fabric.
- Not a multi-public-chain production system.
- Not allowed to present `local_virtual`, replay, or Draft Smoke results as real-chain results.
- Not allowed to present smoke-level results as final paper evidence.

V3 must not claim production security, production availability, public-chain deployment, or complete Fabric replacement.

## 3. Stage Rules

Only one V3 stage may be implemented per round. Do not jump stages. Do not implement future planned components early.

Current V3.3 note: V3.3 absorbs the earlier V3.2b / V3.2.5 Go-backed parity stage. Gate A is Go-backed minimal runtime parity. Gate B is Go-backed MetaTrack plugin combinations and fair ablation. Gate C is the single-chain Composer Draft loop: frontend draft, backend validate-draft, backend run-draft-smoke, result display, local history, and artifact downloads. Do not implement Fabric validation, dual-chain runtime, MetaFlow, AFS, or FDA in V3.3.

Current V3.3.6 + V3.3.7 note: the latest stable V3.3 surface is a single-chain modular research chain composer. It supports composer preview, frontend Draft configuration, backend `validate-draft`, backend `run-draft-smoke`, Draft run history under `.cache/v3_draft_runs/`, and artifact download. Draft Smoke is a local single-configuration smoke path for debugging, demonstration, and configuration traceability. It is not a formal paper experiment, not Fabric-backed, not MetaFlow, and not dual-chain.

V3 stages:

- `V3.0 Planning Scaffold`: docs and skill only. Defines stage roadmap, boundaries, profiles, fair baseline policy, and acceptance checklist. No code.
- `V3.1 Profile Layer`: only ChainProfile / PluginProfile / ExperimentProfile schema, loader, validator, preview, and planned/runnable guard.
- `V3.2 Minimal Single-chain Modular Runtime`: historical minimal single-chain modular research chain runtime target.
- `V3.3 MetaTrack Plugin Evaluation`: only MetaTrack plugin combinations and fair single-chain evaluation on the Go-backed runtime.
- `V3.3.1 Research-chain Role Separation`: only role-separated single-chain runtime abstractions and artifacts for consensus domain, execution shard, state storage unit, placement, routing, and remote state access.
- `V3.3.2 Single-chain Modular Composer Profile / Experiment Templates`: only template/profile/composer preview/fairness scope metadata for single-chain experiments.
- `V3.3.3 Single-chain Composer Frontend MVP`: only the first readable V3 Composer page.
- `V3.3.4 Composer Layout Polish`: only localization and fixed snake layout polish.
- `V3.3.5a Interactive Composer Draft UI`: only frontend local Draft configuration and local validation.
- `V3.3.5b Backend Draft Validation and Draft Smoke`: only backend authority validation and single Draft Smoke run.
- `V3.3.6 Draft Run Result UX and History`: only result display, local history management, and artifact browsing for Draft Smoke.
- `V3.3.7 Boundary and Skill Closure`: only docs, boundary wording, and skill updates.
- `V3.4.0 Runtime Self-check and Scope Realignment`: only self-check, documentation, and roadmap/skill realignment.
- `V3.4.1 Runtime Plugin Hardening: FIFO TxPool`: only real FIFO TxPool runtime behavior, observability, and frontend alignment.
- `V3.4.2 Runtime Plugin Hardening: BlockProducer`: only BlockProducer hardening after TxPool is observable, plus frontend alignment for new producer artifacts/metrics.
- `V3.4.3 Runtime Plugin Hardening: Consensus-light`: only lightweight truthful consensus model hardening, not PBFT/HotStuff/Raft, plus frontend alignment for consensus-light status/metrics.
- `V3.4.4 Single-module Experiment Templates`: only single-module experiment templates, fairness validation, and frontend alignment for those templates.
- `V3.4.9 MetaTrack Ablation Templates`: only MetaTrack ablation template and preset metadata.
- `V3.4.10 Controlled Smoke Runner`: only the controlled five-preset smoke runner, controlled artifacts, readiness report, API, frontend panel, and tests.
- `V3.4.11 Stage / Version / Frontend / Docs / Skill Closure`: only closure alignment; no new runtime mechanism.
- `V3.5.1 Logical Node Topology Runtime`: only frontend topology config, backend topology validation, single-process logical nodes, node/network/message logs, summary metrics, docs, and tests.
- `V3.5.2 Local Multi-process Launcher Preview`: only launch script/address table preview; no real TCP or PBFT.
- `V3.5.3 Local Node Process Runtime`: only local process role entry points after launcher preview.
- `V3.5.4 V3.5 Closure`: only closure alignment and validation.
- `V3.6.1 NetworkAdapter + localhost TCP + typed messages`: configurable NetworkAdapter, in-memory compatibility, localhost TCP typed message preview, and typed message artifacts. Not consensus-light over network, not real PBFT, and not production networking.
- `V3.6.2 Consensus-light over NetworkAdapter + V3.6 Closure`: implemented; consensus-light proposal/vote preview over selected adapter and closure wording. Not PBFT PrePrepare/Prepare/Commit.
- `V3.7.1 ConsensusRuntime Plugin Schema + PBFT State Machine Preview`: implemented; configurable ConsensusRuntime with BlockEmulator-aligned PBFT state machine preview as one optional plugin. Not production PBFT and not a BlockEmulator code copy.
- `V3.7.2 BlockEmulator-aligned PBFT over NetworkAdapter + V3.7 Closure`: implemented; connects PBFT preview to NetworkAdapter and closes V3.7. Not production PBFT.
- `V3.8 CrossShardProtocol Skeleton Closure`: implemented; adds CrossShardProtocol config, cross-shard detection preview, relay_preview skeleton artifacts, frontend summary, and closure. It is not complete Relay, Broker, 2PC, atomic cross-shard commit, state proof, rollback, timeout recovery, or BlockEmulator full cross-shard backend.
- `V3.9 State Authenticity Layer MVP Closure`: implemented; adds selectable state_backend, persistent_kv MVP, merkle_trie_mvp roots, proof generation / verification, witness artifacts, frontend summary, and closure. It is not Ethereum-compatible MPT, not production database durability, not full stateless execution, and not a complete cross-shard state proof protocol.
- `V3.10 Benchmark / Experiment Template Hardening Closure`: implemented; adds benchmark template catalog, baseline profile catalog, local controlled sweep runner MVP, repeatability, reproducibility manifest, benchmark report, frontend summary, and closure. It is not paper-grade benchmark evidence and not a large-scale distributed benchmark.
- `V3.10.1 Frontend UX and Chinese Console Cleanup Closure`: implemented; adds simplified navigation, Chinese console labels, HelpTip progressive disclosure, run progress feedback, lightweight result chart preview, and visual cleanup. It does not change runtime semantics or add V3.11 capability.
- `V3.11 CrossShard Protocol Closure`: implemented; adds runnable `relay_mvp` with SourceLock, RelayCertificate, proof/certificate verification records, target commit, source finalization, timeout/refund/abort paths, artifacts, frontend summary, and closure. It is not production atomic cross-shard commit, complete Broker/2PC/Monoxide, or Byzantine-secure relay.
- `V3.12 Runtime Realism Closure`: implemented; adds local_multi_process dry_run/smoke, max local process guard, node lifecycle artifacts, localhost TCP process path records, committee/epoch MVP, and light reconfiguration plan. It is not multi-server deployment or a production cluster.
- `V3.13 Metaverse Experiment Suite Closure`: implemented; adds controlled metaverse workload catalog, scenario templates, baseline matrix, multi-seed/sweep MVP, and paper table/figure data export scaffold. It is not real platform trace collection or paper-grade evidence.
- `V3-final Fault, Observability, and Reproducibility Closure`: implemented; adds frontend/backend metadata alignment, deterministic fault injection MVP, local observability summary, component health status, final artifact catalog, reproducibility guide, experiment manual, paper experiment mapping, frontend summary, and closure. It is not production fault tolerance, production monitoring, a Byzantine adversary model, or paper-grade evidence.

Stage constraints:

```text
V3.0 only docs and skill.
V3.1 only profile layer.
V3.2 only minimal single-chain runtime semantics.
V3.3 only MetaTrack plugins/evaluation and Composer Draft Smoke.
V3.4.0 only runtime self-check and scope realignment.
V3.4.1 only FIFO TxPool hardening and frontend alignment.
V3.4.2 only BlockProducer hardening and frontend alignment.
V3.4.3 only Consensus-light hardening and frontend alignment.
V3.4.4 only single-module experiment templates and frontend alignment.
V3.4.9 only MetaTrack ablation templates.
V3.4.10 only controlled smoke runner.
V3.4.11 only stage/version/frontend/docs/skill closure.
V3.5.1 only logical node topology runtime.
V3.5.2 only local multi-process launcher preview.
V3.5.3 only local node process runtime.
V3.5.4 only V3.5 closure.
V3.6.1 only NetworkAdapter and TCP typed message preview.
V3.6.2 implemented consensus-light over NetworkAdapter plus V3.6 closure.
V3.7.1 implemented configurable ConsensusRuntime and PBFT state machine preview. V3.7.2 implemented PBFT preview over NetworkAdapter and closed V3.7. V3.8 implemented CrossShardProtocol skeleton and closed V3.8. V3.9 implemented State Authenticity Layer MVP and closed V3.9. V3.10 implemented Benchmark / Experiment Template Hardening and closed V3.10. V3.10.1 implemented Frontend UX and Chinese Console Cleanup and closed V3.10.1. V3.11 implemented CrossShard Protocol Closure and closed V3.11. V3.12 implemented Runtime Realism Closure and closed V3.12. V3.13 implemented Metaverse Experiment Suite Closure and closed V3.13. V3-final implemented Fault, Observability, and Reproducibility Closure and closes V3.
V3.10.1, V3.11, V3.12, V3.13, and V3-final are closed; do not continue adding features after closure unless the user explicitly asks for maintenance or starts V4.
```

## 4. V3.4.1 Runtime Plugin Hardening: FIFO TxPool Boundary

V3.4.1 allows:

- Implement a real FIFO TxPool runtime object.
- Support `Admit`, `SelectForBlock`, dedup, capacity, and reject/backpressure.
- Make BlockProducer select transactions from TxPool instead of directly slicing transactions.
- Add `txpool_log.csv`.
- Make `queue_wait_ms` derive from TxPool wait statistics instead of being fixed to 0.
- Add or realign summary TxPool metrics.
- Reserve or connect artifact allowlist, Draft Smoke history, and frontend display for `txpool_log.csv`.
- Keep V3 Draft Smoke and the existing MetaTrack four combinations runnable.

V3.4.1 frontend alignment requires:

- `frontend/src/api.ts`: add or remain compatible with `txpool_admitted_count`, `txpool_rejected_count`, `txpool_peak_size`, `txpool_avg_wait_ms`, `txpool_p95_wait_ms`, and `queue_wait_ms`.
- `frontend/src/components/v3/DraftRunResultPanel.tsx`: show average queue wait, peak pool size, admitted count, rejected count, and TxPool artifact availability.
- `frontend/src/components/v3/ArtifactGroups.tsx`: include `txpool_log.csv` under Runtime queue logs, TxPool logs, or Chain runtime logs. Do not label it as Fabric or paper-grade evidence.
- `frontend/src/components/v3/DraftRunHistoryPanel.tsx`: show or link historical runs containing `txpool_log.csv`; treat old runs without it as legacy missing artifacts, not errors.
- `frontend/src/components/v3/ModuleDetailPanel.tsx`: distinguish `fifo_pool` as runtime-realized after V3.4.1 implementation, while `priority_pool`, `hotspot_aware_pool`, and `fee_based_pool` remain planned.
- `frontend/src/components/v3/ModuleCard.tsx`: distinguish configured runnable, runtime-supported, preview-only, and planned.
- `frontend/src/pages/V3ComposerPage.tsx`: preserve or strengthen boundary wording: single-chain, Go Runtime, Smoke experiment, non-Fabric, non-MetaFlow, non-PBFT / HotStuff / Raft, and non-multi-node network. It may add TxPool runtime hardening, FIFO pool only, and local modular runtime.

V3.4.1 frontend non-goals:

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

V3.4.1 forbids:

- Real PBFT / HotStuff / Raft implementation.
- Fabric / EVM live backend.
- Fabric Docker / `network.sh` automatic startup.
- MetaFlow.
- Dual-chain runtime.
- Cross-chain bridge.
- Relay / broker / 2PC cross-shard protocol.
- Dynamic resharding.
- Committee lifecycle.
- State migration.
- State root / persistent KV / Merkle proof / snapshot.
- Multi-process / multi-machine networking.
- Describing Draft Smoke as formal paper experiment evidence or real-chain execution.

## 5. V3.4.3 Runtime Plugin Hardening: Consensus-light Boundary

V3.4.3 is Consensus-light only. It may add local virtual-time consensus-light models after V3.4.2 BlockProducer hardening, but it must not become a real PBFT / HotStuff / Raft implementation.

V3.4.3 allows:

- Keep `simple_leader` as the default consensus plugin.
- Add `poa_light` as a lightweight authority confirmation model.
- Add `pbft_light_model` as a PBFT stage and message-count model.
- Model PrePrepare / Prepare / Commit / Finalized stages for observability.
- Account for `f = (N - 1) / 3`, prepare quorum `2f + 1`, and commit quorum `2f + 1`.
- Add consensus-light summary metrics for consensus latency, message count, round count, finalized block count, failed block count, and view change count.
- Add `consensus_log.csv` in the later V3.4.3b code stage.
- Align frontend result panel, history, module detail, module card, Composer page, and artifact grouping for consensus-light fields and `consensus_log.csv`.

V3.4.3 forbids:

- Real PBFT or production PBFT.
- HotStuff.
- Raft.
- Real TCP networking.
- `net.Listen`, `TcpDial`, or `networks.Broadcast`.
- Goroutine-based node message handling.
- Real view-change safety.
- Real Byzantine fault injection.
- Fabric / EVM live backend.
- Fabric Docker / `network.sh`.
- MetaFlow.
- Dual-chain runtime.
- Cross-chain bridge.
- Relay / broker / 2PC.
- Dynamic resharding.
- Committee lifecycle.
- State migration.
- State root / persistent KV / snapshot.
- Multi-process / multi-machine networking.
- Paper-ready sweep or full dashboard.

`pbft_light_model` must always be described as a local light model that simulates PBFT-style stages, quorum accounting, and virtual message counts. It is not real PBFT, not production BFT safety, and not multi-node network consensus.

## 6. Mandatory Start Check

Every V3 round must start with:

```powershell
cd F:\Metaverse_Blockchain_Env
git status --short
```

If `git status --short` is not empty, stop and report unless the user explicitly identifies the existing changes as expected and authorizes continuing on top of them. Do not overwrite user changes.

## 7. No Push Rule

Codex may commit only when explicitly asked and after validation. Codex must not push unless the user explicitly asks for push.

Codex must check `git status --short` before each V3 work round. If the worktree is dirty and the user did not explicitly allow continuing on top of those changes, stop and report.

## 8. Data Truth Rules

V3 must preserve and extend data truth labels:

```text
synthetic_replay
existing_trace_replay
fabric_chain_backed_trace_replay
fabric_live_validation
modular_runtime
modular_runtime_calibrated
public_chain_imported_trace_semantic_unknown
planned_cross_chain_replay
```

Definitions:

- `synthetic_replay`: local generated workload and replay. Not real chain execution.
- `existing_trace_replay`: replay of an existing trace file. No chain is launched.
- `fabric_chain_backed_trace_replay`: replay or analysis of an existing Fabric trace. The web/API layer does not start Fabric.
- `fabric_live_validation`: small-scale validation on a real Fabric network. This is validation, not the main V3 runtime and not a Fabric peer patch.
- `modular_runtime`: result produced by the V3 modular research chain runtime.
- `modular_runtime_calibrated`: V3 modular runtime result using a Fabric calibration profile.
- `public_chain_imported_trace_semantic_unknown`: imported public-chain trace with unknown semantics; no default reliable access-set, delta, or commutativity semantics.
- `planned_cross_chain_replay`: planned cross-chain replay marker; never runnable by itself.

## 9. Backend / Runtime Types

V3 backend/runtime types:

- `local_virtual`: V2 local virtual-time backend. Not real chain execution.
- `trace_replay`: replay backend over existing trace. Does not launch a chain.
- `modular_research_chain`: V3 self-developed modular research chain runtime.
- `fabric_validation`: Fabric-backed validation backend for observation/calibration, not the main experiment kernel.
- `fabric_live_planned`: planned Fabric live backend. Not runnable until its stage explicitly implements it.
- `evm_live_planned`: planned EVM live backend. Not runnable until its stage explicitly implements it.

Backend truth must be displayed in UI, metadata, reports, and artifacts. Planned backend types must not have run buttons or execution paths.

## 10. Fair Baseline Rules

MetaTrack V3.3 comparisons must use identical workload, seed, ChainProfile, hardware profile, block config, consensus config, submit rate, and network profile. Only the following plugin classes may differ:

```text
ShardingPlugin
ExecutionSchedulerPlugin
StateAccessPlugin
CommitPlugin
```

Runtime Hardening / Single-module Test fairness rules:

1. TxPool single-module test:
   Only `TxPoolPlugin` may differ. Workload, BlockProducer, Consensus, Routing, Execution, StateAccess, StateStorage, Commit, Metrics, seed, submit rate, block config, and network profile must be fixed.

2. BlockProducer single-module test:
   Only `BlockProducer` may differ. Workload, TxPool, Consensus, Routing, Execution, StateAccess, StateStorage, Commit, Metrics, seed, submit rate, and network profile must be fixed.

3. Consensus-light single-module test:
   Only `ConsensusPlugin` may differ. Workload, TxPool, BlockProducer, Routing, Execution, StateAccess, StateStorage, Commit, Metrics, seed, submit rate, block config, and network profile must be fixed.

Forbidden fairness shortcuts:

- Do not create advantage by changing workload.
- Do not create advantage by changing seed.
- Do not create advantage by changing submit rate.
- Do not create advantage by changing block config, network profile, or hardware profile.
- Do not create advantage by hiding metrics or artifacts in the frontend.
- Do not present smoke-level results as paper-grade results.

## 11. Artifact Rules

Every V3 modular runtime run after V3.4.1 should include at least:

```text
used_chain_profile.yaml/json
used_plugin_profile.yaml/json
used_experiment_profile.yaml/json
runtime.log
summary.csv/json
report.md
block_log.csv
tx_results.csv
state_commit_log.csv
txpool_log.csv
```

`txpool_log.csv` is a V3.4.1 runtime hardening artifact. It belongs to the local modular runtime / Draft Smoke artifact set. It is not paper-grade final evidence by itself. It must explain TxPool admit, select, reject, queue wait, and pool size changes.

`consensus_log.csv` may be added in the later V3.4.3b code stage. It is a local modular runtime / Draft Smoke artifact for consensus-light stage, quorum, virtual message count, and finality observability. It is not a Fabric artifact and not paper-grade final evidence by itself.

Frontend artifact rules:

- `txpool_log.csv` must be downloadable from Draft Smoke current results and history when present.
- Old Draft Smoke runs that do not contain `txpool_log.csv` must remain viewable.
- Artifact grouping, summary preview, and history detail must align with every new V3.4.x runtime artifact.
- Runtime must not produce a new artifact that the frontend cannot download or explain.

MetaTrack aggregate runs additionally output:

```text
metatrack_summary.csv/json
metatrack_latency.csv
metatrack_mechanism_metrics.csv
```

Future Fabric validation runs, if a later stage explicitly reopens that scope, additionally output:

```text
fabric_validation_summary.csv/json
fabric_tx_results.csv
fabric_commit_latency.csv
fabric_validation_report.md
```

MetaFlow runs remain deferred / future scope and may not become runnable in V3.4.

Do not commit generated artifacts, caches, run directories, large traces, or frontend build output.

Draft Smoke artifacts are temporary local artifacts only:

```text
.cache/v3_draft_runs/<run_id>/
```

They may include `composer_draft.json`, `normalized_draft.json`, `draft_validation.json`, generated experiment/plugin profiles, runtime logs, summaries, transaction/state logs, and, after V3.4.1, `txpool_log.csv`. These files must not be copied into `configs/` and must not be committed to Git.

Built-in smoke vs Draft Smoke:

- built-in smoke runs the existing MetaTrack Go-backed smoke path;
- Draft Smoke runs exactly the current Composer Draft single configuration;
- Draft Smoke must not auto-expand the MetaTrack ablation matrix;
- Draft Smoke must not be described as paper-grade evidence.

## 12. V3.4.1 Frontend Acceptance

After V3.4.1 code implementation:

- Draft Smoke result panel shows TxPool summary metrics.
- Artifact area can download `txpool_log.csv`.
- History can show runs with `txpool_log.csv`; older runs without it do not crash.
- TxPool module detail explains FIFO pool as the hardening target and other TxPool plugins as planned.
- Page still clearly says non-Fabric, non-MetaFlow, non-PBFT, and non-multi-node network.
- `npm.cmd run build` passes.

## 13. Validation Commands

Docs/config-only:

```powershell
git diff --check
git status --short
```

Python backend modified:

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

Go modified:

```powershell
cd executor
go test ./...
cd ..
```

If validation cannot be completed, do not commit and report the blocker.

## 14. Final Report Format

Every V3 final report must include:

```text
1. 本轮阶段
2. 实现内容
3. 新增/修改文件
4. 未实现内容
5. 阶段边界检查
6. Data truth / backend truth 影响
7. Artifacts / outputs
8. 兼容性
9. 测试与验证结果
10. git status
11. commit hash
12. 是否 push：必须说明未 push
```

## 15. Strict Truthfulness

Do not claim V3 runtime exists before V3.2.
Do not claim MetaTrack V3 experiment exists before V3.3.
Do not claim V3.4.1 implements Fabric validation.
Do not claim Fabric validation exists in current V3.4.11 closure.
Do not claim FIFO TxPool hardening makes MBE a full BlockEmulator-like emulator.
Do not claim TxPool hardening provides real multi-node or real network execution.
Do not claim PBFT / HotStuff / Raft before their actual runtime implementation.
Do not claim `pbft_light_model` is real PBFT.
Do not claim V3.4.3 has production-grade BFT safety.
Do not claim V3.4.3 has real multi-node or network consensus.
Do not claim `txpool_log.csv` is paper-grade evidence by itself.
Do not claim `consensus_log.csv` is Fabric evidence or paper-grade evidence by itself.
Do not claim frontend display of TxPool metrics makes Draft Smoke a formal result database or paper-grade result.
Do not claim MetaFlow exists in current V3.4 scope.
Do not claim production bridge support at any point.

Planning docs may describe future goals, interfaces, artifacts, and stage boundaries. They must not present planned capabilities as implemented or runnable.
