# V3 Remaining Roadmap After V3.10.1

## 1. Current Baseline

V3.10.1 is complete. It only cleaned up the Chinese frontend console, simplified navigation, added HelpTip explanations, added run progress feedback, added lightweight result chart previews, and improved UX presentation.

V3.10.1 did not change Go runtime semantics, did not add cross-shard protocol capability, and did not make benchmark output paper-grade evidence. V3.11, V3.12, V3.13, and V3-final are now closed; the next stage is V3 maintenance only unless the user explicitly starts V4.

Latest maintenance capability: saved configuration workflow and workload-comparison formal benchmark. It separates Draft Smoke quick validation from formal controlled benchmark runs, lets users save and reuse method/workload/topology configs, uses explicit numeric parameters, previews the experiment matrix, enforces resource guards, and exports formal_* CSV/JSON data for paper figure/table preparation. It remains local emulator evidence, not production-chain or paper-final proof.

## 2. Existing Foundations

- V3.5 already has logical topology, launcher preview, and node process preview.
- V3.6 already has `NetworkAdapter` and the `localhost_tcp_preview` typed message path.
- V3.7 already has PBFT preview.
- V3.8 already has `relay_preview` skeleton.
- V3.9 already has State Authenticity MVP.
- V3.10 already has benchmark template, baseline, sweep, and report foundations.
- V3.10.1 already has the Chinese frontend console.

Current frontend shard/node settings are logical topology. They do not mean the system has already started the same number of real OS processes.

V3.12 hardened the V3.5 launcher and node process previews into a runnable, observable, stoppable small-scale local multi-process runtime. V3.13 builds on that runtime with controlled metaverse workload and benchmark/export artifacts. V3-final closes the emulator prototype with deterministic fault injection, observability summaries, final artifact catalog, reproducibility guide/manual, and paper experiment mapping.

## 3. Compressed Remaining Route

1. V3.11 CrossShard Protocol Closure. Complete.
2. V3.12 Runtime Realism Closure. Complete.
3. V3.13 Metaverse Experiment Suite Closure. Complete.
4. V3-final Fault, Observability, and Reproducibility Closure. Complete.
5. V3 maintenance only. Next unless V4 is explicitly requested.

Current maintenance notes: `docs/v3_maintenance_metatrack_formal_benchmark_console.md` and `docs/v3_maintenance_saved_config_workflow.md`.

## 4. Stage Details

### V3.11 CrossShard Protocol Closure

Combined scope:

- Relay MVP
- Relay state machine
- SourceLock
- RelayCertificate
- Target verification
- Target commit
- Source finalization
- Proof / certificate verification records
- Timeout / refund / abort paths
- Frontend result summary
- ArtifactGroups integration

After V3.11, MBE has a runnable cross-shard Relay MVP with success and failure paths.

Status: complete.

Boundary: not production atomic commit, not complete Broker / 2PC / Monoxide, and not Byzantine-secure relay.

### V3.12 Runtime Realism Closure

Combined scope:

- Managed local process launcher
- `local_multi_process` runtime mode
- address table
- node process lifecycle
- stdout / stderr logs
- NetworkAdapter / localhost TCP process path
- shard model
- committee model
- epoch model
- light reconfiguration plan
- frontend process / shard / committee summary

After V3.12, MBE can run a small local multi-process sharded runtime with node process status, shard assignment, committee assignment, and epoch summary.

Boundary: local multi-process only, not multi-server deployment, not production cluster, and not production PBFT/HotStuff.

Status: complete.

### V3.13 Metaverse Experiment Suite Closure

Combined scope:

- metaverse workload catalog
- virtual asset transfer
- avatar state update
- popular scene hotspot
- equipment / item transfer
- cross-scene asset migration
- on-chain + off-chain confirmation
- cross-metaverse transfer trace MVP
- baseline matrix
- multi-seed sweep
- paper table CSV
- paper figure data CSV
- frontend metaverse result summary

After V3.13, MBE can run metaverse-oriented experiments and export controlled paper experiment data.

Status: complete.

Boundary: controlled metaverse workload suite and paper export scaffold only. Do not claim real platform trace collection, production integration, or paper-grade results until large-scale controlled experiments are actually run and analyzed.

### V3-final Fault, Observability, and Reproducibility Closure

Combined scope:

- node failure
- node recovery
- message delay
- message drop
- target shard congestion
- proof verification failure
- relay timeout
- observability summary
- final README
- reproducibility guide
- experiment manual
- artifact catalog
- truth boundary
- paper experiment mapping

After V3-final, MBE V3 becomes a reproducible metaverse-oriented modular sharded blockchain emulator prototype.

Boundary: prototype / emulator, not production blockchain, not production fault tolerance, not production monitoring, not Byzantine adversary modeling, not paper-grade evidence, and not a full replacement of BlockEmulator.

Status: complete.

## 5. Truth Boundary

Do not claim production-grade blockchain. Do not claim production PBFT / HotStuff. Do not claim production fault tolerance, production monitoring, or Byzantine adversary modeling. Do not claim Ethereum-compatible MPT unless implemented. Do not claim full stateless execution unless implemented. Do not claim complete Broker / 2PC / Monoxide unless implemented. Do not claim paper-grade benchmark results before actual large-scale experiments. Do not claim BlockEmulator replacement.

Recommended positioning:

```text
MBE is a metaverse-oriented modular sharded blockchain emulator prototype.
MBE 是面向元宇宙场景的模块化分片区块链实验平台原型。
```

## 6. Final V3 Target

Frontend: Chinese experiment console, topology config, module selection, run progress, charts, artifact download.

Backend: experiment templates, baseline catalog, run history, artifact manager, validation guards.

Go runtime: txpool, block producer, consensus preview, routing/sharding, Relay MVP, state authenticity MVP, local multi-process runtime, shard/committee/epoch model, fault injection MVP.

Experiment: metaverse workload, cross-shard experiment, hotspot experiment, cross-scene transfer, on-chain + off-chain confirmation, multi-seed benchmark, paper table / figure export.
