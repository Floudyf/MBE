# V3.7 ConsensusRuntime and BlockEmulator-aligned PBFT Preview Plan

## 1. Goal

V3.7 plans to strengthen consensus from consensus-light into selectable `ConsensusRuntimePlugin` implementations.

PBFT should not be hardcoded as the only consensus. It should be one selectable runtime option. BlockEmulator alignment should enter MBE as an optional implementation path while MBE remains a modular metaverse blockchain experiment platform with its existing composer, plugin profiles, topology profiles, artifact system, metaverse workload, controlled smoke, and baseline/ablation capabilities.

V3.7 is still not production PBFT and should not directly copy BlockEmulator code.

## 2. ConsensusRuntime Options

Planned selectable `ConsensusRuntime` values:

- `simple_leader`
- `poa_light`
- `pbft_light_model`
- `blockemulator_aligned_pbft_preview`
- `future_hotstuff_preview`
- `future_raft_preview`

`simple_leader`, `poa_light`, and `pbft_light_model` may remain lightweight / model-based runtimes. `blockemulator_aligned_pbft_preview` is the V3.7 focus. `future_hotstuff_preview` and `future_raft_preview` are planned placeholders only and should not be implemented in V3.7 unless a later request explicitly opens them.

## 3. V3.7 Stages

### V3.7.1 ConsensusRuntime Plugin Schema + PBFT State Machine Preview

Implemented scope:

- Add a `ConsensusRuntimePlugin` concept.
- Keep `ConsensusPlugin` as the algorithm selection concept.
- Use `ConsensusRuntimePlugin` to describe how that algorithm runs under topology + selected `NetworkAdapter`.
- Introduce `blockemulator_aligned_pbft_preview` as an optional runtime plugin.
- Keep `simple_leader`, `poa_light`, and `pbft_light_model` available; PBFT preview is not hardcoded as the only consensus path.

The BlockEmulator-aligned PBFT preview should align with these core structures:

- `ShardID` / `NodeID`
- validator set
- node address table
- view
- `sequence_id`
- `block_height`
- `request_pool`
- `prepare_confirm_map`
- `commit_confirm_map`
- `pbft_stage`
- leader / primary policy
- `2f + 1` quorum

Planned PBFT stages:

- `PrePrepare`
- `Prepare`
- `Commit`
- `Finalized`

Planned PBFT message types:

- `pbft_preprepare`
- `pbft_prepare`
- `pbft_commit`
- `pbft_finalized`

Expected artifacts:

- `pbft_state_log.csv`
- `pbft_message_log.csv`
- `quorum_log.csv`
- `finalized_block_log.csv`

Expected summary metrics:

- `consensus_runtime_selected`
- `pbft_view`
- `pbft_sequence`
- `pbft_preprepare_count`
- `pbft_prepare_count`
- `pbft_commit_count`
- `pbft_quorum_reached_count`
- `pbft_finalized_block_count`
- `pbft_consensus_latency_ms`
- `pbft_preview_enabled`
- `pbft_quorum_threshold`

V3.7.1 is implemented as a deterministic state machine preview path. It is not full PBFT over TCP and does not implement production view-change, checkpoint, or signature hardening.

### V3.7.2 BlockEmulator-aligned PBFT over NetworkAdapter + V3.7 Closure

Planned scope:

- Run `blockemulator_aligned_pbft_preview` over:
  - `in_memory_message_bus`
  - `localhost_tcp_preview`
- Reuse the V3.6 `MessageEnvelope` and `NetworkAdapter`.
- Let the leader send `PrePrepare`.
- Let validators validate digest / sequence after `PrePrepare`, then broadcast `Prepare`.
- Let validators broadcast `Commit` after enough `Prepare` messages.
- Let validators mark finalized after enough `Commit` messages.
- Output `consensus_network_log.csv`.
- Update the frontend with a small result summary only; do not refactor the main V3 Composer page.

V3.7.2 has not started.

## 4. BlockEmulator Alignment Without Code Copy

V3.7 should align with BlockEmulator concepts where useful:

- node model
- TCP communication model
- PBFT message stages
- quorum statistics
- view / sequence / requestPool / confirm map ideas

V3.7 must not directly copy BlockEmulator code or turn MBE into a BlockEmulator replica. MBE remains a modular experiment platform with:

- frontend composer
- plugin profile
- topology profile
- artifact system
- metaverse workload
- controlled smoke
- baseline / ablation capability

## 5. Truth Boundary

V3.7 is a BlockEmulator-aligned PBFT preview.

V3.7 is not:

- production PBFT
- a claim of full Byzantine safety
- full checkpoint / stable checkpoint unless a later hardening stage explicitly adds it
- full view-change hardening unless explicitly planned later
- signature / verification hardening unless explicitly planned later
- HotStuff/Raft implementation
- a real cross-shard protocol
- Fabric/EVM live backend
- paper-grade benchmark evidence by default

V3.7 results remain smoke / controlled preview output unless later promoted to formal benchmark through explicit validation scope.

## 6. Next Stage

After V3.7 closure, V3.8 should introduce a CrossShardProtocol skeleton. V3.8 should remain separate from PBFT hardening and should not retroactively claim V3.7 implemented a real cross-shard protocol.
