# V3.6 NetworkAdapter and TCP Typed Message Runtime Plan

## 1. Goal

V3.6 plans to move MBE from the V3.5 local node process preview toward a configurable `NetworkAdapter` typed message runtime between local nodes.

V3.6 is not a hardcoded TCP-only stage. The communication path should become a runtime support module that can select an adapter per topology/run. This keeps MBE modular while preparing the local node process runtime for typed node-to-node messages.

V3.6 does not implement full PBFT. PBFT is deferred to V3.7. V3.6 does not implement a real cross-shard protocol; cross-shard protocol skeleton work is deferred to V3.8. V3.6 does not implement Fabric/EVM live backend.

## 2. V3.6 Stages

### V3.6.1 NetworkAdapter + localhost TCP + typed messages

Implemented scope:

- Add a `NetworkAdapter` configuration concept.
- Support selectable `network_adapter` values:
  - `in_memory_message_bus`
  - `localhost_tcp_preview`
- Let the Go runtime identify the selected network adapter.
- Let local node process preview evolve toward TCP listen / dial / send / receive behavior when `localhost_tcp_preview` is selected.
- Keep the existing in-memory path available for deterministic local validation.

Planned `MessageEnvelope` fields:

- `message_id`
- `message_type`
- `from_node_id`
- `to_node_id`
- `shard_id`
- `role`
- `block_height`
- `sequence_id`
- `payload_digest`
- `payload`
- `timestamp_ms`

Initial planned `message_type` values:

- `ping`
- `pong`
- `node_ready`
- `health_check`
- `proposal_preview`
- `vote_preview`
- `execution_request_preview`
- `state_request_preview`
- `commit_notice_preview`

Implemented artifacts:

- `tcp_adapter_status.csv`
- `network_send_log.csv`
- `network_receive_log.csv`
- `typed_message_log.csv`

Implemented summary metrics:

- `network_adapter_selected`
- `tcp_preview_enabled`
- `tcp_listen_node_count`
- `tcp_send_count`
- `tcp_receive_count`
- `typed_message_count`
- `network_error_count`

### V3.6.2 Consensus-light over NetworkAdapter + V3.6 Closure

Implemented scope:

- Connect consensus-light to the selected `NetworkAdapter`.
- If `network_adapter = in_memory_message_bus`, use the existing in-memory typed message path.
- If `network_adapter = localhost_tcp_preview`, use TCP typed message preview.
- Keep `simple_leader`, `poa_light`, and `pbft_light_model` as lightweight consensus models.
- Do not treat `pbft_light_model` as BlockEmulator-aligned PBFT.
- Let a leader send `proposal_preview`.
- Let validators return `vote_preview`.
- Let the leader count a lightweight quorum.

Implemented artifacts:

- `consensus_network_light_log.csv`
- `consensus_message_log.csv`
- `network_consensus_summary.json`

Implemented summary metrics:

- `consensus_over_network_enabled`
- `consensus_runtime_selected`
- `proposal_preview_count`
- `vote_preview_count`
- `light_quorum_reached_count`
- `consensus_network_error_count`
- `consensus_network_path`

V3.6 is closed after V3.6.2. V3.7.1 has since implemented configurable `ConsensusRuntimePlugin` plus a PBFT state machine preview, and V3.7.2 has connected that preview over NetworkAdapter and closed V3.7.

## 3. Frontend Layout Principles

V3.6 should not restructure the existing V3 Composer page.

- Do not change the left navigation.
- Do not insert `NetworkAdapter` into the middle of the transaction main-flow cards.
- `NetworkAdapter` belongs to the Runtime Support Layer near Runtime Topology / Node Process Runtime.
- Keep the main transaction flow as:

```text
Workload -> TxPool -> BlockProducer -> ConsensusRuntime -> CommitteeEpoch -> Routing/Sharding -> Execution -> StateAccess -> StateStorage -> Commit -> MetricsReport
```

The Consensus card may later be renamed or explained as `ConsensusRuntime`, but V3.6 should avoid a large page refactor.

Runtime support layer:

```text
RuntimeTopology / NodeProcessRuntime / NetworkAdapter
```

## 4. Truth Boundary

V3.6 is configurable `NetworkAdapter` and localhost TCP typed message preview.

V3.6 is not:

- real PBFT
- a production network
- HotStuff/Raft
- BlockEmulator backend
- a real cross-shard protocol
- Fabric/EVM live backend
- paper-grade benchmark evidence

Smoke output from V3.6 remains local controlled preview output unless a later stage explicitly promotes a validated benchmark workflow.

## 5. Next Stage

V3.7.1 introduces configurable `ConsensusRuntime` and a BlockEmulator-aligned PBFT state machine preview as one optional consensus runtime plugin, not as the only consensus path. V3.7.2 connects that PBFT preview over NetworkAdapter and closes V3.7. The next planned stage is V3.8 CrossShardProtocol Skeleton.
