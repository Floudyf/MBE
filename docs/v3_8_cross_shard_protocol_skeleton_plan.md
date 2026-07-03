# V3.8 CrossShardProtocol Skeleton Plan

## 1. Goal

V3.8 moves MBE from intra-shard consensus and execution preview toward identifiable, logged, and observable cross-shard transaction skeleton behavior.

V3.8 = CrossShardProtocol Skeleton.

V3.8 is not a complete Relay / Broker / 2PC / atomic cross-shard commit implementation.

V3.5 completed node topology, launcher preview, and local node process preview. V3.6 completed configurable NetworkAdapter, localhost TCP typed message runtime preview, and consensus-light over NetworkAdapter. V3.7 completed configurable ConsensusRuntime plus BlockEmulator-aligned PBFT preview over NetworkAdapter. V3.8 only adds cross-shard transaction detection, a runnable `relay_preview` skeleton, cross-shard artifacts, and summary metrics.

## 2. V3.8 Scope

V3.8 implements:

- A `CrossShardProtocol` configuration entry.
- Cross-shard transaction detection.
- A runnable `relay_preview` skeleton.
- Cross-shard artifacts.
- Cross-shard summary metrics.
- Minimal frontend display.
- V3.8 closure.

V3.8 does not implement:

- Complete Relay.
- Complete Broker.
- Complete 2PC.
- Real atomic cross-shard commit.
- Cross-shard lock / unlock.
- Cross-shard rollback.
- Cross-shard timeout recovery.
- Cross-shard state proof.
- Merkle proof / witness.
- BlockEmulator full cross-shard mechanism.
- Fabric/EVM live backend.
- Paper-grade benchmark evidence.

## 3. CrossShardProtocol Options

```yaml
cross_shard_protocol:
  - none
  - relay_preview
  - broker_preview
  - two_phase_commit_preview
```

Option status:

- `none`: runnable default; no cross-shard protocol behavior beyond detection/logging.
- `relay_preview`: runnable skeleton in V3.8.
- `broker_preview`: planned only; not runnable in V3.8.
- `two_phase_commit_preview`: planned only; not runnable in V3.8.

Do not hardcode cross-shard behavior as Relay only. `relay_preview` is the only runnable cross-shard skeleton in V3.8. Broker and 2PC require a later explicit stage.

## 4. UI / Frontend Layout Rule

V3.8 must not add a new CrossShardProtocol main-flow card.

Forbidden frontend changes:

- Do not add a CrossShardProtocol main-flow card.
- Do not change the number of main-flow cards.
- Do not insert CrossShardProtocol into the transaction main flow.
- Do not refactor the V3 Composer page.
- Do not change the left navigation.
- Do not add a complex multi-page workspace.

The main transaction flow remains:

```text
Workload -> TxPool -> BlockProducer -> ConsensusRuntime -> CommitteeEpoch -> Routing/Sharding -> Execution -> StateAccess -> StateStorage -> Commit -> MetricsReport
```

CrossShardProtocol belongs under Routing/Sharding as a sub-capability:

- `routing_policy`
- `shard_mapping`
- `cross_shard_detection`
- `cross_shard_protocol`

Allowed minimal frontend changes:

- Show `cross_shard_protocol` in Routing/Sharding detail or compact run configuration.
- Add a `cross_shard_protocol` selector in the existing configuration area.
- Add Cross-shard summary in the result panel.
- Add Cross-shard artifacts to ArtifactGroups.
- Treat old runs without V3.8 artifacts as legacy missing, not errors.

## 5. Cross-shard Transaction Detection

Detection output fields:

- `tx_id`
- `is_cross_shard`
- `source_shard`
- `target_shard`
- `involved_state_keys`
- `routing_policy`
- `cross_shard_protocol`
- `detection_rule`
- `skipped_reason`

The detection path should reuse existing Routing/Sharding, state key placement, and shard mapping where available. If the current workload lacks real cross-shard fields, V3.8 may use a deterministic preview rule. Any preview rule must be recorded in `detection_rule` or `skipped_reason`; it must not be presented as real cross-shard routing.

## 6. relay_preview Skeleton Flow

Minimum V3.8 flow:

```text
source shard receives tx
  -> detects cross-shard tx
  -> emits cross_shard_relay message
  -> target shard receives relay preview
  -> records target execution preview
  -> marks cross-shard tx as preview_completed
```

V3.8 does not do source shard state locking, target shard real execution commit, atomic commit, proof verification, rollback, timeout recovery, broker middle account behavior, or 2PC prepare / commit / abort. It is an observable skeleton only.

## 7. Artifacts

V3.8 artifacts:

- `cross_shard_tx_log.csv`: records detection results per transaction.
- `cross_shard_message_log.csv`: records cross-shard preview messages.
- `relay_preview_log.csv`: records relay skeleton source/target preview events.
- `cross_shard_status.csv`: records per-transaction skeleton state.
- `cross_shard_summary.json`: records summary metrics and truth boundary.

`cross_shard_tx_log.csv` fields:

```text
tx_id,is_cross_shard,source_shard,target_shard,involved_state_keys,protocol,detection_rule,status,latency_ms,skipped_reason,error_message
```

`cross_shard_message_log.csv` fields:

```text
message_id,message_type,from_shard,to_shard,tx_id,protocol,network_adapter,timestamp_ms,status,error_message
```

`relay_preview_log.csv` fields:

```text
tx_id,source_shard,target_shard,relay_message_id,relay_emitted,target_received,preview_completed,skipped_reason
```

`cross_shard_status.csv` fields:

```text
tx_id,protocol,state,source_shard,target_shard,completed,failed,reason
```

`cross_shard_summary.json` fields:

```text
cross_shard_protocol_selected
cross_shard_tx_count
cross_shard_ratio
cross_shard_message_count
relay_preview_count
cross_shard_completed_count
cross_shard_failed_count
cross_shard_avg_latency_ms
runtime_truth
```

## 8. Summary Metrics

Required summary metrics:

- `cross_shard_protocol_selected`
- `cross_shard_tx_count`
- `cross_shard_ratio`
- `cross_shard_message_count`
- `relay_preview_count`
- `cross_shard_completed_count`
- `cross_shard_failed_count`
- `cross_shard_avg_latency_ms`

Reserved optional metrics:

- `cross_shard_skipped_count`
- `cross_shard_detection_preview_count`
- `relay_preview_latency_ms`
- `cross_shard_target_receive_count`

## 9. Relationship with Previous Stages

V3.8 reuses:

- V3.5 topology, `shard_count`, and logical node topology.
- V3.6 NetworkAdapter and typed message runtime concepts.
- V3.7 ConsensusRuntime and PBFT preview truth boundary.

V3.8 does not modify:

- V3.6 NetworkAdapter semantics.
- V3.7 PBFT over NetworkAdapter semantics.
- ConsensusRuntime selection logic.
- Main flow card layout.

## 10. Truth Boundary

V3.8 can claim:

- Configurable CrossShardProtocol entry.
- Cross-shard transaction detection preview.
- `relay_preview` skeleton.
- Cross-shard artifacts.
- Cross-shard summary metrics.
- Minimal frontend cross-shard summary.

V3.8 cannot claim:

- Complete Relay protocol.
- Complete Broker protocol.
- Complete 2PC protocol.
- Atomic cross-shard commit.
- Cross-shard state proof.
- Cross-shard rollback / timeout recovery.
- BlockEmulator full cross-shard mechanism.
- Fabric/EVM live backend.
- Production sharding system.
- Paper-grade benchmark evidence.

## 11. Acceptance Criteria

V3.8 is complete when:

- `cross_shard_protocol` configuration is backend validated.
- The Go runtime recognizes selected `cross_shard_protocol`.
- `none` is runnable.
- `relay_preview` runs a skeleton.
- `broker_preview` and `two_phase_commit_preview` remain planned.
- All V3.8 artifacts are generated.
- Summary metrics are visible.
- The frontend shows Cross-shard summary.
- ArtifactGroups can download cross-shard artifacts.
- No CrossShardProtocol main-flow card is added.
- README, execution plan, and skill are updated to V3.8 closure.
- Tests pass.

## 12. Next Stage

V3.9 StateStorage / StateProof Hardening is planned next. V3.9 has not started.
