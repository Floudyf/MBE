# Porygon → MBE mapping — v8

| Porygon concept | MBE v8 mapping |
|---|---|
| Ordering Committee | one physical MBE shard with common PBFT |
| Execution Committee | all validators participating in Porygon execution |
| Execution Sub-Committee | frontend `topology.shards` compiled to logical `execution_shard_count` ESCs inside the single `porygon-global` ordering domain |
| Account partition | stable sender-identity → ESC mapping |
| State partition | stable signed state-key → logical shard mapping |
| Transaction Block witness | proposal evidence containing tx root, signed-access root and full-body digest |
| Global order | `block.TxList` order after PBFT proposal planning |
| Cross-shard locking | signed-AccessList lock/dependency plan |
| Single-Shard Execution | one owning ESC per transaction; replicas inside that ESC independently execute and sign one semantic result digest, forming a strict-majority certificate |
| Multi-Shard Update | deterministic atomic materialization of the transaction write set |
| Storage nodes | signed-access visibility projection over MBE persistent state; separate physical storage-node processes are not claimed |
| BA★ | replaced by common MBE PBFT for comparison fairness |
| inter-block pipeline | logical W/O/E/C protocol-slot evidence only; wall-clock cross-height overlap not claimed |

The critical v8 boundary is that logical Porygon sharding never activates MBE `BatchRoutingPlugin`, `ExecutionRouting.StateVersions`, `applyMetaTrackRemoteDeltas`, or physical Relay/Finalize.
