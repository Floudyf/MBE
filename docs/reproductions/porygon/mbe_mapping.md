# Porygon → MBE mapping — current plugin adaptation

| Porygon concept | MBE mapping |
|---|---|
| Ordering Committee | one physical `porygon-global` MBE ordering domain using the unchanged common PBFT plugin |
| Execution Committee lifecycle | deterministic role rotation over the fixed MBE validator population; this is the MBE adapter, not an exact VRF-sortition claim |
| Execution Sub-Committee | frontend `topology.shards` is compiled to logical `execution_shard_count` ESCs inside the single ordering domain |
| Account/object partition | `porygon_object_sharding` maps the canonical initiating account/object identity to a fixed logical StateOwner partition |
| Access admission | `porygon_access_admission` verifies the signed transaction and requires a complete, unique, known-mode signed `AccessList` before mempool admission |
| Transaction Block | full signed transactions live outside the compact PBFT proposal; durable copies are held only by a deterministic fixed co-located Storage Role partition, while Witness/ESC nodes may keep transient cache copies |
| Witness | authenticated EC witness votes form a real Witness Certificate before PBFT ordering; the MBE adapter reuses validator identities and does not claim extra physical Witness machines |
| Compact Proposal | PBFT orders L/U/T: content-addressed TransactionBlock refs, older CTx updates, and the agreed Proposal.T state anchor |
| Proposal.T state | state projections are verified against the exact partition root carried by Proposal.T; `TStateHeight=0` is a valid genesis/baseline anchor |
| Global order | the compact PBFT proposal commits the Porygon plan/order; hydrated transaction bytes must match TransactionBlock commitments |
| Cross-round conflict protection | only still-protected CTx enter the proposal-height-derived Pending set; local background worker timing is never consensus truth |
| Single-Shard Execution | exactly one owning ESC executes a transaction; authenticated matching results require Porygon `Te=f+1` under the configured Byzantine bound |
| Multi-Shard Update | later Proposal.U plus fixed Storage Role root authentication; affected partition roots require a strict majority before materialization |
| Storage nodes | no extra physical processes are added; fixed Storage Roles are co-located on the existing MBE nodes and are distinct from rotating ESC roles |
| BA★ | not reproduced; common MBE PBFT is retained for baseline fairness |
| inter-block pipeline | PBFT ordering heights remain sequential, while later Ordering may overlap earlier Porygon Execution/Materialization; overlap is claimed only when runtime timestamps prove it |

The frontend Porygon card is therefore a **plugin composition**, not a monolithic runtime path: admission + sharding + routing + block producer + execution + scheduler + block executor + state access + state storage + cross-shard coordinator, together with the shared PBFT/network/commit plugins.

The isolation boundary remains strict: Porygon does not activate MBE `BatchRoutingPlugin`, MetaTrack `ExecutionRouting.StateVersions`/StateReady/CAS, or physical Relay/Finalize.
