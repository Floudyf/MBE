# Porygon acceptance matrix — current MBE adaptation

| Gate | Requirement |
|---|---|
| registration | all 10 Porygon-specific plugins resolve through the normal registry; legacy category defaults remain unchanged |
| frontend/backend parity | the single Porygon method card and backend builtin method select the same admission, sharding, routing, producer, execution, scheduler, executor, state-access, state-storage and cross-shard plugins |
| control-plane isolation | routing does not implement BatchRouting/RuntimeCapabilities; MetaTrack StateReady/StateVersions/CAS remain inactive |
| topology | frontend `topology.shards` maps exactly to Porygon ESC count and therefore to the Porygon ESC/StateOwner partition count; all nodes remain in one `porygon-global` PBFT ordering domain |
| admission truth | invalid signature, empty/duplicate key, unknown mode or mismatched declared AccessList digest is rejected before mempool admission |
| sharding truth | related account/object fields map to the same Porygon StateOwner home and the client uses the injected sharding plugin |
| Proposal.T@0 | an explicit baseline/genesis partition root at `TStateHeight=0` round-trips as an authenticated anchor and never falls back to current-state fetch |
| state proof | local and remote projections must verify against the exact Proposal.T partition root; root/proof mismatch fails closed |
| Pending determinism | only CTx enter cross-round Pending; changing replica-local execution status cannot change the Pending digest for a fixed proposal height |
| TransactionBlock storage | Proposal.L durable source IDs all belong to one deterministic fixed co-located Storage Role partition; non-storage Witness/ESC nodes are cache-only |
| Witness | non-maintenance proposal carries a real authenticated Witness Certificate before PBFT ordering; effective threshold is validated |
| ESC distribution | multi-sender fixture uses more than one dynamic ESC |
| cross-ESC parallelism | disjoint logical CTx may share a wave |
| conflict order | shared-state cross-ESC transactions follow deterministic OC conflict/abandonment rules |
| exactly-once ownership | one execution ESC per transaction; other ESCs never re-execute it |
| ESC quorum authentication | production batch certificate carries independently verifiable Ed25519 attestations meeting `Te=f+1` for the owning ESC |
| Multi-Shard Update | affected fixed Storage Role roots require a strict majority and are bound to Proposal.T/U semantics before materialization |
| certificate recovery | a missed ESC certificate can be requested from the global leader's bounded cache |
| no legacy cross-shard path | no physical MBE Relay/Finalize for Porygon |
| pipeline truth | PBFT ordering remains sequential; real cross-height overlap is reported only when runtime timestamps prove interval intersection |
| fault-path claim | exact future-ESC retry + proposal-carried rollback stays false/fail-closed until that paper path is implemented end-to-end |
| staging | focused Porygon Go/backend regressions, full `go test ./...`, root/backend pytest, V5 plugin/finality/real-cluster/formal gates, frontend production build |

After installation, first run `VERIFY_ONLY.ps1` with the full gates. Then apply and repeat the previously failing small Porygon run before scaling to 200/1000+ transactions. Runtime acceptance requires no Proposal.T root mismatch, no cross-round Pending mismatch, consistent state/certificate evidence, no MetaTrack CAS evidence, no physical relay evidence, and complete method-oracle/finality accounting.
