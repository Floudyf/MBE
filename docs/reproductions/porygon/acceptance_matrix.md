# Porygon v8 acceptance matrix

| Gate | Requirement |
|---|---|
| registration | all 7 Porygon plugins resolve; legacy category defaults unchanged |
| control-plane isolation | routing does not implement BatchRouting/RuntimeCapabilities |
| topology | frontend `topology.shards` maps exactly to Porygon ESC count; all nodes remain in one `porygon-global` PBFT ordering domain |
| access truth | signed AccessList only; scheduling overlay ignored |
| ESC distribution | multi-sender fixture uses more than one ESC |
| cross-ESC parallelism | disjoint logical cross-shard txs may share a wave |
| conflict order | shared-state txs serialize in global order |
| exactly-once ownership | each transaction has exactly one owning ESC; replicas inside that ESC independently execute and sign the same result digest, while other ESCs never re-execute it |
| ESC quorum authentication | production certificates carry independently verifiable Ed25519 attestations from a strict majority of the owning ESC |
| certificate recovery | a missed certificate can be requested from the global leader's bounded certificate cache; minority send failure does not abort an otherwise valid quorum |
| oracle | final state root equals serial deterministic oracle on focused fixtures |
| fail closed | undeclared actual read/write rejects execution |
| no old deadlock path | no MetaTrack remote CAS / physical Relay path for Porygon |
| pipeline truth | logical protocol slots exported; wall-clock overlap not claimed |
| staging | focused Porygon + all V5 Go + `go vet ./v5` + backend formal/default regressions |
| final | full Go, full backend pytest, Go vet, frontend production build |

After installation, run a real 8-node / frontend `shards=4` (four ESCs, one `porygon-global` PBFT domain) 200-tx smoke test, then 1000-tx Alien Worlds. Successful runtime acceptance requires full terminal completion, multiple committed heights, consistent state roots, verified ESC ownership/quorum attestations, zero `remote_state_cas_rejected`, and no physical relay evidence. Real workloads are **not** required to have `maximum_parallel_width > 1`; hotspot contention may legitimately serialize them.
