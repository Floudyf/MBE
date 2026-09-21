# Porygon v8 acceptance matrix

| Gate | Requirement |
|---|---|
| registration | all 7 Porygon plugins resolve; legacy category defaults unchanged |
| control-plane isolation | routing does not implement BatchRouting/RuntimeCapabilities |
| topology | `8 nodes / 1 physical shard / 8 validators`, logical ESC count configured separately |
| access truth | signed AccessList only; scheduling overlay ignored |
| ESC distribution | multi-sender fixture uses more than one ESC |
| cross-ESC parallelism | disjoint logical cross-shard txs may share a wave |
| conflict order | shared-state txs serialize in global order |
| exactly once | one business attempt per transaction |
| oracle | final state root equals serial deterministic oracle on focused fixtures |
| fail closed | undeclared actual read/write rejects execution |
| no old deadlock path | no MetaTrack remote CAS / physical Relay path for Porygon |
| pipeline truth | logical protocol slots exported; wall-clock overlap not claimed |
| staging | focused Porygon + all V5 Go + `go vet ./v5` + backend formal/default regressions |
| final | full Go, full backend pytest, Go vet, frontend production build |

After installation, run a real 8-node/1-shard/4-ESC 200-tx smoke test, then 1000-tx Alien Worlds. Successful runtime acceptance requires full terminal completion, multiple committed heights, consistent state roots, zero `remote_state_cas_rejected`, and no physical relay evidence. Real workloads are **not** required to have `maximum_parallel_width > 1`; hotspot contention may legitimately serialize them.
