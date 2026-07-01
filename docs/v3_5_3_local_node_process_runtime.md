# V3.5.3 Local Node Process Runtime

## 1. Scope

This stage adds a local node process preview entry point. It does not implement real TCP, real PBFT, HotStuff/Raft, a real multi-process network runtime, or real node-to-node messaging.

## 2. Why This Stage

V3.5.3 follows V3.5.2 launcher preview by making the generated node commands start a real preview entry point. The entry point can load `topology.json`, identify a node by `node_id`, validate its role, and write node-local status/log artifacts.

## 3. Implemented Capabilities

- node process preview entry point
- `topology.json` loading
- node self-identification by `node_id`
- role and shard validation
- node-local status/log artifacts
- launcher scripts aligned with `go run ./cmd/replay --mode node-preview`

## 4. Runtime Truth

This is local node process preview only. It is not real TCP, not real PBFT, not HotStuff/Raft, not BlockEmulator backend, and has no real node-to-node messaging.

## 5. Artifacts

- node_process_status.csv
- node_process_manifest.json
- node_process_log_sample.log

## 6. Summary Metrics

- node_process_entrypoint_available
- node_process_preview_available
- node_process_status_available
- node_process_manifest_available
- node_process_preview_only

## 7. Validation

Recorded in `docs/v3_5_4_v3_5_closure.md` because V3.5.3 and V3.5.4 are completed in one implementation round.

## 8. Next Step

V3.5.4 closure is completed in the same round. The next major stage is V3.6 TCP Adapter and Consensus Hardening.
