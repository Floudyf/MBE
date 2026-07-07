# V4.3 BlockEmulator-surpass Realism Closure

## Goal

V4.3 closes the main evidence gaps left after V4.2:

- signed transaction sender/public-key binding;
- cross-shard relay evidence over real localhost TCP P2P;
- P2P fault injection that actually delays or drops messages;
- BlockEmulator-style CSV trace import into verifiable MBE signed transaction JSONL;
- backend and frontend controls for the V4.3 smoke path.

## Implemented Evidence

Implemented and verified:

- `sender_public_key_binding=true` through deterministic `address(public_key)` derivation;
- mempool rejection for sender/public-key mismatch;
- `real_fault_injection=true` for P2P delay/drop behavior in `Transport.Send` and receive handling;
- `real_cross_shard_network_commit=true` for relay-certificate transmission over real TCP P2P plus V4.2 PBFT/state commit evidence;
- `blockemulator_trace_to_signed_tx=true` with signed JSONL output and verification counts;
- `v4_3_realism_final_summary.json`, `v4_3_acceptance_report.json`, and `v4_3_self_check_report.md` artifacts.

## Truth Boundary

Allowed description:

```text
MBE V4.3 matches BlockEmulator core emulator realism and surpasses it in evidence chain, frontend-controlled realism, state/receipt/tx-index observability, real cross-shard network commit MVP, real P2P fault injection, and BlockEmulator trace-to-signed-tx bridge.
```

Non-claims remain:

- `production_pbft=false`
- `full_byzantine_security=false`
- `production_blockchain=false`
- `production_atomic_commit=false`
- `full_blockemulator_compatibility=false`
- Fabric/EVM live backend remains out of scope.

## Smoke Command

```powershell
cd F:\Metaverse_Blockchain_Env\executor
$env:GOCACHE="F:\Metaverse_Blockchain_Env\.cache\go-build"
go run ./cmd/mbe-supervisor --mode v4.3-smoke --nodes 8 --shards 2 --tx-count 20 --enable-cross-shard=true --enable-faults=true --fault-profile mixed_light --blockemulator-tx-limit 20 --data-dir ../.cache/v4_realism_runs/latest
```
