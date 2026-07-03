# V3.11 CrossShard Protocol Closure

## 1. Goal

V3.11 upgrades the V3.8 `relay_preview` skeleton into a runnable local Relay MVP for controlled emulator experiments. It adds observable SourceLock, RelayCertificate, proof/certificate verification records, target verification, target commit, source finalization, and deterministic timeout/refund/abort paths.

## 2. Difference from V3.8 Relay Preview

V3.8 detected cross-shard transactions and emitted relay preview artifacts. V3.11 keeps that compatibility path and adds `cross_shard_protocol = relay_mvp` as the runnable MVP path.

`relay_preview` remains historical preview / skeleton mode. `relay_mvp` is the V3.11 runnable Relay MVP. `broker_preview` and `two_phase_commit_preview` remain planned-only and not runnable.

## 3. Relay MVP State Machine

Normal path:

```text
Detected -> SourceLocked -> RelayCertificateGenerated -> ProofVerified -> TargetVerified -> TargetCommitted -> SourceFinalized
```

Failure paths:

```text
Detected -> SourceLocked -> RelayCertificateGenerated -> ProofFailed -> Refunded
Detected -> SourceLocked -> Timeout -> Refunded
Detected -> SourceLocked -> RelayCertificateGenerated -> ProofVerified -> Aborted
```

The state machine is deterministic and local. It does not implement distributed locking or production atomic commit.

## 4. SourceLock

Each cross-shard transaction under `relay_mvp` receives a SourceLock record with `tx_id`, `block_height`, source/target shards, deterministic `source_lock_id`, lock time, status, and reason. This is an MVP logical lock, not a production smart-contract lock.

## 5. RelayCertificate

The RelayCertificate records `tx_id`, `source_lock_id`, `certificate_id`, `certificate_hash`, `proof_digest`, `state_root_digest`, creation time, status, and truth label.

Truth label:

```text
relay_certificate_mvp_no_byzantine_security
```

V3.11 does not implement signature collection or Byzantine-secure certificates.

## 6. Proof / Certificate Verification MVP

The MVP verifier checks deterministic guards: certificate id is present, tx id matches SourceLock, source/target shards match the routing record, SourceLock is locked, proof digest is present, and deterministic failure injection has not triggered.

If V3.9 state authenticity artifacts are available, the Relay MVP may reference them as `state_authenticity_mvp_linked`; it must not claim Ethereum MPT verification.

## 7. Target Verification / Target Commit

After proof verification, the target shard records target verification and target commit preview rows. These are local MVP records and do not represent production target-shard execution commit.

## 8. Timeout / Refund / Abort

V3.11 supports deterministic preview failure controls:

- `relay_failure_mode = none | proof_fail | timeout | target_reject`
- `relay_force_proof_fail_every_n`
- `relay_force_timeout_every_n`
- `relay_timeout_ms`

The refund / abort outputs are observable artifacts only. They are not real rollback, timeout recovery, or production compensation logic.

## 9. Artifacts

V3.11 adds:

- `relay_state_machine_log.csv`
- `source_lock_log.csv`
- `relay_certificate_log.csv`
- `relay_proof_verification_log.csv`
- `target_verification_log.csv`
- `target_commit_log.csv`
- `source_finalize_log.csv`
- `cross_shard_timeout_refund_log.csv`
- `cross_shard_failure_log.csv`
- `relay_mvp_summary.json`

V3.8 artifacts remain compatible:

- `cross_shard_tx_log.csv`
- `cross_shard_message_log.csv`
- `relay_preview_log.csv`
- `cross_shard_status.csv`
- `cross_shard_summary.json`

## 10. Summary Metrics

V3.11 adds:

- `relay_mvp_enabled`
- `relay_mvp_tx_count`
- `relay_source_lock_count`
- `relay_certificate_count`
- `relay_proof_verified_count`
- `relay_proof_failed_count`
- `relay_target_verified_count`
- `relay_target_commit_count`
- `relay_source_finalized_count`
- `relay_timeout_count`
- `relay_refund_count`
- `relay_abort_count`
- `relay_success_count`
- `relay_failed_count`
- `relay_avg_latency_ms`
- `relay_mvp_truth`

`relay_mvp_truth = relay_mvp_not_production_atomic_commit`.

## 11. Frontend Changes

The V3 Composer keeps the main flow unchanged:

```text
Workload -> TxPool -> BlockProducer -> ConsensusRuntime -> CommitteeEpoch -> Routing/Sharding -> Execution -> StateAccess -> StateStorage -> Commit -> MetricsReport
```

CrossShardProtocol remains a Routing/Sharding sub-capability. The frontend adds `relay_mvp` selection, Relay MVP result metrics, and a Relay MVP artifact download group. It does not add a CrossShardProtocol main-flow card.

## 12. Truth Boundary

V3.11 implements a local Relay MVP for controlled emulator experiments.

It is not:

- production atomic cross-shard commit
- complete Broker / 2PC / Monoxide
- Byzantine-secure relay
- production cross-chain bridge
- BlockEmulator backend
- Fabric/EVM live backend
- paper-grade benchmark evidence

## 13. Validation Commands

Required validation:

```powershell
git diff --check
cd frontend
npm.cmd run build
cd ..
cd executor
go test ./...
cd ..
$env:PYTHONPATH = (Get-Location).Path
python -m pytest backend/tests -q
python -m pytest tests -q
python scripts/v0_sanity.py
```

