# V5 Persistence WAL, Snapshot, And Runtime Root

Status: partially implemented in the V5 Core Hardening round.

## Commit Persistence

The V5 commit path no longer calls `StateDB.Save()` after every block. The
normal path is:

```text
BlockExecutor.ExecuteBlock
-> CommitPlugin.DecideCommit
-> StateDB.ApplyDeterministicBatch
-> StateDB.PersistDelta
-> BlockStore.DurableCommitWithMetrics
-> StateDB.SnapshotIfDue
```

`DurableCommitWithMetrics` batches `receipts.jsonl` and `tx_index.jsonl` by
block. Each file is opened once per committed block, and all receipt/index rows
for that block are written through one buffered writer. The JSONL schemas remain
compatible with older files.

`StateDB.PersistDelta` appends to the active WAL segment,
`state_delta.<generation>.wal`. `state_wal_manifest.json` records the active
generation. Snapshot metadata records the included height/hash/root and the
next WAL generation so crash recovery can choose a consistent snapshot plus WAL
set. Old directories with only `state_delta.wal` remain readable as a legacy
segment.

Each WAL record includes:

- `version`
- `namespace`
- `block_height`
- `block_hash`
- `parent_hash`
- `delta_id`
- ordered `state_updates`
- `state_root_before`
- `state_root_after`
- `checksum`

Recovery loads `state_snapshot.json` and `state_snapshot_metadata.json` when
present, scans `state_delta.<generation>.wal` plus legacy `state_delta.wal`,
skips records already included by the snapshot, and only applies records whose
block hash has a valid `durable_commit_marker_v1` in `commit_markers.jsonl`.
The final torn WAL tail is ignored; malformed committed records, checksum
mismatches, height gaps, parent mismatches, and root-before/root-after
mismatches fail explicitly.

`BlockStore.ReadCommitted`, `ReadCommittedAtHeight`, and `HasTransaction` are
commit-marker aware. Receipt, tx-index, block, and marker append artifacts are
all included in rollback checkpoints.

## Durability Modes

The default mode is correctness-friendly:

```text
durability_mode = strict
fsync_cadence = 1 block
snapshot_cadence_blocks = 128
```

`strict` syncs WAL for every block. `batched` syncs after the configured block
cadence. The V5 state-storage plugin config can set:

- `durability_mode`
- `fsync_cadence`
- `snapshot_cadence_blocks`

Environment overrides are also supported:

- `MBE_DURABILITY_MODE`
- `MBE_FSYNC_CADENCE_BLOCKS`
- `MBE_SNAPSHOT_CADENCE_BLOCKS`

## Runtime Roots

The backend now resolves:

- `MBE_RUNTIME_ROOT`
- `MBE_WORKLOAD_CACHE_ROOT`
- `MBE_FORMAL_RUN_ROOT`

When unset, existing `.cache` behavior is preserved. For large WSL-backed
experiments, use an ext4 path such as:

```bash
export MBE_RUNTIME_ROOT=~/mbe-runtime
export MBE_WORKLOAD_CACHE_ROOT=~/mbe-runtime/workloads
export MBE_FORMAL_RUN_ROOT=~/mbe-runtime/v5-formal-runs
```

Artifacts should expose logical paths rather than local absolute paths.

## Metrics

Block execution summaries now include:

- `wal_append_ms`
- `wal_sync_ms`
- `snapshot_write_ms`
- `receipt_batch_write_ms`
- `tx_index_batch_write_ms`
- `durable_commit_ms`
- `persistence_written_bytes`
- `wal_record_count`
- `snapshot_count`
- `durability_mode`
- `fsync_cadence`

These are separate from block execution and deterministic state apply timing.
