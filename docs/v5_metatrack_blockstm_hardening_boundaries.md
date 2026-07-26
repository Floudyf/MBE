# V5 MetaTrack And Block-STM Hardening Boundaries

Status: partial implementation note for the V5 Core Hardening round.

## MetaTrack

`dual_track_execution` now has a batch-level classifier:

```text
ClassifyBatch(BatchClassificationInput) BatchClassificationResult
```

The single-transaction `Classify` remains a conservative fallback. The batch
classifier can place independent ordinary writes in Fast Track when the batch
contains no demonstrated RAW, WAR, WAW, nonce/order, remote-boundary, or missing
access-list dependency. It keeps unresolved or unsafe cases Conservative and
records reason codes.

Implemented reason-code families include:

- `missing_structured_access_list`
- `remote_or_cross_shard_boundary`
- `batch_independent_write`
- `raw_dependency`
- `war_dependency`
- `waw_dependency`
- `nonce_order_dependency`

Proposal-time scheduling is limited to a deterministic dependency plan. Planned
rows are labeled `planned_ready` or `planned_blocked` and do not emit
`TxBlocked`/`TxWoken` runtime events. The `metatrack_block_executor` owns the
actual execution state machine: FastReadyQueue, ConservativeReadyQueue,
BlockedQueue, dependency counters, reverse dependencies, worker dispatch,
completion events, dependency release, and wakeup transitions. Results are
materialized in consensus transaction order. Work stealing is not synthesized
from remote access; when no actual stealing exists, `stolen_work` remains
false.

Worker jobs execute the single-transaction business path and return receipts
and TxDeltas through the completion channel. The executor then materializes
writes deterministically and recomputes final receipt roots in agreed order.
Execution metrics are split by unit: `configured_worker_count` is configuration;
`max_ready_queue_depth`, `max_fast_ready_queue_depth`,
`max_conservative_ready_queue_depth`, and `max_dependency_frontier_width`
describe scheduling structure; `max_inflight_business_executions` is measured
by atomically incrementing at business execution start and decrementing at
business execution end. It must never exceed `configured_worker_count`.
Completion evidence is reported separately as scheduler dispatch events,
blocked/wakeup events, completion-channel events, worker execution attempts,
validator execution completions, unique final logical completions, and duplicate
final completions.

State ownership is injected through the selected `ShardingPlugin` in canonical
workload replay, client routing, MetaTrack state/transaction placement, runtime
remote fetch, remote writeback, and home-shard checks. Batch routing plans record
`sharding_plugin_id` so plan digests are ownership-configuration sensitive.

MetaTrack placement uses a deterministic cost model. Candidate score evidence
includes co-access locality gain, predicted remote read/writeback cost, shard
load penalties, ordered-state penalty, and movement/writeback penalty. If the
best non-home candidate does not beat the home-shard score, placement falls back
to home/hash ownership and increments `placement_fallback_count`.

`commutative_hot_update_aggregation` now builds commutative-delta materialization
evidence for keys whose successful transactions declare compatible
`commutative_delta`/`add` semantics. Reduction metrics use the same unit:
`pre_aggregation_physical_op_count` versus
`post_aggregation_physical_op_count`. If the execution `StateDelta` is already
key-coalesced, the mechanism records materialization evidence but does not
claim an additional physical write reduction. Mixed commutative and ordinary
writes keep the original deterministic state update and do not mark aggregation
applied.

Remaining MetaTrack hardening work:

- continue tightening predicted-vs-actual remote access aggregation in larger
  reports without claiming performance improvement from current local runs;
- route remote state access through a richer `StateAccessPlugin` interface
  instead of direct runtime helper methods;
- run the existing real dataset windows at 100/1K/10K after the remaining
  plugin wiring and validation gates pass.

## Block-STM

`block_stm_block_executor` now reads:

- `execution_mode`: `correctness` or `performance`
- `oracle_mode`: `full`, `sampled`, or `off`
- `maximum_incarnations`
- `incarnation_limit_action`: `fail` or `serial_fallback`

Correctness/full keeps the Serial oracle. Performance/off skips the full Serial
oracle and reports `serial_oracle_ms = 0`. The performance materialization path
uses captured reads, saved write sets, and saved receipts from the final
validated incarnation. It recomputes receipt per-transaction state roots during
ordered materialization without re-running the whole block; only transactions
whose captured reads fail against current ordered state are aborted and
re-executed.

New metrics include:

- `serial_oracle_ms`
- `materialization_ms`
- `incarnation_limit_hit_count`
- `serial_fallback_count`
- `business_execution_invocation_count`

`business_execution_invocation_count` counts Block-STM speculative execution
and transaction-level re-execution calls. It intentionally excludes the Serial
oracle and ordered materialization, so performance/off runs can prove they do
not hide a second full-block business execution pass.

Remaining Block-STM hardening work:

- complete sampled oracle mode;
- replace the remaining two-phase execute/validate approximation with a fuller
  continuously interleaved worker loop beyond the current staged prototype;
- add the rest of the requested worker-count, cancellation,
  incarnation-limit/fallback, and no-hidden-second-execution tests;
- run `go test -race` in WSL2 with a CGO C compiler.
