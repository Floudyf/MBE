# Serial Acceptance Matrix

The equivalence suite must cover:

1. Single valid transfer.
2. Multiple continuous nonces.
3. Nonce mismatch.
4. Invalid value.
5. Insufficient balance.
6. Shared sender.
7. Shared receiver.
8. Sender equals receiver.
9. New account initialization.
10. Duplicate execution protection path.
11. Cross-shard source transaction.
12. Relay transaction.
13. Synthetic workload.
14. Canonical trace replay workload.
15. Hot key sequence.
16. Empty block or empty input boundary.
17. State rollback or persistence failure regression.

Real-cluster acceptance must check:

- every node instantiates `serial_block_executor`
- plugin snapshot consistency
- plan digest consistency
- state root consistency
- receipt root consistency
- terminal transaction count completeness
- pending commit equals 0
- pending relay equals 0
- reserved transaction count equals 0
- proposal in flight is false
- orphan process count equals 0
- no fallback
- catch-up and recovery replay use the same block executor
- workload-aware drain budget is recorded
- node process runtime duration is at least the workload-aware drain hard
  timeout
- no-progress watchdog is active and fails true stalls
- hard timeout is bounded and derived from workload size plus block producer
  timing, not an arbitrary pass-through value
- runtime status export must not hold the runtime mutex while calling mempool
  accessors
- fresh dataset acceptance includes current-run execution evidence such as
  `block_execution_summary.json`, `execution_plan.jsonl`, and
  `transaction_execution_trace.csv`
- historical Child IDs or deterministic child names cannot be used as proof
  unless the current run namespace and modified timestamps show fresh execution

The synthetic 10K acceptance must include both:

- 4 nodes, 1 shard, 10,000 synthetic transactions
- 16 nodes, 4 shards, 10,000 synthetic transactions with mixed intra-shard and
  cross-shard lifecycle closure

Both must finish with complete terminal counts and empty runtime queues.
