# V5.2 Finality And Metric Semantics

Finality is derived from `transaction_lifecycle.jsonl` and its CSV projection,
not from TCP send completion. A logical transaction is terminal only at durable
commit, source finalization, refund, rejection, or explicit failure.

The runtime records submission, admission, proposal, quorum commit, durable
commit, cross-shard TargetCommit, SourceFinalize, Timeout, Refund, and failure
events. Duplicate validator receipts are aggregated by `logical_tx_id`.

Reported latency is submission-to-terminal lifecycle time. TCP send latency is
excluded. Throughput uses the interval from the first to last successful
terminal event. Percentiles are computed from successful terminal observations;
missing or incomplete observations are not converted to zero.

`drain_quiescent` additionally requires terminal count to equal submitted
unique logical transactions, empty mempools and reservations, no in-flight
proposal or pending commit/cross-shard state, aligned validator heights, and
consistent block/state/receipt roots.

Formal outputs include `finality_summary.json`,
`transaction_finality.csv`, `latency_distribution.csv`, and
`throughput_windows.csv`. These artifacts preserve raw evidence and expose the
runtime truth label.
