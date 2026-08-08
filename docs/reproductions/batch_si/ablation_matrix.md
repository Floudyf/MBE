# Batch-SI ablation matrix

| Method ID | Changed component | Kept identical |
|---|---|---|
| `hash_batch_si` | none; full AWRT + WRBP + OFAS + parallel batch snapshot | complete Batch-SI |
| `hash_batch_si_no_wrbp` | sequential write-conflict batch assignment replaces opportunity reuse | access extraction, OFAS, transaction semantics, snapshot execution |
| `hash_batch_si_no_ofas` | private full dependency-graph ordering replaces OFAS | AWRT, WRBP, transaction semantics, snapshot execution |
| `hash_batch_si_serial_batch` | one worker executes each batch serially | accepted set, WRBP batches, OFAS order, snapshots, materialization |
| `hash_batch_si_txid_priority` | OFAS priority becomes transaction-ID-only | OFAS dependency/abort rules and all other Batch-SI components |

The full method is mandatory in an ablation RunGroup. Plugin IDs may remain the same while private plugin configuration changes; formal validation compares effective plugin ID plus effective configuration.

AWRT versus pairwise WW detection is retained as a possible microbenchmark and is not included in the default end-to-end ablation group because WRBP structurally consumes AWRT rows.
