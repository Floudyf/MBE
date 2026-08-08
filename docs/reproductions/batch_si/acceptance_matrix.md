# Batch-SI acceptance matrix

| Area | Gate |
|---|---|
| Algorithm | WRBP Figure 7 example produces three exact batches and no intra-batch WW |
| Algorithm | OFAS Table 2 example aborts T4 and yields T5,T1,T2,T3 |
| Algorithm | synthetic cycle defers at least one transaction and accepted-set rebuild closes |
| Execution | common snapshot and previous-batch-state tests pass |
| Execution | worker 1/2/4/8 state-root equality passes |
| Execution | all five full/ablation configurations are deterministic |
| Plugin | Batch-SI execution, scheduler, and block executor register and pair only together |
| Consensus | proposer embeds plan; validator semantically recomputes plan |
| Mempool | deferred candidate transactions are released before PBFT |
| Backend | full and four ablation profiles validate; configuration-only ablations are detected |
| Backend | worker scan changes executor workers but not estimated process count |
| Metrics | leader-per-shard Batch-SI aggregation and per-block logical deferral counting pass |
| Frontend | experiment-type cards, two-level ablation selection, process/worker separation pass |
| E2E | one Batch-SI real-cluster main experiment previews, starts, completes, and yields a formal-eligible child |
| Isolation | protected existing algorithm hashes unchanged and static forbidden-call scan empty |
