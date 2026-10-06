# TxAllo source lock

Primary paper: *TxAllo: Dynamic Transaction Allocation in Sharded Blockchain Systems*, ICDE 2023; public preprint arXiv:2212.11584.

Author repository reviewed: `https://github.com/zhangyuanzhe1996/TxAllo`. The public repository does not currently contain a usable implementation of Algorithms 1/2, so MBE does not claim an official-source port. The paper formulas, Algorithm 1 (G-TxAllo) and Algorithm 2 (A-TxAllo) are the source of truth.

MBE preserves these paper properties:

- graph vertices are accounts, not AccessList state keys;
- a transaction touching m distinct accounts contributes all account pairs with weight `1 / C(m,2)`, so its total edge contribution is 1;
- the graph is undirected;
- workload uses `sigma_i = intra + eta * inter`;
- modeled per-shard throughput is capacity-capped by `lambda`, and global modeled throughput is their sum;
- G-TxAllo initializes from deterministic Louvain communities, maps the largest workload communities to shards, places remaining communities, then iteratively moves accounts only for positive throughput gain;
- A-TxAllo starts from the previous mapping, adds newly committed historical transactions and optimizes only affected accounts;
- account traversal is deterministic.

No transaction from the measured evaluation window is allowed to train the mapping used to route that same window.

## 2026-10-02 paper-fidelity v20 closure

- G-TxAllo initialization now uses deterministic **multi-level** Louvain (local move + community aggregation until no further coarsening), not only the first local-moving level.
- Algorithm 1/2 placement uses the paper's local throughput-gain semantics: Eq. (6) for joining and Eq. (8) for leave+join moves. Partial mappings are never completed by silently assigning unknown accounts to shard 0.
- The current v21 measured MBE window is explicitly `paper_g_ratio_snapshot`: all selected pre-evaluation committed history is consumed by one G-TxAllo run. Arbitrary transaction-count chunks are not claimed as A-TxAllo updates.
- A-TxAllo remains implemented for a later committed-block epoch lifecycle; its input contract is newly committed blocks plus the previous mapping, per Algorithm 2.
- `lambda=0` and `epsilon=0` mean the paper's experiment calibration on the selected history window (`|T|/k` and `1e-5*|T|`); explicit positive values remain supported as deployment parameters.


## 2026-10-02 v20.2 reproduction-only closure

No G/A core, Louvain, Eq.(6)/(8), eta/lambda/epsilon, PBFT, worker-count or generic exact-version-substrate tuning is changed. This closure only removes MBE integration artifacts: stale workload cross wrappers, Stateless-TxAllo legacy business relay, and account-identity evidence ambiguity.


## v20.3 evidence-only closure

Adds logical-id execution join evidence, post-oracle v18 recomputation, a fail-closed Stateless-TxAllo multi-shard exact-version serial replay, and preservation of exact-version writeback identity fields. TxAllo core, PBFT, routing/allocation, worker count, block production and exact-version admission behavior are unchanged.


## v20.4 adapted-baseline evidence completion

Stateless-TxAllo now captures the actual business-state projection immediately after StateDB.Open and before runtime work begins; the multi-shard replay starts from that evidence instead of requiring the whole physical state root to be empty. Stateless-TxAllo also writes its existing remote_state_access rows with full exact-version identity so the supervisor/v18 five-tuple dedup can be proved. These are evidence-only adaptations: TxAllo allocation/routing, PBFT, serial execution, worker count, exact-version admission/readiness/fetch/writeback behavior, and block production remain unchanged.

## MBE preceding-history ratio policy v21

MBE selects the latest `ceil(0.10 * evaluation_tx_count)` records from an explicit
pre-evaluation history pool, capped by available history. The 10% ratio is an
MBE experiment policy, not a TxAllo paper invariant.

The entire selected history is one G-TxAllo input. It is never split into a
small G prefix plus artificial A chunks. G mapping is content-address cached.
Empty history/graph/mapping fails closed.

A-TxAllo remains disabled here because the paper requires newly committed
transactions and MBE does not yet expose commit-to-client-routing feedback.
Submitted/pending/future evaluation transactions must not be used as A history.
