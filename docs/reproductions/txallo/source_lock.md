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
