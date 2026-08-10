# Batch-SI paper reproduction protocol

The uploaded Batch-SI paper reports:
- physical machine: Intel i5-12400F, 6 cores / 12 threads, 16 GB RAM;
- simulated 8-node permissioned cluster: 1 client, 4 ordering nodes, 3 execution nodes;
- block size 1000;
- default 8 execution threads;
- five repeats, mean reported;
- Smallbank, 10,000 accounts;
- six transaction types: getBalance, writeCheck, amalgamate, sendPayment, updateSaving, updateBalance;
- read:write profiles 4:1, 1:1, 1:4;
- Zipf skew 0.2..1.0.

MBE does **not** fabricate a Smallbank workload from incomplete access semantics in the Batch-SI paper. The current package closes the algorithms and provides paper golden tests. A numerical Smallbank reproduction should be enabled only after a separately source-locked Smallbank generator defines the six operations' exact state-access/update semantics.

For existing MBE workloads, CG/ACG/BSX/Batch-SI can now be compared under the same PBFT/network/topology/workload contracts, but such runs are generalization experiments rather than numerical reproduction of the paper's Figure 10–15.
