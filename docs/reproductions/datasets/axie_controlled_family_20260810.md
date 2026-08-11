# Axie Infinity controlled workload family integration

Dataset ID: `axie_infinity_controlled_prefix_rmw_v1`

This integration binds the locally validated family under
`dataset/Axie_Infinity/datasets` to MBE's existing dynamic workload registry.

Physical family:
- 3 access profiles: read_heavy / balanced / write_heavy
- target_theta: 0.0 ... 1.2
- 39 master `theta_X.X_10k.csv.gz` files
- 10 validated prefixes per master: 1K ... 10K
- 390 validated prefix entries
- family source identity SHA-256: `6dfe5e271303c8ae28bb2db5b3ce6bb15571101bf9ea3349abf95e5e46011721`

Truth boundary:
- `truth_label = real_derived_controlled`
- real Axie transfer skeletons are retained as the source basis;
- access profile and account-write theta are controlled construction dimensions;
- static direct access semantics are supplied by `axie_controlled_access_v1`;
- every admitted source row must retain at least one RMW key;
- `target_theta` is the source-declared construction parameter.
  A generic post-hoc rank-frequency estimator is diagnostic and is not used
  to relabel the experiment's controlled theta axis.

No execution method owns or rewrites this workload. Serial, Block-STM, Aria,
Groundhog, CG, ACG, BSX, Batch-SI and other compatible methods consume the same
materialized records through the common V5 workload data plane.
