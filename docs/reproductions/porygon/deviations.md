# Porygon Deviations and Truth Boundary

## Implemented

- transaction-block body/access commitments;
- validator-side data-availability/body recomputation;
- deterministic global ordering;
- W/O/E/M logical protocol pipeline;
- Cross-Batch Witness logical overlap;
- execution committee / ESC configuration;
- deterministic execution sharding;
- real parallel execution of conflict-free ESC waves;
- single execution for cross-shard transactions;
- deterministic multi-key update materialization and lock evidence;
- serial-oracle root equivalence checks.

## MBE adaptations

- BA*/paper committee consensus -> shared MBE PBFT-style consensus for formal comparison fairness;
- paper account suffix partition -> deterministic key hash modulo ESC count;
- Porygon state transfer -> existing MBE stateless remote-state request/response transport, without MetaTrack version-chain control metadata;
- business transaction semantics -> MBE canonical transaction semantics.

## Important timing truth boundary

The current integration records W/O/E/M and Cross-Batch Witness as deterministic **logical protocol slots**. It does not claim that MBE's shared block-synchronous PBFT runtime physically overlaps four wall-clock stages across different blocks. Metrics explicitly publish `porygon_pipeline_timing_truth_boundary=logical_protocol_slots;wall_clock_overlap_not_claimed`.

This prevents simulated overlap from being reported as measured speedup.
