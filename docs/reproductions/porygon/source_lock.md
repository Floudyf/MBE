# Porygon Source Lock

Status: paper-locked independent MBE reimplementation.

## Paper

- Title: Porygon: Scaling Blockchain via 3D Parallelism
- Venue: IEEE ICDE 2024
- DOI: 10.1109/ICDE60146.2024.00153
- Public paper: https://www.comp.hkbu.edu.hk/~henrydai/pubs/Porygon_ICDE24__Henry.pdf

## Source availability

The paper reports a Go prototype, but this reproduction did not identify a publicly accessible official Porygon source repository that could be source-locked. No Porygon source code is copied. MBE independently reimplements mechanisms explicitly specified by the paper.

## Locked mechanisms

- storage/stateless separation as the system model;
- transaction blocks and data-availability witness validation;
- Witness -> Ordering -> Execution -> Commit lifecycle;
- multiple execution committees and Cross-Batch Witness logical pipeline;
- execution sub-committees (ESCs) for intra-block execution sharding;
- deterministic global order before execution;
- cross-shard single-shard execution plus multi-shard update semantics;
- state locking evidence for cross-shard transactions;
- execution result/state-root certification boundary.

## MBE fairness adaptation

The paper's committee consensus is not used as an independent network stack in the main comparison profile. MBE retains the shared `pbft_style_consensus` used by all formal baselines. Porygon validators recompute the full transaction-block body and deterministic Porygon plan before voting. This preserves the Porygon data-availability/ordering boundary while keeping consensus constant across methods.
