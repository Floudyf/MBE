# Aria Deviations and Truth Boundary

## Implemented faithfully

- same-snapshot batch execution;
- actual read/write capture;
- minimum-TID read and write reservations;
- read-only optimization;
- RAW, WAR, and WAW analysis;
- default deterministic reordering Rule 2;
- optional basic Rule 1;
- relative-order-preserving retry.

## MBE adaptations

- one consensus block contains one or more internal Aria epochs;
- MBE nonce-gap failures may be retried in a later internal epoch;
- MBE transfer and canonical workload semantics replace Aria's YCSB/Smallbank
  database transaction classes;
- MBE's existing durable commit applies the final block state delta.

## Excluded

- Aria distributed database coordinators and partition ownership;
- remote database read/reservation messages;
- AriaFB fallback locking;
- Calvin-style fallback;
- snapshot-isolation-only mode;
- claims of exact source-code identity or production database equivalence.
