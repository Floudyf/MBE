# Aria Mechanism Specification

## Epoch execution

Every Aria epoch receives an ordered candidate list and one immutable state
snapshot. Candidate transactions execute in parallel against private overlays.
Actual read observations and logical write sets are retained; speculative writes
are not exposed to other transactions in the epoch.

## Reservations

For every observed key, the epoch records:

- the minimum reader TID;
- the minimum writer TID.

TID is the candidate position plus one, matching Aria's deterministic batch
ordering.

## Dependency analysis

For transaction `T`:

- RAW exists when a smaller-TID writer reserved a key read by `T`;
- WAR exists when a smaller-TID reader reserved a key written by `T`;
- WAW exists when a smaller-TID writer reserved a key written by `T`.

Default Rule 2 commits when there is no WAW and the transaction does not have
both RAW and WAR. The optional Rule 1 mode commits only when neither WAW nor RAW
exists. Read-only transactions use the official fast-commit optimization.

## Deferred execution

Conflict-aborted transactions preserve relative order and execute against the
state produced by the previous epoch. Execution repeats until all transactions
are finalized or the configured epoch limit is reached.
