# Batch-SI mechanism specification

## Planning pipeline

1. Extract deterministic read/write sets from each immutable MBE transaction access list.
2. Construct AWRT: logical state key -> ordered writer transaction IDs.
3. Apply WRBP:
   - read-only transactions enter the first batch;
   - no batch contains two writers of the same logical key;
   - a transaction may reuse an earlier write-opportunity batch only when every written key can use the same batch;
   - batches are compacted by batch number without changing membership.
4. Apply OFAS inside each WRBP batch:
   - self read/write keys are excluded from dependency counting;
   - readers are ordered before the unique writer of the key;
   - priorities use fewer reads on the writer's keys, then more reads by the transaction, then smaller transaction ID;
   - a Rule 2 cycle victim is deterministically deferred.
5. Rebuild the plan over the accepted transaction set and bind its digest and payload to the proposed block before PBFT.

## Execution pipeline

For batch `k`:

1. Copy the state produced by batch `k-1` into immutable snapshot `BS_k`.
2. Execute accepted transactions in parallel up to `worker_count`; every transaction reads `BS_k`.
3. Produce private Batch-SI receipts and per-transaction `TxDelta` values.
4. Validate that every observed write was declared by the transaction access list.
5. Materialize deltas deterministically in the committed OFAS order.
6. Continue with the next batch.

## Deferred transactions

OFAS-deferred candidate transactions are removed before consensus and released from the leader's reserved mempool set. They are not represented as failed business receipts in the accepted block.

## Determinism

- no Go map iteration determines transaction or key order;
- keys and transaction IDs are explicitly sorted;
- the accepted block carries the plan payload digest and semantic plan digest;
- validators recompute and verify the accepted-set plan;
- worker counts 1, 2, 4, and 8 must produce the same final state root and receipt order.
