# Groundhog mapping to MBE

## Plugin composition

```text
routing        = hash_routing_baseline
block_producer = groundhog_block_producer
execution      = serial_execution_baseline
scheduler      = fifo_serial_scheduler
block_executor = groundhog_block_executor
commit         = normal_commit
```

`serial_execution_baseline` is used only as the existing classification interface. Groundhog business execution is performed by `groundhog_block_executor`.

## Transaction mapping

- Transfer sender balance: nonnegative integer set-add with `-Value`.
- Transfer receiver balance: nonnegative integer set-add with `+Value`.
- `AccessCommutativeDelta`: nonnegative integer set-add using the declared delta.
- Read-only access: block-start snapshot observation only.
- Ordinary write/read-write access: deterministic byte-string set.
- Replay protection: ordered-set insertion under `groundhog:replay:<sender>` using `TxID` and `Nonce + 1`.

Legacy sequential `nonce:<sender>` updates are intentionally not materialized because Groundhog blocks do not define an intra-block predecessor transaction order.

## Shared-interface changes

`BlockProductionInput` gains optional context, snapshot, and worker-count fields. The runtime populates the state snapshot only when `groundhog_block_producer` is selected, so other block producers do not incur snapshot-copy overhead.

`Proposer.BuildFromReserved` is added in a new file and leaves the existing `Proposer.Build` implementation unchanged.
