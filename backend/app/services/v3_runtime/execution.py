from __future__ import annotations

from backend.app.services.v3_runtime.models import FinalizedBlock, TxResult
from backend.app.services.v3_runtime.sharding import assign_hash_shard
from backend.app.services.v3_runtime.state_access import DirectFetchState


class SerialExecution:
    def __init__(self, shard_count: int, execution_cost_ms: int = 1, commit_delay_ms: int = 1) -> None:
        self.shard_count = shard_count
        self.execution_cost_ms = execution_cost_ms
        self.commit_delay_ms = commit_delay_ms

    def execute_block(
        self,
        finalized: FinalizedBlock,
        state: DirectFetchState,
        admit_times: dict[str, int],
    ) -> list[TxResult]:
        results: list[TxResult] = []
        cursor = finalized.finalized_time_ms
        for tx in finalized.block.txs:
            execution_start = cursor
            state.read(tx.read_keys)
            changes = state.preview_write(tx.write_deltas)
            execution_end = execution_start + self.execution_cost_ms
            commit_time = execution_end + self.commit_delay_ms
            first_key = next(iter(tx.write_deltas or tx.read_keys))
            results.append(
                TxResult(
                    tx_id=tx.tx_id,
                    submit_time_ms=tx.submit_time_ms,
                    admit_time_ms=admit_times[tx.tx_id],
                    block_height=finalized.block.block_height,
                    execution_start_ms=execution_start,
                    execution_end_ms=execution_end,
                    commit_time_ms=commit_time,
                    latency_ms=commit_time - tx.submit_time_ms,
                    status="success",
                    shard_id=assign_hash_shard(first_key, self.shard_count),
                    read_count=len(tx.read_keys),
                    write_count=len(tx.write_deltas),
                    remote_fetch_count=0,
                    deltas=changes,
                )
            )
            cursor = execution_end
        return results
