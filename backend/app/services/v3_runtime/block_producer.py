from __future__ import annotations

from backend.app.services.v3_runtime.models import Block
from backend.app.services.v3_runtime.tx_pool import FifoTxPool


class TimeOrCountBlockProducer:
    def __init__(self, block_interval_ms: int, max_tx_per_block: int) -> None:
        self.block_interval_ms = block_interval_ms
        self.max_tx_per_block = max_tx_per_block

    def cut_blocks(self, pool: FifoTxPool) -> list[Block]:
        blocks: list[Block] = []
        height = 1
        while len(pool):
            txs = pool.select_for_block(self.max_tx_per_block)
            last_submit = max(tx.submit_time_ms for tx in txs)
            cut_time = max(height * self.block_interval_ms, last_submit)
            blocks.append(Block(block_height=height, block_id=f"block_{height:06d}", txs=txs, cut_time_ms=cut_time))
            height += 1
        return blocks
