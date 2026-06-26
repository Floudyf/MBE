from __future__ import annotations

from backend.app.services.v3_runtime.models import Block, FinalizedBlock


class SimpleLeaderConsensus:
    def __init__(self, node_ids: list[str], plugin_id: str = "simple_leader", finality_delay_ms: int = 1) -> None:
        if not node_ids:
            raise ValueError("simple_leader requires at least one logical node")
        self.node_ids = node_ids
        self.plugin_id = plugin_id
        self.finality_delay_ms = finality_delay_ms

    def finalize(self, block: Block) -> FinalizedBlock:
        proposer = self.node_ids[(block.block_height - 1) % len(self.node_ids)]
        ordered_time = block.cut_time_ms + 1
        finalized_time = ordered_time + self.finality_delay_ms
        return FinalizedBlock(
            block=block,
            proposer_node=proposer,
            ordered_time_ms=ordered_time,
            finalized_time_ms=finalized_time,
            consensus_plugin=self.plugin_id,
        )
