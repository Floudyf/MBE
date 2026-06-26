from __future__ import annotations

from collections import deque

from backend.app.services.v3_runtime.models import Transaction


class FifoTxPool:
    def __init__(self, max_pool_size: int, dedup_enabled: bool = True) -> None:
        self.max_pool_size = max_pool_size
        self.dedup_enabled = dedup_enabled
        self._queue: deque[Transaction] = deque()
        self._seen: set[str] = set()
        self.admit_times: dict[str, int] = {}

    def admit(self, tx: Transaction, admit_time_ms: int) -> bool:
        if self.dedup_enabled and tx.tx_id in self._seen:
            return False
        if len(self._queue) >= self.max_pool_size:
            return False
        self._queue.append(tx)
        self._seen.add(tx.tx_id)
        self.admit_times[tx.tx_id] = max(admit_time_ms, tx.submit_time_ms)
        return True

    def select_for_block(self, max_tx_per_block: int) -> list[Transaction]:
        selected: list[Transaction] = []
        while self._queue and len(selected) < max_tx_per_block:
            selected.append(self._queue.popleft())
        return selected

    def __len__(self) -> int:
        return len(self._queue)
