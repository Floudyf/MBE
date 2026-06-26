from __future__ import annotations

import random
from typing import Any

from backend.app.services.v3_runtime.models import Transaction


def generate_synthetic_workload(workload: dict[str, Any], state_key_count: int) -> list[Transaction]:
    tx_count = int(workload.get("tx_count", 20))
    seed = int(workload.get("seed", 42))
    submit_rate = max(1, int(workload.get("submit_rate", 100)))
    key_count = max(1, int(workload.get("key_count", state_key_count)))
    rng = random.Random(seed)
    interval_ms = max(1, round(1000 / submit_rate))
    transactions: list[Transaction] = []
    operations = ["transfer", "update", "reward"]
    for index in range(tx_count):
        key_index = rng.randrange(key_count)
        hot_bias = int(workload.get("hot_key_count", max(1, key_count // 10)))
        if workload.get("hotspot_ratio", 0) and rng.random() < float(workload.get("hotspot_ratio", 0)):
            key_index = rng.randrange(max(1, hot_bias))
        state_key = f"asset_{key_index}"
        op = operations[index % len(operations)]
        delta = 1 + (index % 5)
        transactions.append(
            Transaction(
                tx_id=f"tx_{index:06d}",
                submit_time_ms=index * interval_ms,
                operation=op,
                read_keys=[state_key],
                write_deltas={state_key: delta},
            )
        )
    return transactions
