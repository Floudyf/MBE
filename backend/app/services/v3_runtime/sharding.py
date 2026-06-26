from __future__ import annotations

import hashlib


def assign_hash_shard(state_key: str, shard_count: int) -> int:
    if shard_count <= 0:
        raise ValueError("shard_count must be positive")
    digest = hashlib.sha256(state_key.encode("utf-8")).hexdigest()
    return int(digest[:8], 16) % shard_count
