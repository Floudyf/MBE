from __future__ import annotations

from collections.abc import Mapping


class DirectFetchState:
    def __init__(self, key_count: int = 0) -> None:
        self.values: dict[str, int] = {f"asset_{index}": 0 for index in range(max(0, key_count))}

    def read(self, keys: list[str]) -> tuple[dict[str, int], int, int]:
        values = {key: self.values.get(key, 0) for key in keys}
        return values, len(keys), 0

    def preview_write(self, deltas: Mapping[str, int]) -> dict[str, tuple[int, int, int]]:
        changes: dict[str, tuple[int, int, int]] = {}
        for key, delta in deltas.items():
            old_value = self.values.get(key, 0)
            changes[key] = (old_value, int(delta), old_value + int(delta))
        return changes
