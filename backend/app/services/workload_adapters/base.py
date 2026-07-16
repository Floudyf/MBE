from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterator, Protocol


@dataclass(frozen=True)
class SourceValidationSummary:
    source_sha256: str
    row_count: int
    unique_source_tx_hash_count: int
    time_start_ms: int
    time_end_ms: int
    operation_counts: dict[str, int]


class DatasetAdapter(Protocol):
    adapter_id: str

    def validate_source(self, path: Path, manifest: dict[str, Any], *, expected_sha256: str | None = None) -> SourceValidationSummary:
        ...

    def iter_canonical_records(self, path: Path, manifest: dict[str, Any]) -> Iterator[dict[str, Any]]:
        ...

