"""Streaming JSONL and JSONL.GZ writer for formal V0 transaction traces."""

from __future__ import annotations

import gzip
import json
import io
from collections.abc import Iterable, Mapping
from pathlib import Path
from typing import TextIO

REQUIRED_FIELDS = ("tx_id", "tx_type", "timestamp", "chain_id", "contract", "function", "args", "read_set", "write_set", "access_list", "commutative", "update_type", "status", "chain_latency_ms")


def validate_record(record: Mapping[str, object]) -> None:
    missing = [field for field in REQUIRED_FIELDS if field not in record or record[field] is None]
    if missing:
        raise ValueError(f"trace record is missing required fields: {', '.join(missing)}")


class TraceJSONLWriter:
    """Writes one validated trace record at a time; no trace is buffered in memory."""

    def __init__(self, path: str | Path) -> None:
        self.path = Path(path)
        self._stream: TextIO | None = None

    def __enter__(self) -> "TraceJSONLWriter":
        self.path.parent.mkdir(parents=True, exist_ok=True)
        if self.path.suffix == ".gz":
            raw = self.path.open("wb")
            self._stream = io.TextIOWrapper(gzip.GzipFile(fileobj=raw, mode="wb", mtime=0), encoding="utf-8")
        else:
            self._stream = self.path.open("w", encoding="utf-8")
        return self

    def write(self, record: Mapping[str, object]) -> None:
        if self._stream is None:
            raise RuntimeError("writer is not open")
        validate_record(record)
        self._stream.write(json.dumps(record, ensure_ascii=False, separators=(",", ":")) + "\n")

    def __exit__(self, *_: object) -> None:
        if self._stream is not None:
            self._stream.close()


def write_trace(records: Iterable[Mapping[str, object]], path: str | Path) -> None:
    """Consume ``records`` once and stream each record to JSONL or JSONL.GZ."""
    with TraceJSONLWriter(path) as writer:
        for record in records:
            writer.write(record)
