from __future__ import annotations

import csv
import hashlib
import json
from collections import Counter
from pathlib import Path
from typing import Any, Iterator

from backend.app.services.workload_adapters.base import SourceValidationSummary


class CanonicalCSVAdapter:
    adapter_id = "canonical_csv_v1"

    _columns = (
        "schema_version",
        "dataset_id",
        "source_row_index",
        "source_event_id",
        "source_tx_hash",
        "timestamp_ms",
        "sender_id",
        "receiver_id",
        "operation_type",
        "runtime_value",
        "state_keys",
        "routing_source_key",
        "routing_target_key",
        "skew_keys",
        "provenance",
        "metadata",
    )

    def validate_source(self, path: Path, manifest: dict[str, Any], *, expected_sha256: str | None = None) -> SourceValidationSummary:
        source_hash = _sha256_file(path)
        if expected_sha256 and source_hash != expected_sha256.lower():
            raise ValueError("source SHA-256 mismatch")
        operations: Counter[str] = Counter()
        tx_hashes: set[str] = set()
        start: int | None = None
        end: int | None = None
        count = 0
        for record in self.iter_canonical_records(path, manifest):
            count += 1
            operations[record["operation_type"]] += 1
            tx_hash = record.get("source_tx_hash")
            if tx_hash:
                tx_hashes.add(str(tx_hash))
            timestamp = int(record["timestamp_ms"])
            start = timestamp if start is None else min(start, timestamp)
            end = timestamp if end is None else max(end, timestamp)
        if count == 0:
            raise ValueError("CSV contains no records")
        return SourceValidationSummary(source_hash, count, len(tx_hashes), start or 0, end or 0, dict(sorted(operations.items())))

    def iter_canonical_records(self, path: Path, manifest: dict[str, Any]) -> Iterator[dict[str, Any]]:
        with path.open("r", encoding="utf-8", newline="") as stream:
            reader = csv.DictReader(stream)
            missing = [column for column in self._columns if column not in (reader.fieldnames or [])]
            if missing:
                raise ValueError(f"CSV header missing canonical fields: {', '.join(missing)}")
            for row_index, row in enumerate(reader):
                record = {
                    "schema_version": row["schema_version"].strip() or "mbe_workload_record_v1",
                    "dataset_id": row["dataset_id"].strip() or manifest["dataset_id"],
                    "source_row_index": int(row["source_row_index"] or row_index),
                    "source_event_id": row["source_event_id"].strip(),
                    "source_tx_hash": row["source_tx_hash"].strip() or None,
                    "timestamp_ms": int(row["timestamp_ms"]),
                    "sender_id": row["sender_id"].strip(),
                    "receiver_id": row["receiver_id"].strip() or None,
                    "operation_type": row["operation_type"].strip(),
                    "runtime_value": int(row["runtime_value"] or 1),
                    "state_keys": _parse_array(row["state_keys"]),
                    "routing_source_key": row["routing_source_key"].strip(),
                    "routing_target_key": row["routing_target_key"].strip() or None,
                    "skew_keys": _parse_object(row["skew_keys"]),
                    "provenance": {"adapter_id": self.adapter_id, **_parse_object(row["provenance"])},
                    "metadata": _parse_object(row["metadata"]),
                }
                yield record


def _parse_array(value: str) -> list[str]:
    value = value.strip()
    if not value:
        return []
    if value.startswith("["):
        parsed = json.loads(value)
        if not isinstance(parsed, list):
            raise ValueError("expected JSON array")
        return [str(item) for item in parsed]
    return [item.strip() for item in value.split("|") if item.strip()]


def _parse_object(value: str) -> dict[str, Any]:
    value = value.strip()
    if not value:
        return {}
    parsed = json.loads(value)
    if not isinstance(parsed, dict):
        raise ValueError("expected JSON object")
    return parsed


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()
