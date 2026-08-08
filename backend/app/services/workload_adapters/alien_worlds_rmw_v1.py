from __future__ import annotations

import csv
import gzip
import json
from collections import Counter
from pathlib import Path
from typing import Any, Iterator

from backend.app.services.workload_adapters.base import SourceValidationSummary
from backend.app.services.workload_adapters.common import (
    access_list_digest,
    canonical_v3_record,
    normalize_access_list,
    parse_json_array,
    parse_json_object,
    parse_timestamp_ms,
    sha256_file,
)


class AlienWorldsRMWAdapter:
    adapter_id = "alien_worlds_rmw_v1"

    _required_columns = {
        "schema_version",
        "transaction_id",
        "timestamp",
        "sender_id",
        "receiver_id",
        "operation_type",
        "state_keys",
        "read_keys",
        "write_keys",
        "routing_source_key",
        "routing_target_key",
        "provenance",
        "metadata",
        "access_list_schema",
        "access_list_source",
        "access_list",
        "access_list_digest",
        "target_access_theta",
        "access_profile",
    }

    def validate_source(self, path: Path, manifest: dict[str, Any], *, expected_sha256: str | None = None) -> SourceValidationSummary:
        source_hash = sha256_file(path)
        if expected_sha256 and source_hash != expected_sha256.lower():
            raise ValueError("source SHA-256 mismatch")
        operations: Counter[str] = Counter()
        start: int | None = None
        end: int | None = None
        count = 0
        for record in self.iter_canonical_records(path, manifest):
            count += 1
            operations[record["operation_type"]] += 1
            timestamp = int(record["timestamp_ms"])
            start = timestamp if start is None else min(start, timestamp)
            end = timestamp if end is None else max(end, timestamp)
        if count == 0:
            raise ValueError("CSV.GZ contains no records")
        return SourceValidationSummary(source_hash, count, 0, start or 0, end or 0, dict(sorted(operations.items())))

    def iter_canonical_records(self, path: Path, manifest: dict[str, Any]) -> Iterator[dict[str, Any]]:
        with gzip.open(path, "rt", encoding="utf-8", newline="") as stream:
            reader = csv.DictReader(stream)
            missing = sorted(self._required_columns - set(reader.fieldnames or []))
            if missing:
                raise ValueError(f"Alien World CSV.GZ header missing fields: {', '.join(missing)}")
            for row_index, row in enumerate(reader):
                if (row.get("schema_version") or "").strip() != "mbe_workload_record_v3":
                    raise ValueError(f"row {row_index}: expected mbe_workload_record_v3")
                state_keys = parse_json_array(row["state_keys"], field="state_keys")
                read_keys = set(parse_json_array(row["read_keys"], field="read_keys"))
                write_keys = set(parse_json_array(row["write_keys"], field="write_keys"))
                access_list = normalize_access_list(row["access_list"])
                if sorted(state_keys) != [item["key"] for item in access_list]:
                    raise ValueError(f"row {row_index}: state_keys/access_list mismatch")
                access_read = {item["key"] for item in access_list if item["mode"] in {"read", "read_write", "commutative_delta"}}
                access_write = {item["key"] for item in access_list if item["mode"] in {"write", "read_write", "commutative_delta"}}
                if read_keys != access_read or write_keys != access_write:
                    raise ValueError(f"row {row_index}: read/write key topology mismatch")
                digest = access_list_digest(access_list)
                if digest != (row.get("access_list_digest") or "").strip().lower():
                    raise ValueError(f"row {row_index}: access_list_digest mismatch")
                metadata = parse_json_object(row["metadata"], field="metadata")
                metadata.update({
                    "target_access_theta": float(row["target_access_theta"]),
                    "access_profile": row["access_profile"].strip(),
                    "segment_id": int(row.get("segment_id") or 0),
                    "segment_row_index": int(row.get("segment_row_index") or row_index),
                    "template_source_row_index": int(row.get("template_source_row_index") or row_index),
                    "read_key_count": len(read_keys),
                    "write_key_count": len(write_keys),
                    "read_write_key_count": len(read_keys & write_keys),
                })
                routing_source = row["routing_source_key"].strip()
                routing_target = row["routing_target_key"].strip() or None
                skew_list = parse_json_array(row.get("skew_keys") or row["state_keys"], field="skew_keys")
                yield canonical_v3_record(
                    dataset_id=manifest["dataset_id"],
                    source_row_index=int(row.get("template_source_row_index") or row_index),
                    source_event_id=row["transaction_id"].strip(),
                    source_tx_hash=None,
                    timestamp_ms=parse_timestamp_ms(row["timestamp"]),
                    sender_id=row["sender_id"].strip().lower(),
                    receiver_id=row["receiver_id"].strip().lower() or None,
                    operation_type=row["operation_type"].strip(),
                    state_keys=state_keys,
                    routing_source_key=routing_source,
                    routing_target_key=routing_target,
                    skew_keys={
                        "primary_state_key": skew_list[0],
                        "routing_source": routing_source,
                        "routing_target": routing_target or routing_source,
                    },
                    provenance={"adapter_id": self.adapter_id, "source": row["provenance"].strip()},
                    metadata=metadata,
                    access_list_schema=row["access_list_schema"].strip(),
                    access_list_source=row["access_list_source"].strip(),
                    access_list=access_list,
                )
