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
    parse_timestamp_ms,
    sha256_file,
)


class AxieControlledRMWAdapter:
    adapter_id = "axie_controlled_rmw_v1"

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
        "skew_keys",
        "routing_source_key",
        "routing_target_key",
        "provenance",
        "metadata",
        "access_list_schema",
        "access_list_source",
        "access_list",
        "access_list_digest",
        "segment_id",
        "segment_row_index",
        "template_source_row_index",
        "target_access_theta",
        "target_account_write_theta",
        "access_profile",
    }

    @staticmethod
    def _json_string_list(value: str, *, field: str, allow_empty: bool = False) -> list[str]:
        try:
            parsed = json.loads(value)
        except json.JSONDecodeError as exc:
            raise ValueError(f"{field} must be a JSON array") from exc
        if not isinstance(parsed, list):
            raise ValueError(f"{field} must be a JSON array")
        result = [str(item).strip() for item in parsed]
        if (not allow_empty and not result) or any(not item for item in result):
            raise ValueError(f"{field} contains invalid/empty keys")
        if len(result) != len(set(result)):
            raise ValueError(f"{field} contains duplicate keys")
        return result

    @staticmethod
    def _skew_values(value: str) -> list[str]:
        try:
            parsed = json.loads(value)
        except json.JSONDecodeError as exc:
            raise ValueError("skew_keys must be valid JSON") from exc
        if isinstance(parsed, list):
            result = [str(item).strip() for item in parsed if str(item).strip()]
        elif isinstance(parsed, dict):
            result = [str(item).strip() for item in parsed.values() if str(item).strip()]
        else:
            raise ValueError("skew_keys must be a JSON array or object")
        if not result:
            raise ValueError("skew_keys contains no usable key")
        return result

    @staticmethod
    def _metadata(value: str) -> dict[str, Any]:
        try:
            parsed = json.loads(value)
        except json.JSONDecodeError as exc:
            raise ValueError("metadata must be a JSON object") from exc
        if not isinstance(parsed, dict):
            raise ValueError("metadata must be a JSON object")
        return parsed

    def validate_source(
        self,
        path: Path,
        manifest: dict[str, Any],
        *,
        expected_sha256: str | None = None,
    ) -> SourceValidationSummary:
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
            raise ValueError("Axie controlled CSV.GZ contains no records")
        return SourceValidationSummary(
            source_hash,
            count,
            0,
            start or 0,
            end or 0,
            dict(sorted(operations.items())),
        )

    def iter_canonical_records(
        self,
        path: Path,
        manifest: dict[str, Any],
    ) -> Iterator[dict[str, Any]]:
        with gzip.open(path, "rt", encoding="utf-8-sig", newline="") as stream:
            reader = csv.DictReader(stream)
            missing = sorted(self._required_columns - set(reader.fieldnames or []))
            if missing:
                raise ValueError(
                    "Axie controlled CSV.GZ header missing fields: " + ", ".join(missing)
                )
            for row_index, row in enumerate(reader):
                if (row.get("schema_version") or "").strip() != "mbe_workload_record_v3":
                    raise ValueError(f"row {row_index}: expected mbe_workload_record_v3")
                state_keys = self._json_string_list(row["state_keys"], field="state_keys")
                read_keys = set(
                    self._json_string_list(row["read_keys"], field="read_keys", allow_empty=True)
                )
                write_keys = set(
                    self._json_string_list(row["write_keys"], field="write_keys", allow_empty=True)
                )
                access_list = normalize_access_list(row["access_list"])
                access_keys = [item["key"] for item in access_list]
                if sorted(state_keys) != access_keys:
                    raise ValueError(f"row {row_index}: state_keys/access_list mismatch")
                access_read = {
                    item["key"]
                    for item in access_list
                    if item["mode"] in {"read", "read_write", "commutative_delta"}
                }
                access_write = {
                    item["key"]
                    for item in access_list
                    if item["mode"] in {"write", "read_write", "commutative_delta"}
                }
                if read_keys != access_read or write_keys != access_write:
                    raise ValueError(f"row {row_index}: read/write key topology mismatch")
                if access_list_digest(access_list) != (
                    row.get("access_list_digest") or ""
                ).strip().lower():
                    raise ValueError(f"row {row_index}: access_list_digest mismatch")

                transaction_id = (row.get("transaction_id") or "").strip()
                sender = (row.get("sender_id") or "").strip().lower()
                receiver = (row.get("receiver_id") or "").strip().lower() or None
                routing_source = (row.get("routing_source_key") or "").strip()
                routing_target = (row.get("routing_target_key") or "").strip() or None
                operation = (row.get("operation_type") or "").strip()
                if not transaction_id or not sender or not routing_source or not operation:
                    raise ValueError(f"row {row_index}: missing transaction/routing identity")

                skew_values = self._skew_values(row["skew_keys"])
                metadata = self._metadata(row["metadata"])
                metadata.update(
                    {
                        "target_access_theta": float(row["target_access_theta"]),
                        "target_account_write_theta": float(row["target_account_write_theta"]),
                        "access_profile": row["access_profile"].strip(),
                        "segment_id": int(row.get("segment_id") or 0),
                        "segment_row_index": int(row.get("segment_row_index") or row_index),
                        "template_source_row_index": int(
                            row.get("template_source_row_index") or row_index
                        ),
                        "read_key_count": len(read_keys),
                        "write_key_count": len(write_keys),
                        "read_write_key_count": len(read_keys & write_keys),
                    }
                )
                yield canonical_v3_record(
                    dataset_id=manifest["dataset_id"],
                    source_row_index=int(
                        row.get("template_source_row_index") or row_index
                    ),
                    source_event_id=transaction_id,
                    source_tx_hash=None,
                    timestamp_ms=parse_timestamp_ms(row["timestamp"]),
                    sender_id=sender,
                    receiver_id=receiver,
                    operation_type=operation,
                    state_keys=state_keys,
                    routing_source_key=routing_source,
                    routing_target_key=routing_target,
                    skew_keys={
                        "primary_state_key": skew_values[0],
                        "routing_source": routing_source,
                        "routing_target": routing_target or routing_source,
                    },
                    provenance={
                        "adapter_id": self.adapter_id,
                        "source_chain": "ronin",
                        "source": (row.get("provenance") or "").strip(),
                    },
                    metadata=metadata,
                    access_list_schema=(row.get("access_list_schema") or "").strip(),
                    access_list_source=(row.get("access_list_source") or "").strip(),
                    access_list=access_list,
                )
