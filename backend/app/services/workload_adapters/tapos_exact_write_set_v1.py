from __future__ import annotations

import csv
from collections import Counter
from pathlib import Path
from typing import Any, Iterator

from backend.app.services.workload_adapters.base import SourceValidationSummary
from backend.app.services.workload_adapters.common import canonical_v3_record, parse_json_array, parse_timestamp_ms, sha256_file


class TaposExactWriteSetAdapter:
    adapter_id = "tapos_exact_write_set_v1"

    _required_columns = {
        "schema_version", "transaction_id", "timestamp", "sender_id", "receiver_id",
        "operation_type", "state_keys", "entry_function_id", "write_key_count", "provenance",
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
            raise ValueError("CSV contains no records")
        return SourceValidationSummary(source_hash, count, 0, start or 0, end or 0, dict(sorted(operations.items())))

    def iter_canonical_records(self, path: Path, manifest: dict[str, Any]) -> Iterator[dict[str, Any]]:
        with path.open("r", encoding="utf-8", newline="") as stream:
            reader = csv.DictReader(stream)
            missing = sorted(self._required_columns - set(reader.fieldnames or []))
            if missing:
                raise ValueError(f"Tapos CSV header missing fields: {', '.join(missing)}")
            for row_index, row in enumerate(reader):
                state_keys = parse_json_array(row["state_keys"], field="state_keys")
                expected_count = int(row["write_key_count"])
                if expected_count != len(state_keys):
                    raise ValueError(f"row {row_index}: write_key_count mismatch")
                sender = row["sender_id"].strip().lower()
                receiver = row["receiver_id"].strip().lower()
                if not sender or not receiver:
                    raise ValueError(f"row {row_index}: sender/receiver is empty")
                access = [{"key": key, "mode": "write", "update_semantics": "tapos_observed_exact_write"} for key in state_keys]
                source_key = f"tapos_sender:{sender}"
                primary = state_keys[0]
                yield canonical_v3_record(
                    dataset_id=manifest["dataset_id"],
                    source_row_index=row_index,
                    source_event_id=row["transaction_id"].strip(),
                    source_tx_hash=None,
                    timestamp_ms=parse_timestamp_ms(row["timestamp"]),
                    sender_id=sender,
                    receiver_id=receiver,
                    operation_type=row["operation_type"].strip() or "tapos_play",
                    state_keys=state_keys,
                    routing_source_key=source_key,
                    routing_target_key=primary,
                    skew_keys={"primary_write_key": primary, "entry_function": row["entry_function_id"].strip()},
                    provenance={"adapter_id": self.adapter_id, "source": row["provenance"].strip()},
                    metadata={
                        "entry_function_id": row["entry_function_id"].strip(),
                        "write_key_count": expected_count,
                        "access_precision": "observed_exact_write_set",
                    },
                    access_list_schema="tapos_exact_write_set_v1",
                    access_list_source="observed_exact_write_set",
                    access_list=access,
                )
