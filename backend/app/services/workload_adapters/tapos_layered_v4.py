"""Source-faithful Tapos write-set replay: observed WRITE keys, not invented READ keys.

The CSV is immutable. One input row yields exactly one canonical v4 row. The
runtime state projection and the scheduling declaration have the same known
write keys. The *complete envelope* refers to the modeled replay accesses,
NOT a claim that the original chain's unobserved READ set was measured.
"""
from __future__ import annotations

import csv
import hashlib
import io
import json
from collections import Counter
from pathlib import Path
from typing import Any, Iterator

from backend.app.services.workload_adapters.base import SourceValidationSummary
from backend.app.services.workload_adapters.common import parse_json_array, parse_timestamp_ms, sha256_file


class TaposLayeredV4Adapter:
    adapter_id = "mv_tapos_layered_v4"
    _columns = frozenset({"schema_version", "transaction_id", "timestamp", "sender_id", "receiver_id", "operation_type", "state_keys", "entry_function_id", "write_key_count", "provenance"})

    @staticmethod
    def _digest(rows: list[dict[str, Any]]) -> str:
        return hashlib.sha256(json.dumps(rows, ensure_ascii=False, separators=(",", ":")).encode("utf-8")).hexdigest()

    def _raw_rows(self, path: Path, *, start: int = 0, stop: int | None = None,
                  sparse_offsets: list[list[int]] | None = None) -> Iterator[tuple[int, dict[str, str]]]:
        with path.open("rb") as raw:
            first = raw.readline()
            if not first:
                raise ValueError("empty Tapos CSV")
            headers = next(csv.reader([first.decode("utf-8-sig")]))
            missing = self._columns - set(headers)
            if missing:
                raise ValueError("Tapos CSV header missing fields: " + ", ".join(sorted(missing)))
            origin = 0
            if start and sparse_offsets:
                candidates = [(int(i), int(off)) for i, off in sparse_offsets if 0 <= int(i) <= start and int(off) >= len(first)]
                if candidates:
                    origin, offset = max(candidates)
                    raw.seek(offset)
            reader = csv.DictReader(io.TextIOWrapper(raw, encoding="utf-8", newline=""), fieldnames=headers)
            for row_index, row in enumerate(reader, origin):
                if row_index < start:
                    continue
                if stop is not None and row_index >= stop:
                    break
                yield row_index, row

    def iter_canonical_records(self, path: Path, manifest: dict[str, Any], *, start: int = 0,
                               stop: int | None = None, sparse_offsets: list[list[int]] | None = None) -> Iterator[dict[str, Any]]:
        for idx, row in self._raw_rows(path, start=start, stop=stop, sparse_offsets=sparse_offsets):
            keys = parse_json_array(row["state_keys"], field=f"row {idx} state_keys")
            if int(row["write_key_count"]) != len(keys):
                raise ValueError(f"row {idx}: observed write_key_count does not match actual keys")
            sender, receiver = row["sender_id"].strip().lower(), row["receiver_id"].strip().lower()
            event_id = row["transaction_id"].strip()
            entry = row["entry_function_id"].strip()
            operation = row["operation_type"].strip()
            if not sender or not receiver or not event_id or not entry or not operation:
                raise ValueError(f"row {idx}: missing original Tapos relation")
            if row["schema_version"].strip() != "mbe_workload_record_v1":
                raise ValueError(f"row {idx}: unexpected source CSV schema")
            accesses = [{"key": key, "mode": "write", "update_semantics": "tapos_observed_exact_write"} for key in sorted(keys)]
            # Only observed storage write keys may enter the static envelope.
            # A sender identity is *not* silently inserted as a fabricated write.
            # Routing anchors follow the first and last source-observed keys.
            source_key, target_key = keys[0], keys[-1]
            source = row["provenance"].strip()
            yield {
                "schema_version": "mbe_workload_record_v4", "dataset_id": manifest["dataset_id"],
                "source_row_index": idx, "source_event_id": event_id, "source_tx_hash": None,
                "timestamp_ms": parse_timestamp_ms(row["timestamp"]), "sender_id": sender,
                "receiver_id": receiver, "operation_type": operation, "runtime_value": 1,
                "state_keys": sorted(keys),
                "static_access_envelope": {
                    "keys": sorted(keys), "complete": True,
                    "policy_id": "tapos_observed_write_set_replay_v4",
                    "construction_method": "directly_observed_write_keys_no_invented_chain_reads",
                },
                "scheduling_access_schema": "tapos_declared_write_set_v4",
                "scheduling_access_source": "source_observed_exact_write_keys_no_future_execution_oracle",
                "scheduling_access_list": accesses,
                "scheduling_access_digest": self._digest(accesses),
                "access_list_schema": "tapos_observed_write_set_v4",
                "access_list_source": "observed_exact_write_set_replay_projection",
                "access_list": accesses, "access_list_digest": self._digest(accesses),
                "routing_source_key": source_key, "routing_target_key": target_key,
                "skew_keys": {"primary_write_key": source_key, "entry_function": entry},
                "metadata": {
                    "entry_function_id": entry, "write_key_count": len(keys),
                    "source_write_key_order": keys,
                    "access_precision": "observed_exact_write_only",
                    "unobserved_read_set": "not_available_not_inferred",
                    "routing_projection": "source_first_last_observed_write_key",
                    "source_provenance": source,
                },
                "provenance": {
                    "adapter_id": self.adapter_id, "source": source,
                    "source_event_unit": "one_observed_csv_transaction",
                    "source_write_keys_observed": True,
                    "source_read_keys_observed": False,
                    "source_identity_preserved": True,
                },
            }

    def iter_canonical_window(self, path: Path, manifest: dict[str, Any], *, start: int, stop: int,
                              sparse_offsets: list[list[int]] | None = None) -> Iterator[dict[str, Any]]:
        return self.iter_canonical_records(path, manifest, start=start, stop=stop, sparse_offsets=sparse_offsets)

    def validate_source(self, path: Path, manifest: dict[str, Any], *, expected_sha256: str | None = None) -> SourceValidationSummary:
        source_hash = sha256_file(path)
        if expected_sha256 and source_hash.lower() != expected_sha256.lower():
            raise ValueError("Tapos source SHA256 mismatch")
        count = 0
        previous: tuple[int, int] | None = None
        seen_ids: set[str] = set()
        operations: Counter[str] = Counter()
        start_ms, end_ms = None, None
        for row in self.iter_canonical_records(path, manifest):
            idx = row["source_row_index"]
            key = (int(row["timestamp_ms"]), idx)
            if previous is not None and key < previous:
                raise ValueError(f"Tapos source timestamp order regressed at {idx}")
            previous = key
            eid = row["source_event_id"]
            if eid in seen_ids:
                raise ValueError(f"Tapos source duplicated transaction_id {eid}")
            seen_ids.add(eid)
            operations[row["operation_type"]] += 1
            count += 1
            start_ms = key[0] if start_ms is None else min(start_ms, key[0])
            end_ms = key[0] if end_ms is None else max(end_ms, key[0])
        if count != int(manifest.get("row_count") or 0):
            raise ValueError(f"Tapos source row count mismatch: {count}")
        return SourceValidationSummary(source_hash, count, 0, start_ms or 0, end_ms or 0, dict(sorted(operations.items())))
