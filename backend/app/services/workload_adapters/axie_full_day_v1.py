from __future__ import annotations

import csv
from collections import Counter
from pathlib import Path
from typing import Any, Iterator

from backend.app.services.workload_adapters.base import SourceValidationSummary
from backend.app.services.workload_adapters.common import canonical_v3_record, parse_timestamp_ms, sha256_file


class AxieFullDayAdapter:
    adapter_id = "axie_full_day_v1"

    _required_columns = {
        "tx_seq", "block_time", "block_number", "tx_index", "tx_hash", "tx_from", "tx_to",
        "axie_from", "axie_to", "axie_token_id", "axie_transfer_count", "event_kind",
        "accesses_marketplace_hotspot", "operation_class",
    }

    def validate_source(self, path: Path, manifest: dict[str, Any], *, expected_sha256: str | None = None) -> SourceValidationSummary:
        source_hash = sha256_file(path)
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
            if record.get("source_tx_hash"):
                tx_hashes.add(str(record["source_tx_hash"]))
            timestamp = int(record["timestamp_ms"])
            start = timestamp if start is None else min(start, timestamp)
            end = timestamp if end is None else max(end, timestamp)
        if count == 0:
            raise ValueError("CSV contains no records")
        return SourceValidationSummary(source_hash, count, len(tx_hashes), start or 0, end or 0, dict(sorted(operations.items())))

    def iter_canonical_records(self, path: Path, manifest: dict[str, Any]) -> Iterator[dict[str, Any]]:
        with path.open("r", encoding="utf-8", newline="") as stream:
            reader = csv.DictReader(stream)
            missing = sorted(self._required_columns - set(reader.fieldnames or []))
            if missing:
                raise ValueError(f"Axie CSV header missing fields: {', '.join(missing)}")
            for row_index, row in enumerate(reader):
                source_event_id = row["tx_seq"].strip()
                sender = row["axie_from"].strip().lower() or row["tx_from"].strip().lower()
                receiver = row["axie_to"].strip().lower() or row["tx_to"].strip().lower()
                token = row["axie_token_id"].strip()
                contract = row["tx_to"].strip().lower()
                if not source_event_id or not sender or not receiver or not token:
                    raise ValueError(f"row {row_index}: missing transfer identity")
                actor_from = f"axie_account:{sender}"
                actor_to = f"axie_account:{receiver}"
                token_key = f"axie_token:{token}"
                contract_key = f"axie_contract:{contract}"
                access = [
                    {"key": actor_from, "mode": "read_write", "update_semantics": "axie_owner_debit"},
                    {"key": actor_to, "mode": "read_write", "update_semantics": "axie_owner_credit"},
                    {"key": token_key, "mode": "read_write", "update_semantics": "axie_ownership_transfer"},
                    {"key": contract_key, "mode": "read", "update_semantics": "axie_contract_metadata"},
                ]
                hotspot = row["accesses_marketplace_hotspot"].strip().lower() in {"true", "1", "yes"}
                if hotspot:
                    access.append({"key": "axie_marketplace:global", "mode": "read_write", "update_semantics": "marketplace_hotspot_state"})
                operation_class = row["operation_class"].strip() or "unknown"
                yield canonical_v3_record(
                    dataset_id=manifest["dataset_id"],
                    source_row_index=row_index,
                    source_event_id=source_event_id,
                    source_tx_hash=row["tx_hash"].strip().lower() or None,
                    timestamp_ms=parse_timestamp_ms(row["block_time"]),
                    sender_id=sender,
                    receiver_id=receiver,
                    operation_type="axie_transfer",
                    state_keys=[item["key"] for item in access],
                    routing_source_key=actor_from,
                    routing_target_key=actor_to,
                    skew_keys={"axie_token": token_key, "receiver": actor_to, "contract": contract_key, "marketplace": "axie_marketplace:global" if hotspot else contract_key},
                    provenance={"adapter_id": self.adapter_id, "source_chain": "ronin", "source_dataset": "axie_2021_10_01_full_day"},
                    metadata={
                        "block_number": row["block_number"].strip(),
                        "tx_index": int(float(row["tx_index"] or 0)),
                        "tx_from": row["tx_from"].strip().lower(),
                        "tx_to": contract,
                        "axie_transfer_count": int(float(row["axie_transfer_count"] or 1)),
                        "event_kind": row["event_kind"].strip(),
                        "operation_class": operation_class,
                        "accesses_marketplace_hotspot": hotspot,
                        "access_precision": "observed_transfer_semantic_logical_state",
                    },
                    access_list_schema="axie_transfer_semantic_access_v1",
                    access_list_source="observed_transfer_semantics",
                    access_list=access,
                )
