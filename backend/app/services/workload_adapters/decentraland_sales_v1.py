from __future__ import annotations

import csv
import hashlib
import re
from collections import Counter
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Iterator

from backend.app.services.workload_adapters.base import SourceValidationSummary


class AdapterDataError(ValueError):
    pass


class DecentralandSalesAdapter:
    adapter_id = "decentraland_sales_v1"

    _columns = ("id", "tx_hash", "buyer", "seller", "price", "timestamp", "category", "raw_contract_candidates")
    _address = re.compile(r"^0x[a-fA-F0-9]{40}$")
    _tx_hash = re.compile(r"^0x[a-fA-F0-9]{64}$")

    def validate_source(self, path: Path, manifest: dict[str, Any], *, expected_sha256: str | None = None) -> SourceValidationSummary:
        source_hash = _sha256_file(path)
        if expected_sha256 and source_hash != expected_sha256.lower():
            raise AdapterDataError("source SHA-256 mismatch")
        ids: set[str] = set()
        tx_hashes: set[str] = set()
        operations: Counter[str] = Counter()
        start: int | None = None
        end: int | None = None
        with path.open("r", encoding="utf-8", newline="") as stream:
            reader = csv.DictReader(stream)
            if tuple(reader.fieldnames or ()) != self._columns:
                raise AdapterDataError("CSV header does not match the Decentraland sales adapter contract")
            for source_row_index, row in enumerate(reader):
                self._validate_row(row, source_row_index, ids, tx_hashes)
                timestamp = int(row["timestamp"])
                operation = row["category"].strip()
                operations[operation] += 1
                start = timestamp if start is None else min(start, timestamp)
                end = timestamp if end is None else max(end, timestamp)
        if not ids:
            raise AdapterDataError("CSV contains no records")
        return SourceValidationSummary(source_hash, len(ids), len(tx_hashes), start or 0, end or 0, dict(sorted(operations.items())))

    def iter_canonical_records(self, path: Path, manifest: dict[str, Any]) -> Iterator[dict[str, Any]]:
        with path.open("r", encoding="utf-8", newline="") as stream:
            reader = csv.DictReader(stream)
            for source_row_index, row in enumerate(reader):
                sender = row["buyer"].strip().lower()
                receiver = row["seller"].strip().lower()
                contract = self._single_contract(row["raw_contract_candidates"], source_row_index)
                operation_type = row["category"].strip()
                yield {
                    "schema_version": "mbe_workload_record_v1",
                    "dataset_id": manifest["dataset_id"],
                    "source_row_index": source_row_index,
                    "source_event_id": row["id"].strip(),
                    "source_tx_hash": row["tx_hash"].strip().lower(),
                    "timestamp_ms": int(row["timestamp"]),
                    "sender_id": sender,
                    "receiver_id": receiver,
                    "operation_type": operation_type,
                    "runtime_value": 1,
                    "state_keys": [f"account:sender:{sender}", f"account:receiver:{receiver}", f"contract:{contract}"],
                    "routing_source_key": f"account:sender:{sender}",
                    "routing_target_key": f"contract:{contract}",
                    "skew_keys": {"contract": f"contract:{contract}", "receiver": f"account:receiver:{receiver}"},
                    "provenance": {"source_platform": "decentraland_marketplace", "source_chain": "polygon_mainnet", "adapter_id": self.adapter_id},
                    "metadata": {"price_raw": row["price"].strip(), "price_bucket": _price_bucket(row["price"].strip()), "source_category": operation_type},
                }

    def _validate_row(self, row: dict[str, str], source_row_index: int, ids: set[str], tx_hashes: set[str]) -> None:
        if any(not (row.get(column) or "").strip() for column in self._columns):
            raise AdapterDataError(f"row {source_row_index}: required source field is empty")
        event_id = row["id"].strip()
        if event_id in ids:
            raise AdapterDataError(f"row {source_row_index}: duplicate sale id")
        ids.add(event_id)
        tx_hash = row["tx_hash"].strip()
        if not self._tx_hash.fullmatch(tx_hash):
            raise AdapterDataError(f"row {source_row_index}: invalid tx_hash")
        tx_hashes.add(tx_hash.lower())
        self._require_address(row["buyer"].strip(), "buyer", source_row_index)
        self._require_address(row["seller"].strip(), "seller", source_row_index)
        self._single_contract(row["raw_contract_candidates"], source_row_index)
        if row["category"].strip() not in {"wearable", "emote"}:
            raise AdapterDataError(f"row {source_row_index}: unsupported category")
        try:
            timestamp = int(row["timestamp"])
            if timestamp < 0:
                raise ValueError
        except ValueError as exc:
            raise AdapterDataError(f"row {source_row_index}: timestamp must be a millisecond integer") from exc
        try:
            price = Decimal(row["price"])
        except InvalidOperation as exc:
            raise AdapterDataError(f"row {source_row_index}: price must be a decimal string") from exc
        if not price.is_finite() or price < 0:
            raise AdapterDataError(f"row {source_row_index}: price must be a non-negative decimal")

    def _require_address(self, value: str, name: str, row_index: int) -> str:
        if not self._address.fullmatch(value):
            raise AdapterDataError(f"row {row_index}: invalid {name}")
        return value.lower()

    def _single_contract(self, value: str, row_index: int) -> str:
        candidates = re.findall(r"0x[a-fA-F0-9]{40}", value)
        if len(candidates) != 1:
            raise AdapterDataError(f"row {row_index}: raw_contract_candidates must contain exactly one address")
        return candidates[0].lower()


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _price_bucket(raw: str) -> int:
    digits = raw.split(".", 1)[0].lstrip("0")
    return len(digits) - 1 if digits else 0

