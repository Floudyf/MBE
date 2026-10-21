"""Farmers World WAX action -> one-real-transaction modeled V4 replay adapter.

Only MODELLED actor-state keys are produced. No on-chain storage R/W truth claim.
The original crawler bytes remain read-only and are never touched by this module.
"""
from __future__ import annotations

import gzip
import hashlib
import json
from collections import Counter
from pathlib import Path
from typing import Any, Iterator

from backend.app.services.workload_adapters.base import SourceValidationSummary


class FarmersWorldTxV4Adapter:
    adapter_id = "fw_tx_layered_v4"

    def iter_canonical_records(self, path: Path, manifest: dict[str, Any]) -> Iterator[dict[str, Any]]:
        with gzip.open(path, "rt", encoding="utf-8") as reader:
            for number, line in enumerate(reader):
                if not line.strip():
                    raise ValueError(f"empty normalized FW row {number}")
                record = json.loads(line)
                if record.get("schema_version") != "mbe_workload_record_v4" or record.get("dataset_id") != manifest["dataset_id"]:
                    raise ValueError(f"FW V4 identity mismatch at row {number}")
                if record.get("source_row_index") != number:
                    raise ValueError(f"FW source order mismatch at row {number}")
                if not record.get("source_tx_hash") or record.get("source_event_id") != "fw_tx:" + record["source_tx_hash"]:
                    raise ValueError(f"FW tx evidence missing at row {number}")
                yield record

    def validate_source(self, path: Path, manifest: dict[str, Any], *, expected_sha256: str | None = None) -> SourceValidationSummary:
        h = hashlib.sha256()
        with path.open("rb") as reader:
            for block in iter(lambda: reader.read(1024 * 1024), b""):
                h.update(block)
        actual = h.hexdigest()
        if expected_sha256 and actual != expected_sha256.lower():
            raise ValueError("Farmers World source SHA256 mismatch")
        count = 0
        txids: set[str] = set()
        op = Counter()
        first = last = None
        previous = (-1, -1)
        for record in self.iter_canonical_records(path, manifest):
            idx = record["source_row_index"]
            txid = record["source_tx_hash"]
            if txid in txids:
                raise ValueError(f"duplicate transaction identity: {txid}")
            txids.add(txid)
            t = int(record["timestamp_ms"])
            if (t, idx) < previous:
                raise ValueError(f"transaction chronology regressed at {idx}")
            previous = (t, idx)
            if not record.get("static_access_envelope", {}).get("complete"):
                raise ValueError("incomplete MODELLED access envelope")
            count += 1
            op[record["operation_type"]] += 1
            first = t if first is None else min(first, t)
            last = t if last is None else max(last, t)
        if not count or (manifest.get("row_count") and count != int(manifest["row_count"])):
            raise ValueError("Farmers World expected row count mismatch")
        return SourceValidationSummary(actual, count, len(txids), first or 0, last or 0, dict(op))
