from __future__ import annotations

import hashlib
import json
from collections import Counter
from pathlib import Path
from typing import Any, Iterator

from backend.app.services.workload_adapters.base import SourceValidationSummary


class AlienWorldsLayeredV2Adapter:
    adapter_id = "alien_worlds_layered_v2"
    source_schema = "mbe_workload_record_v2"
    canonical_schema = "mbe_workload_record_v4"
    runtime_access_schema = "alien_worlds_layered_v2_runtime_projection_v1"
    scheduling_access_schema = "alien_worlds_layered_v2_scheduling_v1"

    _FORBIDDEN = {
        "actual_read_keys", "actual_write_keys", "actual_delta",
        "historical_execution_truth", "source_execution_oracle", "runtime_execution_truth",
        "stateversion", "stateversions", "state_version", "state_versions", "stateready", "state_ready",
        "accessstable", "access_stable", "toposafe", "topo_safe", "semanticsafe", "semantic_safe",
        "fast", "conservative", "classification", "final_classification",
        "read_keys", "write_keys", "state_keys", "access_list",
    }
    _SCHED_MODE = {"R": "read", "W": "write", "RW": "read_write", "UNKNOWN": "unknown"}
    _RUNTIME_MODE = {"R": "read", "W": "write", "RW": "read_write", "UNKNOWN": "read_write"}

    def validate_source(self, path: Path, manifest: dict[str, Any], *, expected_sha256: str | None = None) -> SourceValidationSummary:
        if not path.is_file():
            raise ValueError(f"Alien Worlds layered V2 input is missing: {path}")
        digest = _sha256_file(path)
        if expected_sha256 and digest.lower() != str(expected_sha256).lower():
            raise ValueError("Alien Worlds layered V2 input SHA-256 does not match manifest")
        count = 0
        txids: set[str] = set()
        operations: Counter[str] = Counter()
        first_order: int | None = None
        last_order: int | None = None
        for row_number, record in enumerate(self._iter_source(path), 1):
            self._validate_source_record(record, row_number)
            order = int(record["source_order"])
            if first_order is None:
                first_order = order
            if last_order is not None and order <= last_order:
                raise ValueError(f"Alien Worlds layered V2 source_order is not strictly increasing at row {row_number}")
            last_order = order
            count += 1
            txid = str(record.get("source_transaction_id") or "")
            if txid:
                txids.add(txid)
            operations[str(record["operation_type"])] += 1
        if count == 0:
            raise ValueError("Alien Worlds layered V2 input is empty")
        expected_rows = int(manifest.get("row_count") or 0)
        if expected_rows and expected_rows != count:
            raise ValueError(f"Alien Worlds layered V2 row_count mismatch: got {count}, expected {expected_rows}")
        return SourceValidationSummary(
            source_sha256=digest,
            row_count=count,
            unique_source_tx_hash_count=len(txids),
            time_start_ms=int(first_order or 0),
            time_end_ms=int(last_order or 0),
            operation_counts=dict(operations),
        )

    def iter_canonical_records(self, path: Path, manifest: dict[str, Any]) -> Iterator[dict[str, Any]]:
        dataset_id = str(manifest.get("dataset_id") or "")
        if not dataset_id:
            raise ValueError("Alien Worlds layered V2 manifest is missing dataset_id")
        for row_number, record in enumerate(self._iter_source(path), 1):
            self._validate_source_record(record, row_number)
            yield self._canonical_record(record, dataset_id)

    def _iter_source(self, path: Path) -> Iterator[dict[str, Any]]:
        with path.open("rt", encoding="utf-8", newline="") as stream:
            for row_number, line in enumerate(stream, 1):
                if not line.strip():
                    continue
                try:
                    record = json.loads(line)
                except json.JSONDecodeError as exc:
                    raise ValueError(f"Alien Worlds layered V2 invalid JSON at row {row_number}") from exc
                if not isinstance(record, dict):
                    raise ValueError(f"Alien Worlds layered V2 row {row_number} is not an object")
                yield record

    def _validate_source_record(self, record: dict[str, Any], row_number: int) -> None:
        if record.get("schema_version") != self.source_schema:
            raise ValueError(f"Alien Worlds layered V2 row {row_number}: unexpected schema_version")
        self._reject_forbidden(record, row_number)
        for key in ("record_id", "source_order", "sender_id", "receiver_id", "operation_type", "static_access_envelope", "declared_scheduling_evidence", "routing_source_key"):
            if record.get(key) in (None, "", []):
                raise ValueError(f"Alien Worlds layered V2 row {row_number}: missing {key}")
        envelope = record.get("static_access_envelope")
        if not isinstance(envelope, dict) or envelope.get("complete") is not True:
            raise ValueError(f"Alien Worlds layered V2 row {row_number}: static_access_envelope must be complete")
        keys = envelope.get("keys")
        if not isinstance(keys, list) or not keys or any(not isinstance(k, str) or not k.strip() for k in keys):
            raise ValueError(f"Alien Worlds layered V2 row {row_number}: invalid envelope keys")
        if len(keys) != len(set(keys)):
            raise ValueError(f"Alien Worlds layered V2 row {row_number}: duplicate envelope keys")
        evidence = record.get("declared_scheduling_evidence")
        items = evidence.get("items") if isinstance(evidence, dict) else None
        if not isinstance(items, list) or not items:
            raise ValueError(f"Alien Worlds layered V2 row {row_number}: missing declared_scheduling_evidence.items")
        declared_keys: list[str] = []
        for index, item in enumerate(items):
            if not isinstance(item, dict):
                raise ValueError(f"Alien Worlds layered V2 row {row_number}: scheduling item {index} is not an object")
            key = str(item.get("key") or "").strip()
            mode = str(item.get("access_mode") or "").strip().upper()
            semantic = str(item.get("update_semantic") or "").strip().upper()
            if not key or mode not in self._SCHED_MODE or not semantic:
                raise ValueError(f"Alien Worlds layered V2 row {row_number}: invalid scheduling item {index}")
            declared_keys.append(key)
        if len(declared_keys) != len(set(declared_keys)) or set(declared_keys) != set(keys):
            raise ValueError(f"Alien Worlds layered V2 row {row_number}: scheduling keys must equal the static envelope")
        routing_source = str(record.get("routing_source_key") or "")
        routing_target = str(record.get("routing_target_key") or "")
        if routing_source not in set(keys):
            raise ValueError(f"Alien Worlds layered V2 row {row_number}: routing_source_key is outside the static envelope")
        if routing_target and routing_target not in set(keys):
            raise ValueError(f"Alien Worlds layered V2 row {row_number}: routing_target_key is outside the static envelope")

    def _reject_forbidden(self, value: Any, row_number: int, path: str = "$") -> None:
        if isinstance(value, dict):
            for key, child in value.items():
                normalized = str(key).replace("-", "_").lower()
                if normalized in self._FORBIDDEN:
                    raise ValueError(f"Alien Worlds layered V2 row {row_number}: forbidden field {path}.{key}")
                self._reject_forbidden(child, row_number, f"{path}.{key}")
        elif isinstance(value, list):
            for index, child in enumerate(value):
                self._reject_forbidden(child, row_number, f"{path}[{index}]")

    def _canonical_record(self, source: dict[str, Any], dataset_id: str) -> dict[str, Any]:
        envelope = source["static_access_envelope"]
        evidence = source["declared_scheduling_evidence"]
        state_keys = [str(key) for key in envelope["keys"]]
        scheduling: list[dict[str, Any]] = []
        runtime: list[dict[str, Any]] = []
        for raw in evidence["items"]:
            source_mode = str(raw["access_mode"]).upper()
            semantic = str(raw["update_semantic"]).lower()
            scheduling.append({
                "key": str(raw["key"]),
                "mode": self._SCHED_MODE[source_mode],
                "update_semantics": semantic,
            })
            runtime_semantic = "none" if semantic == "none" else "layered_v2_replay_projection_unknown" if semantic == "unknown" else semantic
            runtime.append({
                "key": str(raw["key"]),
                "mode": self._RUNTIME_MODE[source_mode],
                "update_semantics": runtime_semantic,
            })
        scheduling = _normalize_access(scheduling)
        runtime = _normalize_access(runtime)
        scheduling_digest = _access_digest(scheduling)
        runtime_digest = _access_digest(runtime)
        metadata = dict(source.get("workload_metadata") or {})
        metadata.update({
            "layered_v2_source_schema": self.source_schema,
            "runtime_access_semantics": "static_declaration_replay_projection_no_oracle",
            "timestamp_semantics": "deterministic_source_order_only",
            "source_kind": (source.get("provenance") or {}).get("source_kind"),
        })
        skew_source = source.get("skew_keys") or []
        skew_key = str(skew_source[0]) if isinstance(skew_source, list) and skew_source else str(source["routing_source_key"])
        return {
            "schema_version": self.canonical_schema,
            "dataset_id": dataset_id,
            "source_row_index": int(source["source_order"]),
            "source_event_id": str(source["record_id"]),
            "source_tx_hash": source.get("source_transaction_id"),
            "timestamp_ms": int(source["source_order"]),
            "sender_id": str(source["sender_id"]).lower(),
            "receiver_id": str(source.get("receiver_id") or "m.federation").lower(),
            "operation_type": str(source["operation_type"]),
            "runtime_value": 1,
            "state_keys": state_keys,
            "static_access_envelope": {
                "keys": state_keys,
                "complete": True,
                "policy_id": str(envelope.get("policy_id") or ""),
                "construction_method": str(envelope.get("construction_method") or ""),
            },
            "scheduling_access_schema": self.scheduling_access_schema,
            "scheduling_access_source": str(evidence.get("policy_id") or "layered_v2_declared_scheduling"),
            "scheduling_access_list": scheduling,
            "scheduling_access_digest": scheduling_digest,
            "access_list_schema": self.runtime_access_schema,
            "access_list_source": "static_declaration_projection_no_oracle",
            "access_list": runtime,
            "access_list_digest": runtime_digest,
            "routing_source_key": str(source["routing_source_key"]),
            "routing_target_key": str(source.get("routing_target_key") or ""),
            "skew_keys": {"state_key": skew_key},
            "provenance": dict(source.get("provenance") or {}),
            "metadata": metadata,
        }


def _normalize_access(items: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return sorted(items, key=lambda item: (item["key"], item["mode"], item["update_semantics"], int(item.get("delta") or 0)))


def _access_digest(items: list[dict[str, Any]]) -> str:
    return hashlib.sha256(json.dumps(items, ensure_ascii=False, separators=(",", ":")).encode("utf-8")).hexdigest()


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()
