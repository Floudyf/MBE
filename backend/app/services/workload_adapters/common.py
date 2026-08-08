from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

_ALLOWED_ACCESS_MODES = {"read", "write", "read_write", "commutative_delta"}


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def parse_json_array(value: str, *, field: str) -> list[str]:
    try:
        parsed = json.loads(value)
    except json.JSONDecodeError as exc:
        raise ValueError(f"{field} must be a JSON array") from exc
    if not isinstance(parsed, list):
        raise ValueError(f"{field} must be a JSON array")
    result = [str(item).strip() for item in parsed]
    if not result or any(not item for item in result):
        raise ValueError(f"{field} must contain non-empty strings")
    if len(result) != len(set(result)):
        raise ValueError(f"{field} contains duplicate keys")
    return result


def parse_json_object(value: str, *, field: str) -> dict[str, Any]:
    try:
        parsed = json.loads(value)
    except json.JSONDecodeError as exc:
        raise ValueError(f"{field} must be a JSON object") from exc
    if not isinstance(parsed, dict):
        raise ValueError(f"{field} must be a JSON object")
    return parsed


def normalize_access_list(value: str | list[dict[str, Any]], *, field: str = "access_list") -> list[dict[str, Any]]:
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
        except json.JSONDecodeError as exc:
            raise ValueError(f"{field} must be a JSON array") from exc
    else:
        parsed = value
    if not isinstance(parsed, list) or not parsed:
        raise ValueError(f"{field} must be a non-empty JSON array")
    by_key: dict[str, dict[str, Any]] = {}
    for index, raw in enumerate(parsed):
        if not isinstance(raw, dict):
            raise ValueError(f"{field}[{index}] must be an object")
        key = str(raw.get("key") or "").strip()
        mode = str(raw.get("mode") or "").strip()
        semantics = str(raw.get("update_semantics") or "").strip()
        delta = int(raw.get("delta") or 0)
        if not key:
            raise ValueError(f"{field}[{index}] has an empty key")
        if mode not in _ALLOWED_ACCESS_MODES:
            raise ValueError(f"{field}[{index}] has an invalid mode")
        if not semantics:
            raise ValueError(f"{field}[{index}] has empty update_semantics")
        if key in by_key:
            raise ValueError(f"{field} contains duplicate key {key}")
        item: dict[str, Any] = {"key": key, "mode": mode, "update_semantics": semantics}
        if delta:
            item["delta"] = delta
        by_key[key] = item
    return [by_key[key] for key in sorted(by_key)]


def access_list_digest(items: list[dict[str, Any]]) -> str:
    normalized = normalize_access_list(items)
    payload = json.dumps(normalized, ensure_ascii=False, separators=(",", ":"))
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def parse_timestamp_ms(value: str) -> int:
    text = value.strip()
    if not text:
        raise ValueError("timestamp is empty")
    if text.isdigit():
        numeric = int(text)
        return numeric if numeric >= 10_000_000_000 else numeric * 1000
    normalized = text.replace(" UTC", "+00:00").replace("Z", "+00:00")
    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError as exc:
        raise ValueError(f"invalid timestamp: {value}") from exc
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return int(parsed.timestamp() * 1000)


def canonical_v3_record(
    *,
    dataset_id: str,
    source_row_index: int,
    source_event_id: str,
    source_tx_hash: str | None,
    timestamp_ms: int,
    sender_id: str,
    receiver_id: str | None,
    operation_type: str,
    state_keys: list[str],
    routing_source_key: str,
    routing_target_key: str | None,
    skew_keys: dict[str, Any],
    provenance: dict[str, Any],
    metadata: dict[str, Any],
    access_list_schema: str,
    access_list_source: str,
    access_list: list[dict[str, Any]],
    runtime_value: int = 1,
) -> dict[str, Any]:
    normalized_access = normalize_access_list(access_list)
    normalized_state = sorted({str(key).strip() for key in state_keys if str(key).strip()})
    access_keys = [item["key"] for item in normalized_access]
    if normalized_state != access_keys:
        raise ValueError("state_keys must equal the direct access-list key set")
    return {
        "schema_version": "mbe_workload_record_v3",
        "dataset_id": dataset_id,
        "source_row_index": source_row_index,
        "source_event_id": source_event_id,
        "source_tx_hash": source_tx_hash,
        "timestamp_ms": timestamp_ms,
        "sender_id": sender_id,
        "receiver_id": receiver_id,
        "operation_type": operation_type,
        "runtime_value": max(1, int(runtime_value)),
        "state_keys": normalized_state,
        "routing_source_key": routing_source_key,
        "routing_target_key": routing_target_key,
        "skew_keys": skew_keys,
        "provenance": provenance,
        "metadata": metadata,
        "access_list_schema": access_list_schema,
        "access_list_source": access_list_source,
        "access_list": normalized_access,
        "access_list_digest": access_list_digest(normalized_access),
    }
