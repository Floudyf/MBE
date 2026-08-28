from __future__ import annotations

import csv
import gzip
import hashlib
import json
from pathlib import Path
from typing import Any


SCHEMA_VERSION = "mbe_v5_serial_order_oracle_v1"
EMPTY_STATE_ROOT = hashlib.sha256(b"mbe-state-merkle-treap-v2:empty").hexdigest()
_SUPPORTED_MODES = {"read", "write", "read_write", "commutative_delta"}


def _canonical_digest(value: object) -> str:
    payload = json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _stable_direct_access_value(*, logical_tx_id: str, key: str, semantics: str, previous: str) -> str:
    # Go execution.stableDigest(json.Marshal(struct{LogicalTxID,Key,Semantics,Previous}))
    payload: dict[str, str] = {
        "logical_tx_id": logical_tx_id,
        "key": key,
        "semantics": semantics,
    }
    if previous:
        payload["previous"] = previous
    raw = json.dumps(payload, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
    return hashlib.sha256(raw).hexdigest()


def _qualify(shard_id: str, key: str) -> str:
    return key if "::" in key else f"{shard_id}::{key}"


def _business_state_digest(snapshot: dict[str, str]) -> str:
    rows: list[str] = []
    for key in sorted(snapshot):
        logical = key.split("::", 1)[1] if "::" in key else key
        if logical.startswith("relay_commit:") or logical.startswith("protocol:"):
            continue
        rows.append(f"{key}={snapshot[key]}")
    return hashlib.sha256("\n".join(rows).encode("utf-8")).hexdigest()


def _parse_bool(value: object) -> bool:
    return str(value or "").strip().lower() in {"1", "true", "yes"}


def _load_access_entries(path: Path) -> tuple[dict[int, dict[str, Any]], str, list[str]]:
    by_index: dict[int, dict[str, Any]] = {}
    canonical_rows: list[dict[str, Any]] = []
    blockers: list[str] = []
    try:
        with gzip.open(path, "rt", encoding="utf-8") as handle:
            for line_number, line in enumerate(handle, start=1):
                if not line.strip():
                    continue
                row = json.loads(line)
                index = row.get("index")
                if isinstance(index, bool) or not isinstance(index, int) or index < 0:
                    blockers.append(f"resolved_access_invalid_index:{line_number}")
                    continue
                if index in by_index:
                    blockers.append(f"resolved_access_duplicate_index:{index}")
                    continue
                logical_id = str(row.get("logical_id") or "").strip()
                tx_id = str(row.get("tx_id") or "").strip()
                if not logical_id:
                    blockers.append(f"resolved_access_missing_logical_id:{index}")
                accesses = row.get("access_list")
                if not isinstance(accesses, list):
                    blockers.append(f"resolved_access_missing_access_list:{index}")
                    accesses = []
                normalized_accesses: list[dict[str, Any]] = []
                for ordinal, access in enumerate(accesses):
                    if not isinstance(access, dict):
                        blockers.append(f"resolved_access_invalid_access:{index}:{ordinal}")
                        continue
                    key = str(access.get("key") or "")
                    mode = str(access.get("mode") or "")
                    semantics = str(access.get("update_semantics") or "")
                    delta = access.get("delta") or 0
                    if not key:
                        blockers.append(f"resolved_access_missing_key:{index}:{ordinal}")
                    if mode not in _SUPPORTED_MODES:
                        blockers.append(f"resolved_access_unsupported_mode:{index}:{ordinal}:{mode}")
                    if isinstance(delta, bool) or not isinstance(delta, int):
                        try:
                            delta = int(delta)
                        except (TypeError, ValueError):
                            blockers.append(f"resolved_access_invalid_delta:{index}:{ordinal}")
                            delta = 0
                    normalized_accesses.append(
                        {
                            "key": key,
                            "mode": mode,
                            "update_semantics": semantics,
                            "delta": int(delta),
                        }
                    )
                normalized = {
                    "index": index,
                    "logical_id": logical_id,
                    "tx_id": tx_id,
                    "access_list_schema": str(row.get("access_list_schema") or ""),
                    "access_list_source": str(row.get("access_list_source") or ""),
                    "access_list": normalized_accesses,
                }
                by_index[index] = normalized
                canonical_rows.append(
                    {
                        "index": index,
                        "logical_id": logical_id,
                        "access_list": normalized_accesses,
                    }
                )
    except (OSError, EOFError, gzip.BadGzipFile, UnicodeError, json.JSONDecodeError) as exc:
        return {}, "", [f"resolved_access_unreadable:{type(exc).__name__}"]
    canonical_rows.sort(key=lambda item: item["index"])
    return by_index, _canonical_digest(canonical_rows) if canonical_rows else "", blockers


def _load_node_order(path: Path) -> tuple[list[int], list[str], list[str]]:
    indexes: list[int] = []
    tx_ids: list[str] = []
    blockers: list[str] = []
    try:
        with path.open(newline="", encoding="utf-8-sig") as handle:
            for row_number, row in enumerate(csv.DictReader(handle), start=2):
                if not _parse_bool(row.get("success")):
                    continue
                try:
                    original_index = int(str(row.get("original_index") or "").strip())
                except (TypeError, ValueError):
                    blockers.append(f"trace_invalid_original_index:{path.parent.name}:{row_number}")
                    continue
                indexes.append(original_index)
                tx_ids.append(str(row.get("tx_id") or "").strip())
    except (OSError, csv.Error, UnicodeError) as exc:
        return [], [], [f"trace_unreadable:{path.parent.name}:{type(exc).__name__}"]
    return indexes, tx_ids, blockers


def _initial_state_evidence(run_dir: Path) -> tuple[str, str, list[str]]:
    roots: dict[str, set[str]] = {}
    blockers: list[str] = []
    for path in sorted((run_dir / "nodes").glob("*/committed_chain.csv")):
        try:
            with path.open(newline="", encoding="utf-8-sig") as handle:
                first = next(csv.DictReader(handle), None)
        except (OSError, csv.Error, UnicodeError) as exc:
            blockers.append(f"initial_chain_unreadable:{path.parent.name}:{type(exc).__name__}")
            continue
        if not first:
            blockers.append(f"initial_chain_empty:{path.parent.name}")
            continue
        shard = str(first.get("shard_id") or "").strip()
        root = str(first.get("state_root_before") or "").strip()
        if not shard or not root:
            blockers.append(f"initial_chain_missing_state_root:{path.parent.name}")
            continue
        roots.setdefault(shard, set()).add(root)
    if len(roots) != 1:
        blockers.append(f"serial_oracle_requires_single_shard:{sorted(roots)}")
        return "", "", blockers
    shard_id = next(iter(roots))
    values = roots[shard_id]
    if len(values) != 1:
        blockers.append("initial_state_root_replica_mismatch")
        return shard_id, "", blockers
    root = next(iter(values))
    if root != EMPTY_STATE_ROOT:
        blockers.append("serial_oracle_nonempty_initial_state_unsupported")
    return shard_id, root, blockers


def _actual_business_digest(run_dir: Path, shard_id: str) -> tuple[str, list[str]]:
    digests: set[str] = set()
    blockers: list[str] = []
    for path in sorted((run_dir / "nodes").glob("*/node_summary.json")):
        try:
            row = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, UnicodeError, json.JSONDecodeError) as exc:
            blockers.append(f"node_summary_unreadable:{path.parent.name}:{type(exc).__name__}")
            continue
        if str(row.get("shard_id") or "") != shard_id:
            blockers.append(f"node_summary_shard_mismatch:{path.parent.name}")
            continue
        digest = str(row.get("business_state_digest") or "").strip()
        if not digest:
            blockers.append(f"node_summary_missing_business_digest:{path.parent.name}")
            continue
        digests.add(digest)
    if len(digests) != 1:
        blockers.append("business_state_digest_replica_mismatch")
        return "", blockers
    return next(iter(digests)), blockers


def evaluate(run_dir: Path, *, result_summary: dict | None = None) -> dict[str, Any]:
    """Validate execution against a serial replay of the *observed commit order*.

    The oracle is deliberately fail-closed and currently supports the formal
    single-shard direct-access workload used by CG/ACG/Batch-SI comparisons.
    It does not require different schedulers to choose the same serialization.
    """

    run_dir = Path(run_dir)
    blockers: list[str] = []
    shard_id, initial_root, initial_blockers = _initial_state_evidence(run_dir)
    blockers.extend(initial_blockers)

    access_path = run_dir / "client" / "resolved_access_lists.jsonl.gz"
    if not access_path.is_file():
        blockers.append("serial_oracle_missing_resolved_access_lists")
        access_entries: dict[int, dict[str, Any]] = {}
        input_digest = ""
    else:
        access_entries, input_digest, access_blockers = _load_access_entries(access_path)
        blockers.extend(access_blockers)

    node_orders: dict[str, list[int]] = {}
    node_tx_orders: dict[str, list[str]] = {}
    for path in sorted((run_dir / "nodes").glob("*/transaction_execution_trace.csv")):
        indexes, tx_ids, trace_blockers = _load_node_order(path)
        blockers.extend(trace_blockers)
        node_orders[path.parent.name] = indexes
        node_tx_orders[path.parent.name] = tx_ids
    if not node_orders:
        blockers.append("serial_oracle_missing_transaction_execution_trace")

    reference_node = sorted(node_orders)[0] if node_orders else ""
    order = node_orders.get(reference_node, [])
    if node_orders and any(indexes != order for indexes in node_orders.values()):
        blockers.append("serial_oracle_replica_commit_order_mismatch")

    expected_indexes = sorted(access_entries)
    if len(order) != len(expected_indexes):
        blockers.append(f"serial_oracle_success_count_mismatch:{len(order)}:{len(expected_indexes)}")
    if sorted(order) != expected_indexes:
        blockers.append("serial_oracle_success_transaction_set_mismatch")
    if len(set(order)) != len(order):
        blockers.append("serial_oracle_duplicate_successful_original_index")

    actual_business_digest, actual_blockers = _actual_business_digest(run_dir, shard_id) if shard_id else ("", ["serial_oracle_missing_shard_id"])
    blockers.extend(actual_blockers)

    replay_state: dict[str, str] = {}
    logical_order: list[str] = []
    if not blockers:
        for original_index in order:
            entry = access_entries[original_index]
            logical_id = str(entry.get("logical_id") or entry.get("tx_id") or "")
            logical_order.append(logical_id)
            for access in entry.get("access_list") or []:
                key = str(access.get("key") or "")
                mode = str(access.get("mode") or "")
                semantics = str(access.get("update_semantics") or "")
                qualified = _qualify(shard_id, key)
                if mode == "read":
                    continue
                if mode == "read_write":
                    previous = replay_state.get(qualified, "")
                    replay_state[qualified] = _stable_direct_access_value(
                        logical_tx_id=logical_id,
                        key=key,
                        semantics=semantics,
                        previous=previous,
                    )
                    continue
                if mode == "write":
                    replay_state[qualified] = _stable_direct_access_value(
                        logical_tx_id=logical_id,
                        key=key,
                        semantics=semantics,
                        previous="",
                    )
                    continue
                if mode == "commutative_delta":
                    current_text = replay_state.get(qualified, "")
                    try:
                        current = int(current_text or "0")
                    except ValueError:
                        blockers.append(f"serial_oracle_non_integer_commutative_base:{key}")
                        break
                    replay_state[qualified] = str(current + int(access.get("delta") or 0))
                    continue
                blockers.append(f"serial_oracle_unsupported_mode:{mode}")
                break
            if blockers:
                break

    replay_business_digest = _business_state_digest(replay_state) if not blockers else ""
    replay_global_business_digest = _canonical_digest({shard_id: replay_business_digest}) if shard_id and replay_business_digest else ""
    summary_global_business = str((result_summary or {}).get("global_business_state_digest") or "").strip()
    equivalent = bool(
        not blockers
        and replay_business_digest
        and actual_business_digest
        and replay_business_digest == actual_business_digest
        and (not summary_global_business or replay_global_business_digest == summary_global_business)
    )
    if not blockers and not equivalent:
        blockers.append("serial_oracle_business_digest_mismatch")

    return {
        "serial_order_oracle_schema": SCHEMA_VERSION,
        "serial_order_oracle_status": "passed" if equivalent else "failed",
        "serial_order_replay_equivalent": equivalent,
        "serial_order_replay_blockers": blockers,
        "serial_order_replay_supported_scope": "single_shard_empty_initial_direct_access_v1",
        "serial_order_replay_initial_state_empty": initial_root == EMPTY_STATE_ROOT if initial_root else False,
        "serial_order_replay_initial_state_root": initial_root,
        "serial_order_replay_shard_id": shard_id,
        "serial_order_replay_transaction_count": len(order),
        "serial_order_replay_unique_transaction_count": len(set(order)),
        "serial_order_replay_input_digest": input_digest,
        "serial_order_replay_commit_order_digest": _canonical_digest(logical_order) if logical_order else "",
        "serial_order_replay_business_state_digest": replay_business_digest,
        "serial_order_actual_business_state_digest": actual_business_digest,
        "serial_order_replay_global_business_state_digest": replay_global_business_digest,
        "serial_order_actual_global_business_state_digest": summary_global_business,
        "serial_order_replay_business_key_count": len(replay_state),
        "serial_order_replay_replica_order_consistent": bool(node_orders) and all(indexes == order for indexes in node_orders.values()),
        "serial_order_replay_replica_count": len(node_orders),
        "serial_order_replay_reference_node": reference_node,
    }
