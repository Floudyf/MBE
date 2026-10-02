from __future__ import annotations

import csv
import gzip
import hashlib
import json
import re
from pathlib import Path
from typing import Any

SCHEMA_VERSION = "mbe_v5_porygon_partitioned_oracle_v2"
_SUPPORTED_MODES = {"read", "write", "read_write", "commutative_delta"}


def _canonical_digest(value: object) -> str:
    payload = json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _stable_direct_access_value(*, logical_tx_id: str, key: str, semantics: str, previous: str) -> str:
    payload: dict[str, str] = {"logical_tx_id": logical_tx_id, "key": key, "semantics": semantics}
    if previous:
        payload["previous"] = previous
    raw = json.dumps(payload, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
    return hashlib.sha256(raw).hexdigest()


def _business_state_digest(snapshot: dict[str, str]) -> str:
    rows: list[str] = []
    for key in sorted(snapshot):
        logical = key.split("::", 1)[1] if "::" in key else key
        if logical.startswith("relay_commit:") or logical.startswith("protocol:"):
            continue
        rows.append(f"{key}={snapshot[key]}")
    return hashlib.sha256("\n".join(rows).encode("utf-8")).hexdigest()


def _logical_business_state_digest(snapshot: dict[str, str]) -> str:
    logical: dict[str, str] = {}
    for key, value in snapshot.items():
        bare = key.split("::", 1)[1] if "::" in key else key
        if bare.startswith("relay_commit:") or bare.startswith("protocol:"):
            continue
        prior = logical.get(bare)
        if prior is not None and prior != value:
            return ""
        logical[bare] = value
    rows = [f"{key}={logical[key]}" for key in sorted(logical)]
    return hashlib.sha256("\n".join(rows).encode("utf-8")).hexdigest()


def _owner_identity(key: str) -> str:
    key = str(key or "").strip().lower()
    if not key:
        return ""
    parts = [part for part in re.split(r"[/|:#]+", key) if part]
    return parts[-1] if parts else key


def _stable_key(value: str) -> int:
    return sum(ord(ch) for ch in value)


def _state_home(key: str, shard_count: int) -> str:
    if shard_count <= 0:
        return ""
    return f"s{_stable_key(_owner_identity(key)) % shard_count}"


def _parse_bool(value: object) -> bool:
    return str(value or "").strip().lower() in {"1", "true", "yes"}


def _load_access(path: Path) -> tuple[dict[str, dict[str, Any]], str, list[str]]:
    """Load Porygon replay entries while sharing the common logical workload digest.

    Durable replay still uses tx_id, but cross-method fairness must hash the
    method-independent workload projection used by v5_serial_order_oracle:
    (index, logical_id, normalized AccessList). Physical tx ids and Porygon
    storage placement are deliberately excluded from that digest.
    """
    blockers: list[str] = []
    by_tx: dict[str, dict[str, Any]] = {}
    canonical: list[dict[str, Any]] = []
    seen_indexes: set[int] = set()
    try:
        with gzip.open(path, "rt", encoding="utf-8") as handle:
            for line_number, line in enumerate(handle, 1):
                if not line.strip():
                    continue
                row = json.loads(line)
                index = row.get("index")
                if isinstance(index, bool) or not isinstance(index, int) or index < 0:
                    blockers.append(f"porygon_oracle_invalid_resolved_access_index:{line_number}")
                    continue
                if index in seen_indexes:
                    blockers.append(f"porygon_oracle_duplicate_resolved_access_index:{index}")
                    continue
                seen_indexes.add(index)
                tx_id = str(row.get("tx_id") or "").strip()
                logical_id = str(row.get("logical_id") or "").strip()
                accesses = row.get("access_list")
                if not tx_id:
                    blockers.append(f"porygon_oracle_missing_tx_id:{index}")
                if not logical_id:
                    blockers.append(f"porygon_oracle_missing_logical_id:{index}")
                if not isinstance(accesses, list):
                    blockers.append(f"porygon_oracle_missing_access_list:{index}")
                    accesses = []
                normalized: list[dict[str, Any]] = []
                for ordinal, access in enumerate(accesses):
                    if not isinstance(access, dict):
                        blockers.append(f"porygon_oracle_invalid_access:{index}:{ordinal}")
                        continue
                    key = str(access.get("key") or "")
                    mode = str(access.get("mode") or "")
                    semantics = str(access.get("update_semantics") or "")
                    delta = access.get("delta") or 0
                    if not key:
                        blockers.append(f"porygon_oracle_missing_key:{index}:{ordinal}")
                    if mode not in _SUPPORTED_MODES:
                        blockers.append(f"porygon_oracle_unsupported_mode:{index}:{ordinal}:{mode}")
                    if isinstance(delta, bool) or not isinstance(delta, int):
                        try:
                            delta = int(delta)
                        except (TypeError, ValueError):
                            blockers.append(f"porygon_oracle_invalid_delta:{index}:{ordinal}")
                            delta = 0
                    normalized.append({
                        "key": key,
                        "mode": mode,
                        "update_semantics": semantics,
                        "delta": int(delta),
                    })
                entry = {
                    "index": index,
                    "tx_id": tx_id,
                    "logical_id": logical_id,
                    "access_list_schema": str(row.get("access_list_schema") or ""),
                    "access_list_source": str(row.get("access_list_source") or ""),
                    "access_list": normalized,
                }
                if tx_id and tx_id in by_tx:
                    blockers.append(f"porygon_oracle_duplicate_tx_id:{tx_id}")
                elif tx_id:
                    by_tx[tx_id] = entry
                canonical.append({
                    "index": index,
                    "logical_id": logical_id,
                    "access_list": normalized,
                })
    except (OSError, EOFError, gzip.BadGzipFile, UnicodeError, json.JSONDecodeError) as exc:
        blockers.append(f"porygon_oracle_access_unreadable:{type(exc).__name__}")
    canonical.sort(key=lambda item: item["index"])
    return by_tx, _canonical_digest(canonical) if canonical else "", blockers

def _load_blocks(path: Path) -> tuple[list[dict[str, Any]], list[str]]:
    blockers: list[str] = []
    rows: list[dict[str, Any]] = []
    try:
        with path.open(newline="", encoding="utf-8-sig") as handle:
            for row in csv.DictReader(handle):
                block_hash = str(row.get("block_hash") or "").strip()
                try:
                    height = int(str(row.get("height") or "0"))
                    tx_count = int(str(row.get("tx_count") or "0"))
                except ValueError:
                    blockers.append("porygon_oracle_invalid_committed_chain_row")
                    continue
                if block_hash and height > 0:
                    rows.append({"block_hash": block_hash, "height": height, "tx_count": tx_count})
    except (OSError, csv.Error, UnicodeError) as exc:
        blockers.append(f"porygon_oracle_committed_chain_unreadable:{type(exc).__name__}")
    rows.sort(key=lambda item: int(item["height"]))
    return rows, blockers


def _load_trace(path: Path) -> tuple[list[dict[str, Any]], list[str]]:
    blockers: list[str] = []
    rows: list[dict[str, Any]] = []
    try:
        with path.open(newline="", encoding="utf-8-sig") as handle:
            for row in csv.DictReader(handle):
                block_hash = str(row.get("block_hash") or "").strip()
                tx_id = str(row.get("tx_id") or "").strip()
                if not block_hash or not tx_id:
                    continue
                rows.append({"block_hash": block_hash, "tx_id": tx_id, "success": _parse_bool(row.get("success"))})
    except (OSError, csv.Error, UnicodeError) as exc:
        blockers.append(f"porygon_oracle_trace_unreadable:{type(exc).__name__}")
    return rows, blockers


def _porygon_block_assignments(node_dir: Path) -> tuple[dict[str, dict[str, dict[str, Any]]], list[str]]:
    blockers: list[str] = []
    out: dict[str, dict[str, dict[str, Any]]] = {}
    path = node_dir / "block_execution_summary.json"
    try:
        payload = json.loads(path.read_text(encoding="utf-8-sig"))
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        return out, [f"porygon_oracle_block_summary_unreadable:{node_dir.name}:{type(exc).__name__}"]
    blocks = payload.get("blocks") if isinstance(payload, dict) else None
    if not isinstance(blocks, list):
        return out, [f"porygon_oracle_block_summary_missing_blocks:{node_dir.name}"]
    for block in blocks:
        if not isinstance(block, dict):
            continue
        block_hash = str(block.get("block_hash") or "")
        assignments = block.get("porygon_execution_assignments")
        if not block_hash or not isinstance(assignments, list):
            continue
        by_tx: dict[str, dict[str, Any]] = {}
        for item in assignments:
            if not isinstance(item, dict):
                continue
            tx_id = str(item.get("tx_id") or "")
            if tx_id:
                by_tx[tx_id] = item
        out[block_hash] = by_tx
    return out, blockers


def _declared_access_lists_conflict(left: list[dict[str, Any]], right: list[dict[str, Any]]) -> bool:
    right_by_key = {str(item.get("key") or ""): str(item.get("mode") or "") for item in right if str(item.get("key") or "")}
    for item in left:
        key = str(item.get("key") or "")
        if not key or key not in right_by_key:
            continue
        left_mode = str(item.get("mode") or "")
        right_mode = right_by_key[key]
        if left_mode != "read" or right_mode != "read":
            return True
    return False


def _node_partition_truth(run_dir: Path) -> tuple[dict[str, str], dict[str, str], list[str]]:
    blockers: list[str] = []
    physical_by_partition: dict[str, set[str]] = {}
    logical_by_partition: dict[str, set[str]] = {}
    for path in sorted((run_dir / "nodes").glob("*/node_summary.json")):
        try:
            row = json.loads(path.read_text(encoding="utf-8-sig"))
        except (OSError, UnicodeError, json.JSONDecodeError) as exc:
            blockers.append(f"porygon_oracle_node_summary_unreadable:{path.parent.name}:{type(exc).__name__}")
            continue
        if str(row.get("block_executor_id") or "") != "porygon_block_executor":
            continue
        partition = str(row.get("porygon_storage_partition_id") or "").strip()
        physical = str(row.get("physical_partition_business_state_digest") or row.get("business_state_digest") or "").strip()
        logical = str(row.get("logical_business_state_digest") or "").strip()
        if not partition:
            blockers.append(f"porygon_oracle_missing_storage_partition_id:{path.parent.name}")
            continue
        if not physical:
            blockers.append(f"porygon_oracle_missing_partition_digest:{path.parent.name}")
            continue
        physical_by_partition.setdefault(partition, set()).add(physical)
        if logical:
            logical_by_partition.setdefault(partition, set()).add(logical)
    physical_out: dict[str, str] = {}
    logical_out: dict[str, str] = {}
    for partition, values in sorted(physical_by_partition.items()):
        if len(values) != 1:
            blockers.append(f"porygon_oracle_partition_replica_digest_mismatch:{partition}")
        else:
            physical_out[partition] = next(iter(values))
    for partition, values in sorted(logical_by_partition.items()):
        if len(values) != 1:
            blockers.append(f"porygon_oracle_partition_logical_digest_mismatch:{partition}")
        else:
            logical_out[partition] = next(iter(values))
    return physical_out, logical_out, blockers


def evaluate_porygon_partitioned(run_dir: Path, summary: dict[str, Any]) -> dict[str, Any]:
    run_dir = Path(run_dir)
    blockers: list[str] = []
    access_path = run_dir / "client" / "resolved_access_lists.jsonl.gz"
    if not access_path.is_file():
        blockers.append("porygon_oracle_missing_resolved_access_lists")
        access, input_digest = {}, ""
    else:
        access, input_digest, access_blockers = _load_access(access_path)
        blockers.extend(access_blockers)

    physical_actual, logical_actual_parts, node_blockers = _node_partition_truth(run_dir)
    blockers.extend(node_blockers)
    partitions = sorted(physical_actual)
    shard_count = len(partitions)
    if shard_count < 1:
        blockers.append("porygon_oracle_no_storage_partitions")

    reference_node = ""
    for path in sorted((run_dir / "nodes").glob("*/node_summary.json")):
        try:
            row = json.loads(path.read_text(encoding="utf-8-sig"))
        except Exception:
            continue
        if str(row.get("block_executor_id") or "") == "porygon_block_executor":
            reference_node = path.parent.name
            break
    if not reference_node:
        blockers.append("porygon_oracle_missing_reference_node")
    node_dir = run_dir / "nodes" / reference_node if reference_node else run_dir / "nodes" / "__missing__"
    blocks, block_blockers = _load_blocks(node_dir / "committed_chain.csv")
    trace, trace_blockers = _load_trace(node_dir / "transaction_execution_trace.csv")
    assignments, assignment_blockers = _porygon_block_assignments(node_dir)
    blockers.extend(block_blockers + trace_blockers + assignment_blockers)
    trace_by_block: dict[str, list[dict[str, Any]]] = {}
    for row in trace:
        trace_by_block.setdefault(str(row["block_hash"]), []).append(row)

    committed_order: list[str] = []
    abandoned: set[str] = set()
    block_tx_ids: dict[str, list[str]] = {}
    for block in blocks:
        block_hash = str(block["block_hash"])
        tx_count = int(block["tx_count"])
        if tx_count <= 0:
            continue
        rows = trace_by_block.get(block_hash, [])
        if len(rows) < tx_count or len(rows) % tx_count != 0:
            blockers.append(f"porygon_oracle_trace_count_mismatch:{block['height']}:{len(rows)}:{tx_count}")
            continue
        chunk = rows[-tx_count:]
        block_tx_ids[block_hash] = [str(row["tx_id"]) for row in chunk]
        block_assignments = assignments.get(block_hash, {})
        for row in chunk:
            tx_id = str(row["tx_id"])
            assignment = block_assignments.get(tx_id, {})
            is_abandoned = bool(assignment.get("abandoned"))
            if is_abandoned:
                abandoned.add(tx_id)
                if bool(row["success"]):
                    blockers.append(f"porygon_oracle_abandoned_tx_succeeded:{tx_id}")
            elif not bool(row["success"]):
                blockers.append(f"porygon_oracle_nonabandoned_tx_failed:{tx_id}")
            committed_order.append(tx_id)

    nonabandoned_cross_esc_conflict_pairs = 0
    for block in blocks:
        block_hash = str(block["block_hash"])
        tx_ids = block_tx_ids.get(block_hash, [])
        block_assignments = assignments.get(block_hash, {})
        for left_index, left_tx in enumerate(tx_ids):
            left_assignment = block_assignments.get(left_tx, {})
            if bool(left_assignment.get("abandoned")):
                continue
            try:
                left_esc = int(left_assignment.get("execution_shard"))
            except (TypeError, ValueError):
                continue
            left_access = (access.get(left_tx) or {}).get("access_list") or []
            for right_tx in tx_ids[left_index + 1:]:
                right_assignment = block_assignments.get(right_tx, {})
                if bool(right_assignment.get("abandoned")):
                    continue
                try:
                    right_esc = int(right_assignment.get("execution_shard"))
                except (TypeError, ValueError):
                    continue
                if left_esc == right_esc:
                    continue
                right_access = (access.get(right_tx) or {}).get("access_list") or []
                if _declared_access_lists_conflict(left_access, right_access):
                    nonabandoned_cross_esc_conflict_pairs += 1
        if nonabandoned_cross_esc_conflict_pairs:
            blockers.append(f"porygon_oracle_nonabandoned_cross_esc_conflict:{block['height']}:{nonabandoned_cross_esc_conflict_pairs}")
            break

    expected = set(access)
    if set(committed_order) != expected or len(committed_order) != len(expected):
        blockers.append("porygon_oracle_committed_transaction_set_mismatch")
    if len(committed_order) != len(set(committed_order)):
        blockers.append("porygon_oracle_duplicate_committed_tx")

    replay_by_partition: dict[str, dict[str, str]] = {partition: {} for partition in partitions}
    logical_order: list[str] = []
    if not blockers:
        for tx_id in committed_order:
            entry = access.get(tx_id)
            if entry is None:
                blockers.append(f"porygon_oracle_missing_access:{tx_id}")
                break
            logical_id = str(entry.get("logical_id") or tx_id)
            logical_order.append(logical_id)
            if tx_id in abandoned:
                continue
            for item in entry.get("access_list") or []:
                key = str(item.get("key") or "")
                mode = str(item.get("mode") or "")
                semantics = str(item.get("update_semantics") or "")
                home = _state_home(key, shard_count)
                if home not in replay_by_partition:
                    blockers.append(f"porygon_oracle_unknown_state_home:{key}:{home}")
                    break
                state = replay_by_partition[home]
                qualified = f"{home}::{key}"
                if mode == "read":
                    continue
                if mode == "read_write":
                    state[qualified] = _stable_direct_access_value(logical_tx_id=logical_id, key=key, semantics=semantics, previous=state.get(qualified, ""))
                elif mode == "write":
                    state[qualified] = _stable_direct_access_value(logical_tx_id=logical_id, key=key, semantics=semantics, previous="")
                elif mode == "commutative_delta":
                    try:
                        current = int(state.get(qualified, "") or "0")
                    except ValueError:
                        blockers.append(f"porygon_oracle_non_integer_commutative_base:{key}")
                        break
                    state[qualified] = str(current + int(item.get("delta") or 0))
                else:
                    blockers.append(f"porygon_oracle_unsupported_mode:{mode}")
                    break
            if blockers:
                break

    replay_physical = {partition: _business_state_digest(state) for partition, state in replay_by_partition.items()} if not blockers else {}
    for partition in partitions:
        if replay_physical.get(partition) and physical_actual.get(partition) != replay_physical[partition]:
            blockers.append(f"porygon_oracle_partition_business_digest_mismatch:{partition}")

    replay_global = _canonical_digest(dict(sorted(replay_physical.items()))) if replay_physical else ""
    actual_global = _canonical_digest(dict(sorted(physical_actual.items()))) if len(physical_actual) == shard_count and shard_count else ""
    # global_business_state_digest in older supervisors encodes physical shard homes.
    # Porygon validates the partition vector directly and exports its own canonical
    # physical and logical digests instead of treating a layout difference as a
    # business-state mismatch.
    if replay_global and actual_global != replay_global:
        blockers.append("porygon_oracle_global_business_digest_mismatch")

    replay_logical_state: dict[str, str] = {}
    for state in replay_by_partition.values():
        for qualified, value in state.items():
            bare = qualified.split("::", 1)[1] if "::" in qualified else qualified
            prior = replay_logical_state.get(bare)
            if prior is not None and prior != value:
                blockers.append(f"porygon_oracle_logical_key_conflict:{bare}")
            replay_logical_state[bare] = value
    replay_logical_digest = _logical_business_state_digest(replay_logical_state) if not blockers else ""

    blockers = list(dict.fromkeys(blockers))
    valid = not blockers and bool(committed_order) and bool(partitions)
    return {
        "serial_order_oracle_schema": SCHEMA_VERSION,
        "serial_order_oracle_status": "passed" if valid else "failed",
        "serial_order_replay_applicable": True,
        "serial_order_replay_not_applicable_reason": "",
        "serial_order_replay_equivalent": valid,
        "serial_order_replay_blockers": blockers,
        "serial_order_replay_structural_blockers": blockers,
        "serial_order_replay_supported_scope": "porygon_partitioned_global_order_statehome_v2",
        "serial_order_replay_identity_basis": "logical_id_access_list_digest_for_workload;tx_id_for_durable_trace",
        "serial_order_replay_order_basis": "global_pbft_committed_chain_then_porygon_execution_trace",
        "serial_order_replay_original_index_semantics": "block_local_diagnostic_only",
        "serial_order_replay_initial_state_empty": True,
        "serial_order_replay_initial_state_root": "partitioned_empty",
        "serial_order_replay_initial_state_sources": {},
        "serial_order_replay_shard_id": "porygon-partitioned",
        "serial_order_replay_transaction_count": len(committed_order),
        "serial_order_replay_unique_transaction_count": len(set(committed_order)),
        "serial_order_replay_committed_block_count": len(blocks),
        "serial_order_replay_trace_reexecution_count": 0,
        "serial_order_replay_input_digest": input_digest,
        "porygon_logical_workload_access_digest": input_digest,
        "serial_order_replay_commit_order_digest": _canonical_digest(logical_order) if logical_order else "",
        "serial_order_replay_tx_id_order_digest": _canonical_digest(committed_order) if committed_order else "",
        "serial_order_replay_business_state_digest": replay_global,
        "serial_order_actual_business_state_digest": actual_global,
        "serial_order_replay_global_business_state_digest": replay_global,
        "serial_order_actual_global_business_state_digest": actual_global,
        "serial_order_replay_logical_business_state_digest": replay_logical_digest,
        "porygon_global_physical_business_state_digest": actual_global,
        "porygon_global_logical_business_state_digest": replay_logical_digest,
        "serial_order_replay_business_key_count": sum(len(state) for state in replay_by_partition.values()),
        "serial_order_replay_replica_order_consistent": True,
        "serial_order_replay_replica_count": sum(1 for _ in (run_dir / "nodes").glob("*/node_summary.json")),
        "serial_order_replay_reference_node": reference_node,
        "porygon_oracle_storage_partitions": partitions,
        "porygon_oracle_partition_replay_digests": replay_physical,
        "porygon_oracle_partition_actual_digests": physical_actual,
        "porygon_oracle_abandoned_cross_shard_tx_count": len(abandoned),
        "porygon_oracle_nonabandoned_cross_esc_conflict_pair_count": nonabandoned_cross_esc_conflict_pairs,
        "method_correctness_oracle_kind": "porygon_partitioned_global_order_serial_replay_v2",
        "method_correctness_oracle_status": "passed" if valid else "failed",
        "method_correctness_oracle_valid": valid,
        "method_correctness_oracle_blockers": blockers,
        "method_correctness_oracle_scope": "porygon_partitioned_global_order_statehome_v2",
    }
