from __future__ import annotations

import csv
import gzip
import hashlib
import json
from pathlib import Path
from typing import Any


SCHEMA_VERSION = "mbe_v5_serial_order_oracle_v2"
EMPTY_STATE_ROOT = hashlib.sha256(b"mbe-state-merkle-treap-v2:empty").hexdigest()
_SUPPORTED_MODES = {"read", "write", "read_write", "commutative_delta"}
_LEGACY_INITIAL_ROOT_PLACEHOLDERS = {"", "empty"}


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


def _load_access_entries(path: Path) -> tuple[dict[str, dict[str, Any]], str, list[str]]:
    by_tx_id: dict[str, dict[str, Any]] = {}
    seen_indexes: set[int] = set()
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
                if index in seen_indexes:
                    blockers.append(f"resolved_access_duplicate_index:{index}")
                    continue
                seen_indexes.add(index)
                logical_id = str(row.get("logical_id") or "").strip()
                tx_id = str(row.get("tx_id") or "").strip()
                if not logical_id:
                    blockers.append(f"resolved_access_missing_logical_id:{index}")
                if not tx_id:
                    blockers.append(f"resolved_access_missing_tx_id:{index}")
                elif tx_id in by_tx_id:
                    blockers.append(f"resolved_access_duplicate_tx_id:{tx_id}")
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
                if tx_id and tx_id not in by_tx_id:
                    by_tx_id[tx_id] = normalized
                canonical_rows.append(
                    {
                        "index": index,
                        "tx_id": tx_id,
                        "logical_id": logical_id,
                        "access_list": normalized_accesses,
                    }
                )
    except (OSError, EOFError, gzip.BadGzipFile, UnicodeError, json.JSONDecodeError) as exc:
        return {}, "", [f"resolved_access_unreadable:{type(exc).__name__}"]
    canonical_rows.sort(key=lambda item: item["index"])
    return by_tx_id, _canonical_digest(canonical_rows) if canonical_rows else "", blockers


def _load_committed_blocks(path: Path) -> tuple[list[dict[str, Any]], list[str]]:
    blocks: list[dict[str, Any]] = []
    blockers: list[str] = []
    try:
        with path.open(newline="", encoding="utf-8-sig") as handle:
            for row_number, row in enumerate(csv.DictReader(handle), start=2):
                try:
                    height = int(str(row.get("height") or "").strip())
                    tx_count = int(str(row.get("tx_count") or "0").strip())
                except (TypeError, ValueError):
                    blockers.append(f"committed_chain_invalid_numeric:{path.parent.name}:{row_number}")
                    continue
                shard_id = str(row.get("shard_id") or "").strip()
                block_hash = str(row.get("block_hash") or "").strip()
                previous_hash = str(row.get("parent_hash") or row.get("previous_hash") or "").strip()
                if not shard_id or not block_hash or height <= 0 or tx_count < 0:
                    blockers.append(f"committed_chain_invalid_identity:{path.parent.name}:{row_number}")
                    continue
                blocks.append(
                    {
                        "height": height,
                        "shard_id": shard_id,
                        "block_hash": block_hash,
                        "previous_hash": previous_hash,
                        "tx_count": tx_count,
                        "state_root_before": str(row.get("state_root_before") or "").strip(),
                    }
                )
    except (OSError, csv.Error, UnicodeError) as exc:
        return [], [f"committed_chain_unreadable:{path.parent.name}:{type(exc).__name__}"]
    blocks.sort(key=lambda item: item["height"])
    seen_heights: set[int] = set()
    seen_hashes: set[str] = set()
    for index, block in enumerate(blocks):
        if block["height"] in seen_heights or block["block_hash"] in seen_hashes:
            blockers.append(f"committed_chain_duplicate_block:{path.parent.name}:{block['height']}")
        seen_heights.add(block["height"])
        seen_hashes.add(block["block_hash"])
        if index > 0:
            previous = blocks[index - 1]
            if block["height"] != previous["height"] + 1:
                blockers.append(f"committed_chain_height_gap:{path.parent.name}:{previous['height']}:{block['height']}")
            if block["previous_hash"] != previous["block_hash"]:
                blockers.append(f"committed_chain_parent_mismatch:{path.parent.name}:{block['height']}")
    return blocks, blockers


def _matching_execution_roots(value: object, block_hash: str) -> set[str]:
    roots: set[str] = set()
    if isinstance(value, dict):
        if str(value.get("block_hash") or "").strip() == block_hash:
            root = str(value.get("state_root_before") or "").strip()
            if root:
                roots.add(root)
        for child in value.values():
            if isinstance(child, (dict, list)):
                roots.update(_matching_execution_roots(child, block_hash))
    elif isinstance(value, list):
        for child in value:
            roots.update(_matching_execution_roots(child, block_hash))
    return roots


def _execution_root_before(node_dir: Path, block_hash: str) -> tuple[str, list[str]]:
    path = node_dir / "block_execution_summary.json"
    if not path.is_file():
        return "", []
    try:
        payload = json.loads(path.read_text(encoding="utf-8-sig"))
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        return "", [f"block_execution_summary_unreadable:{node_dir.name}:{type(exc).__name__}"]
    roots = _matching_execution_roots(payload, block_hash)
    if len(roots) > 1:
        return "", [f"initial_execution_root_ambiguous:{node_dir.name}:{block_hash}"]
    return (next(iter(roots)) if roots else ""), []


def _initial_state_evidence(run_dir: Path) -> tuple[str, str, list[str], dict[str, str]]:
    roots: dict[str, set[str]] = {}
    sources: dict[str, str] = {}
    blockers: list[str] = []
    node_dirs = sorted(path.parent for path in (run_dir / "nodes").glob("*/committed_chain.csv"))
    if not node_dirs:
        return "", "", ["serial_oracle_missing_committed_chain"], sources
    for node_dir in node_dirs:
        blocks, chain_blockers = _load_committed_blocks(node_dir / "committed_chain.csv")
        blockers.extend(chain_blockers)
        if not blocks:
            blockers.append(f"initial_chain_empty:{node_dir.name}")
            continue
        first = blocks[0]
        chain_root = str(first.get("state_root_before") or "").strip()
        execution_root, execution_blockers = _execution_root_before(node_dir, str(first["block_hash"]))
        blockers.extend(execution_blockers)
        if execution_root:
            if chain_root not in _LEGACY_INITIAL_ROOT_PLACEHOLDERS and chain_root != execution_root:
                blockers.append(f"initial_state_evidence_mismatch:{node_dir.name}")
                continue
            root = execution_root
            sources[node_dir.name] = "block_execution_summary" if chain_root in _LEGACY_INITIAL_ROOT_PLACEHOLDERS else "committed_chain+block_execution_summary"
        elif chain_root not in _LEGACY_INITIAL_ROOT_PLACEHOLDERS:
            root = chain_root
            sources[node_dir.name] = "committed_chain"
        else:
            blockers.append(f"serial_oracle_initial_state_evidence_unresolved:{node_dir.name}")
            continue
        roots.setdefault(str(first["shard_id"]), set()).add(root)
    if len(roots) != 1:
        blockers.append(f"serial_oracle_requires_single_shard:{sorted(roots)}")
        return "", "", blockers, sources
    shard_id = next(iter(roots))
    values = roots[shard_id]
    if len(values) != 1:
        blockers.append("initial_state_root_replica_mismatch")
        return shard_id, "", blockers, sources
    root = next(iter(values))
    if root != EMPTY_STATE_ROOT:
        blockers.append("serial_oracle_nonempty_initial_state_unsupported")
    return shard_id, root, blockers, sources


def _load_trace_rows(path: Path) -> tuple[list[dict[str, Any]], list[str]]:
    rows: list[dict[str, Any]] = []
    blockers: list[str] = []
    try:
        with path.open(newline="", encoding="utf-8-sig") as handle:
            for row_number, row in enumerate(csv.DictReader(handle), start=2):
                block_hash = str(row.get("block_hash") or "").strip()
                tx_id = str(row.get("tx_id") or "").strip()
                try:
                    height = int(str(row.get("height") or "0").strip())
                except (TypeError, ValueError):
                    height = 0
                if not block_hash or not tx_id:
                    blockers.append(f"trace_missing_identity:{path.parent.name}:{row_number}")
                    continue
                rows.append(
                    {
                        "block_hash": block_hash,
                        "height": height,
                        "tx_id": tx_id,
                        "success": _parse_bool(row.get("success")),
                        # OriginalIndex is intentionally diagnostic only.  It is
                        # block-local in execution.TxDelta and is not a workload ID.
                        "original_index": str(row.get("original_index") or "").strip(),
                    }
                )
    except (OSError, csv.Error, UnicodeError) as exc:
        return [], [f"trace_unreadable:{path.parent.name}:{type(exc).__name__}"]
    return rows, blockers


def _committed_tx_order(node_dir: Path) -> tuple[list[str], list[tuple[int, str, int]], int, list[str]]:
    blocks, blockers = _load_committed_blocks(node_dir / "committed_chain.csv")
    trace_rows, trace_blockers = _load_trace_rows(node_dir / "transaction_execution_trace.csv")
    blockers.extend(trace_blockers)
    if not blocks:
        return [], [], 0, blockers
    rows_by_hash: dict[str, list[dict[str, Any]]] = {}
    for row in trace_rows:
        rows_by_hash.setdefault(str(row["block_hash"]), []).append(row)
    order: list[str] = []
    repeated_execution_count = 0
    signature: list[tuple[int, str, int]] = []
    for block in blocks:
        block_hash = str(block["block_hash"])
        height = int(block["height"])
        tx_count = int(block["tx_count"])
        signature.append((height, block_hash, tx_count))
        rows = rows_by_hash.get(block_hash, [])
        if tx_count == 0:
            if any(row["success"] for row in rows):
                blockers.append(f"serial_oracle_system_block_has_success_trace:{node_dir.name}:{height}")
            continue
        if len(rows) == 0 or len(rows) % tx_count != 0:
            blockers.append(f"serial_oracle_committed_trace_count_mismatch:{node_dir.name}:{height}:{len(rows)}:{tx_count}")
            continue
        chunks = [rows[start:start + tx_count] for start in range(0, len(rows), tx_count)]
        sequences: list[list[str]] = []
        for chunk_index, chunk in enumerate(chunks):
            if any(int(row["height"]) not in {0, height} for row in chunk):
                blockers.append(f"serial_oracle_trace_height_mismatch:{node_dir.name}:{height}:{chunk_index}")
            if any(not row["success"] for row in chunk):
                blockers.append(f"serial_oracle_committed_tx_execution_failed:{node_dir.name}:{height}:{chunk_index}")
            tx_ids = [str(row["tx_id"]) for row in chunk]
            if len(set(tx_ids)) != len(tx_ids):
                blockers.append(f"serial_oracle_duplicate_tx_id_within_block:{node_dir.name}:{height}:{chunk_index}")
            sequences.append(tx_ids)
        if sequences and any(sequence != sequences[0] for sequence in sequences[1:]):
            blockers.append(f"serial_oracle_reexecution_order_mismatch:{node_dir.name}:{height}")
        if sequences:
            order.extend(sequences[-1])
            repeated_execution_count += max(0, len(sequences) - 1)
    return order, signature, repeated_execution_count, blockers


def _actual_business_digest(run_dir: Path, shard_id: str) -> tuple[str, list[str]]:
    digests: set[str] = set()
    blockers: list[str] = []
    for path in sorted((run_dir / "nodes").glob("*/node_summary.json")):
        try:
            row = json.loads(path.read_text(encoding="utf-8-sig"))
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



def _summary_bool(summary: dict, name: str) -> bool | None:
    value = summary.get(name)
    return value if isinstance(value, bool) else None


def _finality_int(summary: dict, name: str) -> int | None:
    finality = summary.get("finality_evidence") if isinstance(summary.get("finality_evidence"), dict) else {}
    value = finality.get(name)
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return None
    return int(value)


def _groundhog_correctness_blockers(summary: dict, structural_blockers: list[str]) -> list[str]:
    blockers = list(structural_blockers)
    if str(summary.get("block_executor_id") or "") != "groundhog_block_executor":
        blockers.append("groundhog_oracle_executor_identity_mismatch")
    for field in ("block_executor_consistent", "state_root_consistent", "receipt_root_consistent", "plan_digest_consistent", "no_fallback", "ready_to_commit"):
        if _summary_bool(summary, field) is not True:
            blockers.append(f"groundhog_oracle_{field}_not_true")
    submitted = _finality_int(summary, "submitted_unique_tx_count")
    terminal = _finality_int(summary, "terminal_unique_tx_count")
    finalized = _finality_int(summary, "finalized_unique_logical_tx_count")
    incomplete = _finality_int(summary, "incomplete_unique_tx_count")
    cross_failed = _finality_int(summary, "cross_shard_failed_unique_count")
    if submitted is None or submitted <= 0:
        blockers.append("groundhog_oracle_submitted_count_missing")
    else:
        if terminal != submitted:
            blockers.append("groundhog_oracle_terminal_not_equal_submitted")
        if finalized != submitted:
            blockers.append("groundhog_oracle_finalized_not_equal_submitted")
        executed = summary.get("executed_logical_transaction_count")
        if isinstance(executed, bool) or not isinstance(executed, (int, float)) or int(executed) != submitted:
            blockers.append("groundhog_oracle_executed_not_equal_submitted")
    if incomplete != 0:
        blockers.append("groundhog_oracle_incomplete_not_zero")
    if cross_failed != 0:
        blockers.append("groundhog_oracle_cross_shard_failed_not_zero")
    return list(dict.fromkeys(blockers))

def evaluate(run_dir: Path, *, result_summary: dict | None = None) -> dict[str, Any]:
    """Validate durable transaction identity/order and method-appropriate state correctness.

    Direct-state executors use observed-order serial replay. Groundhog explicitly
    declares SerialEquivalent=false and uses typed commutative materialization,
    so it is checked by a fail-closed replica-determinism/completion contract
    instead of being falsely compared with direct-state serial values.

    The oracle is fail-closed.  It first identifies the durable committed block
    chain, then derives each committed block's deterministic materialization
    order from transaction_execution_trace.csv by tx_id.  execution.TxDelta's
    OriginalIndex is block-local and therefore never serves as a workload-wide
    identity.  Different schedulers may choose different legal serial orders.
    """
    run_dir = Path(run_dir)
    summary = result_summary if isinstance(result_summary, dict) else {}
    executor_id = str(summary.get("block_executor_id") or "")
    groundhog_mode = executor_id == "groundhog_block_executor"
    blockers: list[str] = []
    shard_id, initial_root, initial_blockers, initial_sources = _initial_state_evidence(run_dir)
    blockers.extend(initial_blockers)

    access_path = run_dir / "client" / "resolved_access_lists.jsonl.gz"
    if not access_path.is_file():
        blockers.append("serial_oracle_missing_resolved_access_lists")
        access_entries: dict[str, dict[str, Any]] = {}
        input_digest = ""
    else:
        access_entries, input_digest, access_blockers = _load_access_entries(access_path)
        blockers.extend(access_blockers)

    node_orders: dict[str, list[str]] = {}
    node_signatures: dict[str, list[tuple[int, str, int]]] = {}
    node_reexecution_counts: dict[str, int] = {}
    for node_dir in sorted(path.parent for path in (run_dir / "nodes").glob("*/committed_chain.csv")):
        trace_path = node_dir / "transaction_execution_trace.csv"
        if not trace_path.is_file():
            blockers.append(f"serial_oracle_missing_transaction_execution_trace:{node_dir.name}")
            continue
        order, signature, reexecution_count, node_blockers = _committed_tx_order(node_dir)
        blockers.extend(node_blockers)
        node_orders[node_dir.name] = order
        node_signatures[node_dir.name] = signature
        node_reexecution_counts[node_dir.name] = reexecution_count
    if not node_orders:
        blockers.append("serial_oracle_missing_transaction_execution_trace")

    reference_node = sorted(node_orders)[0] if node_orders else ""
    order = node_orders.get(reference_node, [])
    reference_signature = node_signatures.get(reference_node, [])
    if node_orders and any(candidate != order for candidate in node_orders.values()):
        blockers.append("serial_oracle_replica_commit_order_mismatch")
    if node_signatures and any(candidate != reference_signature for candidate in node_signatures.values()):
        blockers.append("serial_oracle_replica_committed_chain_mismatch")

    expected_tx_ids = set(access_entries)
    if len(order) != len(expected_tx_ids):
        blockers.append(f"serial_oracle_success_count_mismatch:{len(order)}:{len(expected_tx_ids)}")
    if set(order) != expected_tx_ids:
        blockers.append("serial_oracle_success_transaction_set_mismatch")
    if len(set(order)) != len(order):
        blockers.append("serial_oracle_duplicate_successful_tx_id")

    actual_business_digest, actual_blockers = _actual_business_digest(run_dir, shard_id) if shard_id else ("", ["serial_oracle_missing_shard_id"])
    blockers.extend(actual_blockers)

    if groundhog_mode:
        correctness_blockers = _groundhog_correctness_blockers(summary, blockers)
        correctness_valid = not correctness_blockers
        summary_global_business = str(summary.get("global_business_state_digest") or "").strip()
        return {
            "serial_order_oracle_schema": SCHEMA_VERSION,
            "serial_order_oracle_status": "not_applicable",
            "serial_order_replay_applicable": False,
            "serial_order_replay_not_applicable_reason": "groundhog_typed_commutative_semantics_serial_equivalent_false",
            "serial_order_replay_equivalent": None,
            "serial_order_replay_blockers": [],
            "serial_order_replay_structural_blockers": blockers,
            "serial_order_replay_supported_scope": "single_shard_empty_initial_direct_access_committed_txid_v2",
            "serial_order_replay_identity_basis": "tx_id",
            "serial_order_replay_order_basis": "durable_committed_chain_then_transaction_execution_trace",
            "serial_order_replay_original_index_semantics": "block_local_diagnostic_only",
            "serial_order_replay_initial_state_empty": initial_root == EMPTY_STATE_ROOT if initial_root else False,
            "serial_order_replay_initial_state_root": initial_root,
            "serial_order_replay_initial_state_sources": initial_sources,
            "serial_order_replay_shard_id": shard_id,
            "serial_order_replay_transaction_count": len(order),
            "serial_order_replay_unique_transaction_count": len(set(order)),
            "serial_order_replay_committed_block_count": len(reference_signature),
            "serial_order_replay_trace_reexecution_count": node_reexecution_counts.get(reference_node, 0),
            "serial_order_replay_input_digest": input_digest,
            "serial_order_replay_commit_order_digest": "",
            "serial_order_replay_tx_id_order_digest": _canonical_digest(order) if order else "",
            "serial_order_replay_business_state_digest": "",
            "serial_order_actual_business_state_digest": actual_business_digest,
            "serial_order_replay_global_business_state_digest": "",
            "serial_order_actual_global_business_state_digest": summary_global_business,
            "serial_order_replay_business_key_count": 0,
            "serial_order_replay_replica_order_consistent": bool(node_orders) and all(candidate == order for candidate in node_orders.values()),
            "serial_order_replay_replica_count": len(node_orders),
            "serial_order_replay_reference_node": reference_node,
            "method_correctness_oracle_kind": "groundhog_replica_determinism_completion_v1",
            "method_correctness_oracle_status": "passed" if correctness_valid else "failed",
            "method_correctness_oracle_valid": correctness_valid,
            "method_correctness_oracle_blockers": correctness_blockers,
            "method_correctness_oracle_scope": "single_shard_empty_initial_groundhog_typed_commutative_replica_v1",
        }

    replay_state: dict[str, str] = {}
    logical_order: list[str] = []
    if not blockers:
        for tx_id in order:
            entry = access_entries[tx_id]
            logical_id = str(entry.get("logical_id") or tx_id)
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
    summary_global_business = str(summary.get("global_business_state_digest") or "").strip()
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
        "serial_order_replay_applicable": True,
        "serial_order_replay_not_applicable_reason": "",
        "serial_order_replay_equivalent": equivalent,
        "serial_order_replay_blockers": blockers,
        "serial_order_replay_supported_scope": "single_shard_empty_initial_direct_access_committed_txid_v2",
        "serial_order_replay_identity_basis": "tx_id",
        "serial_order_replay_order_basis": "durable_committed_chain_then_transaction_execution_trace",
        "serial_order_replay_original_index_semantics": "block_local_diagnostic_only",
        "serial_order_replay_initial_state_empty": initial_root == EMPTY_STATE_ROOT if initial_root else False,
        "serial_order_replay_initial_state_root": initial_root,
        "serial_order_replay_initial_state_sources": initial_sources,
        "serial_order_replay_shard_id": shard_id,
        "serial_order_replay_transaction_count": len(order),
        "serial_order_replay_unique_transaction_count": len(set(order)),
        "serial_order_replay_committed_block_count": len(reference_signature),
        "serial_order_replay_trace_reexecution_count": node_reexecution_counts.get(reference_node, 0),
        "serial_order_replay_input_digest": input_digest,
        "serial_order_replay_commit_order_digest": _canonical_digest(logical_order) if logical_order else "",
        "serial_order_replay_tx_id_order_digest": _canonical_digest(order) if order else "",
        "serial_order_replay_business_state_digest": replay_business_digest,
        "serial_order_actual_business_state_digest": actual_business_digest,
        "serial_order_replay_global_business_state_digest": replay_global_business_digest,
        "serial_order_actual_global_business_state_digest": summary_global_business,
        "serial_order_replay_business_key_count": len(replay_state),
        "serial_order_replay_replica_order_consistent": bool(node_orders) and all(candidate == order for candidate in node_orders.values()),
        "serial_order_replay_replica_count": len(node_orders),
        "serial_order_replay_reference_node": reference_node,
        "method_correctness_oracle_kind": "direct_state_observed_order_serial_replay_v2",
        "method_correctness_oracle_status": "passed" if equivalent else "failed",
        "method_correctness_oracle_valid": equivalent,
        "method_correctness_oracle_blockers": blockers,
        "method_correctness_oracle_scope": "single_shard_empty_initial_direct_access_committed_txid_v2",
    }
