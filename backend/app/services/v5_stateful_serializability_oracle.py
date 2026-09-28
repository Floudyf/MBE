from __future__ import annotations

import csv
import hashlib
import json
import re
from pathlib import Path
from typing import Any

SCHEMA_VERSION = "mbe_v5_stateful_serializability_oracle_v2"
SUPPORTED_SCOPE = "stateful_local_legacy_partitioned_effect_digest_serializability_v2"
EMPTY_STATE_ROOT = hashlib.sha256(b"mbe-state-merkle-treap-v2:empty").hexdigest()
EMPTY_VALUE_DIGEST = hashlib.sha256(b"").hexdigest()
_LEGACY_INITIAL_ROOT_PLACEHOLDERS = {"", "empty"}
_PROTOCOL_PREFIXES = ("relay_commit:", "protocol:")
_HEX_64 = re.compile(r"^[0-9a-fA-F]{64}$")


def _canonical_digest(value: object) -> str:
    payload = json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _value_digest(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def _qualify(shard_id: str, key: str) -> str:
    return key if "::" in key else f"{shard_id}::{key}"


def _logical_key(key: str) -> str:
    return key.split("::", 1)[1] if "::" in key else key


def _is_business_key(key: str) -> bool:
    logical = _logical_key(key)
    return not any(logical.startswith(prefix) for prefix in _PROTOCOL_PREFIXES)


def _business_state_digest(snapshot: dict[str, str]) -> str:
    rows = [f"{key}={snapshot[key]}" for key in sorted(snapshot) if _is_business_key(key)]
    return hashlib.sha256("\n".join(rows).encode("utf-8")).hexdigest()


def _business_effect_digest(snapshot: dict[str, str]) -> str:
    return _canonical_digest({key: snapshot[key] for key in sorted(snapshot) if _is_business_key(key)})


def _summary_bool(summary: dict[str, Any], name: str) -> bool | None:
    value = summary.get(name)
    return value if isinstance(value, bool) else None


def _finality_int(summary: dict[str, Any], name: str) -> int | None:
    finality = summary.get("finality_evidence") if isinstance(summary.get("finality_evidence"), dict) else {}
    value = finality.get(name)
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return None
    return int(value)


def matches_stateful_local_legacy(summary: dict[str, Any]) -> bool:
    return (
        str(summary.get("comparison_semantics_class") or "") == "stateful_local_legacy_v1"
        and str(summary.get("state_home_mapping_policy") or "") == "execution_shard_local_namespace"
        and str(summary.get("remote_fetch_policy") or "") == "none"
        and str(summary.get("remote_writeback_policy") or "") == "none"
        and str(summary.get("block_executor_id") or "") in {"serial_block_executor", "block_stm_block_executor"}
    )


def _parse_bool(value: object) -> bool:
    return str(value or "").strip().lower() in {"1", "true", "yes"}


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
                    blockers.append(f"stateful_serializability_invalid_chain_numeric:{path.parent.name}:{row_number}")
                    continue
                shard_id = str(row.get("shard_id") or "").strip()
                block_hash = str(row.get("block_hash") or "").strip()
                parent_hash = str(row.get("parent_hash") or row.get("previous_hash") or "").strip()
                state_root_before = str(row.get("state_root_before") or "").strip()
                if not shard_id or not block_hash or height <= 0 or tx_count < 0:
                    blockers.append(f"stateful_serializability_invalid_chain_identity:{path.parent.name}:{row_number}")
                    continue
                blocks.append(
                    {
                        "height": height,
                        "shard_id": shard_id,
                        "block_hash": block_hash,
                        "parent_hash": parent_hash,
                        "tx_count": tx_count,
                        "state_root_before": state_root_before,
                    }
                )
    except (OSError, csv.Error, UnicodeError) as exc:
        return [], [f"stateful_serializability_chain_unreadable:{path.parent.name}:{type(exc).__name__}"]
    blocks.sort(key=lambda item: int(item["height"]))
    seen_heights: set[int] = set()
    seen_hashes: set[str] = set()
    for index, block in enumerate(blocks):
        height = int(block["height"])
        block_hash = str(block["block_hash"])
        if height in seen_heights or block_hash in seen_hashes:
            blockers.append(f"stateful_serializability_duplicate_committed_block:{path.parent.name}:{height}")
        seen_heights.add(height)
        seen_hashes.add(block_hash)
        if index:
            previous = blocks[index - 1]
            if height != int(previous["height"]) + 1:
                blockers.append(
                    f"stateful_serializability_chain_height_gap:{path.parent.name}:{previous['height']}:{height}"
                )
            if str(block["parent_hash"]) != str(previous["block_hash"]):
                blockers.append(f"stateful_serializability_chain_parent_mismatch:{path.parent.name}:{height}")
    return blocks, blockers


def _load_execution_summary(node_dir: Path) -> tuple[dict[str, list[dict[str, Any]]], list[str]]:
    path = node_dir / "block_execution_summary.json"
    if not path.is_file():
        return {}, []
    try:
        payload = json.loads(path.read_text(encoding="utf-8-sig"))
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        return {}, [f"stateful_serializability_execution_summary_unreadable:{node_dir.name}:{type(exc).__name__}"]
    out: dict[str, list[dict[str, Any]]] = {}
    blockers: list[str] = []
    for index, row in enumerate(payload.get("blocks") if isinstance(payload.get("blocks"), list) else []):
        if not isinstance(row, dict):
            blockers.append(f"stateful_serializability_invalid_execution_block:{node_dir.name}:{index}")
            continue
        block_hash = str(row.get("block_hash") or "").strip()
        if block_hash:
            out.setdefault(block_hash, []).append(row)
    return out, blockers


def _resolve_initial_root(
    node_dir: Path,
    first_block: dict[str, Any],
    execution_by_hash: dict[str, list[dict[str, Any]]],
) -> tuple[str, str, list[str]]:
    blockers: list[str] = []
    chain_root = str(first_block.get("state_root_before") or "").strip()
    candidates: set[str] = set()
    for row in execution_by_hash.get(str(first_block["block_hash"]), []):
        root = str(row.get("state_root_before_wal") or row.get("state_root_before") or "").strip()
        if root:
            candidates.add(root)
    if len(candidates) > 1:
        return "", "", [f"stateful_serializability_initial_root_ambiguous:{node_dir.name}"]
    execution_root = next(iter(candidates)) if candidates else ""
    if execution_root:
        if chain_root not in _LEGACY_INITIAL_ROOT_PLACEHOLDERS and chain_root != execution_root:
            blockers.append(f"stateful_serializability_initial_root_evidence_mismatch:{node_dir.name}")
            return "", "", blockers
        return execution_root, "block_execution_summary", blockers
    if chain_root not in _LEGACY_INITIAL_ROOT_PLACEHOLDERS:
        return chain_root, "committed_chain", blockers
    return "", "", [f"stateful_serializability_initial_root_unresolved:{node_dir.name}"]


def _load_transaction_order(
    node_dir: Path,
    blocks: list[dict[str, Any]],
) -> tuple[dict[str, list[dict[str, Any]]], list[str]]:
    path = node_dir / "transaction_execution_trace.csv"
    blockers: list[str] = []
    try:
        with path.open(newline="", encoding="utf-8-sig") as handle:
            rows = list(csv.DictReader(handle))
    except (OSError, csv.Error, UnicodeError) as exc:
        return {}, [f"stateful_serializability_tx_trace_unreadable:{node_dir.name}:{type(exc).__name__}"]
    by_hash: dict[str, list[dict[str, str]]] = {}
    for row_number, row in enumerate(rows, start=2):
        block_hash = str(row.get("block_hash") or "").strip()
        tx_id = str(row.get("tx_id") or "").strip()
        if not block_hash or not tx_id:
            blockers.append(f"stateful_serializability_tx_trace_missing_identity:{node_dir.name}:{row_number}")
            continue
        by_hash.setdefault(block_hash, []).append(row)

    result: dict[str, list[dict[str, Any]]] = {}
    for block in blocks:
        block_hash = str(block["block_hash"])
        height = int(block["height"])
        tx_count = int(block["tx_count"])
        rows_for_block = by_hash.get(block_hash, [])
        if tx_count == 0:
            if any(_parse_bool(row.get("success")) for row in rows_for_block):
                blockers.append(f"stateful_serializability_system_block_has_tx_trace:{node_dir.name}:{height}")
            result[block_hash] = []
            continue
        if not rows_for_block or len(rows_for_block) % tx_count != 0:
            blockers.append(
                f"stateful_serializability_tx_trace_count_mismatch:{node_dir.name}:{height}:{len(rows_for_block)}:{tx_count}"
            )
            result[block_hash] = []
            continue
        chunks = [rows_for_block[start : start + tx_count] for start in range(0, len(rows_for_block), tx_count)]
        normalized_chunks: list[list[dict[str, Any]]] = []
        for chunk_index, chunk in enumerate(chunks):
            normalized: list[dict[str, Any]] = []
            for ordinal, row in enumerate(chunk):
                try:
                    row_height = int(str(row.get("height") or "0").strip())
                except (TypeError, ValueError):
                    row_height = 0
                try:
                    original_index = int(str(row.get("original_index") or "-1").strip())
                except (TypeError, ValueError):
                    original_index = -1
                tx_id = str(row.get("tx_id") or "").strip()
                if row_height not in {0, height}:
                    blockers.append(
                        f"stateful_serializability_tx_trace_height_mismatch:{node_dir.name}:{height}:{chunk_index}:{ordinal}"
                    )
                if original_index != ordinal:
                    blockers.append(
                        f"stateful_serializability_tx_trace_order_mismatch:{node_dir.name}:{height}:{ordinal}:{original_index}"
                    )
                normalized.append(
                    {
                        "tx_id": tx_id,
                        "original_index": original_index,
                        "success": _parse_bool(row.get("success")),
                        "error": str(row.get("error") or ""),
                    }
                )
            normalized_chunks.append(normalized)
        first = normalized_chunks[0]
        if any(candidate != first for candidate in normalized_chunks[1:]):
            blockers.append(f"stateful_serializability_tx_trace_reexecution_mismatch:{node_dir.name}:{height}")
        result[block_hash] = normalized_chunks[-1]
    return result, blockers


def _load_observed_access(
    node_dir: Path,
) -> tuple[dict[tuple[str, str, int], dict[str, list[dict[str, str]]]], list[str]]:
    path = node_dir / "observed_state_access.csv"
    blockers: list[str] = []
    out: dict[tuple[str, str, int], dict[str, list[dict[str, str]]]] = {}
    try:
        with path.open(newline="", encoding="utf-8-sig") as handle:
            for row_number, row in enumerate(csv.DictReader(handle), start=2):
                block_hash = str(row.get("block_hash") or "").strip()
                tx_id = str(row.get("tx_id") or "").strip()
                access_type = str(row.get("access_type") or "").strip()
                key = str(row.get("state_key") or "").strip()
                value_digest = str(row.get("value_digest") or "").strip().lower()
                source = str(row.get("source") or "").strip()
                try:
                    original_index = int(str(row.get("original_index") or "-1").strip())
                except (TypeError, ValueError):
                    original_index = -1
                if not block_hash or not tx_id or original_index < 0 or access_type not in {"read", "write"} or not key:
                    blockers.append(f"stateful_serializability_observed_access_invalid:{node_dir.name}:{row_number}")
                    continue
                if not _HEX_64.fullmatch(value_digest):
                    blockers.append(f"stateful_serializability_observed_digest_invalid:{node_dir.name}:{row_number}")
                    continue
                effect = out.setdefault((block_hash, tx_id, original_index), {"reads": [], "writes": []})
                effect["reads" if access_type == "read" else "writes"].append(
                    {"key": key, "value_digest": value_digest, "source": source}
                )
    except (OSError, csv.Error, UnicodeError) as exc:
        return {}, [f"stateful_serializability_observed_access_unreadable:{node_dir.name}:{type(exc).__name__}"]
    return out, blockers


def _wal_generation(path: Path) -> int:
    match = re.fullmatch(r"state_delta\.(\d+)\.wal", path.name)
    return int(match.group(1)) if match else 0


def _load_persisted_state(
    node_dir: Path,
    shard_id: str,
    committed_hashes: set[str],
) -> tuple[dict[str, str], list[str]]:
    blockers: list[str] = []
    state: dict[str, str] = {}
    snapshot_height = 0
    snapshot_path = node_dir / "state_snapshot.json"
    if snapshot_path.is_file():
        try:
            raw = json.loads(snapshot_path.read_text(encoding="utf-8-sig"))
            if not isinstance(raw, dict) or any(not isinstance(k, str) or not isinstance(v, str) for k, v in raw.items()):
                blockers.append(f"stateful_serializability_snapshot_invalid:{node_dir.name}")
            else:
                state = dict(raw)
        except (OSError, UnicodeError, json.JSONDecodeError) as exc:
            blockers.append(f"stateful_serializability_snapshot_unreadable:{node_dir.name}:{type(exc).__name__}")
    metadata_path = node_dir / "state_snapshot_metadata.json"
    if metadata_path.is_file():
        try:
            metadata = json.loads(metadata_path.read_text(encoding="utf-8-sig"))
            if str(metadata.get("version") or "") != "state_snapshot_metadata_v1":
                blockers.append(f"stateful_serializability_snapshot_metadata_version:{node_dir.name}")
            if str(metadata.get("namespace") or "") not in {"", shard_id}:
                blockers.append(f"stateful_serializability_snapshot_namespace_mismatch:{node_dir.name}")
            snapshot_height = int(metadata.get("included_height") or 0)
        except (OSError, UnicodeError, json.JSONDecodeError, TypeError, ValueError) as exc:
            blockers.append(f"stateful_serializability_snapshot_metadata_unreadable:{node_dir.name}:{type(exc).__name__}")

    wal_paths = sorted(node_dir.glob("state_delta.*.wal"), key=lambda path: (_wal_generation(path), path.name))
    applied_delta_ids: set[str] = set()
    records: list[tuple[int, int, dict[str, Any], Path]] = []
    for wal_path in wal_paths:
        try:
            lines = wal_path.read_text(encoding="utf-8-sig").splitlines()
        except (OSError, UnicodeError) as exc:
            blockers.append(f"stateful_serializability_wal_unreadable:{node_dir.name}:{wal_path.name}:{type(exc).__name__}")
            continue
        for line_number, line in enumerate(lines, start=1):
            if not line.strip():
                continue
            try:
                record = json.loads(line)
            except json.JSONDecodeError:
                # Mirror the DB loader's tolerance for one torn final WAL line.
                if line_number == len(lines):
                    break
                blockers.append(
                    f"stateful_serializability_wal_invalid_json:{node_dir.name}:{wal_path.name}:{line_number}"
                )
                continue
            if not isinstance(record, dict):
                blockers.append(
                    f"stateful_serializability_wal_invalid_record:{node_dir.name}:{wal_path.name}:{line_number}"
                )
                continue
            try:
                height = int(record.get("block_height") or 0)
            except (TypeError, ValueError):
                height = 0
            records.append((height, line_number, record, wal_path))

    records.sort(key=lambda item: (item[0], _wal_generation(item[3]), item[1]))
    for height, line_number, record, wal_path in records:
        if height <= snapshot_height:
            continue
        if str(record.get("version") or "") != "state_delta_wal_v1":
            blockers.append(f"stateful_serializability_wal_version:{node_dir.name}:{wal_path.name}:{line_number}")
            continue
        namespace = str(record.get("namespace") or "").strip()
        if namespace and namespace != shard_id:
            blockers.append(f"stateful_serializability_wal_namespace_mismatch:{node_dir.name}:{namespace}:{shard_id}")
            continue
        block_hash = str(record.get("block_hash") or "").strip()
        if block_hash not in committed_hashes:
            continue
        delta_id = str(record.get("delta_id") or "").strip()
        if delta_id and delta_id in applied_delta_ids:
            continue
        if delta_id:
            applied_delta_ids.add(delta_id)
        updates = record.get("state_updates")
        if not isinstance(updates, list):
            blockers.append(f"stateful_serializability_wal_updates_missing:{node_dir.name}:{height}")
            continue
        for update_index, update in enumerate(updates):
            if not isinstance(update, dict):
                blockers.append(f"stateful_serializability_wal_update_invalid:{node_dir.name}:{height}:{update_index}")
                continue
            if bool(update.get("ordering_noop")):
                continue
            key = str(update.get("key") or "").strip()
            if not key:
                blockers.append(f"stateful_serializability_wal_key_missing:{node_dir.name}:{height}:{update_index}")
                continue
            qualified = _qualify(shard_id, key)
            semantics = str(update.get("update_semantics") or "")
            if semantics == "commutative_delta":
                try:
                    current = int(state.get(qualified, "") or "0")
                except ValueError:
                    blockers.append(f"stateful_serializability_wal_non_integer_base:{node_dir.name}:{height}:{qualified}")
                    continue
                if qualified not in state and bool(update.get("has_initial_value")):
                    try:
                        current = int(update.get("initial_value") or 0)
                    except (TypeError, ValueError):
                        blockers.append(f"stateful_serializability_wal_invalid_initial_value:{node_dir.name}:{height}:{qualified}")
                        continue
                try:
                    delta = int(update.get("delta") or 0)
                except (TypeError, ValueError):
                    blockers.append(f"stateful_serializability_wal_invalid_delta:{node_dir.name}:{height}:{qualified}")
                    continue
                state[qualified] = str(current + delta)
                continue
            base_digest = str(update.get("base_value_digest") or "").strip().lower()
            try:
                produced_version = int(update.get("produced_version") or 0)
            except (TypeError, ValueError):
                produced_version = 0
            if produced_version == 0 and base_digest:
                actual = _value_digest(state.get(qualified, ""))
                if actual != base_digest:
                    blockers.append(f"stateful_serializability_wal_cas_mismatch:{node_dir.name}:{height}:{qualified}")
                    continue
            state[qualified] = str(update.get("value") or "")
    return state, blockers


def _node_business_digest(node_dir: Path) -> tuple[str, list[str]]:
    path = node_dir / "node_summary.json"
    try:
        payload = json.loads(path.read_text(encoding="utf-8-sig"))
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        return "", [f"stateful_serializability_node_summary_unreadable:{node_dir.name}:{type(exc).__name__}"]
    digest = str(payload.get("business_state_digest") or "").strip()
    if not digest:
        return "", [f"stateful_serializability_node_business_digest_missing:{node_dir.name}"]
    return digest, []


def _structural_completion_blockers(summary: dict[str, Any]) -> list[str]:
    blockers: list[str] = []
    for field in (
        "block_executor_consistent",
        "state_root_consistent",
        "receipt_root_consistent",
        "plan_digest_consistent",
        "no_fallback",
        "ready_to_commit",
    ):
        if _summary_bool(summary, field) is not True:
            blockers.append(f"stateful_serializability_{field}_not_true")
    submitted = _finality_int(summary, "submitted_unique_tx_count")
    terminal = _finality_int(summary, "terminal_unique_tx_count")
    finalized = _finality_int(summary, "finalized_unique_logical_tx_count")
    incomplete = _finality_int(summary, "incomplete_unique_tx_count")
    cross_failed = _finality_int(summary, "cross_shard_failed_unique_count")
    if submitted is None or submitted <= 0:
        blockers.append("stateful_serializability_submitted_count_missing")
    else:
        if terminal != submitted:
            blockers.append("stateful_serializability_terminal_not_equal_submitted")
        if finalized != submitted:
            blockers.append("stateful_serializability_finalized_not_equal_submitted")
        executed = summary.get("executed_logical_transaction_count")
        if isinstance(executed, bool) or not isinstance(executed, (int, float)) or int(executed) != submitted:
            blockers.append("stateful_serializability_executed_not_equal_submitted")
    if incomplete != 0:
        blockers.append("stateful_serializability_incomplete_not_zero")
    if cross_failed != 0:
        blockers.append("stateful_serializability_cross_shard_failed_not_zero")
    for field in ("initial_state_digest", "global_final_state_digest", "global_business_state_digest"):
        if not str(summary.get(field) or "").strip():
            blockers.append(f"stateful_serializability_missing_{field}")
    return blockers


# MBE_V38_0_5_BLOCKSTM_REPLICA_EFFECT_SOURCE_CANONICALIZATION:
# Block-STM replicas may reach the same committed read through different
# speculative incarnation numbers (for example tx_8_inc_0 vs tx_8_inc_1).
# Incarnation is an execution-attempt detail, not committed semantics. Keep
# the producer transaction index so a true predecessor disagreement still
# fails closed.
_STM_MVMEMORY_TX_INCARNATION = re.compile(r"^(stm_mvmemory_tx_\d+)_inc_\d+$")


def _replica_effect_source(source: str) -> str:
    value = str(source or "")
    match = _STM_MVMEMORY_TX_INCARNATION.fullmatch(value)
    return match.group(1) if match else value


def _normalize_effect(
    effect: dict[str, list[dict[str, str]]],
) -> dict[str, Any]:
    return {
        "reads": [
            {
                "key": item["key"],
                "value_digest": item["value_digest"],
                "source": _replica_effect_source(item["source"]),
            }
            for item in effect.get("reads", [])
        ],
        "writes": sorted(
            (
                {"key": item["key"], "value_digest": item["value_digest"]}
                for item in effect.get("writes", [])
            ),
            key=lambda item: (item["key"], item["value_digest"]),
        ),
    }


def evaluate_stateful_local_partitioned(
    run_dir: Path,
    summary: dict[str, Any],
) -> dict[str, Any] | None:
    run_dir = Path(run_dir)
    if not matches_stateful_local_legacy(summary):
        return None

    blockers = _structural_completion_blockers(summary)
    executor_id = str(summary.get("block_executor_id") or "")
    node_dirs = sorted(path.parent for path in (run_dir / "nodes").glob("*/committed_chain.csv"))
    if not node_dirs:
        blockers.append("stateful_serializability_missing_committed_chain")

    chains: dict[str, list[dict[str, Any]]] = {}
    execution_summaries: dict[str, dict[str, list[dict[str, Any]]]] = {}
    tx_orders: dict[str, dict[str, list[dict[str, Any]]]] = {}
    accesses: dict[str, dict[tuple[str, str, int], dict[str, list[dict[str, str]]]]] = {}
    shard_nodes: dict[str, list[str]] = {}
    initial_roots_by_shard: dict[str, set[str]] = {}
    initial_sources: dict[str, str] = {}
    persisted_states: dict[str, dict[str, str]] = {}
    node_business_digests: dict[str, str] = {}

    for node_dir in node_dirs:
        node = node_dir.name
        chain, chain_blockers = _load_committed_blocks(node_dir / "committed_chain.csv")
        blockers.extend(chain_blockers)
        chains[node] = chain
        if not chain:
            blockers.append(f"stateful_serializability_empty_chain:{node}")
            continue
        shard = str(chain[0]["shard_id"])
        if any(str(block["shard_id"]) != shard for block in chain):
            blockers.append(f"stateful_serializability_node_multi_shard_chain:{node}")
        shard_nodes.setdefault(shard, []).append(node)

        execution_by_hash, execution_blockers = _load_execution_summary(node_dir)
        blockers.extend(execution_blockers)
        execution_summaries[node] = execution_by_hash
        initial_root, source, initial_blockers = _resolve_initial_root(node_dir, chain[0], execution_by_hash)
        blockers.extend(initial_blockers)
        if initial_root:
            initial_roots_by_shard.setdefault(shard, set()).add(initial_root)
            initial_sources[node] = source

        order, order_blockers = _load_transaction_order(node_dir, chain)
        blockers.extend(order_blockers)
        tx_orders[node] = order
        observed, access_blockers = _load_observed_access(node_dir)
        blockers.extend(access_blockers)
        accesses[node] = observed

        committed_hashes = {str(block["block_hash"]) for block in chain}
        persisted, persistence_blockers = _load_persisted_state(node_dir, shard, committed_hashes)
        blockers.extend(persistence_blockers)
        persisted_states[node] = persisted
        summary_business, summary_blockers = _node_business_digest(node_dir)
        blockers.extend(summary_blockers)
        node_business_digests[node] = summary_business
        if summary_business and _business_state_digest(persisted) != summary_business:
            blockers.append(f"stateful_serializability_persistence_business_digest_mismatch:{node}")

    shards = sorted(shard_nodes)
    # Leave the established single-shard direct replay path untouched.
    if len(shards) <= 1:
        return None

    initial_roots: dict[str, str] = {}
    for shard in shards:
        roots = initial_roots_by_shard.get(shard, set())
        if len(roots) != 1:
            blockers.append(f"stateful_serializability_initial_root_replica_mismatch:{shard}")
            continue
        initial_roots[shard] = next(iter(roots))
        if initial_roots[shard] != EMPTY_STATE_ROOT:
            blockers.append(f"stateful_serializability_nonempty_initial_state_unsupported:{shard}")

    replay_business_effect_digest_by_shard: dict[str, str] = {}
    actual_business_effect_digest_by_shard: dict[str, str] = {}
    actual_business_digest_by_shard: dict[str, str] = {}
    replay_business_digest_by_shard: dict[str, str] = {}
    replica_trace_consistent = True
    validated_external_read_count = 0
    local_read_count = 0
    physical_partition_tx_count = 0
    tx_order_digest_by_shard: dict[str, str] = {}

    for shard in shards:
        nodes = sorted(shard_nodes.get(shard, []))
        if not nodes:
            continue
        reference = nodes[0]
        reference_chain = chains.get(reference, [])
        reference_signature = [
            (int(block["height"]), str(block["block_hash"]), int(block["tx_count"])) for block in reference_chain
        ]
        for node in nodes[1:]:
            candidate_signature = [
                (int(block["height"]), str(block["block_hash"]), int(block["tx_count"]))
                for block in chains.get(node, [])
            ]
            if candidate_signature != reference_signature:
                blockers.append(f"stateful_serializability_committed_chain_replica_mismatch:{shard}:{node}")

        # Build a canonical final-effect trace for every replica and require equality.
        replica_trace_digests: dict[str, str] = {}
        for node in nodes:
            trace_projection: list[dict[str, Any]] = []
            for block in chains.get(node, []):
                block_hash = str(block["block_hash"])
                ordered_txs = tx_orders.get(node, {}).get(block_hash, [])
                for tx_row in ordered_txs:
                    tx_id = str(tx_row["tx_id"])
                    index = int(tx_row["original_index"])
                    effect = accesses.get(node, {}).get((block_hash, tx_id, index), {"reads": [], "writes": []})
                    trace_projection.append(
                        {
                            "height": int(block["height"]),
                            "block_hash": block_hash,
                            "tx_id": tx_id,
                            "original_index": index,
                            "success": bool(tx_row["success"]),
                            "effect": _normalize_effect(effect),
                        }
                    )
            replica_trace_digests[node] = _canonical_digest(trace_projection)
        if len(set(replica_trace_digests.values())) != 1:
            replica_trace_consistent = False
            blockers.append(f"stateful_serializability_effect_trace_replica_mismatch:{shard}")

        state_digest: dict[str, str] = {}
        tx_id_order: list[str] = []
        for block in reference_chain:
            block_hash = str(block["block_hash"])
            ordered_txs = tx_orders.get(reference, {}).get(block_hash, [])
            if len(ordered_txs) != int(block["tx_count"]):
                blockers.append(
                    f"stateful_serializability_reference_tx_count_mismatch:{shard}:{block['height']}:{len(ordered_txs)}:{block['tx_count']}"
                )
            physical_partition_tx_count += len(ordered_txs)
            for tx_row in ordered_txs:
                tx_id = str(tx_row["tx_id"])
                index = int(tx_row["original_index"])
                tx_id_order.append(tx_id)
                effect = accesses.get(reference, {}).get((block_hash, tx_id, index), {"reads": [], "writes": []})
                for read in effect.get("reads", []):
                    source = str(read.get("source") or "")
                    key = str(read.get("key") or "")
                    digest = str(read.get("value_digest") or "").lower()
                    if executor_id == "block_stm_block_executor":
                        if source == "stm_mvmemory_estimate":
                            blockers.append(
                                f"stateful_serializability_final_estimate_read:{shard}:{block['height']}:{tx_id}:{key}"
                            )
                            continue
                        if source == "stm_local_write":
                            local_read_count += 1
                            continue
                        expected = state_digest.get(_qualify(shard, key), EMPTY_VALUE_DIGEST)
                        validated_external_read_count += 1
                        if digest != expected:
                            blockers.append(
                                f"stateful_serializability_read_digest_mismatch:{shard}:{block['height']}:{tx_id}:{key}"
                            )
                write_values: dict[str, str] = {}
                for write in effect.get("writes", []):
                    key = str(write.get("key") or "")
                    digest = str(write.get("value_digest") or "").lower()
                    previous = write_values.get(key)
                    if previous is not None and previous != digest:
                        blockers.append(
                            f"stateful_serializability_multiple_final_write_digests:{shard}:{block['height']}:{tx_id}:{key}"
                        )
                    write_values[key] = digest
                for key in sorted(write_values):
                    state_digest[_qualify(shard, key)] = write_values[key]

        tx_order_digest_by_shard[shard] = _canonical_digest(tx_id_order)
        replay_effect = {key: value for key, value in state_digest.items() if _is_business_key(key)}
        replay_business_effect_digest_by_shard[shard] = _business_effect_digest(replay_effect)

        actual_effect_candidates: set[str] = set()
        actual_business_candidates: set[str] = set()
        for node in nodes:
            persisted = persisted_states.get(node, {})
            persisted_effect = {key: _value_digest(value) for key, value in persisted.items() if _is_business_key(key)}
            actual_effect_candidates.add(_business_effect_digest(persisted_effect))
            if node_business_digests.get(node):
                actual_business_candidates.add(node_business_digests[node])
        if len(actual_effect_candidates) != 1:
            blockers.append(f"stateful_serializability_persisted_effect_replica_mismatch:{shard}")
            actual_effect = ""
        else:
            actual_effect = next(iter(actual_effect_candidates))
            actual_business_effect_digest_by_shard[shard] = actual_effect
        if len(actual_business_candidates) != 1:
            blockers.append(f"stateful_serializability_business_digest_replica_mismatch:{shard}")
            actual_business = ""
        else:
            actual_business = next(iter(actual_business_candidates))
            actual_business_digest_by_shard[shard] = actual_business

        if replay_business_effect_digest_by_shard[shard] != actual_effect:
            blockers.append(f"stateful_serializability_partition_effect_digest_mismatch:{shard}")
        elif actual_business:
            # We do not reconstruct business values from digests. Once the complete
            # final effect-digest map matches persisted state, the persisted raw
            # business digest is the replay-equivalent final business state evidence.
            replay_business_digest_by_shard[shard] = actual_business

    actual_global = (
        _canonical_digest(dict(sorted(actual_business_digest_by_shard.items())))
        if len(actual_business_digest_by_shard) == len(shards) and shards
        else ""
    )
    replay_global = (
        _canonical_digest(dict(sorted(replay_business_digest_by_shard.items())))
        if len(replay_business_digest_by_shard) == len(shards) and shards
        else ""
    )
    summary_global = str(summary.get("global_business_state_digest") or "").strip()
    if actual_global and summary_global and actual_global != summary_global:
        blockers.append("stateful_serializability_summary_global_business_digest_mismatch")
    if replay_global and actual_global and replay_global != actual_global:
        blockers.append("stateful_serializability_global_business_digest_mismatch")

    blockers = list(dict.fromkeys(blockers))
    valid = (
        not blockers
        and bool(shards)
        and replica_trace_consistent
        and bool(replay_global)
        and replay_global == actual_global == summary_global
    )
    return {
        "serial_order_oracle_schema": SCHEMA_VERSION,
        "serial_order_oracle_status": "passed" if valid else "failed",
        "serial_order_replay_applicable": True,
        "serial_order_replay_not_applicable_reason": "",
        "serial_order_replay_equivalent": valid,
        "serial_order_replay_blockers": blockers,
        "serial_order_replay_structural_blockers": blockers,
        "serial_order_replay_supported_scope": SUPPORTED_SCOPE,
        "serial_order_replay_identity_basis": "tx_id",
        "serial_order_replay_order_basis": "per_shard_pbft_committed_chain_then_final_tx_delta_order",
        "serial_order_replay_original_index_semantics": "block_local_order_check",
        "serial_order_replay_initial_state_empty": bool(initial_roots)
        and len(initial_roots) == len(shards)
        and all(root == EMPTY_STATE_ROOT for root in initial_roots.values()),
        "serial_order_replay_initial_state_root": "partitioned",
        "serial_order_replay_initial_state_sources": initial_sources,
        "serial_order_replay_shard_id": "stateful-partitioned",
        "serial_order_replay_transaction_count": physical_partition_tx_count,
        "serial_order_replay_unique_transaction_count": _finality_int(summary, "submitted_unique_tx_count") or 0,
        "serial_order_replay_committed_block_count": sum(
            len(chains.get(sorted(shard_nodes[shard])[0], [])) for shard in shards if shard_nodes[shard]
        ),
        "serial_order_replay_trace_reexecution_count": 0,
        "serial_order_replay_input_digest": _canonical_digest(replay_business_effect_digest_by_shard)
        if replay_business_effect_digest_by_shard
        else "",
        "serial_order_replay_commit_order_digest": _canonical_digest(tx_order_digest_by_shard)
        if tx_order_digest_by_shard
        else "",
        "serial_order_replay_tx_id_order_digest": _canonical_digest(tx_order_digest_by_shard)
        if tx_order_digest_by_shard
        else "",
        "serial_order_replay_business_state_digest": replay_global,
        "serial_order_actual_business_state_digest": actual_global,
        "serial_order_replay_global_business_state_digest": replay_global,
        "serial_order_actual_global_business_state_digest": actual_global,
        "serial_order_replay_business_key_count": sum(
            len([key for key in persisted_states.get(sorted(shard_nodes[shard])[0], {}) if _is_business_key(key)])
            for shard in shards
            if shard_nodes[shard]
        ),
        "serial_order_replay_replica_order_consistent": replica_trace_consistent,
        "serial_order_replay_replica_count": len(node_dirs),
        "serial_order_replay_reference_node": ",".join(
            sorted(sorted(shard_nodes[shard])[0] for shard in shards if shard_nodes[shard])
        ),
        "method_correctness_oracle_kind": "stateful_local_legacy_partition_serializability_v2",
        "method_correctness_oracle_status": "passed" if valid else "failed",
        "method_correctness_oracle_valid": valid,
        "method_correctness_oracle_blockers": blockers,
        "method_correctness_oracle_scope": SUPPORTED_SCOPE,
        "stateful_serializability_shards": shards,
        "stateful_serializability_replay_partition_business_digests": replay_business_digest_by_shard,
        "stateful_serializability_actual_partition_business_digests": actual_business_digest_by_shard,
        "stateful_serializability_replay_partition_effect_digests": replay_business_effect_digest_by_shard,
        "stateful_serializability_actual_partition_effect_digests": actual_business_effect_digest_by_shard,
        "stateful_serializability_validated_external_read_count": validated_external_read_count,
        "stateful_serializability_local_read_count": local_read_count,
        "stateful_serializability_replica_trace_consistent": replica_trace_consistent,
        "state_home_mapping_required": False,
        "stateful_serializability_evidence_sources": [
            "committed_chain.csv",
            "transaction_execution_trace.csv",
            "observed_state_access.csv",
            "state_snapshot.json",
            "state_delta.*.wal",
            "node_summary.json",
        ],
    }
