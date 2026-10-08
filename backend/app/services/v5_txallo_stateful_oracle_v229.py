from __future__ import annotations

import csv
import gzip
import hashlib
import json
from pathlib import Path
from typing import Any

from backend.app.services import v5_stateful_serializability_oracle as stateful

SCHEMA_VERSION = "mbe_txallo_stateful_replica_oracle_v229"
REPLICA_SCHEMA = "mbe_txallo_replica_commit_v229"
SUPPORTED_SCOPE = "txallo_section_vii_pbft_bound_replicated_state_v229"


def _digest(value: Any) -> str:
    raw = json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def _value_digest(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def _load_access(path: Path) -> tuple[list[dict[str, Any]], list[str]]:
    blockers: list[str] = []
    rows: list[dict[str, Any]] = []
    if not path.is_file():
        return [], ["txallo_stateful_replica_missing_resolved_access_lists"]
    try:
        with gzip.open(path, "rt", encoding="utf-8") as handle:
            for line_number, line in enumerate(handle, start=1):
                if not line.strip():
                    continue
                try:
                    obj = json.loads(line)
                except json.JSONDecodeError:
                    blockers.append(f"txallo_stateful_replica_access_invalid_json:{line_number}")
                    continue
                if not isinstance(obj, dict):
                    blockers.append(f"txallo_stateful_replica_access_invalid_row:{line_number}")
                    continue
                rows.append(obj)
    except (OSError, UnicodeError) as exc:
        blockers.append(f"txallo_stateful_replica_access_unreadable:{type(exc).__name__}")
    try:
        rows.sort(key=lambda row: int(row.get("index", -1)))
    except (TypeError, ValueError):
        blockers.append("txallo_stateful_replica_access_index_invalid")
    return rows, blockers


def _is_write(mode: str) -> bool:
    return mode in {"write", "read_write", "commutative_delta", "unknown"}


def _is_read(mode: str) -> bool:
    return mode in {"read", "read_write", "commutative_delta", "unknown"}


def _is_commutative(access: dict[str, Any]) -> bool:
    return str(access.get("mode") or "") == "commutative_delta" or str(access.get("update_semantics") or "") == "commutative_delta"


def _expected_replica_writes(rows: list[dict[str, Any]]) -> tuple[dict[tuple[str, int], dict[str, Any]], dict[str, int], list[str]]:
    blockers: list[str] = []
    expected: dict[tuple[str, int], dict[str, Any]] = {}
    last_writer: dict[str, int] = {}
    seen_index: set[int] = set()
    for row in rows:
        try:
            index = int(row.get("index"))
        except (TypeError, ValueError):
            blockers.append("txallo_stateful_replica_access_index_invalid")
            continue
        if index < 0 or index in seen_index:
            blockers.append(f"txallo_stateful_replica_access_index_duplicate_or_negative:{index}")
            continue
        seen_index.add(index)
        ordinal = index + 1
        tx_id = str(row.get("tx_id") or "").strip()
        execution_shard = str(row.get("execution_shard") or "").strip()
        if not tx_id or not execution_shard:
            blockers.append(f"txallo_stateful_replica_access_identity_missing:{index}")
            continue
        versions_raw = row.get("state_versions") if isinstance(row.get("state_versions"), list) else []
        versions = {
            str(item.get("key") or ""): item
            for item in versions_raw
            if isinstance(item, dict) and str(item.get("key") or "")
        }
        accesses = row.get("access_list") if isinstance(row.get("access_list"), list) else []
        for access in accesses:
            if not isinstance(access, dict):
                blockers.append(f"txallo_stateful_replica_access_item_invalid:{index}")
                continue
            key = str(access.get("key") or "").strip()
            mode = str(access.get("mode") or "").strip()
            if not key or mode not in {"read", "write", "read_write", "commutative_delta", "unknown"}:
                blockers.append(f"txallo_stateful_replica_access_item_invalid:{index}:{key}")
                continue
            commutative = _is_commutative(access)
            if not commutative:
                dep = versions.get(key)
                if dep is None:
                    blockers.append(f"txallo_stateful_replica_version_missing:{index}:{key}")
                else:
                    try:
                        required = int(dep.get("required_version") or 0)
                        produced = int(dep.get("produced_version") or 0)
                    except (TypeError, ValueError):
                        required = produced = -1
                    if required != last_writer.get(key, 0):
                        blockers.append(f"txallo_stateful_replica_required_version_mismatch:{index}:{key}:{required}:{last_writer.get(key,0)}")
                    want_produced = ordinal if _is_write(mode) else 0
                    if produced != want_produced:
                        blockers.append(f"txallo_stateful_replica_produced_version_mismatch:{index}:{key}:{produced}:{want_produced}")
            if _is_write(mode):
                identity = (key, ordinal)
                if identity in expected:
                    blockers.append(f"txallo_stateful_replica_duplicate_expected_write:{index}:{key}")
                expected[identity] = {
                    "key": key,
                    "ordinal": ordinal,
                    "tx_id": tx_id,
                    "execution_shard": execution_shard,
                    "commutative": commutative,
                }
                if not commutative:
                    last_writer[key] = ordinal
    return expected, last_writer, blockers


def _load_replica_feed(path: Path) -> tuple[dict[tuple[str, int], dict[str, Any]], list[str]]:
    rows: dict[tuple[str, int], dict[str, Any]] = {}
    blockers: list[str] = []
    if not path.is_file():
        return {}, [f"txallo_stateful_replica_feed_missing:{path.parent.name}"]
    try:
        with path.open("r", encoding="utf-8-sig") as handle:
            for line_number, line in enumerate(handle, start=1):
                if not line.strip():
                    continue
                try:
                    row = json.loads(line)
                except json.JSONDecodeError:
                    blockers.append(f"txallo_stateful_replica_feed_invalid_json:{path.parent.name}:{line_number}")
                    continue
                if not isinstance(row, dict) or str(row.get("schema_version") or "") != REPLICA_SCHEMA:
                    blockers.append(f"txallo_stateful_replica_feed_schema:{path.parent.name}:{line_number}")
                    continue
                key = str(row.get("key") or "").strip()
                try:
                    ordinal = int(row.get("routing_ordinal") or 0)
                except (TypeError, ValueError):
                    ordinal = 0
                value = str(row.get("value") or "")
                value_digest = str(row.get("value_digest") or "").lower()
                if not key or ordinal <= 0 or len(value_digest) != 64:
                    blockers.append(f"txallo_stateful_replica_feed_row_invalid:{path.parent.name}:{line_number}")
                    continue
                if _value_digest(value) != value_digest:
                    blockers.append(f"txallo_stateful_replica_feed_value_digest_mismatch:{path.parent.name}:{key}:{ordinal}")
                    continue
                identity = (key, ordinal)
                prior = rows.get(identity)
                if prior is not None and (
                    str(prior.get("source_tx_id") or "") != str(row.get("source_tx_id") or "")
                    or (not bool(row.get("commutative")) and str(prior.get("value_digest") or "") != value_digest)
                ):
                    blockers.append(f"txallo_stateful_replica_feed_conflict:{path.parent.name}:{key}:{ordinal}")
                    continue
                rows[identity] = row
    except (OSError, UnicodeError) as exc:
        blockers.append(f"txallo_stateful_replica_feed_unreadable:{path.parent.name}:{type(exc).__name__}")
    return rows, blockers


def _node_shard(node_dir: Path) -> str:
    summary = node_dir / "node_summary.json"
    try:
        obj = json.loads(summary.read_text(encoding="utf-8-sig"))
        if isinstance(obj, dict):
            return str(obj.get("shard_id") or "").strip()
    except Exception:
        pass
    chain = node_dir / "committed_chain.csv"
    try:
        with chain.open(newline="", encoding="utf-8-sig") as handle:
            row = next(csv.DictReader(handle), None)
            return str((row or {}).get("shard_id") or "").strip()
    except Exception:
        return ""


def _snapshot_height(node_dir: Path) -> int:
    path = node_dir / "state_snapshot_metadata.json"
    if not path.is_file():
        return 0
    try:
        obj = json.loads(path.read_text(encoding="utf-8-sig"))
        return int(obj.get("included_height") or 0) if isinstance(obj, dict) else 0
    except (OSError, UnicodeError, json.JSONDecodeError, TypeError, ValueError):
        return 0


def _committed_chain_rows(node_dir: Path) -> dict[str, dict[str, Any]]:
    path = node_dir / "committed_chain.csv"
    out: dict[str, dict[str, Any]] = {}
    try:
        with path.open(newline="", encoding="utf-8-sig") as handle:
            for row in csv.DictReader(handle):
                block_hash = str(row.get("block_hash") or "").strip()
                if block_hash:
                    out[block_hash] = dict(row)
    except (OSError, csv.Error, UnicodeError):
        return {}
    return out


def _proven_null_noop_wal_blockers(node_dir: Path, shard_id: str, committed_hashes: set[str]) -> set[str]:
    """Return only legacy-oracle blockers proven to describe a legal nil-slice WAL no-op.

    Go serializes a nil []StateKV as JSON null. That is a valid WAL record when the
    committed block has no transactions and both WAL/chain state roots are unchanged.
    Missing state_updates, a changed root, a non-system block, or inconsistent chain
    evidence are deliberately not accepted here.
    """
    safe: set[str] = set()
    chain = _committed_chain_rows(node_dir)
    snapshot_height = _snapshot_height(node_dir)
    paths = sorted(node_dir.glob("state_delta.*.wal"))
    legacy = node_dir / "state_delta.wal"
    if legacy.is_file():
        paths.append(legacy)
    for wal_path in paths:
        try:
            lines = wal_path.read_text(encoding="utf-8-sig").splitlines()
        except (OSError, UnicodeError):
            continue
        for line in lines:
            if not line.strip():
                continue
            try:
                record = json.loads(line)
            except json.JSONDecodeError:
                continue
            if not isinstance(record, dict):
                continue
            try:
                height = int(record.get("block_height") or 0)
            except (TypeError, ValueError):
                continue
            block_hash = str(record.get("block_hash") or "").strip()
            if height <= snapshot_height or block_hash not in committed_hashes:
                continue
            if str(record.get("version") or "") != "state_delta_wal_v1":
                continue
            namespace = str(record.get("namespace") or "").strip()
            if namespace and namespace != shard_id:
                continue
            if "state_updates" not in record or record.get("state_updates") is not None:
                continue
            wal_before = str(record.get("state_root_before") or "").strip()
            wal_after = str(record.get("state_root_after") or "").strip()
            if not wal_before or wal_before != wal_after:
                continue
            chain_row = chain.get(block_hash)
            if not chain_row:
                continue
            try:
                tx_count = int(str(chain_row.get("tx_count") or "0").strip())
                chain_height = int(str(chain_row.get("height") or "0").strip())
            except (TypeError, ValueError):
                continue
            chain_before = str(chain_row.get("state_root_before") or "").strip()
            chain_after = str(chain_row.get("state_root_after") or "").strip()
            if chain_height != height or tx_count != 0:
                continue
            if not chain_before or chain_before != chain_after or chain_before != wal_before:
                continue
            safe.add(f"stateful_serializability_wal_updates_missing:{node_dir.name}:{height}")
    return safe


def _logical_business_state(node_dir: Path, shard_id: str) -> tuple[dict[str, str], list[str], int]:
    chain, blockers = stateful._load_committed_blocks(node_dir / "committed_chain.csv")
    committed = {str(row.get("block_hash") or "") for row in chain}
    persisted, errors = stateful._load_persisted_state(node_dir, shard_id, committed)
    safe_noops = _proven_null_noop_wal_blockers(node_dir, shard_id, committed)
    errors = [item for item in errors if item not in safe_noops]
    blockers.extend(errors)
    logical: dict[str, str] = {}
    for key, value in persisted.items():
        if not stateful._is_business_key(key):
            continue
        logical_key = stateful._logical_key(key)
        old = logical.get(logical_key)
        if old is not None and old != value:
            blockers.append(f"txallo_stateful_replica_duplicate_logical_key_conflict:{node_dir.name}:{logical_key}")
        logical[logical_key] = value
    return logical, blockers, len(safe_noops)


def _network_message_type_counts(path: Path) -> tuple[dict[str, int], list[str]]:
    counts: dict[str, int] = {}
    blockers: list[str] = []
    if not path.is_file():
        return counts, ["txallo_stateful_physical_network_message_summary_missing"]
    try:
        with path.open(newline="", encoding="utf-8-sig") as handle:
            for row_number, row in enumerate(csv.DictReader(handle), start=2):
                if str(row.get("scope") or "").strip() != "message_type":
                    continue
                message_type = str(row.get("message_type") or "").strip()
                if not message_type:
                    continue
                try:
                    count = int(float(str(row.get("message_count") or "0").strip()))
                except (TypeError, ValueError):
                    blockers.append(f"txallo_stateful_physical_network_count_invalid:{row_number}:{message_type}")
                    continue
                counts[message_type] = counts.get(message_type, 0) + count
    except (OSError, csv.Error, UnicodeError) as exc:
        blockers.append(f"txallo_stateful_physical_network_summary_unreadable:{type(exc).__name__}")
    return counts, blockers


def _generic_versioned_wave_evidence(node_dirs: list[Path]) -> tuple[int, int, list[str]]:
    wave_blocks = 0
    wave_count = 0
    blockers: list[str] = []
    for node_dir in node_dirs:
        path = node_dir / "block_execution_summary.json"
        if not path.is_file():
            blockers.append(f"txallo_stateful_physical_block_execution_summary_missing:{node_dir.name}")
            continue
        try:
            payload = json.loads(path.read_text(encoding="utf-8-sig"))
        except (OSError, UnicodeError, json.JSONDecodeError) as exc:
            blockers.append(f"txallo_stateful_physical_block_execution_summary_unreadable:{node_dir.name}:{type(exc).__name__}")
            continue
        rows = payload.get("blocks") if isinstance(payload, dict) and isinstance(payload.get("blocks"), list) else []
        for index, row in enumerate(rows):
            if not isinstance(row, dict):
                blockers.append(f"txallo_stateful_physical_execution_row_invalid:{node_dir.name}:{index}")
                continue
            policy = str(row.get("versioned_wave_execution_policy") or "").strip()
            try:
                count = int(row.get("versioned_state_ready_wave_count") or 0)
            except (TypeError, ValueError):
                blockers.append(f"txallo_stateful_physical_wave_count_invalid:{node_dir.name}:{index}")
                count = 0
            if policy or count > 0:
                wave_blocks += 1
                wave_count += max(count, 1)
    return wave_blocks, wave_count, blockers


def _physical_local_replica_execution_evidence(root: Path, node_dirs: list[Path]) -> tuple[dict[str, Any], list[str]]:
    blockers: list[str] = []
    counts, errors = _network_message_type_counts(root / "network_message_summary.csv")
    blockers.extend(errors)
    request_count = counts.get("V5_STATE_FETCH_REQUEST", 0)
    response_count = counts.get("V5_STATE_FETCH_RESPONSE", 0)
    if request_count != 0:
        blockers.append(f"txallo_stateful_physical_state_fetch_request_nonzero:{request_count}")
    if response_count != 0:
        blockers.append(f"txallo_stateful_physical_state_fetch_response_nonzero:{response_count}")
    wave_blocks, wave_count, errors = _generic_versioned_wave_evidence(node_dirs)
    blockers.extend(errors)
    if wave_blocks != 0 or wave_count != 0:
        blockers.append(f"txallo_stateful_physical_generic_versioned_wave_nonzero:{wave_blocks}:{wave_count}")
    evidence = {
        "txallo_stateful_physical_transport_contract": "local_durable_replica_projection_no_state_fetch_v2296",
        "txallo_stateful_physical_state_fetch_request_count": request_count,
        "txallo_stateful_physical_state_fetch_response_count": response_count,
        "txallo_stateful_physical_generic_versioned_wave_block_count": wave_blocks,
        "txallo_stateful_physical_generic_versioned_wave_count": wave_count,
        "txallo_stateful_physical_zero_fetch_verified": not blockers,
    }
    return evidence, blockers


def evaluate(run_dir: Path, summary: dict[str, Any]) -> dict[str, Any]:
    root = Path(run_dir)
    blockers: list[str] = []
    feed_blockers: list[str] = []
    accepted_null_noop_wal_count = 0
    access_rows, errors = _load_access(root / "client" / "resolved_access_lists.jsonl.gz")
    blockers.extend(errors)
    feed_blockers.extend(errors)
    expected, last_writer, errors = _expected_replica_writes(access_rows)
    blockers.extend(errors)
    feed_blockers.extend(errors)
    if not expected:
        blockers.append("txallo_stateful_replica_expected_write_set_empty")
        feed_blockers.append("txallo_stateful_replica_expected_write_set_empty")

    node_dirs = sorted(path.parent for path in (root / "nodes").glob("*/committed_chain.csv"))
    if not node_dirs:
        blockers.append("txallo_stateful_replica_nodes_missing")
        feed_blockers.append("txallo_stateful_replica_nodes_missing")

    physical_evidence, physical_blockers = _physical_local_replica_execution_evidence(root, node_dirs)
    blockers.extend(physical_blockers)

    feeds: dict[str, dict[tuple[str, int], dict[str, Any]]] = {}
    state_by_node: dict[str, dict[str, str]] = {}
    shard_by_node: dict[str, str] = {}
    for node_dir in node_dirs:
        shard = _node_shard(node_dir)
        shard_by_node[node_dir.name] = shard
        if not shard:
            blocker = f"txallo_stateful_replica_node_shard_missing:{node_dir.name}"
            blockers.append(blocker)
            feed_blockers.append(blocker)
        feed, errors = _load_replica_feed(node_dir / "txallo_replica_commit.jsonl")
        feeds[node_dir.name] = feed
        blockers.extend(errors)
        feed_blockers.extend(errors)
        logical, errors, accepted_noops = _logical_business_state(node_dir, shard)
        accepted_null_noop_wal_count += accepted_noops
        state_by_node[node_dir.name] = logical
        blockers.extend(errors)

    expected_ids = set(expected)
    for node, feed in feeds.items():
        missing = expected_ids.difference(feed)
        extra = set(feed).difference(expected_ids)
        if missing:
            blocker = f"txallo_stateful_replica_missing_durable_tokens:{node}:{len(missing)}"
            blockers.append(blocker)
            feed_blockers.append(blocker)
        if extra:
            blocker = f"txallo_stateful_replica_unexpected_durable_tokens:{node}:{len(extra)}"
            blockers.append(blocker)
            feed_blockers.append(blocker)
        shard = shard_by_node.get(node, "")
        for identity, want in expected.items():
            got = feed.get(identity)
            if got is None:
                continue
            if str(got.get("source_tx_id") or "") != want["tx_id"]:
                blocker = f"txallo_stateful_replica_source_tx_mismatch:{node}:{identity[0]}:{identity[1]}"
                blockers.append(blocker)
                feed_blockers.append(blocker)
            if str(got.get("source_shard") or "") != want["execution_shard"]:
                blocker = f"txallo_stateful_replica_source_shard_mismatch:{node}:{identity[0]}:{identity[1]}"
                blockers.append(blocker)
                feed_blockers.append(blocker)
            role = str(got.get("role") or "")
            expected_role = "source_local" if shard == want["execution_shard"] else "replica_system"
            if role != expected_role:
                blocker = f"txallo_stateful_replica_role_mismatch:{node}:{identity[0]}:{identity[1]}:{role}:{expected_role}"
                blockers.append(blocker)
                feed_blockers.append(blocker)

    # Non-commutative writes represent exact values and must agree at every durable replica.
    for identity, want in expected.items():
        if want["commutative"]:
            continue
        values = {
            str(feed[identity].get("value_digest") or "")
            for feed in feeds.values()
            if identity in feed
        }
        if len(values) > 1:
            blocker = f"txallo_stateful_replica_exact_value_disagreement:{identity[0]}:{identity[1]}"
            blockers.append(blocker)
            feed_blockers.append(blocker)

    # All validators in a shard and all physical shards must reconstruct the same logical business state.
    logical_digests = {node: _digest(dict(sorted(state.items()))) for node, state in state_by_node.items()}
    digest_values = {value for value in logical_digests.values() if value}
    if len(digest_values) != 1:
        blockers.append("txallo_stateful_replica_final_logical_state_not_globally_equal")

    # The latest exact producer recorded by the signed client version chain must match final persisted state.
    reference_state = next(iter(state_by_node.values()), {})
    for key, ordinal in last_writer.items():
        identity = (key, ordinal)
        digest_candidates = {
            str(feed[identity].get("value_digest") or "")
            for feed in feeds.values()
            if identity in feed
        }
        if len(digest_candidates) != 1:
            continue
        if key not in reference_state or _value_digest(reference_state[key]) != next(iter(digest_candidates)):
            blockers.append(f"txallo_stateful_replica_final_exact_value_mismatch:{key}:{ordinal}")

    submitted = (summary.get("finality_evidence") or {}).get("submitted_unique_tx_count") if isinstance(summary.get("finality_evidence"), dict) else None
    if submitted is not None and int(submitted) != len(access_rows):
        blockers.append("txallo_stateful_replica_submitted_access_count_mismatch")

    blockers = list(dict.fromkeys(blockers))
    feed_blockers = list(dict.fromkeys(feed_blockers))
    feed_complete = not feed_blockers and bool(node_dirs) and bool(expected)
    valid = not blockers and bool(node_dirs) and bool(expected) and len(digest_values) == 1
    logical_digest = next(iter(digest_values), "") if len(digest_values) == 1 else ""
    return {
        "serial_order_oracle_schema": SCHEMA_VERSION,
        "serial_order_oracle_status": "passed" if valid else "failed",
        "serial_order_replay_applicable": True,
        "serial_order_replay_not_applicable_reason": "",
        "serial_order_replay_equivalent": valid,
        "serial_order_replay_blockers": blockers,
        "serial_order_replay_structural_blockers": blockers,
        "serial_order_replay_supported_scope": SUPPORTED_SCOPE,
        "serial_order_replay_identity_basis": "resolved_access_index_plus_tx_id",
        "serial_order_replay_order_basis": "signed_per_key_required_produced_version_chain_plus_pbft_bound_replica_materialization",
        "serial_order_replay_input_digest": _digest(access_rows) if access_rows else "",
        "serial_order_replay_business_state_digest": logical_digest,
        "serial_order_actual_business_state_digest": logical_digest,
        "serial_order_replay_global_business_state_digest": logical_digest,
        "serial_order_actual_global_business_state_digest": logical_digest,
        "serial_order_replay_business_key_count": len(reference_state),
        "serial_order_replay_replica_order_consistent": valid,
        "serial_order_replay_replica_count": len(node_dirs),
        "serial_order_replay_reference_node": node_dirs[0].name if node_dirs else "",
        "method_correctness_oracle_kind": SCHEMA_VERSION,
        "method_correctness_oracle_status": "passed" if valid else "failed",
        "method_correctness_oracle_valid": valid,
        "method_correctness_oracle_blockers": blockers,
        "method_correctness_oracle_scope": SUPPORTED_SCOPE,
        "txallo_stateful_replica_expected_write_count": len(expected),
        "txallo_stateful_replica_node_count": len(node_dirs),
        "txallo_stateful_replica_feed_complete": feed_complete,
        "txallo_stateful_replica_feed_blockers": feed_blockers,
        "txallo_stateful_replica_accepted_null_noop_wal_count": accepted_null_noop_wal_count,
        "txallo_stateful_replica_logical_state_digest_by_node": logical_digests,
        "txallo_stateful_replica_final_global_logical_state_digest": logical_digest,
        **physical_evidence,
    }
