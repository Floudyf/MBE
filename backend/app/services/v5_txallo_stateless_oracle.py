from __future__ import annotations

import csv
import gzip
import json
from collections import defaultdict
from pathlib import Path
from typing import Any

from backend.app.services.v5_serial_order_oracle import (
    EMPTY_STATE_ROOT,
    _business_state_digest,
    _canonical_digest,
    _committed_tx_order,
    _stable_direct_access_value,
)

SCHEMA_VERSION = "mbe_txallo_stateless_multishard_serial_oracle_v2"
SUPPORTED_SCOPE = "stateless_txallo_multi_shard_exact_version_logical_replay_v1"
INITIAL_STATE_FILE = "txallo_stateless_initial_business_state_v204.json"


def _read_json(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8-sig"))
        return value if isinstance(value, dict) else {}
    except (OSError, UnicodeError, json.JSONDecodeError):
        return {}


def _requires_exact_version(access: dict[str, Any]) -> bool:
    key = str(access.get("key") or "").strip()
    mode = str(access.get("mode") or "").strip()
    if not key or mode == "commutative_delta":
        return False
    return not (key.startswith("balance:") or key.startswith("nonce:"))


def _load_entries(path: Path) -> tuple[dict[str, dict[str, Any]], str, list[str]]:
    entries: dict[str, dict[str, Any]] = {}
    canonical: list[dict[str, Any]] = []
    blockers: list[str] = []
    try:
        with gzip.open(path, "rt", encoding="utf-8") as handle:
            for line_no, line in enumerate(handle, 1):
                if not line.strip():
                    continue
                row = json.loads(line)
                if not isinstance(row, dict):
                    blockers.append(f"txallo_stateless_resolved_access_invalid:{line_no}")
                    continue
                tx_id = str(row.get("tx_id") or "").strip()
                logical_id = str(row.get("logical_id") or "").strip()
                index = row.get("index")
                execution_shard = str(row.get("execution_shard") or "").strip()
                accesses = row.get("access_list")
                versions = row.get("state_versions")
                if not tx_id or not logical_id or isinstance(index, bool) or not isinstance(index, int) or index < 0:
                    blockers.append(f"txallo_stateless_resolved_access_identity_invalid:{line_no}")
                    continue
                if not execution_shard:
                    blockers.append(f"txallo_stateless_execution_shard_missing:{tx_id}")
                if not isinstance(accesses, list):
                    blockers.append(f"txallo_stateless_access_list_missing:{tx_id}")
                    accesses = []
                if versions is None:
                    versions = []
                if not isinstance(versions, list):
                    blockers.append(f"txallo_stateless_state_versions_invalid:{tx_id}")
                    versions = []
                version_keys = {str(dep.get("key") or "").strip() for dep in versions if isinstance(dep, dict)}
                for access in accesses:
                    if isinstance(access, dict) and _requires_exact_version(access):
                        key = str(access.get("key") or "").strip()
                        if key and key not in version_keys:
                            blockers.append(f"txallo_stateless_state_version_dependency_missing:{tx_id}:{key}")
                entry = {
                    "index": index,
                    "tx_id": tx_id,
                    "logical_id": logical_id,
                    "execution_shard": execution_shard,
                    "access_list": accesses,
                    "state_versions": versions,
                }
                if tx_id in entries:
                    blockers.append(f"txallo_stateless_duplicate_tx_id:{tx_id}")
                else:
                    entries[tx_id] = entry
                canonical.append(entry)
    except (OSError, EOFError, gzip.BadGzipFile, UnicodeError, json.JSONDecodeError) as exc:
        return {}, "", [f"txallo_stateless_resolved_access_unreadable:{type(exc).__name__}"]
    canonical.sort(key=lambda x: int(x["index"]))
    return entries, _canonical_digest(canonical) if canonical else "", blockers


def _load_home_map(path: Path) -> tuple[dict[str, str], list[str]]:
    homes: dict[str, str] = {}
    blockers: list[str] = []
    try:
        with path.open(newline="", encoding="utf-8-sig") as handle:
            for row_no, row in enumerate(csv.DictReader(handle), 2):
                key = str(row.get("state_key") or row.get("home_state_unit") or "").strip()
                home = str(row.get("home_shard") or "").strip()
                if not key or not home:
                    continue
                previous = homes.get(key)
                if previous and previous != home:
                    blockers.append(f"txallo_stateless_state_home_conflict:{key}:{previous}:{home}:{row_no}")
                homes[key] = home
    except (OSError, csv.Error, UnicodeError) as exc:
        return {}, [f"txallo_stateless_placement_plan_unreadable:{type(exc).__name__}"]
    if not homes:
        blockers.append("txallo_stateless_state_home_mapping_empty")
    return homes, blockers


def _node_groups(run_dir: Path) -> tuple[dict[str, list[Path]], dict[str, str], list[str]]:
    by_shard: dict[str, list[Path]] = defaultdict(list)
    actual: dict[str, set[str]] = defaultdict(set)
    blockers: list[str] = []
    for summary_path in sorted((run_dir / "nodes").glob("*/node_summary.json")):
        row = _read_json(summary_path)
        shard = str(row.get("shard_id") or "").strip()
        digest = str(row.get("business_state_digest") or "").strip()
        if not shard:
            blockers.append(f"txallo_stateless_node_summary_shard_missing:{summary_path.parent.name}")
            continue
        by_shard[shard].append(summary_path.parent)
        if digest:
            actual[shard].add(digest)
        else:
            blockers.append(f"txallo_stateless_business_digest_missing:{summary_path.parent.name}")
    actual_one: dict[str, str] = {}
    for shard, values in actual.items():
        if len(values) != 1:
            blockers.append(f"txallo_stateless_business_digest_replica_mismatch:{shard}")
        else:
            actual_one[shard] = next(iter(values))
    if not by_shard:
        blockers.append("txallo_stateless_node_summaries_missing")
    return dict(by_shard), actual_one, blockers


def _legacy_empty_initial_state_proven(node: Path, shard: str) -> bool:
    """Compatibility proof for pre-v20.4 fixtures/runs with no explicit snapshot.

    We may infer an empty business start only when the first committed block on the
    node explicitly records the canonical EMPTY_STATE_ROOT. Any missing, malformed,
    non-empty, or shard-inconsistent evidence remains fail-closed.
    """
    path = node / "committed_chain.csv"
    try:
        with path.open(newline="", encoding="utf-8-sig") as handle:
            rows = list(csv.DictReader(handle))
    except (OSError, csv.Error, UnicodeError):
        return False
    candidates: list[tuple[int, dict[str, str]]] = []
    for row in rows:
        row_shard = str(row.get("shard_id") or "").strip()
        if row_shard and row_shard != shard:
            continue
        try:
            height = int(str(row.get("height") or "0"))
        except ValueError:
            continue
        if height <= 0:
            continue
        candidates.append((height, row))
    if not candidates:
        return False
    _, first = min(candidates, key=lambda item: item[0])
    return str(first.get("state_root_before") or "").strip() == EMPTY_STATE_ROOT


def _initial_state_source_labels(groups: dict[str, list[Path]]) -> dict[str, str]:
    sources: dict[str, str] = {}
    for shard, nodes in sorted(groups.items()):
        explicit = [(node / INITIAL_STATE_FILE).is_file() for node in nodes]
        if explicit and all(explicit):
            sources[shard] = INITIAL_STATE_FILE
        elif explicit and not any(explicit) and all(_legacy_empty_initial_state_proven(node, shard) for node in nodes):
            sources[shard] = "legacy_committed_chain_empty_root_compat_v2041"
        else:
            sources[shard] = "unavailable"
    return sources


def _load_initial_business_states(groups: dict[str, list[Path]]) -> tuple[dict[str, dict[str, str]], dict[str, str], list[str]]:
    """Load the actual pre-runtime business-state projection.

    V20.4+ requires the explicit initial business-state artifact for real runs.
    For historical pre-v20.4 fixtures only, an empty start may be inferred when
    *every* replica in a shard proves that its first committed block started from
    the canonical EMPTY_STATE_ROOT. This compatibility path cannot prove or mask
    a non-empty initial state.
    """
    initial: dict[str, dict[str, str]] = {}
    digests: dict[str, str] = {}
    blockers: list[str] = []
    for shard, nodes in sorted(groups.items()):
        explicit_flags = [(node / INITIAL_STATE_FILE).is_file() for node in nodes]
        if explicit_flags and not any(explicit_flags):
            if all(_legacy_empty_initial_state_proven(node, shard) for node in nodes):
                empty: dict[str, str] = {}
                initial[shard] = empty
                digests[shard] = _business_state_digest(empty)
                continue
            for node in sorted(nodes):
                blockers.append(f"txallo_stateless_initial_business_state_missing:{node.name}")
            blockers.append(f"txallo_stateless_initial_business_state_unavailable:{shard}")
            continue
        if explicit_flags and not all(explicit_flags):
            for node, present in zip(nodes, explicit_flags):
                if not present:
                    blockers.append(f"txallo_stateless_initial_business_state_missing:{node.name}")
            blockers.append(f"txallo_stateless_initial_business_state_replica_evidence_incomplete:{shard}")
            continue

        candidates: list[dict[str, str]] = []
        candidate_digests: list[str] = []
        for node in sorted(nodes):
            path = node / INITIAL_STATE_FILE
            row = _read_json(path)
            state = row.get("business_state")
            if not isinstance(state, dict):
                blockers.append(f"txallo_stateless_initial_business_state_missing:{node.name}")
                continue
            normalized: dict[str, str] = {}
            for raw_key, raw_value in state.items():
                key = str(raw_key)
                if "::" not in key:
                    key = f"{shard}::{key}"
                normalized[key] = str(raw_value)
            digest = _business_state_digest(normalized)
            declared = str(row.get("business_state_digest") or "").strip()
            if declared and declared != digest:
                blockers.append(f"txallo_stateless_initial_business_digest_mismatch:{node.name}")
            declared_shard = str(row.get("shard_id") or "").strip()
            if declared_shard and declared_shard != shard:
                blockers.append(f"txallo_stateless_initial_business_shard_mismatch:{node.name}:{declared_shard}:{shard}")
            candidates.append(normalized)
            candidate_digests.append(digest)
        if not candidates:
            blockers.append(f"txallo_stateless_initial_business_state_unavailable:{shard}")
            continue
        if any(candidate != candidates[0] for candidate in candidates[1:]):
            blockers.append(f"txallo_stateless_initial_business_state_replica_mismatch:{shard}")
        if any(digest != candidate_digests[0] for digest in candidate_digests[1:]):
            blockers.append(f"txallo_stateless_initial_business_digest_replica_mismatch:{shard}")
        initial[shard] = dict(candidates[0])
        digests[shard] = candidate_digests[0]
    return initial, digests, blockers

def _owner_orders(groups: dict[str, list[Path]]) -> tuple[dict[str, list[str]], list[str], int]:
    orders: dict[str, list[str]] = {}
    blockers: list[str] = []
    repeated = 0
    for shard, nodes in sorted(groups.items()):
        candidates: list[list[str]] = []
        signatures: list[list[tuple[int, str, int]]] = []
        for node in sorted(nodes):
            order, signature, repeats, errs = _committed_tx_order(node)
            blockers.extend(errs)
            candidates.append(order)
            signatures.append(signature)
            repeated += repeats
        if candidates and any(x != candidates[0] for x in candidates[1:]):
            blockers.append(f"txallo_stateless_owner_order_replica_mismatch:{shard}")
        if signatures and any(x != signatures[0] for x in signatures[1:]):
            blockers.append(f"txallo_stateless_owner_chain_replica_mismatch:{shard}")
        orders[shard] = candidates[0] if candidates else []
    return orders, blockers, repeated


def _graph(entries: dict[str, dict[str, Any]], owner_orders: dict[str, list[str]]) -> tuple[dict[str, set[str]], dict[str, int], list[str]]:
    edges = {txid: set() for txid in entries}
    indegree = {txid: 0 for txid in entries}
    blockers: list[str] = []
    observed: set[str] = set()
    for shard, order in owner_orders.items():
        previous = ""
        for txid in order:
            if txid not in entries:
                blockers.append(f"txallo_stateless_executed_tx_missing_access_evidence:{txid}")
                continue
            observed.add(txid)
            if entries[txid].get("execution_shard") != shard:
                blockers.append(f"txallo_stateless_execution_owner_mismatch:{txid}:{shard}")
            if previous and txid not in edges[previous]:
                edges[previous].add(txid)
                indegree[txid] += 1
            previous = txid
    if observed != set(entries):
        blockers.append(f"txallo_stateless_execution_trace_missing_transactions:{len(set(entries) - observed)}")
    producer: dict[tuple[str, int], str] = {}
    for txid, entry in entries.items():
        for dep in entry.get("state_versions") or []:
            if not isinstance(dep, dict):
                blockers.append(f"txallo_stateless_state_version_invalid:{txid}")
                continue
            key = str(dep.get("key") or "").strip()
            try:
                produced = int(dep.get("produced_version") or 0)
            except (TypeError, ValueError):
                produced = 0
            if key and produced > 0:
                k = (key, produced)
                if k in producer and producer[k] != txid:
                    blockers.append(f"txallo_stateless_duplicate_version_producer:{key}:{produced}")
                producer[k] = txid
    for txid, entry in entries.items():
        for dep in entry.get("state_versions") or []:
            if not isinstance(dep, dict):
                continue
            key = str(dep.get("key") or "").strip()
            try:
                required = int(dep.get("required_version") or 0)
            except (TypeError, ValueError):
                required = 0
            if not key or required <= 0:
                continue
            pred = producer.get((key, required))
            if not pred:
                blockers.append(f"txallo_stateless_required_version_producer_missing:{key}:{required}:{txid}")
                continue
            if pred != txid and txid not in edges[pred]:
                edges[pred].add(txid)
                indegree[txid] += 1
    return edges, indegree, blockers


def _topological(entries: dict[str, dict[str, Any]], edges: dict[str, set[str]], indegree: dict[str, int]) -> tuple[list[str], list[str]]:
    ready = sorted((int(entries[t]["index"]), t) for t, d in indegree.items() if d == 0)
    order: list[str] = []
    while ready:
        _, txid = ready.pop(0)
        order.append(txid)
        for nxt in sorted(edges[txid], key=lambda t: (int(entries[t]["index"]), t)):
            indegree[nxt] -= 1
            if indegree[nxt] == 0:
                ready.append((int(entries[nxt]["index"]), nxt))
                ready.sort()
    if len(order) == len(entries):
        return order, []
    return order, [f"txallo_stateless_dependency_cycle_or_unresolved:{len(entries) - len(order)}"]


def _replay(entries: dict[str, dict[str, Any]], order: list[str], homes: dict[str, str], initial_by_shard: dict[str, dict[str, str]]) -> tuple[dict[str, dict[str, str]], list[str]]:
    states: dict[str, dict[str, str]] = defaultdict(dict)
    for shard, initial in initial_by_shard.items():
        states[shard] = dict(initial)
    blockers: list[str] = []
    for txid in order:
        entry = entries[txid]
        logical_id = str(entry.get("logical_id") or txid)
        for access in entry.get("access_list") or []:
            if not isinstance(access, dict):
                blockers.append(f"txallo_stateless_access_invalid:{txid}")
                continue
            key = str(access.get("key") or "").strip()
            mode = str(access.get("mode") or "").strip()
            semantics = str(access.get("update_semantics") or "").strip()
            if not key:
                blockers.append(f"txallo_stateless_access_key_missing:{txid}")
                continue
            home = homes.get(key)
            if not home:
                blockers.append(f"txallo_stateless_state_home_missing:{key}")
                continue
            qualified = f"{home}::{key}"
            state = states[home]
            if mode == "read":
                continue
            if mode == "read_write":
                state[qualified] = _stable_direct_access_value(
                    logical_tx_id=logical_id,
                    key=key,
                    semantics=semantics,
                    previous=state.get(qualified, ""),
                )
            elif mode == "write":
                state[qualified] = _stable_direct_access_value(
                    logical_tx_id=logical_id,
                    key=key,
                    semantics=semantics,
                    previous="",
                )
            elif mode == "commutative_delta":
                try:
                    state[qualified] = str(int(state.get(qualified, "") or "0") + int(access.get("delta") or 0))
                except (TypeError, ValueError):
                    blockers.append(f"txallo_stateless_non_integer_commutative_value:{key}")
            else:
                blockers.append(f"txallo_stateless_unsupported_access_mode:{mode}:{key}")
    return dict(states), blockers


def evaluate(run_dir: Path, summary: dict[str, Any]) -> dict[str, Any]:
    blockers: list[str] = []
    entries, input_digest, errs = _load_entries(run_dir / "client" / "resolved_access_lists.jsonl.gz")
    blockers.extend(errs)
    homes, errs = _load_home_map(run_dir / "client" / "placement_plan.csv")
    blockers.extend(errs)
    groups, actual_by_shard, errs = _node_groups(run_dir)
    blockers.extend(errs)
    initial_by_shard, initial_digests, errs = _load_initial_business_states(groups)
    blockers.extend(errs)
    initial_sources = _initial_state_source_labels(groups)
    owner_orders, errs, repeated = _owner_orders(groups)
    blockers.extend(errs)
    edges, indegree, errs = _graph(entries, owner_orders)
    blockers.extend(errs)
    order, errs = _topological(entries, edges, indegree)
    blockers.extend(errs)
    replay_by_shard, errs = _replay(entries, order, homes, initial_by_shard)
    blockers.extend(errs)
    for shard in groups:
        replay_by_shard.setdefault(shard, dict(initial_by_shard.get(shard, {})))
    replay_digests = {shard: _business_state_digest(state) for shard, state in sorted(replay_by_shard.items())}
    all_shards = sorted(set(groups) | set(replay_digests) | set(actual_by_shard))
    for shard in all_shards:
        if replay_digests.get(shard) != actual_by_shard.get(shard):
            blockers.append(f"txallo_stateless_partition_business_digest_mismatch:{shard}")
    replay_global = _canonical_digest(dict(sorted(replay_digests.items()))) if replay_digests and len(replay_digests) == len(all_shards) else ""
    actual_global = _canonical_digest(dict(sorted(actual_by_shard.items()))) if actual_by_shard and len(actual_by_shard) == len(all_shards) else ""
    summary_global = str(summary.get("global_business_state_digest") or "").strip()
    if actual_global and summary_global and actual_global != summary_global:
        blockers.append("txallo_stateless_summary_global_business_digest_mismatch")
    if replay_global and actual_global and replay_global != actual_global:
        blockers.append("txallo_stateless_global_business_digest_mismatch")
    if not entries:
        blockers.append("txallo_stateless_no_transactions")
    blockers = sorted(set(blockers))
    valid = not blockers
    logical_order = [str(entries[t].get("logical_id") or t) for t in order if t in entries]
    initial_empty = bool(initial_by_shard) and all(not state for state in initial_by_shard.values())
    return {
        "serial_order_oracle_schema": SCHEMA_VERSION,
        "serial_order_oracle_status": "passed" if valid else "failed",
        "serial_order_replay_applicable": True,
        "serial_order_replay_not_applicable_reason": "",
        "serial_order_replay_equivalent": valid,
        "serial_order_replay_blockers": blockers,
        "serial_order_replay_structural_blockers": blockers,
        "serial_order_replay_supported_scope": SUPPORTED_SCOPE,
        "serial_order_replay_identity_basis": "signed_tx_id_plus_logical_id_plus_signed_state_versions",
        "serial_order_replay_order_basis": "per_execution_shard_pbft_order_plus_exact_required_version_edges_topological_serialization",
        "serial_order_replay_initial_state_empty": initial_empty,
        "serial_order_replay_initial_state_root": "business_projection_v204",
        "serial_order_replay_initial_state_sources": {shard: initial_sources.get(shard, "unavailable") for shard in sorted(initial_by_shard)},
        "serial_order_replay_initial_business_state_digests": initial_digests,
        "serial_order_replay_transaction_count": len(order),
        "serial_order_replay_unique_transaction_count": len(set(order)),
        "serial_order_replay_trace_reexecution_count": repeated,
        "serial_order_replay_input_digest": input_digest,
        "serial_order_replay_commit_order_digest": _canonical_digest(logical_order) if logical_order else "",
        "serial_order_replay_tx_id_order_digest": _canonical_digest(order) if order else "",
        "serial_order_replay_business_state_digest": replay_global,
        "serial_order_actual_business_state_digest": actual_global,
        "serial_order_replay_global_business_state_digest": replay_global,
        "serial_order_actual_global_business_state_digest": actual_global,
        "serial_order_replay_business_key_count": sum(len(x) for x in replay_by_shard.values()),
        "serial_order_replay_replica_order_consistent": not any("replica_mismatch" in x for x in blockers),
        "serial_order_replay_replica_count": sum(len(x) for x in groups.values()),
        "serial_order_replay_reference_node": ",".join(sorted(nodes[0].name for nodes in groups.values() if nodes)),
        "txallo_stateless_replay_partition_business_digests": replay_digests,
        "txallo_stateless_actual_partition_business_digests": actual_by_shard,
        "txallo_stateless_initial_partition_business_digests": initial_digests,
        "txallo_stateless_exact_version_edge_count": sum(len(x) for x in edges.values()),
        "method_correctness_oracle_kind": "stateless_txallo_multi_shard_exact_version_serial_replay_v2",
        "method_correctness_oracle_status": "passed" if valid else "failed",
        "method_correctness_oracle_valid": valid,
        "method_correctness_oracle_blockers": blockers,
        "method_correctness_oracle_scope": SUPPORTED_SCOPE,
    }
