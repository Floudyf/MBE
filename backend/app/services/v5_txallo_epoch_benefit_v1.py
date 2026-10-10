"""TxAllo committed-epoch mapping benefit audit; read-only, no routing changes.

Uses the original placement.csv and immutable mapping epoch history.  The
counterfactual comparisons use the same workload endpoints and the paper-adapter
fallback function: stableKey(["txallo_account:" + logicalAccount]) % shardCount.
"""
from __future__ import annotations

import argparse
from collections import Counter, defaultdict
import json
from pathlib import Path
from typing import Any

from backend.app.services import v5_txallo_mapping_epoch_v229 as coherence

SCHEMA = "mbe_txallo_epoch_benefit_v1"


def _account(v: Any) -> str:
    return str(v or "").strip().lower()


def _fallback(account: str, shards: list[str]) -> str:
    if not shards:
        raise ValueError("shard inventory must not be empty")
    return shards[sum(ord(c) for c in "txallo_account:" + account) % len(shards)]


def _shards(rows: list[dict[str, str]], epochs: dict[int, dict[str, Any]], explicit: list[str] | None) -> list[str]:
    observed = {s for row in rows for s in (row.get("sender_shard"), row.get("receiver_shard")) if s}
    observed |= {s for ep in epochs.values() for s in ep["mapping"].values() if s}
    if explicit:
        shards = sorted(set(explicit))
        if not observed.issubset(shards):
            raise ValueError("declared shard inventory excludes a placement/mapping shard")
    else:
        shards = sorted(observed)
        # With only the observed shards we cannot prove the source runtime had no
        # unused shards. Only compare against inferred inventory if the caller
        # explicitly accepts this limitation via --allow-inferred-shards.
    if not shards:
        raise ValueError("no execution shards identified")
    return shards


def _choose(mapping: dict[str, str], account: str, shards: list[str]) -> str:
    return mapping.get(account) or _fallback(account, shards)


def audit_run(run_dir: Path | str, shard_ids: list[str] | None = None, *,
              allow_inferred_shards: bool = False) -> dict[str, Any]:
    root = Path(run_dir)
    path = coherence._placement_path(root)
    epochs, history_source, errors = coherence._load_epochs(root)
    blockers = list(errors)
    if path is None or not epochs:
        return {"schema_version": SCHEMA, "passed": False,
                "blockers": blockers + ["missing_mapping_epochs_or_transaction_placement"]}
    rows = coherence._rows(path)
    if not rows:
        return {"schema_version": SCHEMA, "passed": False, "blockers": blockers + ["empty_placement"]}
    if 0 not in epochs:
        blockers.append("missing_initial_G_mapping_epoch_0")
    required = {"tx_index", "routing_epoch", "sender_account", "receiver_account",
                "sender_mapping_source", "receiver_mapping_source", "sender_shard",
                "receiver_shard", "mapping_state_digest", "txallo_cross_shard"}
    if required - set(rows[0]):
        blockers.append("placement_missing_columns:" + ",".join(sorted(required-set(rows[0]))))
    try:
        shards = _shards(rows, epochs, shard_ids)
    except ValueError as exc:
        blockers.append(str(exc))
        shards = []
    if shard_ids is None and not allow_inferred_shards:
        blockers.append("explicit_shard_inventory_required_for_counterfactual")
    if blockers:
        return {"schema_version": SCHEMA, "passed": False, "blockers": blockers}
    indexed: list[tuple[int, int, dict[str, str]]] = []
    for pos, row in enumerate(rows):
        try:
            idx = int(row["tx_index"])
            epoch = int(row["routing_epoch"])
        except (ValueError, TypeError, KeyError):
            blockers.append(f"invalid_tx_index_or_routing_epoch_row_{pos}")
            continue
        if idx < 0 or epoch < 0:
            blockers.append(f"negative_tx_index_or_epoch_row_{pos}")
        indexed.append((idx, epoch, row))
    if blockers:
        return {"schema_version": SCHEMA, "passed": False, "blockers": blockers}
    indexed.sort(key=lambda x: x[0])
    if len({i for i, _, _ in indexed}) != len(indexed):
        blockers.append("duplicate_tx_index")
    if any(indexed[i][1] > indexed[i+1][1] for i in range(len(indexed)-1)):
        blockers.append("routing_epochs_not_monotone_with_tx_order")

    ep_stats = defaultdict(lambda: {
        "transaction_count": 0, "actual_cross_shard_count": 0,
        "initial_g_frozen_cross_shard_count": 0, "hash_only_cross_shard_count": 0,
        "mapped_endpoint_occurrences": 0, "fallback_endpoint_occurrences": 0,
        "new_endpoint_occurrences": 0, "seen_endpoint_occurrences": 0,
        "new_unique_accounts": set(), "used_unique_accounts": set(),
        "mapped_unique_accounts": set(), "fallback_unique_accounts": set(),
    })
    seen_accounts: set[str] = set()
    first_used_at: dict[str, int] = {}
    used_epochs: dict[str, set[int]] = defaultdict(set)
    edges: set[tuple[str, str]] = set()
    neighbors: dict[str, set[str]] = defaultdict(set)
    incidence: Counter[str] = Counter()
    endpoint_occurrences: Counter[str] = Counter()
    first_g = epochs[0]["mapping"]
    for idx, epoch, row in indexed:
        if epoch not in epochs:
            blockers.append(f"row_{idx}_unavailable_mapping_epoch_{epoch}")
            continue
        snap = epochs[epoch]
        if row["mapping_state_digest"] != snap["state_digest"]:
            blockers.append(f"row_{idx}_mapping_state_digest_mismatch")
            continue
        curr = snap["mapping"]
        sender = _account(row["sender_account"])
        receiver = _account(row["receiver_account"])
        if not sender:
            blockers.append(f"row_{idx}_missing_sender")
            continue
        cur_sender = _choose(curr, sender, shards)
        cur_receiver = _choose(curr, receiver, shards) if receiver else cur_sender
        if (cur_sender, cur_receiver) != (row["sender_shard"], row["receiver_shard"]):
            blockers.append(f"row_{idx}_placement_not_matching_active_epoch")
            continue
        actual_cross = int(cur_sender != cur_receiver)
        if _account(row["txallo_cross_shard"]) not in ("true", "false"):
            blockers.append(f"row_{idx}_invalid_cross_flag")
        elif actual_cross != int(_account(row["txallo_cross_shard"]) == "true"):
            blockers.append(f"row_{idx}_reported_cross_mismatch")
        stat = ep_stats[epoch]
        stat["transaction_count"] += 1
        stat["actual_cross_shard_count"] += actual_cross
        stat["initial_g_frozen_cross_shard_count"] += int(_choose(first_g, sender, shards) != (_choose(first_g, receiver, shards) if receiver else _choose(first_g, sender, shards)))
        stat["hash_only_cross_shard_count"] += int(_fallback(sender, shards) != (_fallback(receiver, shards) if receiver else _fallback(sender, shards)))
        for role, account in (("sender", sender), ("receiver", receiver)):
            if not account:
                if role == "receiver" and _account(row["receiver_mapping_source"]) == "none":
                    continue
                blockers.append(f"row_{idx}_{role}_missing_account")
                continue
            source = _account(row[f"{role}_mapping_source"])
            expected = "history_mapping" if account in curr else "fallback_hash"
            if source != expected:
                blockers.append(f"row_{idx}_{role}_mapping_source_mismatch")
            stat["used_unique_accounts"].add(account)
            endpoint_occurrences[account] += 1
            used_epochs[account].add(epoch)
            if source == "history_mapping":
                stat["mapped_endpoint_occurrences"] += 1
                stat["mapped_unique_accounts"].add(account)
            elif source == "fallback_hash":
                stat["fallback_endpoint_occurrences"] += 1
                stat["fallback_unique_accounts"].add(account)
            if account not in seen_accounts:
                stat["new_endpoint_occurrences"] += 1
                stat["new_unique_accounts"].add(account)
                first_used_at[account] = epoch
                seen_accounts.add(account)
            else:
                stat["seen_endpoint_occurrences"] += 1
        if receiver:
            edge = tuple(sorted((sender, receiver)))
            edges.add(edge)
            incidence[sender] += 1
            if sender != receiver:
                incidence[receiver] += 1
                neighbors[sender].add(receiver)
                neighbors[receiver].add(sender)
    if blockers:
        return {"schema_version": SCHEMA, "passed": False, "blockers": blockers[:100],
                "checked_transaction_count": len(indexed), "invalid_count": len(blockers)}

    # An epoch snapshot E is published AFTER its prior source epoch was committed.
    # Accounts mapped newly at E cannot benefit any transaction routed before E.
    changes: list[dict[str, Any]] = []
    for e in sorted(epochs):
        prev = epochs.get(e-1, {"mapping": {}})["mapping"] if e else {}
        mapping = epochs[e]["mapping"]
        added = set(mapping) - set(prev)
        moved = {a for a in set(prev) & set(mapping) if mapping[a] != prev[a]}
        next_use = {a for a in added if e in used_epochs.get(a, set())}
        future_use = {a for a in added if any(x >= e for x in used_epochs.get(a, set()))}
        changes.append({
            "mapping_epoch": e,
            "newly_mapped_account_count": len(added),
            "changed_existing_account_count": len(moved),
            "newly_mapped_used_during_epoch_count": len(next_use),
            "newly_mapped_used_during_or_after_epoch_count": len(future_use),
            "added_mapping_accounts_from_prior_evaluation_count": sum(1 for a in added if first_used_at.get(a, e+1) < e),
        })
    def with_ratio(count: int, total: int) -> float | None:
        return round(count/total, 8) if total else None
    epoch_rows = []
    for e in sorted(ep_stats):
        s = ep_stats[e]
        n = s["transaction_count"]
        epoch_rows.append({
            "routing_epoch": e,
            **{k: s[k] for k in (
                "transaction_count", "actual_cross_shard_count", "initial_g_frozen_cross_shard_count",
                "hash_only_cross_shard_count", "mapped_endpoint_occurrences",
                "fallback_endpoint_occurrences", "new_endpoint_occurrences", "seen_endpoint_occurrences")},
            "new_unique_account_count": len(s["new_unique_accounts"]),
            "unique_account_count": len(s["used_unique_accounts"]),
            "mapped_unique_account_count": len(s["mapped_unique_accounts"]),
            "fallback_unique_account_count": len(s["fallback_unique_accounts"]),
            "actual_cross_shard_ratio": with_ratio(s["actual_cross_shard_count"], n),
            "initial_g_frozen_cross_shard_ratio": with_ratio(s["initial_g_frozen_cross_shard_count"], n),
            "hash_only_cross_shard_ratio": with_ratio(s["hash_only_cross_shard_count"], n),
        })
    total = sum(s["transaction_count"] for s in ep_stats.values())
    act = sum(s["actual_cross_shard_count"] for s in ep_stats.values())
    frozen = sum(s["initial_g_frozen_cross_shard_count"] for s in ep_stats.values())
    hashed = sum(s["hash_only_cross_shard_count"] for s in ep_stats.values())
    distinct_degree = {a:len(neighbors[a]) for a in seen_accounts}
    degree_hist = Counter(distinct_degree.values())
    top = sorted(distinct_degree.items(), key=lambda x: (-x[1],x[0]))[:5]
    graph_nodes = set(seen_accounts)
    return {
        "schema_version": SCHEMA, "passed": True,
        "audit_mode": "read_only_offline_counterfactual_no_algorithm_change",
        "fallback_semantics": "Go_stableKey_unicode_codepoint_sum_txallo_account_prefix",
        "shard_inventory_source": "explicit" if shard_ids is not None else "inferred_unverified",
        "shards": shards,
        "transaction_count": total,
        "unique_evaluation_account_count": len(seen_accounts),
        "accounts_seen_in_multiple_mapping_epochs": sum(1 for x in used_epochs.values() if len(x)>1),
        "accounts_seen_only_once_in_evaluation": sum(1 for a in seen_accounts if endpoint_occurrences[a]==1),
        "accounts_seen_in_only_one_mapping_epoch": sum(1 for x in used_epochs.values() if len(x)==1),
        "actual_cross_shard_ratio": with_ratio(act,total),
        "initial_g_frozen_cross_shard_ratio": with_ratio(frozen,total),
        "hash_only_cross_shard_ratio": with_ratio(hashed,total),
        "delta_actual_minus_frozen_cross_shard_ratio": with_ratio(act-frozen,total),
        "delta_actual_minus_hash_cross_shard_ratio": with_ratio(act-hashed,total),
        "epoch_rows": epoch_rows,
        "mapping_changes": changes,
        "evaluation_graph": {"node_count": len(graph_nodes), "distinct_edge_count": len(edges),
                             "degree_one_count": degree_hist[1],
                             "max_degree": top[0][1] if top else 0,
                             "top_degree_accounts": [{"account": a, "distinct_neighbors": d, "transaction_incidence": incidence[a]} for a,d in top]},
        "mapping_epoch_history_source": history_source,
        "transaction_placement_source": str(path.relative_to(root)).replace("\\", "/"),
        "limitations": ["routing_epoch is a mapping-version epoch, not necessarily a contiguous source-block epoch",
                        "cross-shard ratios are offline placement estimates, not PBFT or E2E TPS",
                        "mapping updates cannot retroactively affect previously routed transactions"],
    }


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--run-dir", required=True)
    ap.add_argument("--shards", help="exact ordered physical execution-shard IDs, comma-separated; e.g. s0,s1")
    ap.add_argument("--allow-inferred-shards", action="store_true", help="counterfactual shard inventory unverified")
    ap.add_argument("--output", help="save JSON result outside the source tree")
    args = ap.parse_args()
    declared = [s.strip() for s in args.shards.split(",") if s.strip()] if args.shards else None
    report = audit_run(args.run_dir, declared, allow_inferred_shards=args.allow_inferred_shards)
    raw = json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True)
    if args.output:
        dest = Path(args.output)
        dest.parent.mkdir(parents=True, exist_ok=True)
        dest.write_text(raw+"\n", encoding="utf-8")
    print(raw)
    return 0 if report["passed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
