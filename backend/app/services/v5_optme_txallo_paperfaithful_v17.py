from __future__ import annotations

import csv
import hashlib
import json
from pathlib import Path
from typing import Any, Iterable

from backend.app.services import v5_optme_txallo_fidelity_closure as v16

CLOSURE_VERSION = "mbe_optme_txallo_paperfaithful_v17"
WRITEBACK_REPLICATION_CONTRACT = "home_all_replicas_failover_preserving_v1"
TXALLO_PAPER_REFERENCE = "TxAllo_ICDE2023_arXiv_2212.11584"
OPTME_PAPER_REFERENCE = "OptME_SC24_pap497"


def _nonempty(value: Any) -> bool:
    return value not in (None, "", [], {}, ())


def _as_int(value: Any) -> int | None:
    try:
        if value in (None, ""):
            return None
        return int(float(value))
    except Exception:
        return None


def _as_float(value: Any) -> float | None:
    try:
        if value in (None, ""):
            return None
        return float(value)
    except Exception:
        return None


def _sha(value: Any) -> str:
    raw = json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(raw).hexdigest()


def _read_json(path: Path) -> Any:
    try:
        return json.loads(path.read_text(encoding="utf-8-sig"))
    except Exception:
        return None


def _read_csv(path: Path) -> list[dict[str, str]]:
    try:
        with path.open("r", encoding="utf-8-sig", newline="") as handle:
            return [dict(row) for row in csv.DictReader(handle)]
    except Exception:
        return []


def _method_id(value: Any, fallback: dict[str, Any] | None = None) -> str:
    if isinstance(value, dict):
        for key in ("method_id", "id"):
            if _nonempty(value.get(key)):
                return str(value[key])
    if isinstance(value, str) and value:
        return value
    source = fallback or {}
    for key in ("method_id", "method_config_id", "method_name"):
        if _nonempty(source.get(key)):
            return str(source[key])
    return ""


def _is_target(method: str) -> bool:
    return method.lower() in {"stateful_optme", "stateless_optme", "stateful_txallo", "stateless_txallo"}


def _is_stateless(method: str) -> bool:
    return method.lower().startswith("stateless_")


def _is_txallo(method: str) -> bool:
    return "txallo" in method.lower()


def _is_optme(method: str) -> bool:
    return "optme" in method.lower()


def _semantic_class(method: str) -> str:
    return "stateless_global_home" if _is_stateless(method) else "stateful_local_partition"


def _candidate_files(root: Path, exact: Iterable[str], patterns: Iterable[str]) -> list[Path]:
    out: list[Path] = []
    seen: set[str] = set()
    for rel in exact:
        p = root / rel
        if p.is_file():
            out.append(p)
            seen.add(str(p.resolve()))
    for pattern in patterns:
        for p in root.rglob(pattern):
            if not p.is_file():
                continue
            key = str(p.resolve())
            if key in seen:
                continue
            out.append(p)
            seen.add(key)
            if len(out) >= 128:
                return out
    return out


def compute_business_state_evidence_v17(run_dir: Path | str, method_id: str, metrics: dict[str, Any]) -> dict[str, Any]:
    """Separate storage-partition truth from global logical-state truth.

    Stateful-local MBE semantics preserve shard namespace in the storage identity.  Stateless
    variants use a global Home/state identity, so a placement-independent logical commitment is
    meaningful there.  V17 never collapses the two semantics into one digest.
    """
    root = Path(run_dir)
    semantic = _semantic_class(method_id)
    paths = sorted(root.glob("nodes/*/business_state_commitments_v17.json"))
    if not paths:
        # V16 artifacts cannot recover partition keys because they intentionally discarded them.
        legacy = metrics.get("global_business_state_digest") or metrics.get("v16_legacy_global_business_state_digest")
        return {
            "status": "legacy_v16_or_older",
            "semantic_class": semantic,
            "storage_partition_digest": legacy if semantic == "stateful_local_partition" and _nonempty(legacy) else None,
            "global_logical_business_digest": metrics.get("v16_logical_business_state_digest") if semantic == "stateless_global_home" else None,
            "global_logical_conflict_key_count": metrics.get("v16_logical_business_state_conflict_key_count") if semantic == "stateless_global_home" else None,
            "replica_consistent": metrics.get("v16_logical_business_state_replica_consistent"),
            "source_files": [],
            "truth_boundary": "legacy_evidence_not_reinterpreted_as_cross_semantic_truth",
        }

    by_shard: dict[str, list[tuple[set[tuple[str, str]], set[tuple[str, str]]]]] = {}
    malformed: list[str] = []
    files: list[str] = []
    for p in paths:
        data = _read_json(p)
        if not isinstance(data, dict) or not isinstance(data.get("commitments"), list):
            malformed.append(str(p.relative_to(root)).replace("\\", "/")); continue
        shard = str(data.get("shard_id") or "").strip()
        if not shard:
            malformed.append(str(p.relative_to(root)).replace("\\", "/")); continue
        partition_pairs: set[tuple[str, str]] = set()
        logical_pairs: set[tuple[str, str]] = set()
        ok = True
        for row in data["commitments"]:
            if not isinstance(row, dict): ok = False; break
            pk = str(row.get("partition_key_digest") or "").strip()
            lk = str(row.get("logical_key_digest") or "").strip()
            vv = str(row.get("value_digest") or "").strip()
            if not pk or not lk or not vv: ok = False; break
            partition_pairs.add((pk, vv)); logical_pairs.add((lk, vv))
        if not ok:
            malformed.append(str(p.relative_to(root)).replace("\\", "/")); continue
        files.append(str(p.relative_to(root)).replace("\\", "/"))
        by_shard.setdefault(shard, []).append((partition_pairs, logical_pairs))
    if malformed or not by_shard:
        return {"status": "malformed", "semantic_class": semantic, "malformed_files": malformed, "source_files": files}

    replica_mismatch: list[str] = []
    agreed_partition: dict[str, set[tuple[str, str]]] = {}
    agreed_logical: dict[str, set[tuple[str, str]]] = {}
    for shard, replicas in sorted(by_shard.items()):
        p0, l0 = replicas[0]
        if any(p != p0 or l != l0 for p, l in replicas[1:]):
            replica_mismatch.append(shard); continue
        agreed_partition[shard] = p0; agreed_logical[shard] = l0
    if replica_mismatch:
        return {
            "status": "replica_inconsistent", "semantic_class": semantic,
            "replica_consistent": False, "replica_mismatch_shards": replica_mismatch,
            "source_files": files,
        }

    partition_rows: list[tuple[str, str, str]] = []
    for shard, pairs in agreed_partition.items():
        for key, value in pairs:
            partition_rows.append((shard, key, value))
    storage_digest = _sha(sorted(partition_rows))

    logical_pairs: set[tuple[str, str]] = set()
    for pairs in agreed_logical.values(): logical_pairs.update(pairs)
    values_by_key: dict[str, set[str]] = {}
    for key, value in logical_pairs: values_by_key.setdefault(key, set()).add(value)
    conflicts = sorted(k for k, vals in values_by_key.items() if len(vals) > 1)
    global_digest = _sha(sorted(logical_pairs)) if not conflicts else None

    return {
        "status": "available",
        "semantic_class": semantic,
        "storage_partition_digest": storage_digest,
        "global_logical_business_digest": global_digest if semantic == "stateless_global_home" else None,
        "global_logical_business_projection_status": (
            "available" if semantic == "stateless_global_home" and not conflicts
            else "conflicting_values" if semantic == "stateless_global_home"
            else "not_applicable_without_explicit_stateful_global_projection"
        ),
        "global_logical_conflict_key_count": len(conflicts),
        "global_logical_conflict_key_digests": conflicts[:32],
        "replica_consistent": True,
        "shard_count": len(agreed_partition),
        "source_files": sorted(files),
        "truth_boundary": "partition_aware_storage_plus_stateless_global_projection_v17",
    }


def _mapping_rows(run_dir: Path) -> tuple[dict[str, str], str | None]:
    files = _candidate_files(run_dir,
        ("client/txallo_account_mapping.csv", "txallo_account_mapping.csv", "aggregate/txallo_account_mapping.csv"),
        ("*txallo*account*mapping*.csv",))
    if not files: return {}, None
    out: dict[str, str] = {}
    for row in _read_csv(files[0]):
        low = {str(k).lower(): "" if v is None else str(v).strip() for k,v in row.items()}
        account = low.get("account") or low.get("account_id") or low.get("address") or low.get("node")
        shard = low.get("shard") or low.get("shard_id") or low.get("community") or low.get("home_shard")
        if account and shard: out[account] = shard
    return out, str(files[0].relative_to(run_dir)).replace("\\", "/")


def _split_accounts(value: str) -> list[str]:
    if not value: return []
    value = value.strip()
    if value.startswith("["):
        try:
            raw = json.loads(value)
            if isinstance(raw, list): return [str(x) for x in raw if str(x)]
        except Exception: pass
    for sep in ("|", ";", ","):
        if sep in value: return [x.strip() for x in value.split(sep) if x.strip()]
    return [value]


def compute_txallo_routing_coherence_v17(run_dir: Path | str) -> dict[str, Any]:
    """Audit frozen-history mapping plus explicitly declared causal fallback.

    v20.2+ placement artifacts export canonical logical accounts and mapping
    provenance. Historical v17 artifacts used sender/receiver without provenance;
    those remain readable under the original mapping-induced placement contract.
    """
    # MBE_TXALLO_REPRO_V202_ROUTING
    root = Path(run_dir)
    account_map, map_source = _mapping_rows(root)
    if not account_map:
        return {"status":"missing_account_mapping", "passed":None, "paper_contract":"transaction_shards_derived_from_account_allocation"}
    files = _candidate_files(root, ("client/txallo_transaction_placement.csv", "txallo_transaction_placement.csv"), ("*txallo*transaction*placement*.csv",))
    if not files:
        return {"status":"missing_transaction_placement", "passed":None, "account_mapping_source":map_source, "paper_contract":"transaction_shards_derived_from_account_allocation"}
    checked=0; mismatch=0; unseen=0; history=set(); fallback=set(); examples=[]; new_schema_rows=0; legacy_rows=0
    for row in _read_csv(files[0]):
        low={str(k).lower():"" if v is None else str(v).strip() for k,v in row.items()}
        new_schema = any(k in low for k in ("sender_account","receiver_account","sender_mapping_source","receiver_mapping_source"))
        if new_schema:
            sender=low.get("sender_account",""); receiver=low.get("receiver_account","")
            if not sender and not receiver:
                continue
            new_schema_rows += 1; checked += 1; expected=set()
            for role,account in (("sender",sender),("receiver",receiver)):
                if not account:
                    continue
                source=low.get(f"{role}_mapping_source",""); shard=low.get(f"{role}_shard",""); bad=False
                if source=="history_mapping":
                    history.add(account); bad=account_map.get(account)!=shard
                elif source=="fallback_hash":
                    fallback.add(account); bad=account in account_map or not shard
                else:
                    bad=True
                if shard:
                    expected.add(shard)
                if bad:
                    mismatch += 1
                    if len(examples)<8:
                        examples.append({"role":role,"account":account,"source":source,"shard":shard,"history_shard":account_map.get(account)})
            claimed=set(_split_accounts(low.get("involved_shards","")))
            if claimed != expected:
                mismatch += 1
                if len(examples)<8:
                    examples.append({"logical_id":low.get("logical_id"),"expected_shards":sorted(expected),"claimed_shards":sorted(claimed)})
            continue

        # Historical artifact path: preserve the original v17 mapping audit.
        accounts=[]
        for key in ("accounts", "account_ids", "involved_accounts", "input_accounts", "output_accounts",
                    "sender", "sender_id", "receiver", "receiver_id", "source_account", "target_account"):
            accounts.extend(_split_accounts(low.get(key,"")))
        accounts=list(dict.fromkeys(a for a in accounts if a))
        if not accounts:
            continue
        expected={account_map[a] for a in accounts if a in account_map}
        if any(a not in account_map for a in accounts):
            unseen += 1
        if not expected:
            continue
        legacy_rows += 1; checked += 1
        claimed=set(_split_accounts(low.get("involved_shards","") or low.get("shards","") or low.get("execution_shards","")))
        if claimed and claimed != expected:
            mismatch += 1
            if len(examples)<8:
                examples.append({"accounts":accounts,"expected_shards":sorted(expected),"claimed_shards":sorted(claimed)})
    status = "available_causal_mapping_and_fallback_evidence" if new_schema_rows else ("available" if checked else "placement_rows_lack_account_identity")
    return {"status":status, "checked_transaction_count":checked, "mismatch_count":mismatch if checked else None,
            "unseen_account_transaction_count":unseen if legacy_rows else None,
            "history_mapped_account_count":len(history) if new_schema_rows else None,
            "fallback_account_count":len(fallback) if new_schema_rows else None,
            "new_schema_row_count":new_schema_rows, "legacy_schema_row_count":legacy_rows,
            "passed":mismatch==0 if checked else None, "examples":examples,
            "account_mapping_source":map_source,
            "transaction_placement_source":str(files[0].relative_to(root)).replace("\\","/"),
            "paper_contract":"frozen_history_mapping_plus_causal_unseen_account_fallback_no_future_training" if new_schema_rows else "transaction_involved_shards_are_induced_by_account_allocation_definition_1"}

def compute_txallo_physical_execution_coherence_v17(run_dir: Path | str) -> dict[str, Any]:
    """Compare paper-derived involved shards with observed stateful execution shards.

    This check is deliberately evidence-driven.  It runs only when the TxAllo
    transaction-placement artifact explicitly exports an involved-shard set and
    transaction execution traces expose the same logical transaction IDs.  It
    never derives a missing shard set from legacy SourceShard/CrossShard flags.
    """
    root = Path(run_dir)
    placement_files = _candidate_files(root,
        ("client/txallo_transaction_placement.csv", "txallo_transaction_placement.csv"),
        ("*txallo*transaction*placement*.csv",))
    if not placement_files:
        return {"status":"missing_transaction_placement", "passed":None}
    expected: dict[str, set[str]] = {}
    for row in _read_csv(placement_files[0]):
        low = {str(k).lower(): "" if v is None else str(v).strip() for k,v in row.items()}
        txid = low.get("logical_tx_id") or low.get("logical_id") or low.get("tx_id") or low.get("transaction_id")
        if not txid:
            continue
        raw = low.get("involved_shards") or low.get("shards") or low.get("execution_shards")
        shards = set(_split_accounts(raw))
        if shards:
            expected[txid] = shards
    if not expected:
        return {
            "status":"placement_rows_lack_explicit_involved_shards", "passed":None,
            "transaction_placement_source":str(placement_files[0].relative_to(root)).replace("\\","/"),
        }

    trace_files = _candidate_files(root,
        (), ("nodes/*/transaction_execution_trace.csv", "*stateful_oracle_evidence*/nodes/*/transaction_execution_trace.csv"))
    observed: dict[str, set[str]] = {}
    used: list[str] = []
    for path in trace_files:
        rows = _read_csv(path)
        if not rows:
            continue
        used.append(str(path.relative_to(root)).replace("\\","/"))
        for row in rows:
            low = {str(k).lower(): "" if v is None else str(v).strip() for k,v in row.items()}
            txid = low.get("logical_tx_id") or low.get("logical_id") or low.get("tx_id") or low.get("transaction_id")
            shard = low.get("shard_id") or low.get("shard")
            if txid and shard:
                observed.setdefault(txid, set()).add(shard)
    if not observed:
        return {"status":"missing_transaction_execution_trace", "passed":None}

    checked = 0; mismatch = 0; missing_trace = 0; examples: list[dict[str,Any]] = []
    for txid, want in expected.items():
        got = observed.get(txid)
        if got is None:
            missing_trace += 1
            continue
        checked += 1
        if got != want:
            mismatch += 1
            if len(examples) < 8:
                examples.append({"logical_tx_id":txid,"expected_involved_shards":sorted(want),"observed_execution_shards":sorted(got)})
    return {
        "status":"available" if checked else "no_joinable_transaction_ids",
        "checked_transaction_count":checked,
        "mismatch_count":mismatch if checked else None,
        "missing_trace_transaction_count":missing_trace,
        "passed":(mismatch == 0 and missing_trace == 0) if checked else None,
        "examples":examples,
        "transaction_placement_source":str(placement_files[0].relative_to(root)).replace("\\","/"),
        "execution_trace_sources":sorted(used),
        "paper_contract":"stateful_transaction_execution_shards_match_account_allocation_induced_involved_shards",
        "legacy_source_shard_cross_shard_flags_used":False,
    }

def compute_txallo_paper_audit_v17(run_dir: Path | str, metrics: dict[str, Any]) -> dict[str, Any]:
    evidence = v16.compute_txallo_evidence(run_dir, metrics)
    allocation = evidence.get("account_allocation") or {}
    eta = _as_float(metrics.get("txallo_eta")); lam = _as_float(metrics.get("txallo_lambda")); eps = _as_float(metrics.get("txallo_epsilon"))
    k = None
    # k may not be exported directly; infer only from a concrete topology artifact, never from eta.
    compiled = _read_json(Path(run_dir) / "compiled_run_plan.json")
    if isinstance(compiled, dict):
        topo = compiled.get("topology") or compiled.get("topology_point") or {}
        if isinstance(topo, dict): k = _as_int(topo.get("shards") or topo.get("shard_count"))
    routing = compute_txallo_routing_coherence_v17(run_dir)
    physical = compute_txallo_physical_execution_coherence_v17(run_dir)
    nonempty = _as_int(allocation.get("nonempty_shard_count"))
    return {
        "reference": TXALLO_PAPER_REFERENCE,
        "account_uniqueness_passed": allocation.get("uniqueness_passed"),
        "account_completeness_passed": allocation.get("completeness_against_runtime_count"),
        "account_mapping_digest": allocation.get("digest"),
        "configured_shard_count": k,
        "nonempty_shard_count": nonempty,
        "empty_community_is_not_intrinsically_invalid": True,
        "single_nonempty_shard_diagnostic": bool(k and nonempty == 1 and k > 1),
        "eta": eta, "lambda": lam, "epsilon": eps,
        "parameter_semantics_passed": bool(eta is not None and eta > 1 and lam is not None and lam > 0 and eps is not None and eps > 0),
        "history_source": metrics.get("txallo_history_source"),
        "global_run_count": _as_int(metrics.get("txallo_g_txallo_run_count")),
        "adaptive_run_count": _as_int(metrics.get("txallo_a_txallo_run_count")),
        "routing_coherence": routing,
        "physical_execution_coherence": physical,
        "paper_definition_1_uniqueness_completeness_enforced": True,
        "paper_definition_transaction_allocation_induced_by_account_mapping_enforced": routing.get("passed"),
        "reported_modeled_throughput": _as_float(metrics.get("txallo_modeled_throughput")),
        "reported_modeled_cross_shard_ratio": _as_float(metrics.get("txallo_modeled_cross_shard_ratio")),
        "reported_modeled_workload_stddev": _as_float(metrics.get("txallo_modeled_workload_stddev")),
        "modeled_throughput_is_not_measured_tps": True,
    }


def compute_writeback_fanout_v17(run_dir: Path | str, metrics: dict[str, Any]) -> dict[str, Any]:
    root = Path(run_dir)
    files = _candidate_files(root,
        ("physical_remote_state_operations.csv", "aggregate/replica_deduplicated_remote_operations.csv"),
        ("*physical_remote_state_operations*.csv", "*replica_deduplicated_remote_operations*.csv"))
    required = ("logical_tx_id", "state_key", "produced_version", "home_shard", "update_semantics")
    for p in files:
        rows = []
        for row in _read_csv(p):
            text = " ".join(str(v) for v in row.values()).lower()
            if "state_delta_apply" in text or "write_apply" in text or "versioned_remote_home" in text:
                rows.append({str(k).lower(): "" if v is None else str(v) for k,v in row.items()})
        if not rows: continue
        keys: set[tuple[str,...]] = set(); missing = set()
        for row in rows:
            vals = []
            for field in required:
                value = row.get(field, "")
                if field == "logical_tx_id" and not value:
                    value = row.get("logical_id", "") or row.get("tx_id", "")
                if field == "state_key" and not value: value = row.get("key", "")
                if not value: missing.add(field)
                vals.append(value)
            if all(vals): keys.add(tuple(vals))
        if missing:
            return {"status":"missing_exact_version_identity", "physical_message_count":len(rows),
                    "unique_logical_delta_count":None, "replica_fanout_ratio":None,
                    "missing_identity_fields":sorted(missing), "source":str(p.relative_to(root)).replace("\\","/"),
                    "dedup_key":list(required), "replication_contract":WRITEBACK_REPLICATION_CONTRACT}
        return {"status":"available_exact_logical_dedup", "physical_message_count":len(rows),
                "unique_logical_delta_count":len(keys),
                "replica_fanout_ratio":len(rows)/len(keys) if keys else None,
                "missing_identity_fields":[], "source":str(p.relative_to(root)).replace("\\","/"),
                "dedup_key":list(required), "replication_contract":WRITEBACK_REPLICATION_CONTRACT}
    legacy = v16.compute_writeback_fanout(run_dir, metrics)
    return {**legacy, "dedup_key":list(required), "replication_contract":WRITEBACK_REPLICATION_CONTRACT,
            "status": "summary_only_no_exact_identity" if legacy.get("physical_message_count") is not None else "missing"}


# MBE_OPTME_V20_TX_EVIDENCE: prefer executor-emitted per-transaction truth.
def compute_optme_transaction_evidence_v17(run_dir: Path | str, metrics: dict[str, Any]) -> dict[str, Any]:
    root = Path(run_dir)
    tx: dict[str, dict[str, Any]] = {}
    source_seen = False
    for path in sorted((root / "nodes").glob("*/block_execution_summary.json")):
        payload = _read_json(path)
        if not isinstance(payload, dict) or payload.get("block_executor_id") not in {"optme_block_executor", "stateless_optme_block_executor"}:
            continue
        for block in payload.get("blocks") if isinstance(payload.get("blocks"), list) else []:
            if not isinstance(block, dict):
                continue
            rows = block.get("optme_transaction_evidence")
            if not isinstance(rows, list):
                continue
            source_seen = True
            for row in rows:
                if not isinstance(row, dict):
                    continue
                txid = str(row.get("logical_tx_id") or row.get("tx_id") or "")
                if not txid:
                    continue
                state = tx.setdefault(txid, {
                    "early_detected": False, "hierarchical_aborted": False, "rescheduled": False,
                    "reexecution": False, "reordered": False, "simulation_failed": False,
                    "reexecution_simulation_failed": False, "second_pass_invalidated": False,
                })
                state["early_detected"] |= bool(row.get("early_detected"))
                state["hierarchical_aborted"] |= bool(row.get("hierarchical_aborted"))
                state["rescheduled"] |= int(row.get("reschedule_epoch") or 0) > 0
                state["reexecution"] |= bool(row.get("reexecuted"))
                state["reordered"] |= bool(row.get("reordered"))
                state["simulation_failed"] |= not bool(row.get("simulation_success"))
                state["reexecution_simulation_failed"] |= bool(row.get("reexecuted")) and not bool(row.get("reexecution_success")) and not bool(row.get("second_pass_invalidated"))
                state["second_pass_invalidated"] |= bool(row.get("second_pass_invalidated"))
    if source_seen and tx:
        return {
            "status": "available",
            "source": "nodes/*/block_execution_summary.json:optme_transaction_evidence",
            "logical_transaction_count": len(tx),
            "logical_early_detection_count": sum(1 for x in tx.values() if x["early_detected"]),
            "logical_hierarchical_abort_count": sum(1 for x in tx.values() if x["hierarchical_aborted"]),
            "logical_rescheduled_count": sum(1 for x in tx.values() if x["rescheduled"]),
            "logical_early_abort_count": sum(1 for x in tx.values() if x["rescheduled"]),
            "logical_reexecution_count": sum(1 for x in tx.values() if x["reexecution"]),
            "logical_reordered_count": sum(1 for x in tx.values() if x["reordered"]),
            "logical_simulation_failed_count": sum(1 for x in tx.values() if x["simulation_failed"]),
            "logical_reexecution_simulation_failed_count": sum(1 for x in tx.values() if x["reexecution_simulation_failed"]),
            "logical_second_pass_invalidated_count": sum(1 for x in tx.values() if x["second_pass_invalidated"]),
            "paper_reference": OPTME_PAPER_REFERENCE,
        }

    files = _candidate_files(root,
        ("aggregate/optme_transaction_fidelity.csv", "optme_transaction_fidelity.csv", "client/optme_transaction_fidelity.csv"),
        ("*optme*transaction*fidelity*.csv", "*optme*schedule*trace*.csv"))
    for path in files:
        rows = _read_csv(path)
        if not rows:
            continue
        legacy: dict[str, dict[str, bool]] = {}
        for row in rows:
            low = {str(k).lower(): "" if v is None else str(v) for k, v in row.items()}
            txid = low.get("logical_tx_id") or low.get("logical_id") or low.get("tx_id")
            if not txid:
                continue
            event = low.get("event", "").lower()
            state = legacy.setdefault(txid, {"early_abort": False, "reexecution": False, "reordered": False})
            state["early_abort"] |= low.get("early_abort", "").lower() in {"1", "true", "yes"} or "early_abort" in event
            state["reexecution"] |= low.get("reexecution", "").lower() in {"1", "true", "yes"} or "reexec" in event
            state["reordered"] |= low.get("reordered", "").lower() in {"1", "true", "yes"} or "reorder" in event
        if legacy:
            return {"status": "available", "source": str(path.relative_to(root)).replace("\\", "/"),
                    "logical_transaction_count": len(legacy),
                    "logical_early_abort_count": sum(1 for x in legacy.values() if x["early_abort"]),
                    "logical_reexecution_count": sum(1 for x in legacy.values() if x["reexecution"]),
                    "logical_reordered_count": sum(1 for x in legacy.values() if x["reordered"]),
                    "paper_reference": OPTME_PAPER_REFERENCE}
    return {"status": "missing_tx_level_fidelity_trace", "source": None, "paper_reference": OPTME_PAPER_REFERENCE,
            "aggregate_early_abort_observation_count": _as_int(metrics.get("optme_early_abort_count")),
            "aggregate_reexecution_observation_count": _as_int(metrics.get("optme_reexecution_count")),
            "aggregate_reordered_observation_count": _as_int(metrics.get("optme_reordered_transaction_count"))}

def enrich_metrics(run_dir: Path | str, method_id: str | None, result: dict[str, Any]) -> dict[str, Any]:
    out = v16.enrich_metrics(run_dir, method_id, result)
    method = _method_id(method_id, out)
    if not _is_target(method): return out
    out["v17_closure_version"] = CLOSURE_VERSION
    out["v17_semantic_class"] = _semantic_class(method)
    state = compute_business_state_evidence_v17(run_dir, method, out)
    out["v17_business_state_evidence"] = state
    out["v17_storage_partition_state_digest"] = state.get("storage_partition_digest")
    out["v17_logical_business_state_digest"] = state.get("global_logical_business_digest")
    out["v17_logical_business_state_projection_status"] = state.get("global_logical_business_projection_status")
    out["v17_logical_business_state_conflict_key_count"] = state.get("global_logical_conflict_key_count")
    # Clean split: no same-name combined initial+business hash.
    initial = out.get("initial_state_digest")
    business = out.get("v17_logical_business_state_digest")
    out["v17_logical_state_digest"] = _sha({"initial":initial,"business":business}) if _nonempty(initial) and _nonempty(business) else None

    wb = compute_writeback_fanout_v17(run_dir, out)
    out["v17_writeback_evidence"] = wb
    out["v17_unique_logical_delta_count"] = wb.get("unique_logical_delta_count")
    out["v17_replica_fanout_ratio"] = wb.get("replica_fanout_ratio")

    if _is_txallo(method):
        audit = compute_txallo_paper_audit_v17(run_dir, out)
        out["v17_txallo_paper_audit"] = audit
        out["v17_txallo_routing_coherence_passed"] = (audit.get("routing_coherence") or {}).get("passed")
    if _is_optme(method):
        oe = compute_optme_transaction_evidence_v17(run_dir, out)
        out["v17_optme_transaction_fidelity"] = oe
        out["v17_optme_logical_early_abort_count"] = oe.get("logical_early_abort_count")
        out["v17_optme_logical_reexecution_count"] = oe.get("logical_reexecution_count")
        out["v17_optme_logical_reordered_count"] = oe.get("logical_reordered_count")

    # Stateful serial oracle v2 already exposes partition digests.  If these match,
    # do not let a legacy node_summary business digest field contradict stronger partition truth.
    actual_b = out.get("stateful_serializability_actual_partition_business_digests")
    replay_b = out.get("stateful_serializability_replay_partition_business_digests")
    actual_e = out.get("stateful_serializability_actual_partition_effect_digests")
    replay_e = out.get("stateful_serializability_replay_partition_effect_digests")
    partition_ok = bool(isinstance(actual_b, dict) and actual_b and actual_b == replay_b and isinstance(actual_e, dict) and actual_e and actual_e == replay_e)
    out["v17_stateful_serial_partition_equivalent"] = partition_ok if not _is_stateless(method) else None
    return out


def _canonical_child(item: dict[str, Any]) -> dict[str, Any]:
    out = v16._canonical_child(item)
    metrics = item.get("metrics") if isinstance(item.get("metrics"), dict) else {}
    method = _method_id(item.get("method"), item)
    out["v17_method_id"] = method
    out["v17_semantic_class"] = _semantic_class(method)
    for key in (
        "v17_storage_partition_state_digest", "v17_logical_business_state_digest", "v17_logical_state_digest",
        "v17_logical_business_state_projection_status", "v17_logical_business_state_conflict_key_count",
        "v17_unique_logical_delta_count", "v17_replica_fanout_ratio", "v17_txallo_paper_audit",
        "v17_txallo_routing_coherence_passed", "v17_optme_transaction_fidelity",
        "v17_optme_logical_early_abort_count", "v17_optme_logical_reexecution_count", "v17_optme_logical_reordered_count",
        "v17_stateful_serial_partition_equivalent",
    ):
        out[key] = metrics.get(key)
    blockers: list[str] = []
    warnings: list[str] = []
    if _is_target(method):
        # logical workload identity remains mandatory
        if not _nonempty(out.get("v16_logical_transaction_identity_digest")):
            blockers.append("logical_transaction_identity_digest_missing")
        if _is_stateless(method):
            if out.get("v17_logical_business_state_projection_status") != "available":
                blockers.append("stateless_global_logical_business_state_not_proven")
        else:
            if not _nonempty(out.get("v17_storage_partition_state_digest")):
                blockers.append("stateful_partition_business_state_not_proven")
        if metrics.get("v16_metric_coherence_passed") is False:
            blockers.extend(str(x) for x in metrics.get("v16_metric_coherence_failures") or [])
        if metrics.get("v16_business_execution_timer_complete") is False:
            blockers.append("business_execution_measurement_incomplete")
    if _is_txallo(method):
        audit = metrics.get("v17_txallo_paper_audit") or {}
        if audit.get("account_uniqueness_passed") is False: blockers.append("txallo_definition1_account_uniqueness_failed")
        if audit.get("account_completeness_passed") is False: blockers.append("txallo_definition1_account_completeness_failed")
        if audit.get("parameter_semantics_passed") is False: blockers.append("txallo_eta_lambda_epsilon_semantics_invalid")
        rc = (audit.get("routing_coherence") or {}).get("passed")
        if rc is False: blockers.append("txallo_transaction_placement_not_induced_by_account_mapping")
        elif rc is None: warnings.append("txallo_account_to_transaction_routing_coherence_not_proven_from_artifacts")
        pc = (audit.get("physical_execution_coherence") or {}).get("passed")
        if not _is_stateless(method):
            if pc is False: blockers.append("txallo_stateful_physical_execution_not_coherent_with_paper_placement")
            elif pc is None: warnings.append("txallo_stateful_physical_execution_coherence_not_proven_from_artifacts")
        if audit.get("single_nonempty_shard_diagnostic"): warnings.append("txallo_single_nonempty_community_observed_not_intrinsically_invalid")
    if _is_optme(method):
        oe = metrics.get("v17_optme_transaction_fidelity") or {}
        if oe.get("status") != "available": warnings.append("optme_tx_level_abort_reorder_reexecution_trace_missing")
        # Reordered==0 is NOT itself invalid for a particular workload.
    if _is_stateless(method):
        wb = metrics.get("v17_writeback_evidence") or {}
        if wb.get("status") != "available_exact_logical_dedup": warnings.append("exact_version_writeback_logical_dedup_not_proven")
    out["v17_fidelity_blockers"] = sorted(set(blockers))
    out["v17_fidelity_warnings"] = sorted(set(warnings))
    out["v17_paper_fidelity_candidate"] = not blockers
    return out


def _strict_equal(values: list[Any]) -> bool | None:
    if len(values) < 2 or any(not _nonempty(x) for x in values): return None
    return len({str(x) for x in values}) == 1


def apply_group_fidelity_gate(items: list[dict[str, Any]], report: dict[str, Any]) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    enriched = [_canonical_child(x) if _is_target(_method_id(x.get("method"), x)) else dict(x) for x in items]
    targets = [x for x in enriched if _is_target(x.get("v17_method_id", ""))]
    out = dict(report or {})
    if not targets: return enriched, out
    workload_equal = _strict_equal([x.get("v16_logical_transaction_identity_digest") for x in targets])
    stateful = [x for x in targets if x.get("v17_semantic_class") == "stateful_local_partition"]
    stateless = [x for x in targets if x.get("v17_semantic_class") == "stateless_global_home"]
    stateless_business_equal = _strict_equal([x.get("v17_logical_business_state_digest") for x in stateless]) if len(stateless) >= 2 else None
    stateful_partition_equal = _strict_equal([x.get("v17_storage_partition_state_digest") for x in stateful]) if len(stateful) >= 2 else None
    blockers: list[str] = []
    if workload_equal is not True: blockers.append("logical_transaction_identity_not_proven_equal")
    if any(x.get("v17_fidelity_blockers") for x in targets): blockers.append("one_or_more_method_v17_fidelity_blockers")
    # We intentionally do not require stateful partition digests to equal TxAllo vs OptME:
    # transaction allocation methods may place the same business data differently.
    if stateless_business_equal is False: blockers.append("stateless_variants_logical_business_state_not_equivalent")
    out.update({
        "paper_fidelity_closure_version": CLOSURE_VERSION,
        "pairwise_logical_transaction_identity_equivalent": workload_equal,
        "stateless_pair_logical_business_state_equivalent": stateless_business_equal,
        "stateful_pair_partition_state_equivalent": stateful_partition_equal,
        "cross_semantic_business_state_equivalence": "not_asserted_without_explicit_canonical_global_projection",
        "stateful_partition_identity_is_not_collapsed": True,
        "txallo_empty_community_is_not_intrinsic_fidelity_failure": True,
        "performance_comparison_valid": False if blockers else bool(out.get("performance_comparison_valid")),
        "v17_group_fidelity_blockers": blockers,
        "writeback_replication_contract": WRITEBACK_REPLICATION_CONTRACT,
    })
    if blockers: out["paper_candidate"] = False
    for x in enriched:
        if not _is_target(x.get("v17_method_id", "")): continue
        x["pairwise_logical_transaction_identity_equivalent"] = workload_equal
        x["stateless_pair_logical_business_state_equivalent"] = stateless_business_equal
        x["stateful_pair_partition_state_equivalent"] = stateful_partition_equal
        if x.get("v17_fidelity_blockers") or workload_equal is not True:
            x["paper_candidate"] = False
            reasons = list(x.get("paper_candidate_reasons") or []) + list(x.get("v17_fidelity_blockers") or [])
            if workload_equal is not True: reasons.append("logical_transaction_identity_not_proven_equal")
            x["paper_candidate_reasons"] = sorted(set(map(str,reasons)))
    return enriched, out


def apply_fairness_fidelity_gate(rows: list[dict[str, Any]], report: dict[str, Any]) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    enriched, out = v16.apply_fairness_fidelity_gate(rows, report)
    out = dict(out)
    out["v17_fairness_contract_version"] = CLOSURE_VERSION
    out["v17_txallo_paper_executor_note"] = "TxAllo is an allocation algorithm; serial execution is not force-upgraded to parallel workers"
    return enriched, out


# Compatibility names used by older wrappers/tests.
def enrich_extracted_metrics(run_dir: Path | str, method_id: str | None, result: dict[str, Any]) -> dict[str, Any]:
    return enrich_metrics(run_dir, method_id, result)


def apply_truth_closure_to_state_equivalence(items: list[dict[str, Any]], report: dict[str, Any]) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    return apply_group_fidelity_gate(items, report)
