from __future__ import annotations

import csv
import gzip
import json
from pathlib import Path
from typing import Any, Iterable

from backend.app.services import v5_optme_txallo_paperfaithful_v17 as v17

CLOSURE_VERSION = "mbe_optme_txallo_method_specific_correctness_v18"
WRITEBACK_REPLICATION_CONTRACT = v17.WRITEBACK_REPLICATION_CONTRACT
TARGETS = {"stateful_optme", "stateless_optme", "stateful_txallo", "stateless_txallo"}


def _nonempty(value: Any) -> bool:
    return value not in (None, "", [], {}, ())


def _as_int(value: Any) -> int | None:
    try:
        if value in (None, ""):
            return None
        return int(float(value))
    except Exception:
        return None


def _method_id(value: Any, fallback: dict[str, Any] | None = None) -> str:
    return v17._method_id(value, fallback)


def _is_target(method: str) -> bool:
    return method.lower() in TARGETS


def _is_stateless(method: str) -> bool:
    return method.lower().startswith("stateless_")


def _is_txallo(method: str) -> bool:
    return "txallo" in method.lower()


def _is_optme(method: str) -> bool:
    return "optme" in method.lower()


def _read_csv(path: Path) -> list[dict[str, str]]:
    try:
        with path.open("r", encoding="utf-8-sig", newline="") as handle:
            return [dict(row) for row in csv.DictReader(handle)]
    except Exception:
        return []


def _read_json(path: Path) -> Any:
    try:
        return json.loads(path.read_text(encoding="utf-8-sig"))
    except Exception:
        return None


def _split(value: str) -> list[str]:
    return v17._split_accounts(value)


def _candidate_files(root: Path, exact: Iterable[str], patterns: Iterable[str]) -> list[Path]:
    return v17._candidate_files(root, exact, patterns)


def compute_method_specific_correctness_v18(method_id: str, metrics: dict[str, Any]) -> dict[str, Any]:
    """Truthful correctness gate.

    The legacy generic ``method_correctness_oracle_status=passed`` is not sufficient for
    these four methods: replica determinism/completion is weaker than method-specific
    serial equivalence. V18 therefore accepts only an actual method-specific replay proof.
    Stateful-local methods may use the existing partitioned serializability oracle v2;
    stateless multi-shard methods need a real multi-shard logical serial replay.
    """
    method = method_id.lower()
    if method not in TARGETS:
        return {"status": "not_applicable", "valid": None, "blockers": []}

    # MBE_OPTME_V23_V18_TRUTH: v22 intentionally retired OptME's old
    # transaction exact-version/multi-PBFT adaptation. Use the method-specific
    # v23 replay proof; TxAllo keeps the historical v18 rules below.
    if method in {"stateful_optme", "stateless_optme"}:
        kind = str(metrics.get("method_correctness_oracle_kind") or "")
        if kind.startswith("optme_v23_"):
            status = str(metrics.get("method_correctness_oracle_status") or "unproven")
            valid = metrics.get("method_correctness_oracle_valid")
            return {
                "status": status,
                "valid": valid if isinstance(valid, bool) else None,
                "kind": kind,
                "scope": metrics.get("method_correctness_oracle_scope"),
                "blockers": list(metrics.get("method_correctness_oracle_blockers") or []),
                "legacy_generic_oracle_status": metrics.get("serial_order_oracle_status"),
            }

    serial_status = str(metrics.get("serial_order_oracle_status") or "").lower()
    serial_equiv = metrics.get("serial_order_replay_equivalent")
    serial_scope = str(metrics.get("serial_order_replay_supported_scope") or metrics.get("method_correctness_oracle_scope") or "")
    serial_reason = str(metrics.get("serial_order_replay_not_applicable_reason") or "")

    if not _is_stateless(method):
        actual_b = metrics.get("stateful_serializability_actual_partition_business_digests")
        replay_b = metrics.get("stateful_serializability_replay_partition_business_digests")
        actual_e = metrics.get("stateful_serializability_actual_partition_effect_digests")
        replay_e = metrics.get("stateful_serializability_replay_partition_effect_digests")
        partition_available = isinstance(actual_b, dict) and bool(actual_b) and isinstance(replay_b, dict) and bool(replay_b)
        effect_available = isinstance(actual_e, dict) and bool(actual_e) and isinstance(replay_e, dict) and bool(replay_e)
        if partition_available and effect_available:
            ok = actual_b == replay_b and actual_e == replay_e
            return {
                "status": "passed" if ok else "failed",
                "valid": ok,
                "kind": "stateful_partitioned_method_specific_serial_replay_v2",
                "scope": "per_shard_pbft_committed_order_plus_effect_digest",
                "blockers": [] if ok else ["stateful_partition_serial_replay_mismatch"],
                "legacy_generic_oracle_status": metrics.get("method_correctness_oracle_status"),
            }
        if serial_status == "passed" and serial_equiv is True:
            return {
                "status": "passed", "valid": True,
                "kind": "stateful_method_specific_serial_replay",
                "scope": serial_scope,
                "blockers": [],
                "legacy_generic_oracle_status": metrics.get("method_correctness_oracle_status"),
            }
        if serial_status == "failed" or serial_equiv is False:
            return {
                "status": "failed", "valid": False,
                "kind": "stateful_method_specific_serial_replay",
                "scope": serial_scope,
                "blockers": ["stateful_method_specific_serial_replay_failed"],
                "legacy_generic_oracle_status": metrics.get("method_correctness_oracle_status"),
            }
        return {
            "status": "unproven", "valid": None,
            "kind": "stateful_method_specific_serial_replay",
            "scope": serial_scope or None,
            "blockers": ["stateful_method_specific_serial_replay_missing"],
            "not_applicable_reason": serial_reason or None,
            "legacy_generic_oracle_status": metrics.get("method_correctness_oracle_status"),
        }

    # Stateless methods must not be called correct merely because all replicas were
    # deterministic. Require a serial replay that is actually applicable to multi-shard
    # execution. The old single-shard oracle's not_applicable state remains UNPROVEN.
    multi_scope = "multi_shard" in serial_scope.lower() or "multishard" in serial_scope.lower()
    if serial_status == "passed" and serial_equiv is True and multi_scope:
        return {
            "status": "passed", "valid": True,
            "kind": "stateless_multishard_method_specific_serial_replay",
            "scope": serial_scope,
            "blockers": [],
            "legacy_generic_oracle_status": metrics.get("method_correctness_oracle_status"),
        }
    if serial_status == "failed" or serial_equiv is False:
        return {
            "status": "failed", "valid": False,
            "kind": "stateless_multishard_method_specific_serial_replay",
            "scope": serial_scope or None,
            "blockers": ["stateless_method_specific_multishard_serial_replay_failed"],
            "legacy_generic_oracle_status": metrics.get("method_correctness_oracle_status"),
        }
    return {
        "status": "unproven", "valid": None,
        "kind": "stateless_multishard_method_specific_serial_replay",
        "scope": serial_scope or None,
        "blockers": ["stateless_method_specific_multishard_serial_replay_missing"],
        "not_applicable_reason": serial_reason or "legacy_single_shard_oracle_is_not_a_multishard_correctness_proof",
        "legacy_generic_oracle_status": metrics.get("method_correctness_oracle_status"),
    }


def compute_txallo_independent_account_coverage_v18(run_dir: Path | str) -> dict[str, Any]:
    """Validate canonical v20.2 accounts while preserving legacy artifact audits.

    New evaluation-only accounts may use deterministic causal fallback. Older
    artifacts that do not export mapping provenance retain the original v18
    completeness check and are not silently reclassified.
    """
    # MBE_TXALLO_REPRO_V202_COVERAGE
    root=Path(run_dir); mapping,mapping_source=v17._mapping_rows(root)
    if not mapping:
        return {"status":"missing_account_mapping","passed":None,"mapping_source":mapping_source}
    files=_candidate_files(root,("client/txallo_transaction_placement.csv","txallo_transaction_placement.csv"),("*txallo*transaction*placement*.csv",))
    if not files:
        return {"status":"missing_transaction_placement","passed":None,"mapping_source":mapping_source}
    rows_data=_read_csv(files[0])
    new_schema=any(any(str(k).lower() in {"sender_account","receiver_account","sender_mapping_source","receiver_mapping_source"} for k in row) for row in rows_data)
    if new_schema:
        accounts=set(); history=set(); fallback=set(); mismatches=[]; rows=0
        for row in rows_data:
            low={str(k).lower():"" if v is None else str(v).strip() for k,v in row.items()}
            if not low.get("sender_account") and not low.get("receiver_account"):
                continue
            rows += 1
            for role in ("sender","receiver"):
                account=low.get(f"{role}_account","")
                if not account:
                    continue
                accounts.add(account); source=low.get(f"{role}_mapping_source",""); shard=low.get(f"{role}_shard","")
                if source=="history_mapping":
                    history.add(account)
                    if mapping.get(account)!=shard and len(mismatches)<16:
                        mismatches.append({"role":role,"account":account,"reason":"history_mapping_shard_mismatch"})
                elif source=="fallback_hash":
                    fallback.add(account)
                    if (account in mapping or not shard) and len(mismatches)<16:
                        mismatches.append({"role":role,"account":account,"reason":"invalid_fallback"})
                elif len(mismatches)<16:
                    mismatches.append({"role":role,"account":account,"reason":"mapping_source_missing_or_invalid"})
        return {"status":"available_causal_mapping_and_fallback_evidence" if rows else "placement_rows_lack_canonical_account_identity",
                "passed":not mismatches if rows else None, "reference_account_count":len(accounts),
                "history_mapped_account_count":len(history), "fallback_account_count":len(fallback),
                "fallback_account_ratio":len(fallback)/len(accounts) if accounts else None,
                "mismatch_count":len(mismatches), "examples":mismatches, "mapping_source":mapping_source,
                "reference_source_files":[str(files[0].relative_to(root)).replace("\\","/")],
                "runtime_mapped_count_not_used_as_reference":True,
                "truth_boundary":"canonical_logical_account_identity_history_mapping_or_causal_fallback"}

    # Historical v18 path: preserve old completeness semantics for artifacts
    # that never exported mapping provenance.
    accounts=set(); sources=[]
    for row in rows_data:
        low={str(k).lower():"" if v is None else str(v).strip() for k,v in row.items()}
        for key in ("accounts", "account_ids", "involved_accounts", "input_accounts", "output_accounts",
                    "sender", "sender_id", "receiver", "receiver_id", "source_account", "target_account"):
            accounts.update(x for x in _split(low.get(key,"")) if x)
    if accounts:
        sources.append(str(files[0].relative_to(root)).replace("\\","/"))
    if not accounts:
        for rel in ("client/resolved_access_lists.jsonl.gz", "client/resolved_access_lists.jsonl"):
            path=root/rel
            if not path.is_file():
                continue
            try:
                opener=gzip.open if path.suffix==".gz" else open
                with opener(path,"rt",encoding="utf-8") as handle:  # type: ignore[arg-type]
                    for line in handle:
                        try: obj=json.loads(line)
                        except Exception: continue
                        for key in ("sender", "sender_id", "receiver", "receiver_id", "source_account", "target_account"):
                            value=obj.get(key) if isinstance(obj,dict) else None
                            if value: accounts.add(str(value))
                if accounts: sources.append(rel)
            except Exception:
                pass
    if not accounts:
        return {"status":"evaluation_transactions_lack_explicit_account_identity","passed":None,"mapping_source":mapping_source,
                "reference_source_files":[],"runtime_mapped_count_not_used_as_reference":True}
    missing=sorted(a for a in accounts if a not in mapping); extras=sorted(a for a in mapping if a not in accounts)
    return {"status":"available","passed":not missing,"reference_account_count":len(accounts),"mapped_account_count":len(mapping),
            "missing_account_count":len(missing),"missing_accounts":missing[:32],"extra_mapping_account_count":len(extras),
            "mapping_source":mapping_source,"reference_source_files":sources,"runtime_mapped_count_not_used_as_reference":True}

def compute_stateless_txallo_relay_guard_v18(run_dir: Path | str) -> dict[str, Any]:
    """Detect accidental legacy relay execution of a stateless TxAllo transaction.

    A Stateless-TxAllo business transaction has one execution owner; remote state is fetched
    from Home shards.  Executing the same logical transaction in more than one *shard* is
    therefore a legacy cross-shard relay leak, not expected replica duplication.
    """
    root = Path(run_dir)
    traces = _candidate_files(root, (), ("nodes/*/transaction_execution_trace.csv",))
    by_tx: dict[str, set[str]] = {}
    sources: list[str] = []
    for path in traces:
        rows = _read_csv(path)
        if not rows:
            continue
        sources.append(str(path.relative_to(root)).replace("\\", "/"))
        for row in rows:
            low = {str(k).lower(): "" if v is None else str(v).strip() for k, v in row.items()}
            txid = low.get("logical_tx_id") or low.get("logical_id") or low.get("tx_id") or low.get("transaction_id")
            shard = low.get("shard_id") or low.get("shard")
            if txid and shard:
                by_tx.setdefault(txid, set()).add(shard)
    if not by_tx:
        return {"status": "missing_transaction_execution_trace", "passed": None, "sources": sources}
    leaked = [(txid, sorted(shards)) for txid, shards in by_tx.items() if len(shards) > 1]
    return {
        "status": "available",
        "passed": not leaked,
        "checked_transaction_count": len(by_tx),
        "multi_shard_execution_transaction_count": len(leaked),
        "examples": [{"logical_tx_id": txid, "execution_shards": shards} for txid, shards in leaked[:16]],
        "sources": sorted(sources),
        "truth_boundary": "one_stateless_execution_owner_remote_home_state_fetch_not_legacy_relay",
    }


def compute_writeback_fanout_v18(run_dir: Path | str, metrics: dict[str, Any]) -> dict[str, Any]:
    """Count only exact-version writeback, never legacy non-version write_apply rows.

    Exact-version StateReady publication currently carries ProducedVersion and
    ApplyOrigin=versioned_remote_home. Its raw UpdateSemantics is allowed to be
    empty because this path is a deterministic versioned set, not the legacy CAS/
    commutative-delta transport. V18 canonicalises that *evidence field only* to
    exact_version_set_default; runtime execution semantics are not changed.
    """
    root = Path(run_dir)
    files = _candidate_files(
        root,
        ("physical_remote_state_operations.csv", "aggregate/replica_deduplicated_remote_operations.csv"),
        ("*physical_remote_state_operations*.csv", "*replica_deduplicated_remote_operations*.csv", "nodes/*/remote_state_access.csv"),
    )
    required = ("logical_tx_id", "state_key", "produced_version", "home_shard", "update_semantics")
    best_missing: dict[str, Any] | None = None
    best_no_exact: dict[str, Any] | None = None
    for path in files:
        physical: list[dict[str, str]] = []
        non_version = 0
        for row in _read_csv(path):
            low = {str(k).lower(): "" if v is None else str(v).strip() for k, v in row.items()}
            blob = " ".join(low.values()).lower()
            if not ("state_delta_apply" in blob or "write_apply" in blob or "versioned_remote_home" in blob):
                continue
            produced = low.get("produced_version", "")
            try:
                version = int(produced or "0")
            except Exception:
                version = 0
            if version <= 0:
                non_version += 1
                continue
            physical.append(low)
        if not physical:
            if non_version:
                candidate = {
                    "status": "no_exact_version_writeback_rows",
                    "physical_message_count": 0,
                    "unique_logical_delta_count": 0,
                    "replica_fanout_ratio": None,
                    "excluded_non_version_write_apply_count": non_version,
                    "dedup_key": list(required),
                    "replication_contract": WRITEBACK_REPLICATION_CONTRACT,
                    "source": str(path.relative_to(root)).replace("\\", "/"),
                }
                if best_no_exact is None or non_version > int(best_no_exact.get("excluded_non_version_write_apply_count") or 0):
                    best_no_exact = candidate
            # A summary/aggregate file can predate the V18 identity columns while
            # node-level remote_state_access.csv already contains them. Never stop
            # searching merely because an earlier candidate has only legacy rows.
            continue
        keys: set[tuple[str, ...]] = set()
        missing: set[str] = set()
        canonicalized_default_set = 0
        logical_id_from_tx_ids = 0
        for row in physical:
            vals: list[str] = []
            for field in required:
                value = row.get(field, "")
                if field == "logical_tx_id" and not value:
                    value = row.get("logical_id", "") or row.get("logical_tx_ids", "") or row.get("tx_id", "")
                    if row.get("logical_tx_ids", "") and value == row.get("logical_tx_ids", ""):
                        logical_id_from_tx_ids += 1
                if field == "state_key" and not value:
                    value = row.get("key", "")
                if field == "update_semantics" and not value:
                    # Source-verified exact-version StateReady writer sets
                    # ApplyOrigin=versioned_remote_home and ProducedVersion>0 but
                    # intentionally leaves UpdateSemantics empty. Canonicalise only
                    # the audit identity, never the runtime request itself.
                    if row.get("apply_origin", "").lower() in {"versioned_remote_home", "optme_txallo_versioned_remote_home_v10"}:  # MBE_TXALLO_EVIDENCE_V203
                        value = "exact_version_set_default"
                        canonicalized_default_set += 1
                if not value:
                    missing.add(field)
                vals.append(value)
            if all(vals):
                keys.add(tuple(vals))
        if missing:
            candidate = {
                "status": "missing_exact_version_identity",
                "physical_message_count": len(physical),
                "unique_logical_delta_count": None,
                "replica_fanout_ratio": None,
                "missing_identity_fields": sorted(missing),
                "excluded_non_version_write_apply_count": non_version,
                "canonicalized_default_set_semantics_count": canonicalized_default_set,
                "logical_identity_from_logical_tx_ids_count": logical_id_from_tx_ids,
                "source": str(path.relative_to(root)).replace("\\", "/"),
                "dedup_key": list(required),
                "replication_contract": WRITEBACK_REPLICATION_CONTRACT,
            }
            if best_missing is None or len(candidate["missing_identity_fields"]) < len(best_missing.get("missing_identity_fields") or []):
                best_missing = candidate
            continue
        return {
            "status": "available_exact_logical_dedup",
            "physical_message_count": len(physical),
            "unique_logical_delta_count": len(keys),
            "replica_fanout_ratio": len(physical) / len(keys) if keys else None,
            "missing_identity_fields": [],
            "excluded_non_version_write_apply_count": non_version,
            "canonicalized_default_set_semantics_count": canonicalized_default_set,
            "logical_identity_from_logical_tx_ids_count": logical_id_from_tx_ids,
            "source": str(path.relative_to(root)).replace("\\", "/"),
            "dedup_key": list(required),
            "canonical_update_semantics_rule": "produced_version_gt_0_and_versioned_remote_home_empty_semantics_means_exact_version_set_default",
            "replication_contract": WRITEBACK_REPLICATION_CONTRACT,
        }
    if best_missing is not None:
        return best_missing
    if best_no_exact is not None:
        return best_no_exact
    legacy = v17.compute_writeback_fanout_v17(root, metrics)
    return {
        **legacy,
        "status": "summary_only_no_exact_version_identity" if legacy.get("physical_message_count") is not None else "missing",
        "dedup_key": list(required),
        "replication_contract": WRITEBACK_REPLICATION_CONTRACT,
    }

def compute_metric_coherence_v18(method_id: str, metrics: dict[str, Any]) -> dict[str, Any]:
    failures: list[str] = []
    physical = _as_int(metrics.get("physical_remote_operation_count"))
    reads = _as_int(metrics.get("physical_remote_fetch_count"))
    writes = _as_int(metrics.get("physical_remote_writeback_count"))
    flat_access = _as_int(metrics.get("remote_state_access_count"))
    if physical is not None and reads is not None and writes is not None and physical != reads + writes:
        failures.append("physical_remote_operation_count_ne_fetch_plus_writeback")
    # A legacy remote_state_access_count of zero is known to represent only a narrower
    # node-local CSV in some versions. It must not override explicit physical transport
    # truth. Record that disagreement diagnostically instead of making a truthful run fail.
    diagnostics: list[str] = []
    if physical not in (None, 0) and flat_access == 0:
        diagnostics.append("legacy_remote_state_access_count_zero_is_narrower_than_physical_transport_truth")
    metatrack_status = "not_applicable" if _is_target(method_id) else None
    return {
        "status": "passed" if not failures else "failed",
        "passed": not failures,
        "failures": failures,
        "diagnostics": diagnostics,
        "canonical_remote_state_source": "physical_remote_state_operations_and_method_preserving_transport_metrics",
        "canonical_physical_remote_operation_count": physical,
        "canonical_physical_remote_fetch_count": reads,
        "canonical_physical_remote_writeback_count": writes,
        "legacy_remote_state_access_count": flat_access,
        "metatrack_metrics_applicability": metatrack_status,
    }


def _method_config_worker_count(item: dict[str, Any]) -> int | None:
    method = item.get("method") if isinstance(item.get("method"), dict) else {}
    overrides = method.get("plugin_config_overrides") if isinstance(method, dict) else {}
    block = overrides.get("block_executor") if isinstance(overrides, dict) else {}
    return _as_int(block.get("worker_count")) if isinstance(block, dict) else None


# MBE_OPTME_V24_LOGICAL_COUNTS: canonical paper/mechanism event counts are
# logical/deduplicated from tx-level evidence. Reference-replica extractor
# metrics remain the primary stage/timing scope; all-replica sums stay explicit.
def _apply_optme_logical_counts_v24(out: dict[str, Any]) -> dict[str, Any]:
    evidence = out.get("v17_optme_transaction_fidelity") or out.get("v18_optme_transaction_fidelity") or {}
    if not isinstance(evidence, dict) or evidence.get("status") != "available":
        return out
    mapping = {
        "early_abort": "logical_early_abort_count",
        "early_detection": "logical_early_detection_count",
        "hierarchical_abort": "logical_hierarchical_abort_count",
        "rescheduled_transaction": "logical_rescheduled_count",
        "reexecution": "logical_reexecution_count",
        "reordered_transaction": "logical_reordered_count",
        "simulation_failed": "logical_simulation_failed_count",
        "reexecution_simulation_failed": "logical_reexecution_simulation_failed_count",
        "reexecution_invalid": "logical_second_pass_invalidated_count",
    }
    for stem, source in mapping.items():
        physical_key = f"optme_{stem}_count"
        logical_key = f"optme_logical_{stem}_count"
        observation_key = f"optme_replica_physical_{stem}_observation_count"
        physical_sum_key = f"optme_replica_physical_{stem}_count_sum"
        if observation_key not in out:
            if physical_sum_key in out:
                out[observation_key] = out.get(physical_sum_key)
            elif physical_key in out:
                out[observation_key] = out.get(physical_key)
        if evidence.get(source) is not None:
            out[logical_key] = evidence.get(source)
            out[physical_key] = evidence.get(source)
    out["optme_logical_transaction_count"] = evidence.get("logical_transaction_count")
    out["optme_algorithm_event_count_scope"] = "logical_tx_deduplicated_from_optme_transaction_evidence"
    return out


def enrich_metrics(run_dir: Path | str, method_id: str | None, result: dict[str, Any]) -> dict[str, Any]:
    out = v17.enrich_metrics(run_dir, method_id, result)
    method = _method_id(method_id, out)
    if not _is_target(method):
        return out
    out["v18_closure_version"] = CLOSURE_VERSION

    correctness = compute_method_specific_correctness_v18(method, out)
    out["v18_method_correctness"] = correctness
    out["v18_method_correctness_oracle_status"] = correctness.get("status")
    out["v18_method_correctness_oracle_valid"] = correctness.get("valid")
    out["v18_method_correctness_oracle_blockers"] = correctness.get("blockers") or []

    # Fix the v17 derived-field timing issue by recomputing after the serial oracle fields
    # already exist in the extracted result.
    if not _is_stateless(method):
        out["v18_stateful_serial_partition_equivalent"] = correctness.get("valid") if correctness.get("status") in {"passed", "failed"} else None
    else:
        out["v18_stateful_serial_partition_equivalent"] = None

    wb = compute_writeback_fanout_v18(run_dir, out)
    out["v18_writeback_evidence"] = wb
    out["v18_unique_exact_version_logical_delta_count"] = wb.get("unique_logical_delta_count")
    out["v18_exact_version_replica_fanout_ratio"] = wb.get("replica_fanout_ratio")

    coherence = compute_metric_coherence_v18(method, out)
    out["v18_metric_coherence"] = coherence
    out["v18_metric_coherence_passed"] = coherence.get("passed")
    out["v18_metatrack_metrics_applicability"] = "not_applicable"
    out["v18_effective_worker_count"] = _as_int(out.get("v16_effective_worker_count")) or _as_int(out.get("worker_count"))

    if _is_txallo(method):
        out["v18_txallo_independent_account_coverage"] = compute_txallo_independent_account_coverage_v18(run_dir)
        # Keep v17 mapping->placement truth, but V18 treats absence as UNPROVEN, never a warning-only candidate.
        audit = out.get("v17_txallo_paper_audit") or v17.compute_txallo_paper_audit_v17(run_dir, out)
        out["v18_txallo_mapping_to_placement"] = v17.compute_txallo_routing_coherence_v17(run_dir)
        if method == "stateful_txallo":
            out["v18_txallo_placement_to_execution"] = audit.get("physical_execution_coherence") or {}
        else:
            out["v18_stateless_txallo_legacy_relay_guard"] = compute_stateless_txallo_relay_guard_v18(run_dir)

    if _is_optme(method):
        # V17 already refuses to infer per-transaction behavior from aggregate counts.
        out["v18_optme_transaction_fidelity"] = out.get("v17_optme_transaction_fidelity") or v17.compute_optme_transaction_evidence_v17(run_dir, out)
        _apply_optme_logical_counts_v24(out)
        # MBE_OPTME_V241_RUNTIME_FIDELITY_GATE: promote raw executor evidence into
        # method truth. Missing fields mean the active Go runtime cannot be
        # proven to contain the fidelity instrumentation, so fail closed later.
        out["v18_optme_runtime_fidelity"] = {
            "status": out.get("optme_runtime_fidelity_evidence_status"),
            "passed": out.get("optme_runtime_fidelity_evidence_passed"),
            "input_access_fidelity": out.get("optme_input_access_fidelity"),
            "projected_unknown_access_count": out.get("optme_projected_unknown_access_count"),
            "execution_window_mapping": out.get("optme_execution_window_mapping"),
            "commit_materialization_model": out.get("optme_commit_materialization_model"),
            "input_access_truth_scope": out.get("optme_input_access_truth_scope"),
            "missing_fields": out.get("optme_runtime_fidelity_missing_fields") or [],
            "failures": out.get("optme_runtime_fidelity_failures") or [],
        }

    return out


def _canonical_child(item: dict[str, Any]) -> dict[str, Any]:
    # Start from V17 canonicalization only to preserve compatibility fields. V18 then
    # replaces the paper-candidate decision with fail-closed method-specific truth.
    out = v17._canonical_child(item)
    metrics = item.get("metrics") if isinstance(item.get("metrics"), dict) else {}
    method = _method_id(item.get("method"), item)
    out["v18_method_id"] = method
    out["v18_optme_runtime_fidelity"] = metrics.get("v18_optme_runtime_fidelity")
    for key in (
        "v18_method_correctness", "v18_method_correctness_oracle_status", "v18_method_correctness_oracle_valid",
        "v18_method_correctness_oracle_blockers", "v18_stateful_serial_partition_equivalent",
        "v18_writeback_evidence", "v18_unique_exact_version_logical_delta_count", "v18_exact_version_replica_fanout_ratio",
        "v18_metric_coherence", "v18_metric_coherence_passed", "v18_metatrack_metrics_applicability",
        "v18_effective_worker_count", "v18_txallo_independent_account_coverage", "v18_txallo_mapping_to_placement",
        "v18_txallo_placement_to_execution", "v18_stateless_txallo_legacy_relay_guard", "v18_optme_transaction_fidelity",
    ):
        out[key] = metrics.get(key)

    blockers: list[str] = []
    diagnostics: list[str] = []
    if not _is_target(method):
        return out

    corr = metrics.get("v18_method_correctness") or compute_method_specific_correctness_v18(method, metrics)
    if corr.get("status") == "failed":
        blockers.extend(corr.get("blockers") or ["method_specific_correctness_failed"])
    elif corr.get("status") != "passed":
        blockers.extend(corr.get("blockers") or ["method_specific_correctness_unproven"])

    if metrics.get("v18_metric_coherence_passed") is False:
        blockers.extend((metrics.get("v18_metric_coherence") or {}).get("failures") or ["metric_coherence_failed"])

    if _is_optme(method):
        oe = metrics.get("v18_optme_transaction_fidelity") or metrics.get("v17_optme_transaction_fidelity") or {}
        if oe.get("status") != "available":
            blockers.append("optme_tx_level_abort_reorder_reexecution_trace_unproven")

    if _is_txallo(method):
        route = metrics.get("v18_txallo_mapping_to_placement") or {}
        if route.get("passed") is False:
            blockers.append("txallo_account_mapping_to_transaction_placement_failed")
        elif route.get("passed") is not True:
            blockers.append("txallo_account_mapping_to_transaction_placement_unproven")
        coverage = metrics.get("v18_txallo_independent_account_coverage") or {}
        if coverage.get("passed") is False:
            blockers.append("txallo_independent_account_completeness_failed")
        elif coverage.get("passed") is not True:
            blockers.append("txallo_independent_account_completeness_unproven")
        if method == "stateful_txallo":
            physical = metrics.get("v18_txallo_placement_to_execution") or {}
            if physical.get("passed") is False:
                blockers.append("txallo_placement_to_stateful_execution_failed")
            elif physical.get("passed") is not True:
                blockers.append("txallo_placement_to_stateful_execution_unproven")
        else:
            relay = metrics.get("v18_stateless_txallo_legacy_relay_guard") or {}
            if relay.get("passed") is False:
                blockers.append("stateless_txallo_legacy_relay_duplicate_execution_detected")
            elif relay.get("passed") is not True:
                blockers.append("stateless_txallo_legacy_relay_guard_unproven")

    if _is_stateless(method) and not _is_optme(method):
        wb = metrics.get("v18_writeback_evidence") or {}
        if wb.get("status") != "available_exact_logical_dedup":
            blockers.append("exact_version_writeback_five_tuple_unproven")

    # Cross-method final business-state equality is diagnostic only. Different methods
    # may legally choose different serial orders. It is not a V18 correctness blocker.
    if metrics.get("v17_logical_business_state_digest"):
        diagnostics.append("cross_method_final_business_digest_is_diagnostic_not_correctness_oracle")

    # MBE_OPTME_V241_RUNTIME_FIDELITY_GATE: require proof that the active Go
    # runtime emitted the v24 fidelity fields. Historical UNKNOWN->RMW evidence
    # is diagnostic, not an OptME algorithm failure.
    if _is_optme(method):
        runtime_fidelity = metrics.get("v18_optme_runtime_fidelity") or {}
        if runtime_fidelity.get("status") != "available" or runtime_fidelity.get("passed") is not True:
            blockers.append("optme_runtime_fidelity_evidence_unproven")
        elif runtime_fidelity.get("input_access_fidelity") == "historical_static_unknown_promoted_to_rmw_projection":
            diagnostics.append("optme_historical_unknown_access_is_projected_rmw_not_native_runtime_rw_truth")

    # MBE_OPTME_V24_V23_SUPERSESSION: v17 predates the OptME-specific apply-order
    # replay oracle. Retire only its obsolete stateless-global-projection blocker
    # when current method correctness and tx-level OptME evidence both pass.
    if _is_optme(method) and corr.get("status") == "passed":
        oe = metrics.get("v18_optme_transaction_fidelity") or metrics.get("v17_optme_transaction_fidelity") or {}
        if isinstance(oe, dict) and oe.get("status") == "available":
            stale = {"stateless_global_logical_business_state_not_proven"}
            legacy_blockers = [str(x) for x in (out.get("v17_fidelity_blockers") or [])]
            retired = [x for x in legacy_blockers if x in stale]
            retained_v17 = [x for x in legacy_blockers if x not in stale]
            if retired:
                out["v18_retired_v17_blockers"] = sorted(set(retired))
                out["v18_retained_v17_fidelity_blockers"] = sorted(set(retained_v17))
                legacy_reasons = [str(x) for x in (out.get("paper_candidate_reasons") or [])]
                retained_reasons = [x for x in legacy_reasons if x not in stale]
                out["paper_candidate_reasons"] = sorted(set(retained_reasons))
                if not retained_v17 and not retained_reasons:
                    out["paper_candidate"] = True

    out["v18_fidelity_blockers"] = sorted(set(map(str, blockers)))
    out["v18_diagnostics"] = sorted(set(diagnostics))
    out["v18_paper_fidelity_candidate"] = not blockers
    out["paper_candidate"] = bool(out.get("paper_candidate", True)) and not blockers
    if blockers:
        reasons = list(out.get("paper_candidate_reasons") or []) + blockers
        out["paper_candidate_reasons"] = sorted(set(map(str, reasons)))
    return out


def apply_group_fidelity_gate(items: list[dict[str, Any]], report: dict[str, Any]) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    # Run V17 first to preserve old fields, then explicitly retire its cross-stateless
    # equality blocker. V18 correctness is method-specific, not pairwise-final-state equality.
    v17_items, v17_report = v17.apply_group_fidelity_gate(items, report)
    enriched = [_canonical_child(x) if _is_target(_method_id(x.get("method"), x)) else dict(x) for x in v17_items]
    targets = [x for x in enriched if _is_target(x.get("v18_method_id", ""))]
    out = dict(v17_report or {})
    if not targets:
        return enriched, out

    blockers: list[str] = []
    for child in targets:
        method = child.get("v18_method_id") or "unknown"
        for reason in child.get("v18_fidelity_blockers") or []:
            blockers.append(f"{method}:{reason}")

    # MBE_OPTME_V24_GROUP_SUPERSESSION: preserve raw V17 diagnostics, but a
    # generic V17 group blocker may be retired only when every underlying V17
    # method blocker was itself explicitly retired by current method truth.
    old_group_blockers = [str(x) for x in out.get("v17_group_fidelity_blockers") or []]
    unresolved_v17 = []
    for child in targets:
        retired_child = {str(x) for x in (child.get("v18_retired_v17_blockers") or [])}
        for reason in child.get("v17_fidelity_blockers") or []:
            reason = str(reason)
            if reason not in retired_child:
                unresolved_v17.append(f"{child.get('v18_method_id') or 'unknown'}:{reason}")
    retired_group = []
    retained = []
    for reason in old_group_blockers:
        if reason == "stateless_variants_logical_business_state_not_equivalent":
            retired_group.append(reason)
        elif reason == "one_or_more_method_v17_fidelity_blockers" and not unresolved_v17:
            retired_group.append(reason)
        else:
            retained.append(reason)
    base_performance_valid = bool((report or {}).get("performance_comparison_valid"))
    base_paper_candidate = bool((report or {}).get("paper_candidate", True))
    current_blockers = sorted(set(blockers + retained + unresolved_v17))
    out.update({
        "paper_fidelity_closure_version": CLOSURE_VERSION,
        "v18_correctness_model": "method_specific_reference_execution_not_cross_method_final_state_equality",
        "v18_method_specific_correctness_required": True,
        "v18_cross_method_final_state_equality_required": False,
        "v18_retired_v17_group_blockers": sorted(set(retired_group)),
        "v18_retained_legacy_group_blockers": sorted(set(retained)),
        "v18_unresolved_v17_method_blockers": sorted(set(unresolved_v17)),
        "v18_group_fidelity_blockers": current_blockers,
        "performance_comparison_valid": base_performance_valid and not current_blockers,
    })
    out["paper_candidate"] = base_paper_candidate and not current_blockers
    return enriched, out


def apply_fairness_fidelity_gate(rows: list[dict[str, Any]], report: dict[str, Any]) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    rows17, report17 = v17.apply_fairness_fidelity_gate(rows, report)
    out_rows: list[dict[str, Any]] = []
    for row in rows17:
        item = dict(row)
        method = _method_id(item.get("method"), item)
        metrics = item.get("metrics") if isinstance(item.get("metrics"), dict) else {}
        topology = item.get("topology_point") if isinstance(item.get("topology_point"), dict) else {}
        requested = _as_int(topology.get("worker_count")) or _as_int(item.get("worker_count"))
        configured = _method_config_worker_count(item)
        observed_effective = _as_int(metrics.get("v18_effective_worker_count")) or _as_int(metrics.get("v16_effective_worker_count")) or _as_int(metrics.get("worker_count"))
        # MBE_OPTME_V24_WORKER_TRUTH: OptME formal rows also compile the topology
        # worker_count into the executor. Registry default 4 is not runtime truth
        # for an 8-worker formal row.
        topology_compiles_worker = method == "stateless_porygon" or _is_optme(method)
        effective = requested if topology_compiles_worker and observed_effective is None else (observed_effective or configured)
        item["requested_worker_count"] = requested
        item["method_config_worker_count"] = configured
        item["effective_worker_count"] = effective
        item["environment_fairness_passed"] = bool((report17 or {}).get("passed", True))
        item["execution_stack_comparable"] = None if not _is_target(method) else False
        out_rows.append(item)
    out = dict(report17 or {})
    out["v18_fairness_contract_version"] = CLOSURE_VERSION
    out["environment_fairness_passed"] = bool(out.get("passed", True))
    out["worker_count_columns"] = ["requested_worker_count", "method_config_worker_count", "effective_worker_count"]
    out["direct_performance_comparable_requires_same_execution_stack"] = True
    return out_rows, out


# Compatibility entry points for wrappers.
def enrich_extracted_metrics(run_dir: Path | str, method_id: str | None, result: dict[str, Any]) -> dict[str, Any]:
    return enrich_metrics(run_dir, method_id, result)


def apply_truth_closure_to_state_equivalence(items: list[dict[str, Any]], report: dict[str, Any]) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    return apply_group_fidelity_gate(items, report)
