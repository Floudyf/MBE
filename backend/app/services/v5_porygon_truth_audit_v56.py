"""Porygon v5.6 read-only exact identity and per-height wait evidence.

Never rewrites finality, paper status, proposer state, consensus, or timeouts.
Missing identity evidence stays explicitly unavailable, never inferred.
"""
from __future__ import annotations

import csv
import json
from collections import defaultdict
from pathlib import Path

IDENTITY_NAME = "porygon_identity_audit_v56.csv"
WAIT_NAME = "porygon_height_wait_v56.csv"
SUMMARY_NAME = "porygon_truth_audit_v56.json"


def _rows(path: Path):
    if not path.is_file():
        return
    with path.open("r", encoding="utf-8-sig", newline="") as source:
        for row in csv.DictReader(source):
            yield row


def _dump_csv(path: Path, fields: list[str], records: list[dict]):
    with path.open("w", encoding="utf-8", newline="") as target:
        writer = csv.DictWriter(target, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(records)


def _number(value) -> float:
    try:
        return max(0.0, float(value or 0))
    except (TypeError, ValueError):
        return 0.0


def _finality_success(v: str) -> bool:
    return str(v).strip().lower() in ("true", "1", "yes")


def export_porygon_truth_audit(run_dir: Path) -> dict[str, object]:
    run_dir = Path(run_dir)
    node_files = sorted((run_dir / "nodes").glob("*/porygon_paper_lifecycle_v56.csv"))
    finality_file = run_dir / "transaction_finality.csv"
    lifecycle_file = run_dir / "transaction_lifecycle.csv"
    status_by_node: dict[str, dict[str, dict]] = {}
    duplicates = []
    for path in node_files:
        current = {}
        for row in _rows(path):
            txid = str(row.get("tx_id") or "").strip()
            if not txid:
                continue
            if txid in current and current[txid] != row:
                duplicates.append({"node": path.parent.name, "tx_id": txid})
            current[txid] = row
        status_by_node[path.parent.name] = current

    finality = list(_rows(finality_file)) if finality_file.is_file() else []
    success = {str(row.get("logical_tx_id") or "").strip(): row for row in finality
               if str(row.get("logical_tx_id") or "").strip() and _finality_success(row.get("success", ""))}
    # Map signed TxID to the supervisor's logical semantic ID from actual
    # lifecycle rows, instead of assuming these two IDs are interchangeable.
    physical_to_logical: dict[str, set[str]] = defaultdict(set)
    logical_to_physical: dict[str, set[str]] = defaultdict(set)
    if success and lifecycle_file.is_file():
        for row in _rows(lifecycle_file):
            logical = str(row.get("logical_tx_id") or "").strip()
            physical = str(row.get("tx_id") or "").strip()
            if logical in success and physical:
                logical_to_physical[logical].add(physical)
                physical_to_logical[physical].add(logical)
    fields = ["logical_tx_id", "terminal_stage", "finality_success", "physical_tx_ids",
              "mapping", "replica_statuses", "paper_any_committed", "paper_all_committed",
              "paper_any_noncommitted", "paper_absent_on_all"]
    comparison = []
    mismatch = 0
    ambiguous = 0
    for logical in sorted(success):
        ids = sorted(logical_to_physical.get(logical, ()))
        mapping = "unique" if len(ids) == 1 and len(physical_to_logical[ids[0]]) == 1 else ("missing" if not ids else "ambiguous")
        if mapping != "unique":
            ambiguous += 1
        statuses = {}
        for node, paper in sorted(status_by_node.items()):
            values = sorted({str(paper[physical]["paper_status"]) for physical in ids if physical in paper})
            statuses[node] = "|".join(values) if values else "absent"
        present = [s for s in statuses.values() if s != "absent"]
        any_committed = any("committed" in s.split("|") for s in present)
        all_committed = bool(statuses) and all(s == "committed" for s in statuses.values())
        any_noncommitted = any(any(x != "committed" for x in s.split("|")) for s in present)
        absent = bool(statuses) and not present
        if mapping != "unique" or not all_committed:
            mismatch += 1
        comparison.append({
            "logical_tx_id":logical,"terminal_stage":success[logical].get("terminal_stage", ""),
            "finality_success":"true","physical_tx_ids":"|".join(ids), "mapping":mapping,
            "replica_statuses":json.dumps(statuses,sort_keys=True,ensure_ascii=False),
            "paper_any_committed":str(any_committed).lower(),"paper_all_committed":str(all_committed).lower(),
            "paper_any_noncommitted":str(any_noncommitted).lower(),"paper_absent_on_all":str(absent).lower(),
        })

    audit_ready = bool(status_by_node) and finality_file.is_file() and lifecycle_file.is_file()
    if audit_ready:
        _dump_csv(run_dir / IDENTITY_NAME, fields, comparison)

    wait_rows=[]
    for path in sorted((run_dir / "nodes").glob("*/block_execution_summary.json")):
        try:
            with path.open("r",encoding="utf-8") as f:
                data=json.load(f)
        except (OSError,ValueError):
            continue
        if data.get("block_executor_id") != "porygon_block_executor":
            continue
        for entry in data.get("blocks",[]):
            if not isinstance(entry,dict): continue
            business=_number(entry.get("porygon_business_execution_us"))
            exchange=_number(entry.get("porygon_result_exchange_wait_us"))
            critical=_number(entry.get("porygon_execution_critical_path_us"))
            wait_rows.append({
              "node_id":path.parent.name,"height":entry.get("height",""),
              "block_hash":entry.get("block_hash", ""),
              "business_us":round(business),"exchange_wait_us":round(exchange),
              "critical_path_us":round(critical),"other_critical_us":round(max(0,critical-business-exchange)),
              "exchange_dominates_business":str(exchange>business).lower(),
              "local_execution_role":entry.get("porygon_local_execution_shard_id", ""),
              "execution_started_ns":entry.get("porygon_pipeline_execution_started_at_ns", ""),
              "execution_finished_ns":entry.get("porygon_pipeline_execution_finished_at_ns", ""),
            })
    wait_rows.sort(key=lambda r:(int(r["height"] or 0),r["node_id"],str(r["block_hash"])))
    if wait_rows:
        _dump_csv(run_dir / WAIT_NAME, ["node_id","height","block_hash","business_us",
            "exchange_wait_us","critical_path_us","other_critical_us",
            "exchange_dominates_business","local_execution_role","execution_started_ns",
            "execution_finished_ns"],wait_rows)
    top_waits=sorted(wait_rows,key=lambda r:-r["exchange_wait_us"])[:12]
    summary={
       "evidence_version":"porygon_truth_audit_v56",
       "identity_available":audit_ready,
       "paper_replica_count":len(status_by_node),
       "successful_finality_count_from_ids":len(success),
       "successful_finality_identity_exception_count":mismatch if audit_ready else None,
       "ambiguous_or_missing_logical_physical_mapping_count":ambiguous if audit_ready else None,
       "paper_duplicate_identity_count":len(duplicates),
       "comparison_semantics":"paper status per replica at final WriteArtifacts, not a certificate; no invented state",
       "wait_available":bool(wait_rows),
       "per_node_block_wait_row_count":len(wait_rows),
       "top_exchange_waits":top_waits,
       "no_protocol_or_finality_mutation":True,
    }
    if audit_ready or wait_rows:
        (run_dir / SUMMARY_NAME).write_text(json.dumps(summary,ensure_ascii=False,indent=2,sort_keys=True)+"\n",encoding="utf-8")
    return {
      "porygon_v56_identity_audit_available":audit_ready,
      "porygon_v56_successful_finality_identity_exception_count":mismatch if audit_ready else None,
      "porygon_v56_ambiguous_mapping_count":ambiguous if audit_ready else None,
      "porygon_v56_wait_audit_available":bool(wait_rows),
      "porygon_v56_wait_audit_row_count":len(wait_rows),
      "porygon_v56_audit_artifacts":([IDENTITY_NAME] if audit_ready else []) +
            ([WAIT_NAME] if wait_rows else []) + ([SUMMARY_NAME] if audit_ready or wait_rows else []),
    }
