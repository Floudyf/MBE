from __future__ import annotations

import json
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[2]
CROSS_CHAIN_SCHEMA = ROOT / "trace/schema/v2_cross_chain_trace.schema.json"
META_SCHEMA = ROOT / "trace/schema/v2_multi_chain_trace_meta.schema.json"

REQUIRED_RECORD_FIELDS = {
    "schema_version",
    "cross_tx_id",
    "stage_id",
    "stage",
    "source_chain",
    "target_chain",
    "chain_id",
    "submit_time_ms",
    "status",
    "data_truth_label",
}
REQUIRED_META_FIELDS = {
    "schema_version",
    "trace_format",
    "data_truth_label",
    "chain_count",
    "chains",
    "cross_tx_count",
    "stage_record_count",
    "limitations",
}
REQUIRED_CHAIN_FIELDS = {"chain_id", "role", "backend", "block_interval_ms", "finality_depth"}
STAGES = {"created", "source_lock", "source_finality", "cert_generated", "target_mint", "target_finality", "completed", "timeout", "refunded", "failed"}
STATUSES = {"created", "submitted", "committed", "finalized", "completed", "timeout", "refunded", "failed"}
DATA_TRUTH_LABELS = {
    "synthetic_replay",
    "existing_trace_replay",
    "fabric_chain_backed_trace_replay",
    "public_chain_imported_trace_semantic_unknown",
    "planned_cross_chain_replay",
}
CHAIN_ROLES = {"source", "target", "intermediate", "observer"}


def load_json_schema(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def validate_cross_chain_record(record: dict[str, Any]) -> dict[str, Any]:
    errors: list[str] = []
    missing = sorted(REQUIRED_RECORD_FIELDS - record.keys())
    if missing:
        errors.append(f"missing required fields: {missing}")
    if record.get("schema_version") != "v2.cross_chain_trace.v1":
        errors.append("schema_version must be v2.cross_chain_trace.v1")
    for field in ("cross_tx_id", "stage_id", "source_chain", "target_chain", "chain_id"):
        if field in record and (not isinstance(record[field], str) or not record[field]):
            errors.append(f"{field} must be a non-empty string")
    if record.get("stage") not in STAGES:
        errors.append(f"invalid stage: {record.get('stage')}")
    if record.get("status") not in STATUSES:
        errors.append(f"invalid status: {record.get('status')}")
    if record.get("data_truth_label") not in DATA_TRUTH_LABELS:
        errors.append(f"invalid data_truth_label: {record.get('data_truth_label')}")
    for field in ("submit_time_ms", "commit_time_ms", "finality_time_ms", "timeout_deadline_ms", "stage_latency_ms"):
        if field in record and (not isinstance(record[field], (int, float)) or record[field] < 0):
            errors.append(f"{field} must be a non-negative number")
    submit = record.get("submit_time_ms")
    commit = record.get("commit_time_ms")
    finality = record.get("finality_time_ms")
    timeout = record.get("timeout_deadline_ms")
    if isinstance(submit, (int, float)) and isinstance(commit, (int, float)) and commit < submit:
        errors.append("commit_time_ms must be >= submit_time_ms")
    if isinstance(commit, (int, float)) and isinstance(finality, (int, float)) and finality < commit:
        errors.append("finality_time_ms must be >= commit_time_ms")
    if isinstance(submit, (int, float)) and isinstance(timeout, (int, float)) and timeout < submit:
        errors.append("timeout_deadline_ms must be >= submit_time_ms")
    if record.get("data_truth_label") == "public_chain_imported_trace_semantic_unknown":
        for optional_field in ("access_list", "read_set", "write_set", "commutative", "update_type"):
            if optional_field in record:
                continue
        # The schema allows these fields, but the validator never requires them for semantic_unknown traces.
    return {"valid": not errors, "errors": errors, "warnings": []}


def validate_cross_chain_trace_file(path: Path) -> dict[str, Any]:
    errors: list[str] = []
    warnings: list[str] = []
    records = 0
    cross_tx_ids: set[str] = set()
    chains: set[str] = set()
    with path.open(encoding="utf-8") as stream:
        for line_number, line in enumerate(stream, start=1):
            if not line.strip():
                continue
            try:
                record = json.loads(line)
            except json.JSONDecodeError as exc:
                errors.append(f"line {line_number}: invalid JSON: {exc.msg}")
                continue
            result = validate_cross_chain_record(record)
            if not result["valid"]:
                errors.extend(f"line {line_number}: {error}" for error in result["errors"])
            records += 1
            if isinstance(record.get("cross_tx_id"), str):
                cross_tx_ids.add(record["cross_tx_id"])
            if isinstance(record.get("chain_id"), str):
                chains.add(record["chain_id"])
    return {
        "valid": not errors,
        "errors": errors,
        "warnings": warnings,
        "stats": {
            "records": records,
            "cross_tx_count": len(cross_tx_ids),
            "chains": sorted(chains),
        },
    }


def validate_multi_chain_trace_meta(path: Path) -> dict[str, Any]:
    errors: list[str] = []
    try:
        meta = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        return {"valid": False, "errors": [f"invalid JSON: {exc.msg}"], "warnings": [], "meta": {}}
    missing = sorted(REQUIRED_META_FIELDS - meta.keys())
    if missing:
        errors.append(f"missing required fields: {missing}")
    if meta.get("schema_version") != "v2.multi_chain_trace_meta.v1":
        errors.append("schema_version must be v2.multi_chain_trace_meta.v1")
    if meta.get("data_truth_label") not in DATA_TRUTH_LABELS:
        errors.append(f"invalid data_truth_label: {meta.get('data_truth_label')}")
    chains = meta.get("chains")
    if not isinstance(chains, list) or not chains:
        errors.append("chains must be a non-empty list")
    else:
        if meta.get("chain_count") != len(chains):
            errors.append("chain_count must match chains length")
        for index, chain in enumerate(chains):
            missing_chain = sorted(REQUIRED_CHAIN_FIELDS - chain.keys())
            if missing_chain:
                errors.append(f"chains[{index}] missing fields: {missing_chain}")
            if chain.get("role") not in CHAIN_ROLES:
                errors.append(f"chains[{index}] invalid role: {chain.get('role')}")
            for field in ("chain_id", "backend"):
                if field in chain and (not isinstance(chain[field], str) or not chain[field]):
                    errors.append(f"chains[{index}].{field} must be a non-empty string")
            if "block_interval_ms" in chain and (not isinstance(chain["block_interval_ms"], (int, float)) or chain["block_interval_ms"] < 0):
                errors.append(f"chains[{index}].block_interval_ms must be non-negative")
            if "finality_depth" in chain and (not isinstance(chain["finality_depth"], int) or chain["finality_depth"] < 0):
                errors.append(f"chains[{index}].finality_depth must be a non-negative integer")
    if not isinstance(meta.get("limitations"), list) or not meta.get("limitations"):
        errors.append("limitations must be a non-empty list")
    return {"valid": not errors, "errors": errors, "warnings": [], "meta": meta}


def validate_trace_and_meta(trace_path: Path, meta_path: Path) -> dict[str, Any]:
    trace_result = validate_cross_chain_trace_file(trace_path)
    meta_result = validate_multi_chain_trace_meta(meta_path)
    errors = [*trace_result["errors"], *meta_result["errors"]]
    meta = meta_result.get("meta", {})
    if trace_result["valid"] and meta_result["valid"]:
        stats = trace_result["stats"]
        if meta.get("stage_record_count") != stats["records"]:
            errors.append("meta stage_record_count must match trace record count")
        if meta.get("cross_tx_count") != stats["cross_tx_count"]:
            errors.append("meta cross_tx_count must match unique trace cross_tx_id count")
        trace_file = meta.get("trace_file")
        if trace_file and trace_file != trace_path.name:
            errors.append("meta trace_file must match trace filename")
    return {
        "valid": not errors,
        "errors": errors,
        "warnings": [*trace_result["warnings"], *meta_result["warnings"]],
        "stats": trace_result["stats"],
    }
