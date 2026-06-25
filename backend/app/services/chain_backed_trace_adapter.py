from __future__ import annotations

import gzip
import json
from pathlib import Path
from typing import Any, Iterable

from backend.app.services.trace_source_service import ROOT

FABRIC_SMOKE_DIR = ROOT / ".cache/fabric_smoke/latest"
FABRIC_SMOKE_COMMAND = "python scripts/v1_fabric_smoke.py --strict --channel mbechannel --out .cache/fabric_smoke/latest"


def detect_fabric_smoke_trace(fabric_smoke_dir: Path = FABRIC_SMOKE_DIR) -> dict[str, Any]:
    trace_path = fabric_smoke_dir / "trace.jsonl.gz"
    meta_path = fabric_smoke_dir / "trace_meta.json"
    ready = trace_path.is_file() and meta_path.is_file()
    warnings = ["This endpoint only checks files. It never starts Docker, Fabric, or network.sh."]
    if not ready:
        warnings.append("Fabric smoke trace is missing. Generate it manually with the CLI command.")
    return {
        "status": "ready" if ready else "missing",
        "ready": ready,
        "trace_path": ".cache/fabric_smoke/latest/trace.jsonl.gz",
        "meta_path": ".cache/fabric_smoke/latest/trace_meta.json",
        "trace_exists": trace_path.is_file(),
        "meta_exists": meta_path.is_file(),
        "data_truth_label": "fabric_chain_backed_trace_replay",
        "web_starts_fabric": False,
        "cli_command": FABRIC_SMOKE_COMMAND,
        "warnings": warnings,
    }


def _open_text(path: Path):
    if path.suffix == ".gz":
        return gzip.open(path, "rt", encoding="utf-8")
    return path.open(encoding="utf-8")


def iter_jsonl(path: Path) -> Iterable[dict[str, Any]]:
    with _open_text(path) as stream:
        for line in stream:
            if line.strip():
                yield json.loads(line)


def load_chain_backed_trace_meta(meta_path: Path) -> dict[str, Any]:
    if not meta_path.is_file():
        return {}
    try:
        return json.loads(meta_path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return {"warning": "trace_meta.json is not valid JSON"}


def _record_id(record: dict[str, Any], index: int) -> str:
    for field in ("stage_id", "tx_id", "transaction_id", "id"):
        if record.get(field):
            return str(record[field])
    return f"record_{index:06d}"


def _observed_latency(record: dict[str, Any]) -> Any:
    if record.get("stage_latency_ms") not in {None, ""}:
        return record.get("stage_latency_ms")
    submit = record.get("submit_time_ms")
    finality = record.get("finality_time_ms")
    commit = record.get("commit_time_ms")
    if isinstance(submit, (int, float)) and isinstance(finality, (int, float)):
        return finality - submit
    if isinstance(submit, (int, float)) and isinstance(commit, (int, float)):
        return commit - submit
    return ""


def extract_observed_records(trace_path: Path, limit: int | None = None) -> dict[str, Any]:
    records: list[dict[str, Any]] = []
    warnings: list[str] = []
    for index, record in enumerate(iter_jsonl(trace_path), start=1):
        adapted = {
            "record_id": _record_id(record, index),
            "tx_id": str(record.get("tx_id") or record.get("transaction_id") or record.get("id") or ""),
            "cross_tx_id": str(record.get("cross_tx_id") or ""),
            "stage_id": str(record.get("stage_id") or _record_id(record, index)),
            "stage": str(record.get("stage") or record.get("operation") or "unknown"),
            "chain_id": str(record.get("chain_id") or record.get("channel") or "fabric_smoke"),
            "submit_time_ms": record.get("submit_time_ms", ""),
            "observed_commit_time_ms": record.get("commit_time_ms", ""),
            "observed_finality_time_ms": record.get("finality_time_ms", ""),
            "observed_latency_ms": _observed_latency(record),
        }
        if not adapted["cross_tx_id"]:
            warnings.append("Fabric smoke trace is chain-backed but not full cross-chain trace.")
        if adapted["observed_commit_time_ms"] == "" and adapted["observed_finality_time_ms"] == "":
            warnings.append("Observed timing fields are incomplete for at least one record.")
        records.append(adapted)
        if limit and len(records) >= limit:
            break
    return {
        "records": records,
        "warnings": sorted(set(warnings)),
        "scope": "cross_chain_trace" if all(record.get("cross_tx_id") for record in records) else "single_chain_fabric_smoke",
    }


def adapt_trace_for_calibration(trace_path: Path, meta_path: Path | None = None) -> dict[str, Any]:
    observed = extract_observed_records(trace_path)
    meta = load_chain_backed_trace_meta(meta_path) if meta_path else {}
    warnings = list(observed["warnings"])
    if observed["scope"] == "single_chain_fabric_smoke":
        warnings.append("Single-chain Fabric smoke can calibrate replay timing but does not represent a full cross-chain protocol trace.")
    return {
        "status": "ready",
        "scope": observed["scope"],
        "meta": meta,
        "records": observed["records"],
        "warnings": sorted(set(warnings)),
    }
