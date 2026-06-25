from __future__ import annotations

from pathlib import Path
from typing import Any

from backend.app.services.trace_source_service import ROOT, TraceSourceRegistry, load_trace_sources

FABRIC_SMOKE_DIR = ROOT / ".cache/fabric_smoke/latest"
FABRIC_SMOKE_COMMAND = "python scripts/v1_fabric_smoke.py --strict --channel mbechannel --out .cache/fabric_smoke/latest"
PUBLIC_CHAIN_SEMANTIC_WARNING = "semantic_unknown: access_list/read_set/write_set/commutative/update_type are not guaranteed."


def validate_trace_source(
    payload: dict[str, Any],
    registry: TraceSourceRegistry | None = None,
    workspace_root: Path = ROOT,
    fabric_smoke_dir: Path = FABRIC_SMOKE_DIR,
) -> dict[str, Any]:
    registry = registry or load_trace_sources()
    source_id = str(payload.get("source_id", ""))
    source = registry.get_source(source_id)
    if source_id == "synthetic":
        return validate_synthetic(source)
    if source_id == "existing_trace":
        return validate_existing_trace(source, payload, workspace_root)
    if source_id == "fabric_chain_backed_trace":
        return validate_fabric_chain_backed_trace(source, fabric_smoke_dir)
    if source_id == "public_chain_imported_trace":
        return validate_public_chain_imported_trace(source, payload, workspace_root)
    return _result(source, "invalid", False, warnings=[], blocked_by=[f"unsupported_source:{source_id}"])


def validate_synthetic(source: dict[str, Any]) -> dict[str, Any]:
    return _result(
        source,
        "ready",
        True,
        warnings=["Synthetic replay is not real chain execution."],
        blocked_by=[],
    )


def validate_existing_trace(source: dict[str, Any], payload: dict[str, Any], workspace_root: Path) -> dict[str, Any]:
    trace_path_text = str(payload.get("trace_path", ""))
    path_result = _safe_workspace_path(trace_path_text, workspace_root)
    if path_result["error"]:
        return _result(source, "invalid", False, trace_path=trace_path_text, warnings=[], blocked_by=[path_result["error"]])
    trace_path = path_result["path"]
    if not trace_path.is_file():
        return _result(source, "missing_file", False, trace_path=str(trace_path), warnings=[], blocked_by=["trace_path_missing"])
    warnings = []
    if not (trace_path.name == "trace.jsonl.gz" or trace_path.name.endswith(".jsonl.gz") or trace_path.name.endswith(".jsonl")):
        warnings.append("Trace filename is accepted but should normally be trace.jsonl.gz or *.jsonl.gz.")
    meta_path = trace_path.with_name("trace_meta.json")
    result = _result(source, "ready", True, trace_path=str(trace_path), warnings=warnings, blocked_by=[])
    result["meta_detected"] = meta_path.is_file()
    result["size_bytes"] = trace_path.stat().st_size
    return result


def validate_fabric_chain_backed_trace(source: dict[str, Any], fabric_smoke_dir: Path) -> dict[str, Any]:
    trace_path = fabric_smoke_dir / "trace.jsonl.gz"
    meta_path = fabric_smoke_dir / "trace_meta.json"
    ready = trace_path.is_file() and meta_path.is_file()
    result = _result(
        source,
        "ready" if ready else "missing",
        ready,
        trace_path=str(trace_path),
        warnings=["This API only checks Fabric smoke trace files; it never starts Docker, Fabric, or network.sh."],
        blocked_by=[] if ready else ["fabric_smoke_trace_missing"],
    )
    result["ready"] = ready
    result["trace_exists"] = trace_path.is_file()
    result["meta_detected"] = meta_path.is_file()
    result["cli_command"] = FABRIC_SMOKE_COMMAND
    return result


def validate_public_chain_imported_trace(source: dict[str, Any], payload: dict[str, Any], workspace_root: Path) -> dict[str, Any]:
    trace_path_text = str(payload.get("trace_path", ""))
    warnings = [PUBLIC_CHAIN_SEMANTIC_WARNING, "No live public-chain ingestion is implemented in V2.3."]
    blocked_by = ["live public-chain ingestion is not implemented"]
    status = "experimental"
    runnable = False
    trace_path = ""
    if trace_path_text:
        path_result = _safe_workspace_path(trace_path_text, workspace_root)
        if path_result["error"]:
            return _result(source, "invalid", False, trace_path=trace_path_text, warnings=warnings, blocked_by=[path_result["error"], *blocked_by])
        trace = path_result["path"]
        trace_path = str(trace)
        if not trace.is_file():
            status = "missing_file"
            blocked_by.insert(0, "trace_path_missing")
    return _result(source, status, runnable, trace_path=trace_path, warnings=warnings, blocked_by=blocked_by)


def _safe_workspace_path(path_text: str, workspace_root: Path) -> dict[str, Any]:
    if not path_text:
        return {"path": None, "error": "trace_path_required"}
    path = Path(path_text)
    if not path.is_absolute():
        path = workspace_root / path
    resolved_root = workspace_root.resolve()
    resolved_path = path.resolve()
    try:
        resolved_path.relative_to(resolved_root)
    except ValueError:
        return {"path": None, "error": "trace_path_outside_workspace"}
    return {"path": resolved_path, "error": ""}


def _result(
    source: dict[str, Any],
    status: str,
    runnable: bool,
    warnings: list[str],
    blocked_by: list[str],
    trace_path: str = "",
) -> dict[str, Any]:
    return {
        "source_id": source["id"],
        "status": status,
        "runnable": runnable,
        "data_truth_label": source["data_truth_label"],
        "trace_path": trace_path,
        "capabilities": source["capabilities"],
        "limitations": source["limitations"],
        "warnings": warnings,
        "blocked_by": blocked_by,
    }
