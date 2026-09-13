from __future__ import annotations

import json
import shutil
import tempfile
import zipfile
from datetime import UTC, datetime
from pathlib import Path

from backend.app.core.paths import ROOT, RUNTIME_ROOT
from backend.app.services.v5_formal_run_store import children, group_dir, read_group, write_child, write_group
from backend.app.services.v5_paper_exporter import export as export_paper
from backend.app.services.v5_timeout_extrapolation import analyze_run_timeout_extrapolation

SCHEMA_VERSION = "mbe_v5_timeout_extrapolation_backfill_v1"
TERMINAL_GROUP_STATUSES = {"completed", "completed_with_failures", "failed", "cancelled"}


def _is_hard_timeout_child(child: dict) -> bool:
    status = str(child.get("status") or "")
    if status not in {"failed", "timed_out"}:
        return False
    error = str(child.get("error") or "").lower()
    result = child.get("result") if isinstance(child.get("result"), dict) else {}
    summary = result.get("summary") if isinstance(result.get("summary"), dict) else {}
    root_failure = str(summary.get("root_failure") or "").lower()
    return "hard timeout" in error or "hard_timeout" in error or "hard timeout" in root_failure or "hard_timeout" in root_failure


def _inside(path: Path, root: Path) -> bool:
    try:
        path.relative_to(root)
        return True
    except ValueError:
        return False


def _resolved_output_dir(child: dict) -> Path | None:
    result = child.get("result") if isinstance(child.get("result"), dict) else {}
    raw = str(result.get("output_dir") or "").strip()
    if not raw:
        return None
    normalized = raw.replace("\\", "/")
    runtime_prefix = "$MBE_RUNTIME_ROOT/"
    if normalized.startswith(runtime_prefix):
        candidate = (RUNTIME_ROOT / normalized[len(runtime_prefix):]).resolve()
        runtime_root = RUNTIME_ROOT.resolve()
        return candidate if candidate == runtime_root or _inside(candidate, runtime_root) else None
    candidate = (ROOT / normalized).resolve()
    repo_root = ROOT.resolve()
    if candidate == repo_root or _inside(candidate, repo_root):
        return candidate
    runtime_root = RUNTIME_ROOT.resolve()
    if candidate == runtime_root or _inside(candidate, runtime_root):
        return candidate
    return None


_REQUIRED_DIAGNOSTIC_FILES = (
    "drain_progress.csv",
    "drain_status.json",
    "stalled_runtime_report.json",
    "client/client_submission_complete.json",
)


def _bundle_diagnostics(directory: Path, child_id: str, scratch_root: Path) -> Path | None:
    bundle = directory / "artifacts.zip"
    if not bundle.is_file():
        return None
    prefix = f"runtime_diagnostics/{child_id}/"
    try:
        with zipfile.ZipFile(bundle, "r") as archive:
            names = set(archive.namelist())
            required = [prefix + relative for relative in _REQUIRED_DIAGNOSTIC_FILES]
            if any(name not in names for name in required):
                return None
            target = scratch_root / child_id
            for relative, name in zip(_REQUIRED_DIAGNOSTIC_FILES, required):
                destination = target / Path(relative)
                destination.parent.mkdir(parents=True, exist_ok=True)
                destination.write_bytes(archive.read(name))
            return target
    except (OSError, zipfile.BadZipFile, KeyError):
        return None


def _find_diagnostics(directory: Path, child: dict, scratch_root: Path) -> tuple[Path | None, str]:
    child_id = str(child.get("child_run_id") or "")
    direct = directory / "runtime_diagnostics" / child_id
    if (direct / "drain_progress.csv").is_file():
        return direct, "run_group_runtime_diagnostics"
    resolved = _resolved_output_dir(child)
    if resolved is not None and (resolved / "drain_progress.csv").is_file():
        return resolved, "live_runtime_output"
    bundled = _bundle_diagnostics(directory, child_id, scratch_root)
    if bundled is not None:
        return bundled, "run_group_artifacts_zip"
    return None, ""


def _attach_evidence(child: dict, evidence: dict, analyzed_at: str) -> dict:
    updated = dict(child)
    result = dict(updated.get("result") or {})
    summary = dict(result.get("summary") or {})
    qualified = evidence.get("qualified_for_tps_estimate") is True
    summary.update({
        "timeout_extrapolation": evidence,
        "throughput_measurement_mode": "timeout_extrapolated" if qualified else "timeout_unstable",
        "throughput_estimate_eligible": qualified,
        "estimated_end_to_end_tps": evidence.get("estimated_end_to_end_tps") if qualified else None,
        "tail_regression_tps": evidence.get("tail_regression_tps"),
        "timeout_extrapolation_note": (
            "historical hard-timeout analysis; estimated fixed-workload completion TPS, original execution status preserved"
            if qualified
            else "historical hard-timeout analysis; tail stability criteria were not satisfied"
        ),
    })
    result["summary"] = summary
    updated["result"] = result
    updated["throughput_measurement_mode"] = summary["throughput_measurement_mode"]
    updated["throughput_estimate_eligible"] = qualified
    updated["timeout_extrapolation_analyzed_at"] = analyzed_at
    return updated


def analyze_group_timeout_extrapolation(group_id: str) -> dict:
    group = read_group(group_id)
    status = str(group.get("status") or "")
    if status not in TERMINAL_GROUP_STATUSES:
        raise ValueError("timeout extrapolation analysis requires a terminal RunGroup")

    directory = group_dir(group_id)
    items = children(group_id)
    analyzed_at = datetime.now(UTC).isoformat()
    candidates = [item for item in items if _is_hard_timeout_child(item)]
    qualified_rows: list[dict] = []
    unstable_rows: list[dict] = []
    skipped_rows: list[dict] = []
    updates: list[tuple[dict, dict]] = []

    with tempfile.TemporaryDirectory(prefix="mbe_timeout_extrapolation_") as scratch:
        scratch_root = Path(scratch)
        for child in candidates:
            child_id = str(child.get("child_run_id") or "")
            diagnostics, evidence_source = _find_diagnostics(directory, child, scratch_root)
            if diagnostics is None:
                skipped_rows.append({"child_run_id": child_id, "reason": "runtime_diagnostics_not_found_in_live_or_bundle"})
                continue
            evidence = analyze_run_timeout_extrapolation(diagnostics)
            if not evidence:
                skipped_rows.append({"child_run_id": child_id, "reason": "hard_timeout_evidence_not_confirmed", "evidence_source": evidence_source})
                continue
            evidence = {**evidence, "historical_evidence_source": evidence_source}
            updates.append((child, evidence))
            row = {
                "child_run_id": child_id,
                "method_config_id": child.get("method_config_id"),
                "method_name": (child.get("method") or {}).get("display_name"),
                "seed": child.get("seed"),
                "repeat_index": child.get("repeat_index"),
                "scan_variable": child.get("scan_variable"),
                "scan_value": child.get("scan_value"),
                "target_theta": (child.get("workload_point") or {}).get("target_theta"),
                "qualified": evidence.get("qualified_for_tps_estimate") is True,
                "measurement_type": evidence.get("measurement_type"),
                "evidence_source": evidence_source,
                "estimated_end_to_end_tps": evidence.get("estimated_end_to_end_tps"),
                "tail_regression_tps": evidence.get("tail_regression_tps"),
                "tail_cv": evidence.get("tail_cv"),
                "tail_relative_drift": evidence.get("tail_relative_drift"),
                "tail_terminal_r2": evidence.get("tail_terminal_r2"),
                "estimated_remaining_seconds": evidence.get("estimated_remaining_seconds"),
                "estimated_total_duration_seconds": evidence.get("estimated_total_duration_seconds"),
                "reasons": evidence.get("reasons") or [],
            }
            (qualified_rows if row["qualified"] else unstable_rows).append(row)

    backup_dir: Path | None = None
    if updates:
        backup_dir = directory / ".timeout_extrapolation_backups" / datetime.now(UTC).strftime("%Y%m%d_%H%M%S_%f")
        backup_dir.mkdir(parents=True, exist_ok=False)
        evidence_dir = directory / "timeout_extrapolation_evidence"
        evidence_dir.mkdir(parents=True, exist_ok=True)
        for child, evidence in updates:
            child_id = str(child.get("child_run_id") or "")
            child_path = directory / "children" / f"{child_id}.json"
            if child_path.is_file():
                shutil.copy2(child_path, backup_dir / child_path.name)
            (evidence_dir / f"{child_id}.json").write_text(
                json.dumps(evidence, indent=2, sort_keys=True) + "\n", encoding="utf-8"
            )
            updated = _attach_evidence(child, evidence, analyzed_at)
            # Preserve execution truth exactly.
            for field in ("status", "execution_status", "formal_eligibility", "paper_candidate", "error"):
                if updated.get(field) != child.get(field):
                    raise RuntimeError(f"timeout extrapolation attempted to change protected child field: {field}")
            write_child(group_id, updated)

    refreshed_children = children(group_id)
    aggregate = export_paper(directory, group, refreshed_children)
    group["aggregate"] = aggregate
    group["timeout_extrapolation_analysis"] = {
        "schema_version": SCHEMA_VERSION,
        "analyzed_at": analyzed_at,
        "candidate_count": len(candidates),
        "analyzed_count": len(updates),
        "qualified_count": len(qualified_rows),
        "unstable_count": len(unstable_rows),
        "skipped_count": len(skipped_rows),
        "original_execution_truth_preserved": True,
        "latency_extrapolation_enabled": False,
    }
    write_group(group)

    return {
        "schema_version": SCHEMA_VERSION,
        "run_group_id": group_id,
        "group_status": status,
        "candidate_count": len(candidates),
        "analyzed_count": len(updates),
        "qualified_count": len(qualified_rows),
        "unstable_count": len(unstable_rows),
        "skipped_count": len(skipped_rows),
        "qualified": qualified_rows,
        "unstable": unstable_rows,
        "skipped": skipped_rows,
        "backup_dir": str(backup_dir.relative_to(directory)) if backup_dir is not None else None,
        "timeout_tps_csv": "timeout_extrapolated_tps.csv",
        "paper_figure_tps_csv": "paper_figure_tps_with_timeout_extrapolation.csv",
        "original_execution_truth_preserved": True,
        "latency_extrapolation_enabled": False,
        "bundle_rebuild_recommended": bool(updates),
    }
