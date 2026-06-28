from __future__ import annotations

import csv
import json
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import yaml

from backend.app.services.v3_composer_draft_runner import V3_DRAFT_RUNS_ROOT, list_draft_artifacts


REQUIRED_HISTORY_FILES = (
    "composer_draft.json",
    "normalized_draft.json",
    "draft_validation.json",
    "generated_experiment_profile.json",
    "generated_plugin_profile.json",
)


class DraftRunHistoryError(ValueError):
    """Raised when a draft run history request is malformed."""


class DraftRunNotFound(FileNotFoundError):
    """Raised when a draft run directory does not exist."""


def list_v3_draft_runs(limit: int = 20, root: Path | None = None) -> dict[str, list[dict[str, Any]]]:
    base = root or V3_DRAFT_RUNS_ROOT
    if not base.exists():
        return {"runs": []}
    runs = [summarize_draft_run(path, base) for path in _run_dirs(base)]
    runs.sort(key=lambda item: str(item.get("created_at", "")), reverse=True)
    return {"runs": runs[: max(1, min(limit, 100))]}


def get_v3_draft_run_detail(run_id: str, root: Path | None = None) -> dict[str, Any]:
    base = root or V3_DRAFT_RUNS_ROOT
    run_dir = _safe_run_dir(run_id, base)
    if not run_dir.is_dir():
        raise DraftRunNotFound(f"draft run not found: {run_id}")

    composer_draft = _read_json(run_dir / "composer_draft.json")
    normalized_draft = _read_json(run_dir / "normalized_draft.json")
    validation = _read_json(run_dir / "draft_validation.json")
    generated_experiment_profile = _read_json_or_yaml(run_dir, "generated_experiment_profile")
    generated_plugin_profile = _read_json_or_yaml(run_dir, "generated_plugin_profile")
    artifacts = list_draft_artifacts(run_dir, run_id)

    return {
        "run_id": run_id,
        "created_at": _mtime(run_dir),
        "composer_draft": composer_draft,
        "normalized_draft": normalized_draft,
        "validation": validation,
        "generated_experiment_profile": generated_experiment_profile,
        "generated_plugin_profile": generated_plugin_profile,
        "artifact_groups": artifact_groups(artifacts),
        "artifacts": artifacts,
        "summary_preview": _summary_preview(run_dir),
        "missing_files": _missing_files(run_dir),
    }


def summarize_draft_run(run_dir: Path, root: Path | None = None) -> dict[str, Any]:
    base = root or V3_DRAFT_RUNS_ROOT
    run_id = run_dir.name
    _safe_run_dir(run_id, base)
    normalized = _read_json(run_dir / "normalized_draft.json")
    validation = _read_json(run_dir / "draft_validation.json")
    composer = _read_json(run_dir / "composer_draft.json")
    artifacts = list_draft_artifacts(run_dir, run_id)
    selected_plugins = _selected_plugins(normalized, composer)

    return {
        "run_id": run_id,
        "created_at": _mtime(run_dir),
        "template_id": normalized.get("template_id") or composer.get("template_id", ""),
        "run_mode": normalized.get("run_mode") or validation.get("run_mode", "draft_smoke"),
        "is_valid": bool(validation.get("is_valid", False)),
        "is_runnable": bool(validation.get("is_runnable", False)),
        "selected_plugins": selected_plugins,
        "variable_modules": normalized.get("variable_modules", validation.get("variable_modules", [])),
        "fixed_modules": normalized.get("fixed_modules", validation.get("fixed_modules", [])),
        "disabled_modules": normalized.get("disabled_modules", validation.get("disabled_modules", [])),
        "output_modules": normalized.get("output_modules", validation.get("output_modules", [])),
        "artifact_count": len(artifacts),
        "artifact_groups": artifact_groups(artifacts),
        "summary_preview": _summary_preview(run_dir),
        "missing_files": _missing_files(run_dir),
    }


def artifact_groups(artifacts: list[dict[str, Any]]) -> list[dict[str, Any]]:
    groups = [
        ("Draft config", {"composer_draft.json", "normalized_draft.json", "draft_validation.json", "generated_experiment_profile.json", "generated_experiment_profile.yaml", "generated_plugin_profile.json", "generated_plugin_profile.yaml"}),
        ("Run summary", {"summary.csv", "summary.json", "report.md", "latency.csv"}),
        ("Chain runtime logs", {"runtime.log", "block_log.csv", "tx_results.csv", "state_commit_log.csv"}),
        ("MetaTrack metrics", {"metatrack_summary.csv", "metatrack_summary.json", "metatrack_latency.csv", "metatrack_mechanism_metrics.csv", "metatrack_ablation_report.md"}),
        ("Used profiles", {"used_chain_profile.yaml", "used_plugin_profile.yaml", "used_experiment_profile.yaml", "used_chain_profile.json", "used_plugin_profile.json", "used_experiment_profile.json"}),
    ]
    by_name = {artifact["name"]: artifact for artifact in artifacts}
    result = []
    for title, filenames in groups:
        files = [by_name[name] for name in sorted(filenames) if name in by_name]
        if files:
            result.append({"title": title, "files": files})
    return result


def _run_dirs(root: Path) -> list[Path]:
    return [
        path
        for path in root.iterdir()
        if path.is_dir() and path.name != "latest" and _valid_run_id(path.name)
    ]


def _safe_run_dir(run_id: str, root: Path) -> Path:
    if not _valid_run_id(run_id):
        raise DraftRunHistoryError("invalid draft run id")
    base = root.resolve()
    path = (root / run_id).resolve()
    try:
        path.relative_to(base)
    except ValueError as exc:
        raise DraftRunHistoryError("draft run path must stay inside .cache/v3_draft_runs") from exc
    return path


def _valid_run_id(run_id: str) -> bool:
    return bool(re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.-]{0,120}", run_id)) and run_id not in {".", "..", "latest"}


def _read_json(path: Path) -> dict[str, Any]:
    if not path.is_file():
        return {}
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return value if isinstance(value, dict) else {}


def _read_json_or_yaml(run_dir: Path, stem: str) -> dict[str, Any]:
    json_value = _read_json(run_dir / f"{stem}.json")
    if json_value:
        return json_value
    yaml_path = run_dir / f"{stem}.yaml"
    if not yaml_path.is_file():
        return {}
    try:
        value = yaml.safe_load(yaml_path.read_text(encoding="utf-8"))
    except (OSError, yaml.YAMLError):
        return {}
    return value if isinstance(value, dict) else {}


def _summary_preview(run_dir: Path) -> dict[str, Any]:
    json_value = _read_json(run_dir / "summary.json")
    if json_value:
        return _pick_summary_fields(json_value)
    csv_path = run_dir / "summary.csv"
    if not csv_path.is_file():
        return {}
    try:
        with csv_path.open(encoding="utf-8", newline="") as stream:
            row = next(csv.DictReader(stream), {})
    except OSError:
        return {}
    return _pick_summary_fields(row)


def _pick_summary_fields(summary: dict[str, Any]) -> dict[str, Any]:
    keys = ("tx_count", "success_count", "failure_count", "failed_count", "avg_latency_ms", "p95_latency_ms", "p99_latency_ms")
    return {key: summary[key] for key in keys if key in summary}


def _selected_plugins(normalized: dict[str, Any], composer: dict[str, Any]) -> dict[str, str]:
    selection = normalized.get("plugin_selection")
    if isinstance(selection, dict):
        return {str(key): str(value) for key, value in selection.items()}
    modules = composer.get("modules", {})
    if isinstance(modules, dict):
        return {
            str(module_id): str(module.get("plugin", ""))
            for module_id, module in modules.items()
            if isinstance(module, dict) and module.get("plugin")
        }
    return {}


def _missing_files(run_dir: Path) -> list[str]:
    missing = [filename for filename in REQUIRED_HISTORY_FILES if not (run_dir / filename).is_file()]
    if not (run_dir / "summary.csv").is_file() and not (run_dir / "summary.json").is_file():
        missing.append("summary.csv or summary.json")
    if not (run_dir / "runtime.log").is_file():
        missing.append("runtime.log")
    return missing


def _mtime(path: Path) -> str:
    return datetime.fromtimestamp(path.stat().st_mtime, tz=timezone.utc).isoformat()
