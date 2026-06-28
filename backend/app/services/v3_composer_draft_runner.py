from __future__ import annotations

import json
import re
import shutil
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import yaml

from backend.app.models.v3_composer_draft import V3ComposerDraftRequest, V3DraftValidationResponse
from backend.app.services.artifact_manager import ARTIFACT_ALLOWLIST, get_artifact_path
from backend.app.services.job_manager import JobManager
from backend.app.services.v3_composer_catalog import GO_RUNTIME_PLUGIN_CLASSES
from backend.app.services.v3_composer_draft_validator import model_dump, validate_v3_composer_draft
from backend.app.services.v3_go_runtime_runner import ROLE_SEPARATED_CHAIN_PROFILE, run_go_v3_runtime


ROOT = Path(__file__).resolve().parents[3]
V3_DRAFT_RUNS_ROOT = ROOT / ".cache" / "v3_draft_runs"
DRAFT_PLUGIN_PROFILE_ID = "composer_draft_single"


class DraftSmokeNotRunnable(ValueError):
    def __init__(self, validation: V3DraftValidationResponse) -> None:
        super().__init__("Composer Draft is not runnable")
        self.validation = validation


def draft_job_manager(root: Path = V3_DRAFT_RUNS_ROOT) -> JobManager:
    return JobManager(root)


def run_v3_composer_draft_smoke(request: V3ComposerDraftRequest, root: Path = V3_DRAFT_RUNS_ROOT) -> dict[str, Any]:
    validation = validate_v3_composer_draft(request)
    if not validation.is_runnable or validation.normalized_draft is None:
        raise DraftSmokeNotRunnable(validation)

    manager = draft_job_manager(root)
    metadata = manager.create_run(
        source="v3_composer_draft",
        experiment_name="composer_draft_smoke",
        data_truth_label="modular_runtime",
        stage="V3.3.5b",
        extra_metadata={
            "backend_type": "modular_research_chain",
            "runtime_mode": "go_backed",
            "run_mode": "draft_smoke",
            "template_id": request.template_id,
        },
    )
    run_id = metadata["run_id"]
    run_dir = manager.run_dir(run_id)
    manager.mark_running(run_id)

    try:
        write_draft_inputs(run_dir, request, validation)
        experiment_profile_path = run_dir / "generated_experiment_profile.yaml"
        plugin_profile_path = run_dir / "generated_plugin_profile.yaml"
        result = run_go_v3_runtime(
            experiment_profile_path=experiment_profile_path,
            plugin_profile_path=plugin_profile_path,
            plugin_profile_id=DRAFT_PLUGIN_PROFILE_ID,
            chain_profile_path=ROLE_SEPARATED_CHAIN_PROFILE,
            output_dir=run_dir,
        )
        _mirror_latest(run_dir, root / "latest")
        completed = manager.mark_completed(run_id, data_truth_label="modular_runtime")
        return {
            "run_id": run_id,
            "job_id": run_id,
            "status": "completed",
            "stage": "V3.3.5b",
            "output_dir": str(run_dir),
            "data_truth_label": "modular_runtime",
            "backend_type": "modular_research_chain",
            "runtime_mode": "go_backed",
            "run_mode": "draft_smoke",
            "validation": model_dump(validation),
            "summary": model_dump(result.summary) if hasattr(result.summary, "__dict__") else result.summary,
            "artifacts": list_draft_artifacts(run_dir, run_id),
            "run": completed,
        }
    except Exception:
        manager.mark_failed(run_id, "draft smoke failed")
        raise


def write_draft_inputs(run_dir: Path, request: V3ComposerDraftRequest, validation: V3DraftValidationResponse) -> None:
    run_dir.mkdir(parents=True, exist_ok=True)
    normalized = validation.normalized_draft or {}
    experiment_profile = build_experiment_profile(normalized)
    plugin_profile = build_plugin_profile(normalized)

    write_json(run_dir / "composer_draft.json", model_dump(request))
    write_json(run_dir / "normalized_draft.json", normalized)
    write_json(run_dir / "draft_validation.json", model_dump(validation))
    write_json(run_dir / "generated_experiment_profile.json", experiment_profile)
    write_json(run_dir / "generated_plugin_profile.json", plugin_profile)
    write_yaml(run_dir / "generated_experiment_profile.yaml", experiment_profile)
    write_yaml(run_dir / "generated_plugin_profile.yaml", plugin_profile)


def build_experiment_profile(normalized: dict[str, Any]) -> dict[str, Any]:
    modules = normalized.get("modules", {})
    workload = modules.get("Workload", {})
    workload_params = workload.get("params", {}) if isinstance(workload, dict) else {}
    block_params = modules.get("BlockProducer", {}).get("params", {}) if isinstance(modules.get("BlockProducer"), dict) else {}
    storage_params = modules.get("StateStorage", {}).get("params", {}) if isinstance(modules.get("StateStorage"), dict) else {}

    tx_count = bounded_int(workload_params.get("tx_count"), 24, 1, 500)
    seed = bounded_int(workload_params.get("seed"), 42, 0, 1_000_000)
    submit_rate = bounded_int(workload_params.get("submit_rate_tps"), 120, 1, 10_000)
    hotspot_ratio = bounded_float(workload_params.get("hotspot_ratio"), 0.25, 0.0, 1.0)

    return {
        "profile_id": f"draft_smoke_{datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S')}",
        "stage": "v3.3.5b",
        "type": "draft_smoke",
        "truth_label": "modular_runtime",
        "backend_type": "modular_research_chain",
        "runtime_mode": "go_backed",
        "experiment_template": normalized.get("template_id", "metatrack_ablation"),
        "chain_profile": "single_chain_research_default",
        "run_level": "smoke",
        "tx_count": tx_count,
        "seed": seed,
        "submit_rate": submit_rate,
        "key_count": 32,
        "hot_key_count": 4,
        "hotspot_ratio": hotspot_ratio,
        "access_list_enabled": normalized.get("plugin_selection", {}).get("StateAccess") == "access_list_prefetch",
        "aggregation_candidates_enabled": normalized.get("plugin_selection", {}).get("Commit") == "hot_update_aggregation_commit",
        "block_interval_ms": bounded_int(block_params.get("block_interval_ms"), 100, 1, 60_000),
        "max_tx_per_block": bounded_int(block_params.get("max_tx_per_block"), 500, 1, 10_000),
        "state": {
            "storage_unit_count": bounded_int(storage_params.get("storage_unit_count"), 4, 1, 128),
            "placement_policy": storage_params.get("placement_policy", "hash_state_storage"),
            "remote_fetch_cost_ms": bounded_int(storage_params.get("remote_fetch_cost_ms"), 1, 0, 10_000),
        },
        "plugin_selection": normalized.get("plugin_selection", {}),
    }


def build_plugin_profile(normalized: dict[str, Any]) -> dict[str, Any]:
    selection = normalized.get("plugin_selection", {})
    plugins = {
        plugin_class: selection.get(module_id)
        for module_id, plugin_class in GO_RUNTIME_PLUGIN_CLASSES.items()
        if selection.get(module_id)
    }
    return {
        "profile_type": "plugin_profile_collection",
        "version": "v3",
        "stage": "v3.3.5b",
        "profiles": [
            {
                "plugin_profile_id": DRAFT_PLUGIN_PROFILE_ID,
                "label": "Composer Draft Single Smoke",
                "domain": "metatrack",
                "status": "runnable",
                "min_stage": "v3.3.5b",
                "runnable": True,
                "description": "Single Composer Draft Smoke plugin selection.",
                "plugins": plugins,
                "module_plugins": selection,
                "tags": ["draft_smoke", "single_chain", "go_backed"],
                "blocking_reasons": [],
            }
        ],
    }


def list_draft_artifacts(run_dir: Path, run_id: str) -> list[dict[str, object]]:
    artifacts = []
    for filename in sorted(ARTIFACT_ALLOWLIST):
        path = run_dir / filename
        if path.is_file():
            artifacts.append({
                "name": filename,
                "download_url": f"/api/v3/composer/draft-runs/{run_id}/artifacts/{filename}",
                "size_bytes": path.stat().st_size,
            })
    return artifacts


def get_draft_artifact_path(run_id: str, filename: str, root: Path = V3_DRAFT_RUNS_ROOT) -> Path:
    if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.-]{0,120}", run_id) or run_id in {".", "..", "latest"}:
        raise ValueError("invalid draft run id")
    return get_artifact_path(draft_job_manager(root).run_dir(run_id), filename)


def write_json(path: Path, payload: Any) -> None:
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")


def write_yaml(path: Path, payload: Any) -> None:
    path.write_text(yaml.safe_dump(payload, allow_unicode=True, sort_keys=False), encoding="utf-8")


def bounded_int(value: Any, default: int, minimum: int, maximum: int) -> int:
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        parsed = default
    return max(minimum, min(maximum, parsed))


def bounded_float(value: Any, default: float, minimum: float, maximum: float) -> float:
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        parsed = default
    return max(minimum, min(maximum, parsed))


def _mirror_latest(run_dir: Path, latest_dir: Path) -> None:
    if latest_dir.exists():
        shutil.rmtree(latest_dir)
    latest_dir.parent.mkdir(parents=True, exist_ok=True)
    shutil.copytree(run_dir, latest_dir)
