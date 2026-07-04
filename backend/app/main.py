from __future__ import annotations

import csv
import json
import shutil
import subprocess
import sys
from pathlib import Path

import yaml
from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse

from backend.app.models.v3_composer_draft import V3ComposerDraftRequest
from backend.app.services.config_validator_v2 import validate_planned_topology_file
from backend.app.services.calibration_runner_v2 import CALIBRATION_CONFIGS, CalibrationBlocked, CalibrationError, get_calibration_config, list_calibration_configs, run_calibration_job, summarize_calibration_config
from backend.app.services.chain_backend import list_backend_capabilities
from backend.app.services.chain_backed_trace_adapter import detect_fabric_smoke_trace
from backend.app.services.dual_chain_profiles import DualChainConfigError
from backend.app.services.dual_chain_replay import run_dual_chain_replay_job
from backend.app.services.cross_chain_protocols import list_cross_chain_protocols
from backend.app.services.experiment_composer_v2 import preview_experiment
from backend.app.services.protocol_replay import ProtocolReplayError, run_protocol_replay_job
from backend.app.services.artifact_manager import ArtifactError, ArtifactForbidden, ArtifactMissing, get_artifact_path, list_artifacts, mirror_run_to_latest
from backend.app.services.job_manager import DEFAULT_JOBS_ROOT, JobManager, JobNotFound
from backend.app.services.plugin_registry import PluginRegistryError, load_registry, registry_payload
from backend.app.services.sweep_runner_v2 import SWEEP_CONFIGS, SweepError, get_sweep_config, list_sweeps, run_sweep_job, summarize_sweep_config
from backend.app.services.trace_source_service import TraceSourceError, TraceSourceNotFound, describe_capabilities, infer_data_truth_label, list_trace_sources, load_trace_sources
from backend.app.services.trace_source_validator import validate_trace_source
from backend.app.services.v3_experiment_templates import list_templates
from backend.app.services.v3_composer_draft_runner import DraftSmokeNotRunnable, get_draft_artifact_path, run_v3_composer_draft_smoke
from backend.app.services.v3_composer_draft_validator import model_dump, validate_v3_composer_draft
from backend.app.services.v3_controlled_smoke_runner import ControlledSmokeError, get_controlled_artifact_path, run_v3_4_10_controlled_smoke
from backend.app.services.v3_draft_run_history import DraftRunHistoryError, DraftRunNotFound, get_v3_draft_run_detail, list_v3_draft_runs
from backend.app.services.v3_go_runtime_runner import run_metatrack_go_backed_ablation
from backend.app.models.v3_metatrack_formal_benchmark import V3FormalMetatrackBenchmarkRequest
from backend.app.services.v3_metatrack_formal_benchmark_runner import FormalBenchmarkNotRunnable, get_formal_artifact_path, preview_formal_metatrack_benchmark, run_formal_metatrack_benchmark
from backend.app.services.v3_profile_preview import preview_profile
from backend.app.services.v3_runtime_topology import stage_metadata as v3_runtime_stage_metadata

ROOT = Path(__file__).resolve().parents[2]
CONFIG = ROOT / "configs/experiments/v0_default_asset_hotspot.yaml"
DEFAULT_COMPONENTS = ROOT / "configs/plugins/default_components.yaml"
V1_EXPERIMENTS = ROOT / "configs/experiments"
V1_TEMPLATES = ROOT / "configs/templates"
V1_SWEEP_OUT = ROOT / ".cache/v1_8_sweeps/latest"
V1_CUSTOM_OUT = ROOT / ".cache/v1_custom_runs/latest"
V2_JOBS_ROOT = DEFAULT_JOBS_ROOT
V2_DUAL_CHAIN_SAMPLE_CONFIG = ROOT / "configs/experiments/v2_dual_chain_sample.yaml"
V2_PROTOCOL_SAMPLE_CONFIG = ROOT / "configs/experiments/v2_cross_chain_protocol_sample.yaml"
V1_FABRIC_SMOKE_OUT = ROOT / ".cache/fabric_smoke/latest"
RUN = ROOT / "experiments/runs/v0_default_asset_hotspot"
DOWNLOADABLE_OUTPUT_FILES = frozenset({"config.yaml", "trace_meta.json", "summary.csv", "latency.csv", "runtime.log"})
V1_SWEEP_DOWNLOADABLE_FILES = frozenset({"report.md", "sweep_summary.csv", "sweep_summary.json"})
V1_CUSTOM_DOWNLOADABLE_FILES = frozenset({"trace_meta.json", "summary.csv", "latency.csv", "runtime.log", "report.md", "used_config.yaml", "used_config.json", "config.yaml"})
app = FastAPI(title="MBE V0")

ABLATION_PRESETS = {
    "baseline_hash_only": {"routing_policy": "hash", "dual_track_enabled": False, "hot_update_aggregation_enabled": False},
    "co_access_only": {"routing_policy": "co_access", "dual_track_enabled": False, "hot_update_aggregation_enabled": False},
    "co_access_dual_track": {"routing_policy": "co_access", "dual_track_enabled": True, "hot_update_aggregation_enabled": False},
    "full_v1": {"routing_policy": "co_access", "dual_track_enabled": True, "hot_update_aggregation_enabled": True},
}
FABRIC_SMOKE_COMMAND = "python scripts/v1_fabric_smoke.py --strict --channel mbechannel --out .cache/fabric_smoke/latest"


def check(experiment_id: str) -> None:
    if experiment_id != "v0_default_asset_hotspot":
        raise HTTPException(404, "unknown V0 experiment")


def run(command: list[str], cwd: Path) -> str:
    result = subprocess.run(command, cwd=cwd, text=True, capture_output=True)
    if result.returncode != 0:
        raise HTTPException(500, detail={"message": "process failed", "stderr": result.stderr})
    return result.stdout


def v1_experiment_documents() -> list[dict]:
    documents = []
    for path in sorted(V1_EXPERIMENTS.glob("v1_*.yaml")):
        document = yaml.safe_load(path.read_text(encoding="utf-8"))
        experiment = document["experiment"]
        documents.append({
            "id": experiment["name"],
            "stage": experiment["stage"],
            "description": experiment["description"],
            "runnable": bool(experiment["runnable"]),
            "implemented": bool(experiment["implemented"]),
            "template": document["template"],
            "config": document,
        })
    return documents


def read_summary_csv(path: Path) -> dict[str, str]:
    with path.open(encoding="utf-8", newline="") as stream:
        return next(csv.DictReader(stream))


def safe_existing_trace(path_text: str) -> Path:
    path = Path(path_text)
    if not path.is_absolute():
        path = ROOT / path
    resolved = path.resolve()
    try:
        resolved.relative_to(ROOT.resolve())
    except ValueError as exc:
        raise HTTPException(400, "trace_path must stay inside the project workspace") from exc
    if not resolved.is_file():
        raise HTTPException(404, "trace_path does not exist")
    return resolved


def custom_files() -> list[dict]:
    files = []
    for filename in sorted(V1_CUSTOM_DOWNLOADABLE_FILES):
        path = V1_CUSTOM_OUT / filename
        if path.is_file():
            files.append({"name": filename, "download_url": f"/api/v1/custom-run/latest/files/{filename}", "size_bytes": path.stat().st_size})
    return files


def job_manager() -> JobManager:
    return JobManager(V2_JOBS_ROOT)


def v3_stage_metadata() -> dict[str, str]:
    return v3_runtime_stage_metadata()


def v3_current_stage() -> str:
    return v3_stage_metadata()["current_stage"]


def v2_artifacts_response(run_id: str) -> dict:
    manager = job_manager()
    metadata = manager.get_run(run_id)
    artifacts = list_artifacts(manager.run_dir(run_id), run_id)
    return {"run_id": run_id, "status": "ready" if artifacts else "missing", "artifacts": artifacts, "run": metadata}


def v1_truth_labels(source_type: str) -> tuple[str, str]:
    if source_type == "synthetic":
        return "synthetic_replay", "Synthetic replay: generated workload replay, not real chain execution."
    if source_type == "existing_trace":
        return "existing_trace_replay", "Existing trace replay: replaying a local trace file, not launching a chain."
    return "fabric_chain_backed_trace_replay", "Chain-backed replay: trace was produced by Fabric smoke CLI/WSL; the web UI only replays it."


def config_for_custom_run(payload: dict) -> dict:
    preset = str(payload.get("preset", "full_v1"))
    settings = dict(ABLATION_PRESETS.get(preset, ABLATION_PRESETS["full_v1"]))
    if preset == "custom":
        settings = {
            "routing_policy": payload.get("routing_policy", "co_access"),
            "dual_track_enabled": bool(payload.get("dual_track_enabled", True)),
            "hot_update_aggregation_enabled": bool(payload.get("hot_update_aggregation_enabled", True)),
        }
    workload_name = str(payload.get("workload", "asset_hotspot_v1"))
    tx_count = max(1, min(int(payload.get("tx_count", 100)), 100_000))
    workload = {
        "plugin": workload_name if workload_name in {"asset_hotspot_v1", "reward_burst"} else "asset_hotspot_v1",
        "tx_count": tx_count,
        "hot_tx_ratio": float(payload.get("hot_tx_ratio", 0.6)),
        "conflict_injection_ratio": float(payload.get("conflict_injection_ratio", 0.3)),
        "commutative_update_ratio": float(payload.get("commutative_update_ratio", 0.35)),
        "access_set_size": max(1, int(payload.get("access_set_size", 4))),
        "multi_hotspot_count": max(1, int(payload.get("multi_hotspot_count", 3))),
        "arrival_rate": max(1.0, float(payload.get("arrival_rate", 100.0))),
        "burst_rate": max(1.0, float(payload.get("burst_rate", 500.0))),
        "cross_shard_ratio": float(payload.get("cross_shard_ratio", 0.2)),
        "read_write_ratio": float(payload.get("read_write_ratio", 0.4)),
    }
    if workload["plugin"] == "reward_burst":
        workload = {key: workload[key] for key in ("plugin", "tx_count", "commutative_update_ratio", "burst_rate", "multi_hotspot_count")}
    return {
        "experiment": {"name": "v1_custom_interactive", "version": "v1", "stage": "v1-final-plus", "seed": int(payload.get("seed", 42))},
        "workload": workload,
        "state_sharding": {"shard_count": 4},
        "execution_sharding": {"shard_count": 4},
        "routing": {"policy": settings["routing_policy"], "co_access_min_weight": 1, "co_access_max_group_size": 64, "co_access_balance_weight": 1},
        "execution": {"dual_track_enabled": settings["dual_track_enabled"], "fast_track_max_access_size": 2, "conservative_on_conflict_hint": True, "conservative_on_missing_access_set": True, "scheduler_policy": "fast_first"},
        "commit": {"hot_update_aggregation_enabled": settings["hot_update_aggregation_enabled"], "aggregation_min_hot_count": 2, "aggregation_max_group_size": 64, "aggregation_require_fast_track": True, "conservative_on_constraint_failure": True, "aggregation_policy": "by_primary_key"},
        "truth": {"source_type": payload.get("source_type", "synthetic"), "preset": preset},
    }


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok"}


@app.get("/api/v0/config/default")
def config() -> dict:
    return yaml.safe_load(CONFIG.read_text(encoding="utf-8"))


@app.get("/api/v0/composer/default")
def composer_preview() -> dict:
    """Return the complete, valid default V0 component composition."""
    composition = yaml.safe_load(DEFAULT_COMPONENTS.read_text(encoding="utf-8"))
    components = [
        {"type": component_type, "plugin": plugin_name}
        for component_type, plugin_name in composition["plugins"].items()
    ]
    return {
        "composer": "default_composer",
        "schema_version": composition["schema_version"],
        "valid": True,
        "errors": [],
        "components": components,
    }


@app.get("/api/v1/composer/templates")
def v1_templates() -> list[dict]:
    return [yaml.safe_load(path.read_text(encoding="utf-8")) for path in sorted(V1_TEMPLATES.glob("v1_*.yaml"))]


@app.get("/api/v1/composer/experiments")
def v1_experiments() -> list[dict]:
    return [{key: value for key, value in document.items() if key != "config"} for document in v1_experiment_documents()]


@app.get("/api/v1/status")
def v1_status() -> dict:
    return {
        "phase": "v1_final_acceptance_ui",
        "scope": "single_chain_v1_acceptance",
        "stages": [
            {"id": "v1_1_topology_guide", "label": "V1.1 topology-first experiment guide", "status": "completed"},
            {"id": "v1_2_executor_sharding", "label": "V1.2 executor sharding prototype", "status": "completed"},
            {"id": "v1_3_workload_trace", "label": "V1.3 workload and trace enhancement", "status": "completed"},
            {"id": "v1_4_fabric_chain_backed_trace", "label": "V1.4 Fabric chain-backed trace smoke", "status": "completed_cli_only"},
            {"id": "v1_5_co_access_routing", "label": "V1.5 co-access routing", "status": "completed"},
            {"id": "v1_6_dual_track_execution", "label": "V1.6 dual-track execution", "status": "completed"},
            {"id": "v1_7_hot_update_aggregation", "label": "V1.7 hot update aggregation", "status": "completed"},
            {"id": "v1_8_baseline_sweep_report", "label": "V1.8 baseline sweep report", "status": "completed"},
        ],
        "boundaries": {
            "fabric": "CLI/WSL only; the web UI never starts Docker, Fabric, network.sh, deployCC, or peer invoke.",
            "v2_v3": "dual-chain, multi-chain, cross-chain protocols, MetaFlow, committee bridge, and Pending Pool remain planned.",
        },
    }


@app.get("/api/v1/workloads")
def v1_workloads() -> dict:
    return {
        "items": [
            {
                "id": "asset_hotspot_v1",
                "label": "Synthetic: Asset Hotspot V1",
                "description": "V1.3 synthetic asset hotspot trace with access-set, conflict, commutative, and hotspot annotations.",
                "source_type": "synthetic",
                "supported_params": ["tx_count", "seed", "hot_tx_ratio", "conflict_injection_ratio", "commutative_update_ratio", "access_set_size", "multi_hotspot_count", "arrival_rate", "burst_rate"],
                "limitations": ["Synthetic replay only; not real chain execution."],
            },
            {
                "id": "reward_burst",
                "label": "Synthetic: Reward Burst",
                "description": "V1.3 reward-pool burst trace for hot-update aggregation experiments.",
                "source_type": "synthetic",
                "supported_params": ["tx_count", "seed", "commutative_update_ratio", "multi_hotspot_count", "burst_rate"],
                "limitations": ["Synthetic replay only; useful for aggregation-path visibility."],
            },
            {
                "id": "existing_trace",
                "label": "Existing Trace Replay",
                "description": "Replay an existing trace.jsonl.gz already present inside the project workspace.",
                "source_type": "existing_trace",
                "supported_params": ["trace_path", "preset", "routing_policy", "dual_track_enabled", "hot_update_aggregation_enabled"],
                "limitations": ["Does not generate workload; trace_path must stay inside the workspace."],
            },
            {
                "id": "fabric_chain_backed_trace",
                "label": "Fabric Chain-backed Trace Replay",
                "description": "Replay .cache/fabric_smoke/latest/trace.jsonl.gz generated by the CLI/WSL Fabric smoke runner.",
                "source_type": "chain_backed",
                "supported_params": ["preset", "routing_policy", "dual_track_enabled", "hot_update_aggregation_enabled"],
                "limitations": ["The web UI never starts Docker, Fabric, network.sh, deployCC, or peer invoke."],
            },
        ]
    }


@app.get("/api/v1/ablation-presets")
def v1_ablation_presets() -> dict:
    items = [
        {"id": name, **settings, "description": f"{name} preset for V1-final-plus interactive replay."}
        for name, settings in ABLATION_PRESETS.items()
    ]
    items.append({"id": "custom", "routing_policy": "co_access", "dual_track_enabled": True, "hot_update_aggregation_enabled": True, "description": "Manually choose routing, dual-track, and hot-update aggregation toggles."})
    return {"items": items}


@app.get("/api/v1/fabric/trace-status")
def v1_fabric_trace_status() -> dict:
    files = {
        "trace": V1_FABRIC_SMOKE_OUT / "trace.jsonl.gz",
        "trace_meta": V1_FABRIC_SMOKE_OUT / "trace_meta.json",
        "raw_chain_log": V1_FABRIC_SMOKE_OUT / "raw_chain_log.jsonl",
        "summary": V1_FABRIC_SMOKE_OUT / "summary.json",
    }
    existing = {name: path.is_file() for name, path in files.items()}
    ready = existing["trace"] and existing["trace_meta"]
    return {
        "status": "ready" if ready else "missing",
        "ready": ready,
        "output_dir": str(V1_FABRIC_SMOKE_OUT),
        "files": {name: {"path": str(path), "exists": exists} for name, (path, exists) in zip(files.keys(), zip(files.values(), existing.values()))},
        "message": "Fabric smoke trace is ready for chain-backed replay." if ready else "Fabric smoke trace is missing; run the CLI/WSL command first.",
        "cli_command": FABRIC_SMOKE_COMMAND,
        "limitations": ["Status check only; this API does not start Docker, Fabric, network.sh, deployCC, or peer invoke."],
    }


@app.post("/api/v1/custom-run")
def v1_custom_run(payload: dict) -> dict:
    source_type = str(payload.get("source_type", "synthetic"))
    if source_type not in {"synthetic", "existing_trace", "chain_backed"}:
        raise HTTPException(400, "source_type must be synthetic, existing_trace, or chain_backed")

    data_truth_label, truth_label = v1_truth_labels(source_type)
    manager = job_manager()
    metadata = manager.create_run(source="v1_custom_run", experiment_name="v1_custom_interactive", data_truth_label=data_truth_label)
    run_id = metadata["run_id"]
    run_dir = manager.run_dir(run_id)
    manager.mark_running(run_id)

    config_doc = config_for_custom_run(payload)
    config_path = run_dir / "used_config.yaml"
    config_json_path = run_dir / "used_config.json"
    try:
        config_path.write_text(yaml.safe_dump(config_doc, sort_keys=False), encoding="utf-8")
        config_json_path.write_text(json.dumps(config_doc, indent=2) + "\n", encoding="utf-8")

        stdout_parts = []
        if source_type == "synthetic":
            workload = config_doc["workload"]["plugin"]
            command = [sys.executable, "-m", f"workload.{workload}.cli", "--config", str(config_path), "--output", str(run_dir)]
            generated = subprocess.run(command, cwd=ROOT, text=True, capture_output=True)
            if generated.returncode != 0:
                raise HTTPException(500, detail={"message": "synthetic workload generation failed", "stdout": generated.stdout, "stderr": generated.stderr})
            stdout_parts.append(generated.stdout)
            trace_path = run_dir / "trace.jsonl.gz"
        elif source_type == "existing_trace":
            trace_path = safe_existing_trace(str(payload.get("trace_path", "")))
            meta = trace_path.with_name("trace_meta.json")
            if meta.is_file():
                shutil.copy2(meta, run_dir / "trace_meta.json")
        else:
            trace_path = V1_FABRIC_SMOKE_OUT / "trace.jsonl.gz"
            if not trace_path.is_file():
                raise HTTPException(400, detail={"message": "Fabric trace missing", "cli_command": FABRIC_SMOKE_COMMAND})
            meta = V1_FABRIC_SMOKE_OUT / "trace_meta.json"
            if meta.is_file():
                shutil.copy2(meta, run_dir / "trace_meta.json")

        replay_command = ["go", "run", "./cmd/replay", "-config", str(config_path.resolve()), "-trace", str(trace_path.resolve()), "-output", str(run_dir.resolve())]
        replay_result = subprocess.run(replay_command, cwd=ROOT / "executor", text=True, capture_output=True)
        if replay_result.returncode != 0:
            raise HTTPException(500, detail={"message": "executor replay failed", "stdout": replay_result.stdout, "stderr": replay_result.stderr})
        stdout_parts.append(replay_result.stdout)

        summary_path = run_dir / "summary.csv"
        summary = read_summary_csv(summary_path)
        report = [
            "# V1 custom interactive run",
            "",
            truth_label,
            "",
            f"- run_id: {run_id}",
            f"- source_type: {source_type}",
            f"- workload: {config_doc['workload']['plugin']}",
            f"- preset: {config_doc['truth']['preset']}",
            f"- routing_policy: {summary.get('routing_policy', '')}",
            f"- dual_track_enabled: {summary.get('dual_track_enabled', '')}",
            f"- hot_update_aggregation_enabled: {summary.get('hot_update_aggregation_enabled', '')}",
            f"- tx_count: {summary.get('tx_count', '')}",
        ]
        (run_dir / "report.md").write_text("\n".join(report) + "\n", encoding="utf-8")
        completed = manager.mark_completed(run_id, data_truth_label=data_truth_label)
        mirror_run_to_latest(run_dir, V1_CUSTOM_OUT)
        return {
            "run_id": run_id,
            "status": "completed",
            "output_dir": str(run_dir),
            "latest_compat_dir": str(V1_CUSTOM_OUT),
            "source_type": source_type,
            "truth_label": truth_label,
            "data_truth_label": data_truth_label,
            "summary": summary,
            "files": custom_files(),
            "stdout": "".join(stdout_parts),
            "metadata": completed,
        }
    except HTTPException as exc:
        manager.mark_failed(run_id, str(exc.detail))
        raise
    except Exception as exc:
        manager.mark_failed(run_id, str(exc))
        raise


@app.get("/api/v1/custom-run/latest/summary")
def v1_custom_latest_summary() -> dict:
    path = V1_CUSTOM_OUT / "summary.csv"
    if not path.exists():
        return {"status": "not_run", "message": "No V1 custom run has been generated yet.", "summary": {}, "source_type": "", "truth_label": ""}
    truth_path = V1_CUSTOM_OUT / "used_config.json"
    source_type = ""
    if truth_path.exists():
        source_type = json.loads(truth_path.read_text(encoding="utf-8")).get("truth", {}).get("source_type", "")
    return {"status": "ready", "summary": read_summary_csv(path), "source_type": source_type, "truth_label": "Synthetic replay or trace replay; inspect used_config.json for source details.", "output_dir": str(V1_CUSTOM_OUT)}


@app.get("/api/v1/custom-run/latest/files")
def v1_custom_latest_files() -> dict:
    return {"status": "ready" if custom_files() else "not_run", "output_dir": str(V1_CUSTOM_OUT), "files": custom_files()}


@app.get("/api/v1/custom-run/latest/files/{filename}")
def v1_custom_download_file(filename: str) -> FileResponse:
    if "/" in filename or "\\" in filename or filename in {".", ".."}:
        raise HTTPException(400, "invalid custom run output filename")
    if filename not in V1_CUSTOM_DOWNLOADABLE_FILES:
        raise HTTPException(403, "custom run output file is not downloadable")
    path = V1_CUSTOM_OUT / filename
    if not path.is_file():
        raise HTTPException(404, "custom run output file not generated")
    return FileResponse(path, filename=filename)


@app.post("/api/v1/sweep/run")
def v1_sweep_run() -> dict:
    command = [sys.executable, str(ROOT / "scripts/v1_8_sweep.py"), "--out", str(V1_SWEEP_OUT)]
    result = subprocess.run(command, cwd=ROOT, text=True, capture_output=True)
    if result.returncode != 0:
        raise HTTPException(
            500,
            detail={
                "message": "V1.8 sweep failed",
                "returncode": result.returncode,
                "stdout": result.stdout,
                "stderr": result.stderr,
            },
        )
    return {
        "status": "completed",
        "command": command,
        "output_dir": str(V1_SWEEP_OUT),
        "stdout": result.stdout,
        "stderr": result.stderr,
        "files": v1_sweep_files()["files"],
    }


@app.get("/api/v1/sweep/summary")
def v1_sweep_summary() -> dict:
    path = V1_SWEEP_OUT / "sweep_summary.json"
    if not path.exists():
        return {"status": "not_run", "message": "V1.8 sweep has not been run yet.", "rows": []}
    return {"status": "ready", "output_dir": str(V1_SWEEP_OUT), "rows": json.loads(path.read_text(encoding="utf-8"))}


@app.get("/api/v1/sweep/report")
def v1_sweep_report() -> dict[str, str]:
    path = V1_SWEEP_OUT / "report.md"
    if not path.exists():
        return {"status": "not_run", "message": "V1.8 report.md has not been generated yet.", "content": ""}
    return {"status": "ready", "path": str(path), "content": path.read_text(encoding="utf-8")}


@app.get("/api/v1/sweep/files")
def v1_sweep_files() -> dict:
    files = []
    for filename in sorted(V1_SWEEP_DOWNLOADABLE_FILES):
        path = V1_SWEEP_OUT / filename
        if path.is_file():
            files.append({"name": filename, "download_url": f"/api/v1/sweep/files/{filename}", "size_bytes": path.stat().st_size})
    return {"status": "ready" if files else "not_run", "output_dir": str(V1_SWEEP_OUT), "files": files}


@app.get("/api/v1/sweep/files/{filename}")
def v1_sweep_download_file(filename: str) -> FileResponse:
    if "/" in filename or "\\" in filename or filename in {".", ".."}:
        raise HTTPException(400, "invalid sweep output filename")
    if filename not in V1_SWEEP_DOWNLOADABLE_FILES:
        raise HTTPException(403, "sweep output file is not downloadable")
    path = V1_SWEEP_OUT / filename
    if not path.is_file():
        raise HTTPException(404, "sweep output file not generated")
    return FileResponse(path, filename=filename)


@app.post("/api/v1/composer/preview")
def v1_preview(payload: dict[str, str]) -> dict:
    experiment_id = payload.get("experiment_id")
    for document in v1_experiment_documents():
        if document["id"] == experiment_id:
            return {
                "experiment_id": document["id"],
                "stage": document["stage"],
                "description": document["description"],
                "runnable": document["runnable"],
                "implemented": document["implemented"],
                "status": "runnable" if document["runnable"] and document["implemented"] else "planned",
                "config": document["config"],
            }
    raise HTTPException(404, "unknown V1 experiment")


@app.get("/api/v2/plugins")
def v2_plugins() -> dict:
    try:
        return registry_payload(load_registry())
    except PluginRegistryError as exc:
        raise HTTPException(500, str(exc)) from exc


@app.get("/api/v2/plugins/{plugin_type}")
def v2_plugins_by_type(plugin_type: str) -> dict:
    try:
        registry = load_registry()
    except PluginRegistryError as exc:
        raise HTTPException(500, str(exc)) from exc
    return {"type": plugin_type, "plugins": registry.list_plugins(plugin_type)}


@app.post("/api/v2/composer/preview")
def v2_composer_preview(payload: dict) -> dict:
    try:
        return preview_experiment(payload)
    except PluginRegistryError as exc:
        raise HTTPException(500, str(exc)) from exc


@app.get("/api/v2/topologies/v2_dual_chain_planned/validation")
def v2_planned_topology_validation() -> dict:
    return validate_planned_topology_file()


@app.get("/api/v2/trace-sources")
def v2_trace_sources() -> dict:
    try:
        return {"items": list_trace_sources(load_trace_sources())}
    except TraceSourceError as exc:
        raise HTTPException(500, str(exc)) from exc


@app.get("/api/v2/trace-sources/{source_id}")
def v2_trace_source_detail(source_id: str) -> dict:
    try:
        registry = load_trace_sources()
        source = registry.get_source(source_id)
        return {**source, "capabilities": describe_capabilities(source_id, registry), "data_truth_label": infer_data_truth_label(source_id, registry)}
    except TraceSourceNotFound as exc:
        raise HTTPException(404, str(exc)) from exc
    except TraceSourceError as exc:
        raise HTTPException(500, str(exc)) from exc


@app.post("/api/v2/trace-sources/validate")
def v2_validate_trace_source(payload: dict) -> dict:
    try:
        return validate_trace_source(payload, load_trace_sources(), workspace_root=ROOT, fabric_smoke_dir=V1_FABRIC_SMOKE_OUT)
    except TraceSourceNotFound as exc:
        raise HTTPException(404, str(exc)) from exc
    except TraceSourceError as exc:
        raise HTTPException(500, str(exc)) from exc


@app.get("/api/v2/runs")
def v2_runs(limit: int = 50) -> dict:
    return {"items": job_manager().list_runs(limit=limit)}


@app.get("/api/v2/runs/latest")
def v2_latest_run() -> dict:
    try:
        return job_manager().get_latest_run()
    except JobNotFound as exc:
        raise HTTPException(404, str(exc)) from exc


@app.get("/api/v2/runs/{run_id}")
def v2_run(run_id: str) -> dict:
    try:
        return job_manager().get_run(run_id)
    except JobNotFound as exc:
        raise HTTPException(404, str(exc)) from exc


@app.get("/api/v2/runs/{run_id}/artifacts")
def v2_run_artifacts(run_id: str) -> dict:
    try:
        return v2_artifacts_response(run_id)
    except JobNotFound as exc:
        raise HTTPException(404, str(exc)) from exc


@app.get("/api/v2/runs/{run_id}/artifacts/{filename}")
def v2_download_artifact(run_id: str, filename: str) -> FileResponse:
    try:
        manager = job_manager()
        manager.get_run(run_id)
        path = get_artifact_path(manager.run_dir(run_id), filename)
    except JobNotFound as exc:
        raise HTTPException(404, str(exc)) from exc
    except ArtifactForbidden as exc:
        raise HTTPException(403, str(exc)) from exc
    except ArtifactMissing as exc:
        raise HTTPException(404, str(exc)) from exc
    except ArtifactError as exc:
        raise HTTPException(400, str(exc)) from exc
    return FileResponse(path, filename=filename)


@app.get("/api/v2/chain-backends")
def v2_chain_backends() -> dict:
    return {"items": list_backend_capabilities()}


@app.get("/api/v2/dual-chain/sample-config")
def v2_dual_chain_sample_config() -> dict:
    return {
        "path": V2_DUAL_CHAIN_SAMPLE_CONFIG.relative_to(ROOT).as_posix(),
        "config": yaml.safe_load(V2_DUAL_CHAIN_SAMPLE_CONFIG.read_text(encoding="utf-8")),
    }


@app.post("/api/v2/dual-chain/replay")
def v2_dual_chain_replay(payload: dict) -> dict:
    config_path = Path(str(payload.get("config_path", V2_DUAL_CHAIN_SAMPLE_CONFIG.relative_to(ROOT))))
    try:
        result = run_dual_chain_replay_job(config_path, jobs_root=V2_JOBS_ROOT, root=ROOT)
    except DualChainConfigError as exc:
        raise HTTPException(400, str(exc)) from exc
    return result


@app.get("/api/v2/cross-chain/protocols")
def v2_cross_chain_protocols() -> dict:
    return {"items": list_cross_chain_protocols()}


@app.get("/api/v2/cross-chain/sample-config")
def v2_cross_chain_sample_config() -> dict:
    return {
        "path": V2_PROTOCOL_SAMPLE_CONFIG.relative_to(ROOT).as_posix(),
        "config": yaml.safe_load(V2_PROTOCOL_SAMPLE_CONFIG.read_text(encoding="utf-8")),
    }


@app.post("/api/v2/cross-chain/protocol-replay")
def v2_cross_chain_protocol_replay(payload: dict) -> dict:
    config_path = Path(str(payload.get("config_path", V2_PROTOCOL_SAMPLE_CONFIG.relative_to(ROOT))))
    try:
        return run_protocol_replay_job(config_path, jobs_root=V2_JOBS_ROOT, root=ROOT)
    except ProtocolReplayError as exc:
        raise HTTPException(400, str(exc)) from exc


@app.get("/api/v2/sweeps")
def v2_sweeps() -> dict:
    try:
        return {"items": list_sweeps(ROOT)}
    except SweepError as exc:
        raise HTTPException(400, str(exc)) from exc


@app.get("/api/v2/sweeps/{sweep_id}")
def v2_sweep_detail(sweep_id: str) -> dict:
    try:
        config = get_sweep_config(sweep_id, ROOT)
    except SweepError as exc:
        raise HTTPException(404, str(exc)) from exc
    return {
        "path": SWEEP_CONFIGS[sweep_id].relative_to(ROOT).as_posix(),
        "summary": summarize_sweep_config(config),
        "config": config,
    }


@app.post("/api/v2/sweeps/run")
def v2_sweep_run(payload: dict) -> dict:
    try:
        if payload.get("sweep_id"):
            sweep_id = str(payload["sweep_id"])
            if sweep_id not in SWEEP_CONFIGS:
                raise SweepError(f"unknown V2.8 sweep_id: {sweep_id}")
            config_path = SWEEP_CONFIGS[sweep_id].relative_to(ROOT)
        else:
            config_path = Path(str(payload.get("config_path", SWEEP_CONFIGS["v2_baseline_sweep"].relative_to(ROOT))))
        return run_sweep_job(config_path, jobs_root=V2_JOBS_ROOT, root=ROOT)
    except SweepError as exc:
        raise HTTPException(400, str(exc)) from exc


@app.get("/api/v2/calibration/configs")
def v2_calibration_configs() -> dict:
    try:
        return {"items": list_calibration_configs(ROOT)}
    except CalibrationError as exc:
        raise HTTPException(400, str(exc)) from exc


@app.get("/api/v2/calibration/fabric-smoke/status")
def v2_calibration_fabric_smoke_status() -> dict:
    return detect_fabric_smoke_trace()


@app.post("/api/v2/calibration/run")
def v2_calibration_run(payload: dict) -> dict:
    try:
        if payload.get("config_id"):
            config_id = str(payload["config_id"])
            if config_id not in CALIBRATION_CONFIGS:
                raise CalibrationError(f"unknown V2.9 calibration config_id: {config_id}")
            config_path = CALIBRATION_CONFIGS[config_id].relative_to(ROOT)
        else:
            config_path = Path(str(payload.get("config_path", CALIBRATION_CONFIGS["v2_synthetic_calibration_sample"].relative_to(ROOT))))
        return run_calibration_job(config_path, jobs_root=V2_JOBS_ROOT, root=ROOT)
    except CalibrationBlocked as exc:
        return exc.payload
    except CalibrationError as exc:
        raise HTTPException(400, str(exc)) from exc


@app.get("/api/v2/calibration/configs/{config_id}")
def v2_calibration_config_detail(config_id: str) -> dict:
    try:
        config = get_calibration_config(config_id, ROOT)
    except CalibrationError as exc:
        raise HTTPException(404, str(exc)) from exc
    return {
        "path": CALIBRATION_CONFIGS[config_id].relative_to(ROOT).as_posix(),
        "summary": summarize_calibration_config(config),
        "config": config,
    }


@app.get("/api/v3/composer/templates")
def v3_composer_templates() -> dict:
    templates = list_templates()
    items = [
        {
            "template_id": item["template_id"],
            "stage": item.get("stage", ""),
            "chain_mode": item.get("chain_mode", ""),
            "runnable": bool(item.get("runnable")),
            "preview_only": bool(item.get("preview_only")),
            "description": item.get("description", ""),
            "template_name": item.get("template_name", item.get("name", item.get("template_id", ""))),
            "variable_module": item.get("variable_module", ""),
            "allowed_variable_plugins": item.get("allowed_variable_plugins", []),
            "locked_modules": item.get("locked_modules", item.get("fairness", {}).get("locked_modules", {})),
            "fairness_rule": item.get("fairness_rule", item.get("fairness", {}).get("fairness_rule", "")),
            "status": item.get("status", "runnable" if item.get("runnable") else "preview_only"),
            "truthfulness_note": item.get("truthfulness_note", ""),
            "default_preset_id": item.get("default_preset_id", ""),
            "presets": item.get("presets", []),
            "variable_modules": item.get("variable_modules", []),
            "fixed_modules": item.get("fixed_modules", []),
            "disabled_modules": item.get("disabled_modules", []),
            "planned_modules": item.get("planned_modules", []),
            "output_modules": item.get("output_modules", []),
        }
        for item in templates
    ]
    return {"stage": v3_current_stage(), **v3_stage_metadata(), "items": items}


@app.get("/api/v3/composer/preview")
def v3_composer_preview(experiment_profile_id: str = "metatrack_go_backed_ablation_smoke") -> dict:
    try:
        preview = preview_profile("experiment_profile", experiment_profile_id)
    except (KeyError, ValueError) as exc:
        raise HTTPException(404, str(exc)) from exc
    return {
        "experiment_profile_id": experiment_profile_id,
        "stage": v3_current_stage(),
        **v3_stage_metadata(),
        "profile_preview": preview,
        "composer_preview": preview.get("composer_preview", {}),
        "experiment_template": preview.get("experiment_template", ""),
        "module_graph": preview.get("module_graph", {}),
        "plugin_matrix": preview.get("plugin_matrix", []),
        "fairness_scope": preview.get("fairness_scope", {}),
        "runnable": preview.get("runnable", False),
    }


@app.post("/api/v3/composer/validate-draft")
def v3_composer_validate_draft(request: V3ComposerDraftRequest) -> dict:
    validation = validate_v3_composer_draft(request)
    return model_dump(validation)


@app.post("/api/v3/composer/run-draft-smoke")
def v3_composer_run_draft_smoke(request: V3ComposerDraftRequest) -> dict:
    try:
        return run_v3_composer_draft_smoke(request)
    except DraftSmokeNotRunnable as exc:
        raise HTTPException(400, detail={"message": "Composer Draft is not runnable", "validation": model_dump(exc.validation)}) from exc
    except Exception as exc:
        raise HTTPException(500, str(exc)) from exc


@app.post("/api/v3/composer/formal-metatrack/preview")
def v3_composer_formal_metatrack_preview(request: V3FormalMetatrackBenchmarkRequest) -> dict:
    try:
        return preview_formal_metatrack_benchmark(request)
    except Exception as exc:
        raise HTTPException(400, str(exc)) from exc


@app.post("/api/v3/composer/formal-metatrack/run")
def v3_composer_formal_metatrack_run(request: V3FormalMetatrackBenchmarkRequest) -> dict:
    try:
        return run_formal_metatrack_benchmark(request)
    except FormalBenchmarkNotRunnable as exc:
        raise HTTPException(400, detail={"message": "Formal MetaTrack benchmark is not runnable", "preview": exc.preview}) from exc
    except Exception as exc:
        raise HTTPException(500, str(exc)) from exc


@app.get("/api/v3/composer/formal-metatrack/{run_id}/artifacts/{filename}")
def v3_composer_formal_metatrack_artifact(run_id: str, filename: str) -> FileResponse:
    try:
        path = get_formal_artifact_path(run_id, filename)
    except ArtifactMissing as exc:
        raise HTTPException(404, str(exc)) from exc
    except (ArtifactError, ArtifactForbidden, JobNotFound, ValueError) as exc:
        raise HTTPException(400, str(exc)) from exc
    return FileResponse(path)


@app.get("/api/v3/composer/draft-runs")
def v3_composer_draft_runs(limit: int = 20) -> dict:
    return list_v3_draft_runs(limit=limit)


@app.get("/api/v3/composer/draft-runs/{run_id}")
def v3_composer_draft_run_detail(run_id: str) -> dict:
    try:
        return get_v3_draft_run_detail(run_id)
    except DraftRunNotFound as exc:
        raise HTTPException(404, str(exc)) from exc
    except DraftRunHistoryError as exc:
        raise HTTPException(400, str(exc)) from exc


@app.get("/api/v3/composer/draft-runs/{run_id}/artifacts/{filename}")
def v3_composer_draft_artifact(run_id: str, filename: str) -> FileResponse:
    try:
        path = get_draft_artifact_path(run_id, filename)
    except ArtifactMissing as exc:
        raise HTTPException(404, str(exc)) from exc
    except ArtifactForbidden as exc:
        raise HTTPException(403, str(exc)) from exc
    except (ArtifactError, JobNotFound, ValueError) as exc:
        raise HTTPException(400, str(exc)) from exc
    return FileResponse(path)


@app.post("/api/v3/composer/run-controlled-smoke")
def v3_composer_run_controlled_smoke() -> dict:
    try:
        return {**run_v3_4_10_controlled_smoke(), **v3_stage_metadata()}
    except ControlledSmokeError as exc:
        raise HTTPException(400, str(exc)) from exc
    except Exception as exc:
        raise HTTPException(500, str(exc)) from exc


@app.get("/api/v3/composer/controlled-smoke/{run_id}/artifacts/{filename}")
def v3_composer_controlled_smoke_artifact(run_id: str, filename: str) -> FileResponse:
    try:
        path = get_controlled_artifact_path(run_id, filename)
    except ArtifactMissing as exc:
        raise HTTPException(404, str(exc)) from exc
    except (ArtifactError, ArtifactForbidden, JobNotFound, ValueError) as exc:
        raise HTTPException(400, str(exc)) from exc
    return FileResponse(path)


@app.post("/api/v3/composer/run-smoke")
def v3_composer_run_smoke() -> dict:
    manager = job_manager()
    metadata = manager.create_run(
        source="v3_composer_frontend",
        experiment_name="metatrack_go_backed_ablation_smoke",
        data_truth_label="modular_runtime",
        stage=v3_current_stage(),
        extra_metadata={
            "backend_type": "modular_research_chain",
            "runtime_mode": "go_backed",
            "experiment_profile_id": "metatrack_go_backed_ablation_smoke",
            **v3_stage_metadata(),
        },
    )
    run_id = metadata["run_id"]
    manager.mark_running(run_id)
    try:
        result = run_metatrack_go_backed_ablation(output_root=V2_JOBS_ROOT, run_id=run_id)
        completed = manager.mark_completed(run_id, data_truth_label="modular_runtime")
        artifacts = list_artifacts(Path(result["output_dir"]), run_id)
        return {
            "run_id": run_id,
            "status": "completed",
            "stage": v3_current_stage(),
            **v3_stage_metadata(),
            "output_dir": str(result["output_dir"]),
            "data_truth_label": "modular_runtime",
            "backend_type": "modular_research_chain",
            "runtime_mode": "go_backed",
            "summary": {"plugin_combinations": ["baseline_hash_only", "co_access_only", "co_access_dual_track", "full_MetaTrack"]},
            "artifacts": artifacts,
            "run": completed,
        }
    except Exception as exc:
        manager.mark_failed(run_id, str(exc))
        raise HTTPException(500, str(exc)) from exc


@app.post("/api/v0/experiments")
def create() -> dict[str, str]:
    return {"experiment_id": "v0_default_asset_hotspot", "output_dir": str(RUN)}


@app.post("/api/v0/experiments/{experiment_id}/generate-trace")
def generate_trace(experiment_id: str) -> dict[str, str]:
    check(experiment_id)
    return {"output": run([sys.executable, "-m", "workload.asset_hotspot.cli", "--config", str(CONFIG), "--output", str(RUN)], ROOT)}


@app.post("/api/v0/experiments/{experiment_id}/replay")
def replay(experiment_id: str) -> dict[str, str]:
    check(experiment_id)
    return {"output": run(["go", "run", "./cmd/replay", "--config", "../configs/experiments/v0_default_asset_hotspot.yaml", "--trace", "../experiments/runs/v0_default_asset_hotspot/trace.jsonl.gz", "--output", "../experiments/runs/v0_default_asset_hotspot"], ROOT / "executor")}


@app.post("/api/v0/experiments/{experiment_id}/run")
def run_all(experiment_id: str) -> dict[str, str]:
    generate_trace(experiment_id)
    return replay(experiment_id)


@app.get("/api/v0/experiments/{experiment_id}/summary")
def summary(experiment_id: str) -> dict[str, str]:
    check(experiment_id)
    path = RUN / "summary.csv"
    if not path.exists():
        raise HTTPException(404, "summary not generated")
    with path.open(encoding="utf-8", newline="") as stream:
        return next(csv.DictReader(stream))


@app.get("/api/v0/experiments/{experiment_id}/files")
def files(experiment_id: str) -> list[str]:
    check(experiment_id)
    return [name for name in ("config.yaml", "trace.jsonl.gz", "trace_meta.json", "summary.csv", "latency.csv", "runtime.log") if (RUN / name).exists()]


@app.get("/api/v0/experiments/{experiment_id}/files/{filename}")
def download_file(experiment_id: str, filename: str) -> FileResponse:
    check(experiment_id)
    if "/" in filename or "\\" in filename or filename in {".", ".."}:
        raise HTTPException(400, "invalid output filename")
    if filename not in DOWNLOADABLE_OUTPUT_FILES:
        raise HTTPException(403, "output file is not downloadable")
    path = RUN / filename
    if not path.is_file():
        raise HTTPException(404, "output file not generated")
    return FileResponse(path, filename=filename)


@app.get("/api/v0/experiments/{experiment_id}/logs")
def logs(experiment_id: str) -> dict[str, str]:
    check(experiment_id)
    path = RUN / "runtime.log"
    if not path.exists():
        raise HTTPException(404, "runtime log not generated")
    return {"log": path.read_text(encoding="utf-8")}
