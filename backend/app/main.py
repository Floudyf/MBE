from __future__ import annotations

import csv
import json
import subprocess
import sys
from pathlib import Path

import yaml
from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse

ROOT = Path(__file__).resolve().parents[2]
CONFIG = ROOT / "configs/experiments/v0_default_asset_hotspot.yaml"
DEFAULT_COMPONENTS = ROOT / "configs/plugins/default_components.yaml"
V1_EXPERIMENTS = ROOT / "configs/experiments"
V1_TEMPLATES = ROOT / "configs/templates"
V1_SWEEP_OUT = ROOT / ".cache/v1_8_sweeps/latest"
RUN = ROOT / "experiments/runs/v0_default_asset_hotspot"
DOWNLOADABLE_OUTPUT_FILES = frozenset({"config.yaml", "trace_meta.json", "summary.csv", "latency.csv", "runtime.log"})
V1_SWEEP_DOWNLOADABLE_FILES = frozenset({"report.md", "sweep_summary.csv", "sweep_summary.json"})
app = FastAPI(title="MBE V0")


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
