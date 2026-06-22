from __future__ import annotations

import csv
import subprocess
import sys
from pathlib import Path

import yaml
from fastapi import FastAPI, HTTPException

ROOT = Path(__file__).resolve().parents[2]
CONFIG = ROOT / "configs/experiments/v0_default_asset_hotspot.yaml"
RUN = ROOT / "experiments/runs/v0_default_asset_hotspot"
app = FastAPI(title="MBE V0")


def check(experiment_id: str) -> None:
    if experiment_id != "v0_default_asset_hotspot":
        raise HTTPException(404, "unknown V0 experiment")


def run(command: list[str], cwd: Path) -> str:
    result = subprocess.run(command, cwd=cwd, text=True, capture_output=True)
    if result.returncode != 0:
        raise HTTPException(500, detail={"message": "process failed", "stderr": result.stderr})
    return result.stdout


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok"}


@app.get("/api/v0/config/default")
def config() -> dict:
    return yaml.safe_load(CONFIG.read_text(encoding="utf-8"))


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


@app.get("/api/v0/experiments/{experiment_id}/logs")
def logs(experiment_id: str) -> dict[str, str]:
    check(experiment_id)
    path = RUN / "runtime.log"
    if not path.exists():
        raise HTTPException(404, "runtime log not generated")
    return {"log": path.read_text(encoding="utf-8")}
