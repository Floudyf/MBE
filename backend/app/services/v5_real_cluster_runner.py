from __future__ import annotations

import json
import subprocess
from datetime import UTC, datetime
from pathlib import Path
from uuid import uuid4

from backend.app.core.paths import ROOT
from backend.app.models.v5_experiment_spec import V5ExperimentSpec
from backend.app.services.v5_experiment_compiler import compile_plan
from backend.app.services import v5_real_cluster_artifacts


RUNS_ROOT = ROOT / ".cache" / "v5_real_cluster_runs"


def status() -> dict:
    return {
        "runtime_stage": "v5_1_real_plugin_driven_multi_process_multishard_runtime",
        "runtime_truth": "v5_real_cluster_candidate",
        "execution_backend": "real_cluster",
        "implemented": True,
        "one_node_one_os_process": True,
        "automatic_fallback": False,
        "production_blockchain": False,
        "production_pbft": False,
        "full_byzantine_security": False,
    }


def compile_only(spec: V5ExperimentSpec) -> dict:
    run_dir = RUNS_ROOT / "preview"
    plan = compile_plan(spec, run_dir, source_saved_config_id=spec.saved_config_id)
    return {"compatibility": True, "plan": plan.model_dump()}


def run(spec: V5ExperimentSpec) -> dict:
    if spec.execution_backend != "real_cluster":
        raise ValueError("real cluster endpoint requires execution_backend=real_cluster; no fallback is available")
    run_id = "v5_" + datetime.now(UTC).strftime("%Y%m%d_%H%M%S_") + uuid4().hex[:8]
    run_dir = RUNS_ROOT / run_id
    run_dir.mkdir(parents=True, exist_ok=False)
    try:
        plan = compile_plan(spec, run_dir, source_saved_config_id=spec.saved_config_id)
    except ValueError:
        raise
    plan_path = run_dir / "compiled_run_plan.json"
    plan_path.write_text(json.dumps(plan.model_dump(), indent=2) + "\n", encoding="utf-8")
    command = ["go", "run", "./cmd/mbe-supervisor", "--mode", "v5-real-cluster", "--plan", str(plan_path), "--data-dir", str(run_dir)]
    result = subprocess.run(command, cwd=ROOT / "executor", text=True, capture_output=True, timeout=(spec.duration_ms // 1000) + 90)
    (run_dir / "supervisor_stdout.log").write_text(result.stdout, encoding="utf-8")
    (run_dir / "supervisor_stderr.log").write_text(result.stderr, encoding="utf-8")
    summary = v5_real_cluster_artifacts.read_summary(run_dir)
    status_value = "completed" if result.returncode == 0 and summary.get("ready_to_commit") is True else "failed"
    return {"run_id": run_id, "status": status_value, "output_dir": str(run_dir), "summary": summary, "artifacts": v5_real_cluster_artifacts.list_artifacts(run_dir, run_id), "stdout": result.stdout, "stderr": result.stderr, "no_fallback": True}


def run_dir(run_id: str) -> Path:
    if not run_id.startswith("v5_") or "/" in run_id or "\\" in run_id:
        raise ValueError("invalid V5 run id")
    return RUNS_ROOT / run_id
