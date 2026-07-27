from __future__ import annotations


import json
import os
import subprocess
from datetime import UTC, datetime
from pathlib import Path
from uuid import uuid4

from backend.app.core.paths import ROOT, V5_REAL_CLUSTER_RUNS_ROOT
from backend.app.models.v5_experiment_spec import V5ExperimentSpec
from backend.app.services.v5_experiment_compiler import compile_plan
from backend.app.services import v5_real_cluster_artifacts


RUNS_ROOT = V5_REAL_CLUSTER_RUNS_ROOT


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
        "runtime_root": str(RUNS_ROOT),
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
    configured_timeout_seconds = int(
        os.environ.get("MBE_V5_REAL_CLUSTER_TIMEOUT_SECONDS", "7200")
    )

    runner_timeout_seconds = max(
        configured_timeout_seconds,
        (spec.duration_ms // 1000) + 90,
    )

    try:
        result = subprocess.run(
            command,
            cwd=ROOT / "executor",
            text=True,
            capture_output=True,
            timeout=runner_timeout_seconds,
        )
    except subprocess.TimeoutExpired as exc:
        stdout = exc.stdout or ""
        stderr = exc.stderr or ""

        if isinstance(stdout, bytes):
            stdout = stdout.decode("utf-8", errors="replace")
        if isinstance(stderr, bytes):
            stderr = stderr.decode("utf-8", errors="replace")

        (run_dir / "supervisor_stdout.log").write_text(
            stdout,
            encoding="utf-8",
        )
        (run_dir / "supervisor_stderr.log").write_text(
            stderr
            + f"\nreal cluster timed out after "
              f"{runner_timeout_seconds} seconds\n",
            encoding="utf-8",
        )
        raise
    (run_dir / "supervisor_stdout.log").write_text(result.stdout, encoding="utf-8")
    (run_dir / "supervisor_stderr.log").write_text(result.stderr, encoding="utf-8")
    summary = v5_real_cluster_artifacts.read_summary(run_dir)
    completion_gate = _completion_gate(run_dir, summary)
    summary["completion_gate"] = completion_gate
    status_value = "completed" if result.returncode == 0 and summary.get("ready_to_commit") is True and completion_gate["passed"] else "failed"
    return {
        "run_id": run_id,
        "status": status_value,
        "output_dir": _logical_output_dir(run_dir),
        "summary": summary,
        "artifacts": v5_real_cluster_artifacts.list_artifacts(run_dir, run_id),
        "stdout": result.stdout,
        "stderr": result.stderr,
        "no_fallback": True,
    }


def run_dir(run_id: str) -> Path:
    if not run_id.startswith("v5_") or "/" in run_id or "\\" in run_id:
        raise ValueError("invalid V5 run id")
    return RUNS_ROOT / run_id


def _logical_output_dir(path: Path) -> str:
    try:
        return str(path.relative_to(ROOT))
    except ValueError:
        try:
            return str(Path("$MBE_RUNTIME_ROOT") / path.relative_to(RUNS_ROOT.parent))
        except ValueError:
            return "$MBE_RUNTIME_ROOT"


def _completion_gate(run_dir: Path, summary: dict) -> dict:
    blockers: list[str] = []
    drain = _read_json(run_dir / "drain_status.json")
    finality = _read_json(run_dir / "finality_summary.json")
    if drain.get("completion_reason") != "drain_quiescent":
        blockers.append("drain_status.json:completion_reason_not_drain_quiescent")
    if not _positive_number(drain.get("drain_finished_at")):
        blockers.append("drain_status.json:missing_drain_finished_at")
    required_finality = [
        "logical_window_start_ms",
        "logical_window_end_ms",
        "logical_finality_duration_ms",
        "logical_finality_tps",
        "drain_finished_at_ms",
        "drain_duration_ms",
        "completion_window_start_ms",
        "completion_window_end_ms",
        "completion_duration_ms",
        "end_to_end_tps",
        "throughput_tps",
    ]
    for field in required_finality:
        if field not in finality:
            blockers.append(f"finality_summary.json:missing_{field}")
    if _number(finality.get("completion_window_end_ms")) < _number(finality.get("logical_window_end_ms")):
        blockers.append("finality_summary.json:completion_ends_before_logical_finality")
    if _number(finality.get("completion_duration_ms")) < _number(finality.get("logical_finality_duration_ms")):
        blockers.append("finality_summary.json:completion_duration_less_than_logical_duration")
    if finality.get("throughput_tps") != finality.get("end_to_end_tps"):
        blockers.append("finality_summary.json:throughput_tps_not_end_to_end")
    if summary.get("finality_evidence", {}).get("incomplete_unique_tx_count") not in {0, 0.0, None}:
        blockers.append("real_cluster_summary.json:incomplete_transactions")
    latest_status = _latest_node_statuses(run_dir)
    for status in latest_status:
        node_id = str(status.get("node_id", "unknown"))
        for field in ("pending_state_delta_count", "pending_state_delta_key_count", "ready_state_delta_count", "pending_commit_count"):
            if _number(status.get(field)) != 0:
                blockers.append(f"node_runtime_status:{node_id}:{field}_not_zero")
        if status.get("proposal_in_flight") is True:
            blockers.append(f"node_runtime_status:{node_id}:proposal_in_flight")
    return {"passed": not blockers, "blockers": blockers}


def _latest_node_statuses(run_dir: Path) -> list[dict]:
    statuses: list[dict] = []
    for path in sorted(run_dir.glob("node_*/node_runtime_status.json")):
        status = _read_json(path)
        if status:
            statuses.append(status)
    return statuses


def _read_json(path: Path) -> dict:
    if not path.is_file():
        return {}
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return {}
    return data if isinstance(data, dict) else {}


def _number(value: object) -> float:
    try:
        return float(value or 0)
    except (TypeError, ValueError):
        return 0.0


def _positive_number(value: object) -> bool:
    return _number(value) > 0
