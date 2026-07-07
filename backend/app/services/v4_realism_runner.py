from __future__ import annotations

import json
import subprocess
from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4

from backend.app.models.v4_realism import V4RealismSmokeRequest

ROOT = Path(__file__).resolve().parents[3]
RUNS_ROOT = ROOT / ".cache" / "v4_realism_runs"


def status() -> dict:
    return {
        "runtime_stage": "v4_3_blockemulator_surpass_realism_closure",
        "runtime_truth": "v4_blockemulator_surpass_realism_closure",
        "real_signed_tx": True,
        "sender_public_key_binding": True,
        "signed_tx_authenticity": True,
        "per_node_mempool": True,
        "real_p2p": True,
        "pbft_style_consensus": True,
        "real_pbft_messages": True,
        "production_pbft": False,
        "full_byzantine_security": False,
        "persistent_state_db": True,
        "state_root_from_real_state_updates": True,
        "real_cross_shard_state_machine": True,
        "real_cross_shard_network_commit": True,
        "recovery_supported": True,
        "fault_injection_supported": True,
        "real_fault_injection": True,
        "blockemulator_trace_to_signed_tx": True,
        "blockemulator_bridge_upgraded": True,
        "frontend_realism_mode": True,
        "fabric_evm_backend": False,
        "production_blockchain": False,
        "production_atomic_commit": False,
        "full_blockemulator_compatibility": False,
    }


def run_smoke(payload: V4RealismSmokeRequest) -> dict:
    run_id = "v4_" + datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S_") + uuid4().hex[:8]
    out_dir = RUNS_ROOT / run_id
    out_dir.mkdir(parents=True, exist_ok=True)
    cmd = [
        "go",
        "run",
        "./cmd/mbe-supervisor",
        "--mode",
        "v4.3-smoke",
        "--nodes",
        str(payload.nodes),
        "--shards",
        str(payload.shards),
        "--tx-count",
        str(payload.tx_count),
        f"--enable-cross-shard={str(payload.enable_cross_shard).lower()}",
        f"--enable-faults={str(payload.enable_faults).lower()}",
        "--fault-profile",
        payload.fault_profile,
        "--blockemulator-tx-limit",
        str(payload.blockemulator_tx_limit),
        "--run-duration-ms",
        str(payload.run_duration_ms),
        "--data-dir",
        str(out_dir),
    ]
    if payload.blockemulator_csv:
        cmd.extend(["--blockemulator-csv", payload.blockemulator_csv])
    result = subprocess.run(cmd, cwd=ROOT / "executor", text=True, capture_output=True)
    if result.returncode != 0:
        return {"run_id": run_id, "status": "failed", "output_dir": str(out_dir), "stdout": result.stdout, "stderr": result.stderr}
    summary = get_summary(run_id)
    return {"run_id": run_id, "status": "completed", "output_dir": str(out_dir), "summary": summary, "artifacts": list_artifacts(run_id), "stdout": result.stdout}


def get_summary(run_id: str) -> dict:
    run_dir = _run_dir(run_id)
    path = run_dir / "v4_3_realism_final_summary.json"
    if not path.is_file():
        path = run_dir / "v4_2_realism_final_summary.json"
    if not path.is_file():
        return {}
    return json.loads(path.read_text(encoding="utf-8"))


def list_artifacts(run_id: str) -> list[dict]:
    run_dir = _run_dir(run_id)
    if not run_dir.is_dir():
        return []
    files = []
    for path in sorted(run_dir.rglob("*")):
        if path.is_file():
            rel = path.relative_to(run_dir).as_posix()
            files.append({"name": rel, "size_bytes": path.stat().st_size, "download_url": f"/api/v4/realism/runs/{run_id}/artifacts/{rel}"})
    return files


def artifact_path(run_id: str, filename: str) -> Path:
    run_dir = _run_dir(run_id).resolve()
    path = (run_dir / filename).resolve()
    path.relative_to(run_dir)
    if not path.is_file():
        raise FileNotFoundError(filename)
    return path


def _run_dir(run_id: str) -> Path:
    if not run_id.startswith("v4_"):
        raise ValueError("invalid run_id")
    return RUNS_ROOT / run_id
