from __future__ import annotations

import csv
import subprocess
from pathlib import Path

from fastapi.testclient import TestClient

from backend.app import main


client = TestClient(main.app)


def test_v1_workloads_and_ablation_presets_are_exposed() -> None:
    workloads = client.get("/api/v1/workloads")
    presets = client.get("/api/v1/ablation-presets")

    assert workloads.status_code == 200
    ids = {item["id"] for item in workloads.json()["items"]}
    assert {"asset_hotspot_v1", "reward_burst", "existing_trace", "fabric_chain_backed_trace"} <= ids
    assert presets.status_code == 200
    preset_ids = {item["id"] for item in presets.json()["items"]}
    assert {"baseline_hash_only", "co_access_only", "co_access_dual_track", "full_v1", "custom"} <= preset_ids


def test_fabric_trace_status_missing_does_not_start_fabric(tmp_path, monkeypatch) -> None:
    monkeypatch.setattr(main, "V1_FABRIC_SMOKE_OUT", tmp_path)

    response = client.get("/api/v1/fabric/trace-status")

    assert response.status_code == 200
    payload = response.json()
    assert payload["status"] == "missing"
    assert "v1_fabric_smoke.py" in payload["cli_command"]
    assert "Docker" in payload["limitations"][0]


def test_custom_run_synthetic_uses_local_workload_and_replay_only(tmp_path, monkeypatch) -> None:
    latest_dir = tmp_path / "latest"
    jobs_dir = tmp_path / "jobs"
    monkeypatch.setattr(main, "V1_CUSTOM_OUT", latest_dir)
    monkeypatch.setattr(main, "V2_JOBS_ROOT", jobs_dir)
    commands: list[str] = []

    def fake_run(command, cwd, text, capture_output):  # noqa: ANN001
        commands.append(" ".join(str(part) for part in command))
        output_dir = Path(command[-1])
        if "workload.asset_hotspot_v1.cli" in commands[-1]:
            (output_dir / "trace.jsonl.gz").write_bytes(b"fake")
            (output_dir / "trace_meta.json").write_text("{}", encoding="utf-8")
            return subprocess.CompletedProcess(command, 0, stdout="generated\n", stderr="")
        if "cmd/replay" in commands[-1]:
            with (output_dir / "summary.csv").open("w", encoding="utf-8", newline="") as stream:
                writer = csv.DictWriter(stream, fieldnames=["tx_count", "routing_policy", "dual_track_enabled", "hot_update_aggregation_enabled"])
                writer.writeheader()
                writer.writerow({"tx_count": "10", "routing_policy": "co_access", "dual_track_enabled": "true", "hot_update_aggregation_enabled": "true"})
            (output_dir / "latency.csv").write_text("tx_id\n", encoding="utf-8")
            (output_dir / "runtime.log").write_text("ok\n", encoding="utf-8")
            return subprocess.CompletedProcess(command, 0, stdout="replayed\n", stderr="")
        return subprocess.CompletedProcess(command, 1, stdout="", stderr="unexpected")

    monkeypatch.setattr(main.subprocess, "run", fake_run)

    response = client.post("/api/v1/custom-run", json={"source_type": "synthetic", "workload": "asset_hotspot_v1", "tx_count": 10, "preset": "full_v1"})

    assert response.status_code == 200
    payload = response.json()
    assert payload["source_type"] == "synthetic"
    assert payload["run_id"].startswith("v2run_")
    assert payload["latest_compat_dir"] == str(latest_dir)
    assert "not real chain" in payload["truth_label"]
    assert payload["summary"]["tx_count"] == "10"
    joined = " ".join(commands).lower()
    assert all(forbidden not in joined for forbidden in ("docker", "fabric", "network.sh", "peer "))
    files = client.get("/api/v1/custom-run/latest/files")
    assert files.status_code == 200
    assert {item["name"] for item in files.json()["files"]} >= {"summary.csv", "latency.csv", "runtime.log", "trace_meta.json", "used_config.yaml", "used_config.json", "report.md"}
