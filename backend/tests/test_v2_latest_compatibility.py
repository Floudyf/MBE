from __future__ import annotations

import csv
import subprocess
from pathlib import Path

from fastapi.testclient import TestClient

from backend.app import main


client = TestClient(main.app)


def test_v1_custom_run_records_v2_job_and_preserves_latest_endpoints(tmp_path: Path, monkeypatch) -> None:
    latest_dir = tmp_path / "latest"
    jobs_dir = tmp_path / "jobs"
    monkeypatch.setattr(main, "V1_CUSTOM_OUT", latest_dir)
    monkeypatch.setattr(main, "V2_JOBS_ROOT", jobs_dir)

    def fake_run(command, cwd, text, capture_output):  # noqa: ANN001
        output_dir = Path(command[-1])
        if "workload.asset_hotspot_v1.cli" in " ".join(str(part) for part in command):
            (output_dir / "trace.jsonl.gz").write_bytes(b"fake")
            (output_dir / "trace_meta.json").write_text("{}", encoding="utf-8")
            return subprocess.CompletedProcess(command, 0, stdout="generated\n", stderr="")
        with (output_dir / "summary.csv").open("w", encoding="utf-8", newline="") as stream:
            writer = csv.DictWriter(stream, fieldnames=["tx_count", "routing_policy", "dual_track_enabled", "hot_update_aggregation_enabled"])
            writer.writeheader()
            writer.writerow({"tx_count": "5", "routing_policy": "hash", "dual_track_enabled": "false", "hot_update_aggregation_enabled": "false"})
        (output_dir / "latency.csv").write_text("tx_id\n", encoding="utf-8")
        (output_dir / "runtime.log").write_text("ok\n", encoding="utf-8")
        return subprocess.CompletedProcess(command, 0, stdout="replayed\n", stderr="")

    monkeypatch.setattr(main.subprocess, "run", fake_run)

    response = client.post("/api/v1/custom-run", json={"source_type": "synthetic", "workload": "asset_hotspot_v1", "tx_count": 5, "preset": "baseline_hash_only"})

    assert response.status_code == 200
    payload = response.json()
    run_id = payload["run_id"]
    assert run_id.startswith("v2run_")
    assert payload["output_dir"] == str(jobs_dir / run_id)
    assert payload["latest_compat_dir"] == str(latest_dir)
    assert payload["data_truth_label"] == "synthetic_replay"
    assert (jobs_dir / run_id / "metadata.json").is_file()
    assert (latest_dir / "summary.csv").is_file()

    latest_summary = client.get("/api/v1/custom-run/latest/summary")
    latest_files = client.get("/api/v1/custom-run/latest/files")
    v2_artifacts = client.get(f"/api/v2/runs/{run_id}/artifacts")

    assert latest_summary.status_code == 200
    assert latest_summary.json()["summary"]["tx_count"] == "5"
    assert latest_files.status_code == 200
    assert {item["name"] for item in latest_files.json()["files"]} >= {"summary.csv", "latency.csv", "runtime.log", "trace_meta.json", "used_config.yaml", "used_config.json", "report.md"}
    assert v2_artifacts.status_code == 200
    assert {item["name"] for item in v2_artifacts.json()["artifacts"]} >= {"summary.csv", "latency.csv", "runtime.log", "trace_meta.json", "used_config.yaml", "used_config.json", "report.md"}
