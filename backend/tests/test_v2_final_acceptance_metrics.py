from __future__ import annotations

import csv
import json
from pathlib import Path

from fastapi.testclient import TestClient

import backend.app.main as main


client = TestClient(main.app)


def test_v1_config_builder_passes_mechanism_switches() -> None:
    config = main.config_for_custom_run({"preset": "full_v1", "source_type": "synthetic"})

    assert config["routing"]["policy"] == "co_access"
    assert config["execution"]["dual_track_enabled"] is True
    assert config["commit"]["hot_update_aggregation_enabled"] is True
    assert config["truth"]["source_type"] == "synthetic"


def test_v1_custom_run_returns_distinct_latency_fields_without_parser_aliasing(monkeypatch, tmp_path) -> None:
    monkeypatch.setattr(main, "V1_CUSTOM_OUT", tmp_path / "latest")
    monkeypatch.setattr(main, "V2_JOBS_ROOT", tmp_path / "jobs")

    def fake_run(command, cwd=None, capture_output=None, text=None, timeout=None, check=None):
        output_dir = Path(command[-1])
        output_dir.mkdir(parents=True, exist_ok=True)
        if "go" in command[0].lower():
            with (output_dir / "summary.csv").open("w", encoding="utf-8", newline="") as stream:
                writer = csv.DictWriter(
                    stream,
                    fieldnames=["tx_count", "success_count", "failed_count", "throughput_tps", "avg_latency_ms", "p95_latency_ms", "p99_latency_ms"],
                )
                writer.writeheader()
                writer.writerow({
                    "tx_count": 3,
                    "success_count": 3,
                    "failed_count": 0,
                    "throughput_tps": 12.5,
                    "avg_latency_ms": 1,
                    "p95_latency_ms": 2,
                    "p99_latency_ms": 3,
                })
            (output_dir / "latency.csv").write_text("tx_id,latency_ms\n1,1\n2,2\n3,3\n", encoding="utf-8")
            (output_dir / "runtime.log").write_text("fake replay\n", encoding="utf-8")
        else:
            (output_dir / "trace.jsonl.gz").write_bytes(b"fake")
            (output_dir / "trace_meta.json").write_text(json.dumps({"generated_by": "test"}), encoding="utf-8")

        class Completed:
            returncode = 0
            stdout = "ok"
            stderr = ""

        return Completed()

    monkeypatch.setattr(main.subprocess, "run", fake_run)

    response = client.post("/api/v1/custom-run", json={"source_type": "synthetic", "preset": "full_v1", "tx_count": 3})
    assert response.status_code == 200
    summary = response.json()["summary"]
    assert summary["avg_latency_ms"] == "1"
    assert summary["p95_latency_ms"] == "2"
    assert summary["p99_latency_ms"] == "3"


def test_v1_ablation_presets_expose_distinct_switches() -> None:
    response = client.get("/api/v1/ablation-presets")
    assert response.status_code == 200
    items = {item["id"]: item for item in response.json()["items"]}

    assert items["baseline_hash_only"]["routing_policy"] == "hash"
    assert items["co_access_only"]["routing_policy"] == "co_access"
    assert items["co_access_dual_track"]["dual_track_enabled"] is True
    assert items["full_v1"]["hot_update_aggregation_enabled"] is True


def test_v2_replay_contracts_return_run_identity_and_truth(monkeypatch, tmp_path) -> None:
    monkeypatch.setattr(main, "V2_JOBS_ROOT", tmp_path / "jobs")

    dual = client.post("/api/v2/dual-chain/replay", json={}).json()
    assert dual["run_id"].startswith("v2run_")
    assert dual["data_truth_label"] == "synthetic_replay"
    assert dual["backend_type"] == "local_virtual"
    assert dual["summary"]["source_backend_type"] == "local_virtual"
    assert dual["artifacts"]

    protocol = client.post("/api/v2/cross-chain/protocol-replay", json={}).json()
    assert protocol["run_id"].startswith("v2run_")
    assert protocol["data_truth_label"] == "synthetic_replay"
    assert protocol["backend_type"] == "local_virtual"
    assert protocol["protocol_truth"] == "local_baseline_model"
    assert protocol["artifacts"]


def test_v2_sweep_and_calibration_contracts_still_expose_truth_fields() -> None:
    sweeps = client.get("/api/v2/sweeps").json()["items"]
    assert sweeps
    assert all("data_truth_label" in item for item in sweeps)
    assert all("backend_type" in item for item in sweeps)

    calibrations = client.get("/api/v2/calibration/configs").json()["items"]
    assert calibrations
    assert all("data_truth_label" in item for item in calibrations)
    assert all("backend_type" in item for item in calibrations)
