from __future__ import annotations

from pathlib import Path

from fastapi.testclient import TestClient

import backend.app.main as main

client = TestClient(main.app)


def test_v3_composer_preview_api_returns_additive_preview_fields() -> None:
    response = client.get("/api/v3/composer/preview", params={"experiment_profile_id": "metatrack_go_backed_ablation_smoke"})

    assert response.status_code == 200
    payload = response.json()
    composer = payload["composer_preview"]
    assert payload["experiment_profile_id"] == "metatrack_go_backed_ablation_smoke"
    assert payload["stage"] == "V3.5.2"
    assert payload["current_stage"] == "V3.5.2"
    assert payload["latest_runtime_stage"] == "V3.5.1 Logical Node Topology Runtime"
    assert payload["latest_completed_runtime_stage"] == "V3.5.1 Logical Node Topology Runtime"
    assert payload["current_capability"] == "launcher preview artifacts generated from logical node topology"
    assert payload["runtime_truth"] == "launcher_preview_only_not_real_tcp_not_real_pbft"
    assert payload["next_stage"] == "V3.5.3 Local Node Process Runtime"
    assert composer["view"] == "single_chain"
    assert composer["template_id"] == "metatrack_ablation"
    assert composer["runnable"] is True
    assert [module["module_id"] for module in composer["modules"]][:4] == ["Workload", "TxPool", "BlockProducer", "Consensus"]
    variable = {module["module_id"] for module in composer["modules"] if module["status"] == "variable"}
    assert {"Routing", "Execution", "StateAccess", "Commit"} <= variable
    assert {row["method_id"] for row in composer["plugin_matrix"]} == {
        "baseline_hash_only",
        "co_access_only",
        "co_access_dual_track",
        "full_MetaTrack",
    }
    assert payload["fairness_scope"]["planned_modules"] == ["CommitteeEpoch"]


def test_v3_composer_templates_api_keeps_planned_templates_non_runnable() -> None:
    response = client.get("/api/v3/composer/templates")

    assert response.status_code == 200
    payload = response.json()
    assert payload["stage"] == "V3.5.2"
    assert payload["current_stage"] == "V3.5.2"
    assert payload["latest_runtime_stage"] == "V3.5.1 Logical Node Topology Runtime"
    templates = {item["template_id"]: item for item in payload["items"]}
    assert templates["metatrack_ablation"]["runnable"] is True
    assert templates["committee_lifecycle_planned"]["preview_only"] is True
    assert templates["committee_lifecycle_planned"]["runnable"] is False


def test_v3_composer_run_smoke_registers_downloadable_artifacts_without_fabric(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setattr(main, "V2_JOBS_ROOT", tmp_path)

    def fake_run(*, output_root: Path, run_id: str) -> dict:
        run_dir = output_root / run_id
        run_dir.mkdir(parents=True, exist_ok=True)
        (run_dir / "metatrack_summary.csv").write_text("plugin_combination,tx_count\nbaseline_hash_only,1\n", encoding="utf-8")
        (run_dir / "metatrack_summary.json").write_text("[]\n", encoding="utf-8")
        (run_dir / "metatrack_ablation_report.md").write_text("Not Fabric. Not MetaFlow.\n", encoding="utf-8")
        (run_dir / "used_experiment_profile.yaml").write_text("profile_id: metatrack_go_backed_ablation_smoke\n", encoding="utf-8")
        return {"run_id": run_id, "output_dir": run_dir, "runs": []}

    monkeypatch.setattr(main, "run_metatrack_go_backed_ablation", fake_run)
    response = client.post("/api/v3/composer/run-smoke")

    assert response.status_code == 200
    payload = response.json()
    assert payload["stage"] == "V3.5.2"
    assert payload["current_stage"] == "V3.5.2"
    assert payload["latest_runtime_stage"] == "V3.5.1 Logical Node Topology Runtime"
    assert payload["runtime_truth"] == "launcher_preview_only_not_real_tcp_not_real_pbft"
    assert payload["runtime_mode"] == "go_backed"
    artifact_names = {artifact["name"] for artifact in payload["artifacts"]}
    assert {"metatrack_summary.csv", "metatrack_summary.json", "metatrack_ablation_report.md", "used_experiment_profile.yaml"} <= artifact_names
    assert "fabric_validation_summary.csv" not in artifact_names
    assert "metaflow_events.csv" not in artifact_names
