import importlib.util
import json
import subprocess
import sys
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[1]


def load_sweep_module():
    spec = importlib.util.spec_from_file_location("v1_8_sweep", ROOT / "scripts/v1_8_sweep.py")
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_v18_sweep_dry_run_generates_baselines_and_report(tmp_path):
    spec = yaml.safe_load((ROOT / "configs/sweeps/v1_8_baselines.yaml").read_text())
    assert [x["name"] for x in spec["baselines"]] == [
        "baseline_hash_only",
        "co_access_only",
        "co_access_dual_track",
        "full_v1",
    ]
    out = tmp_path / "out"
    subprocess.run([sys.executable, "scripts/v1_8_sweep.py", "--dry-run", "--out", str(out)], cwd=ROOT, check=True)
    rows = json.loads((out / "sweep_summary.json").read_text())
    assert len(rows) == 4 and (out / "report.md").is_file()


def test_v18_baseline_config_matches_executor_fields():
    sweep = load_sweep_module()
    spec = yaml.safe_load((ROOT / "configs/sweeps/v1_8_baselines.yaml").read_text())
    generated = {item["name"]: sweep.config_for(item, spec["execution_shards"]) for item in spec["baselines"]}

    assert generated["baseline_hash_only"]["routing"]["policy"] == "hash"
    assert generated["baseline_hash_only"]["execution"]["dual_track_enabled"] is False
    assert generated["baseline_hash_only"]["commit"]["hot_update_aggregation_enabled"] is False
    assert generated["co_access_only"]["routing"]["policy"] == "co_access"
    assert generated["co_access_dual_track"]["execution"]["dual_track_enabled"] is True
    assert generated["full_v1"]["commit"]["hot_update_aggregation_enabled"] is True
    assert "co_access_min_weight" in generated["full_v1"]["routing"]
    assert "fast_track_max_access_size" in generated["full_v1"]["execution"]
    assert "aggregation_policy" in generated["full_v1"]["commit"]


def test_v18_report_preserves_zero_and_false_values():
    sweep = load_sweep_module()
    text = sweep.report([
        {
            "name": "full_v1",
            "tx_count": "2",
            "routing_policy": "co_access",
            "dual_track_enabled": "true",
            "fast_track_tx_count": "0",
            "conservative_track_tx_count": "2",
            "hot_update_aggregation_enabled": "true",
            "aggregated_commit_count": "0",
            "aggregation_saved_commit_count": "0",
        }
    ])

    assert "dual_track_enabled" in text
    assert "aggregation_saved_commit_count" in text
    assert "| full_v1 | 2 | co_access | true | 0 | 2 | true | 0 | 0 |" in text
