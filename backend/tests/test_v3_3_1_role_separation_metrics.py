import csv
import json

from backend.app.services.v3_go_runtime_runner import run_metatrack_go_backed_ablation


def test_metatrack_mechanism_metrics_include_role_separation_fields(tmp_path):
    result = run_metatrack_go_backed_ablation(tmp_path)
    output_dir = result["output_dir"]

    rows = _rows(output_dir / "metatrack_mechanism_metrics.csv")
    required = {
        "execution_shard_count",
        "state_storage_unit_count",
        "cross_state_unit_access_count",
        "remote_state_fetch_count",
        "state_locality_ratio",
        "execution_shard_load_balance",
        "state_unit_load_balance",
    }
    assert required <= set(rows[0])
    assert {row["plugin_combination"] for row in rows} == {"baseline_hash_only", "co_access_only", "co_access_dual_track", "full_MetaTrack"}
    assert all(row["execution_shard_count"] == "4" for row in rows)
    assert all(row["state_storage_unit_count"] == "4" for row in rows)


def test_metatrack_combinations_share_role_separated_chain_profile(tmp_path):
    result = run_metatrack_go_backed_ablation(tmp_path)
    output_dir = result["output_dir"]

    summary = json.loads((output_dir / "metatrack_summary.json").read_text(encoding="utf-8"))
    fairness_keys = {
        (
            row["tx_count"],
            row["chain_profile_id"],
            row["execution_shard_count"],
            row["state_storage_unit_count"],
            row["stage"],
        )
        for row in summary
    }
    assert fairness_keys == {(100, "single_chain_research_default", 4, 4, "v3.3")}

    report = (output_dir / "metatrack_ablation_report.md").read_text(encoding="utf-8")
    assert "does not migrate persistent state placement" in report


def _rows(path):
    with path.open(newline="", encoding="utf-8") as fh:
        return list(csv.DictReader(fh))
