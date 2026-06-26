import csv

from backend.app.services.v3_go_runtime_runner import (
    MINIMAL_PLUGIN_PROFILE,
    ROLE_SEPARATED_CHAIN_PROFILE,
    ROLE_SEPARATION_SMOKE_PROFILE,
    run_go_v3_runtime,
)


def test_role_separation_go_artifacts_include_new_fields(tmp_path):
    result = run_go_v3_runtime(
        experiment_profile_path=ROLE_SEPARATION_SMOKE_PROFILE,
        plugin_profile_path=MINIMAL_PLUGIN_PROFILE,
        plugin_profile_id="v3_2_minimal_single_chain",
        chain_profile_path=ROLE_SEPARATED_CHAIN_PROFILE,
        output_dir=tmp_path / "role_go",
    )

    assert result.summary["truth_label"] == "modular_runtime"
    assert result.summary["execution_shard_count"] == 4
    assert result.summary["state_storage_unit_count"] == 4
    assert result.summary["runtime_mode"] == "go_backed_minimal_runtime"

    block_header = _header(result.output_dir / "block_log.csv")
    tx_header = _header(result.output_dir / "tx_results.csv")
    commit_header = _header(result.output_dir / "state_commit_log.csv")

    assert {"consensus_domain_id", "validator_count", "execution_shard_count", "state_storage_unit_count"} <= set(block_header)
    assert {"execution_shard_id", "accessed_state_unit_ids", "remote_state_unit_count", "remote_fetch_count", "cross_state_unit_access"} <= set(tx_header)
    assert {"state_storage_unit_id", "execution_shard_id", "is_remote_commit", "placement_policy", "routing_plugin"} <= set(commit_header)

    tx_rows = _rows(result.output_dir / "tx_results.csv")
    assert len(tx_rows) == result.summary["tx_count"]
    assert all(row["execution_shard_id"] for row in tx_rows)
    assert all(row["accessed_state_unit_ids"] for row in tx_rows)
    assert all(row["shard_id"] == row["execution_shard_id"] for row in tx_rows)

    commit_rows = _rows(result.output_dir / "state_commit_log.csv")
    assert commit_rows
    assert all(row["placement_policy"] == "hash_state_storage" for row in commit_rows)

    report = (result.output_dir / "report.md").read_text(encoding="utf-8")
    assert "does not migrate persistent state storage placement" in report
    produced = {path.name for path in result.output_dir.iterdir()}
    assert "fabric_validation_summary.csv" not in produced
    assert "metaflow_events.csv" not in produced


def _header(path):
    with path.open(newline="", encoding="utf-8") as fh:
        return next(csv.reader(fh))


def _rows(path):
    with path.open(newline="", encoding="utf-8") as fh:
        return list(csv.DictReader(fh))
