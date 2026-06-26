import csv
import json

from backend.app.services.v3_runtime.runtime import run_v3_single_chain_runtime


REQUIRED_BLOCK_FIELDS = {
    "block_height",
    "block_id",
    "proposer_node",
    "tx_count",
    "cut_time_ms",
    "ordered_time_ms",
    "finalized_time_ms",
    "consensus_plugin",
    "status",
}
REQUIRED_TX_FIELDS = {
    "tx_id",
    "submit_time_ms",
    "admit_time_ms",
    "block_height",
    "execution_start_ms",
    "execution_end_ms",
    "commit_time_ms",
    "latency_ms",
    "status",
    "shard_id",
    "read_count",
    "write_count",
    "remote_fetch_count",
}
REQUIRED_SUMMARY_FIELDS = {
    "run_id",
    "stage",
    "backend_type",
    "truth_label",
    "chain_profile_id",
    "plugin_profile_id",
    "experiment_profile_id",
    "tx_count",
    "success_count",
    "failure_count",
    "block_count",
    "throughput_tps",
    "avg_latency_ms",
    "p95_latency_ms",
    "p99_latency_ms",
    "runtime_mode",
}
FORBIDDEN_ARTIFACTS = {
    "metatrack_mechanism_metrics.csv",
    "control_decisions.csv",
    "metaflow_events.csv",
    "fabric_validation_summary.csv",
    "fabric_tx_results.csv",
    "fabric_commit_latency.csv",
    "fabric_block_log.csv",
}


def test_runtime_artifacts_exist_and_have_required_fields(tmp_path):
    result = run_v3_single_chain_runtime("single_chain_runtime_smoke", tmp_path)

    for name in (
        "used_chain_profile.yaml",
        "used_plugin_profile.yaml",
        "used_experiment_profile.yaml",
        "runtime.log",
        "summary.csv",
        "summary.json",
        "report.md",
        "block_log.csv",
        "tx_results.csv",
        "state_commit_log.csv",
    ):
        assert (result.output_dir / name).exists()

    assert REQUIRED_BLOCK_FIELDS <= _csv_fields(result.output_dir / "block_log.csv")
    assert REQUIRED_TX_FIELDS <= _csv_fields(result.output_dir / "tx_results.csv")
    assert REQUIRED_SUMMARY_FIELDS <= _csv_fields(result.output_dir / "summary.csv")

    tx_rows = _csv_rows(result.output_dir / "tx_results.csv")
    commit_rows = _csv_rows(result.output_dir / "state_commit_log.csv")
    assert len(tx_rows) == 24
    assert len(commit_rows) >= 24

    summary = json.loads((result.output_dir / "summary.json").read_text(encoding="utf-8"))
    assert summary["truth_label"] == "modular_runtime"
    assert summary["tx_count"] == 24


def test_report_truth_boundaries_and_forbidden_artifacts(tmp_path):
    result = run_v3_single_chain_runtime("single_chain_runtime_smoke", tmp_path)

    report = (result.output_dir / "report.md").read_text(encoding="utf-8")
    assert "not Fabric live execution" in report
    assert "not MetaTrack full evaluation" in report
    assert "not final paper-scale performance evidence" in report

    produced = {path.name for path in result.output_dir.iterdir()}
    assert produced.isdisjoint(FORBIDDEN_ARTIFACTS)


def test_runtime_path_does_not_require_go_docker_fabric_or_network(tmp_path, monkeypatch):
    import subprocess

    def fail_if_called(*_args, **_kwargs):
        raise AssertionError("runtime must not start external commands")

    monkeypatch.setattr(subprocess, "run", fail_if_called)

    result = run_v3_single_chain_runtime("single_chain_runtime_smoke", tmp_path)

    assert result.summary.runtime_mode == "python_reference_single_process_logical_nodes"


def _csv_fields(path):
    with path.open(newline="", encoding="utf-8") as fh:
        return set(csv.DictReader(fh).fieldnames or [])


def _csv_rows(path):
    with path.open(newline="", encoding="utf-8") as fh:
        return list(csv.DictReader(fh))
