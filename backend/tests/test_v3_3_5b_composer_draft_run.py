from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

import pytest

from backend.app.models.v3_composer_draft import V3ComposerDraftModule, V3ComposerDraftRequest
from backend.app.services import v3_composer_draft_runner as draft_runner


def valid_draft() -> V3ComposerDraftRequest:
    plugins = {
        "Workload": ("fixed", "synthetic_hotspot"),
        "TxPool": ("fixed", "fifo_pool"),
        "BlockProducer": ("fixed", "time_or_count_block_producer"),
        "Consensus": ("fixed", "simple_leader"),
        "CommitteeEpoch": ("disabled", "disabled"),
        "Routing": ("variable", "co_access_sharding"),
        "Execution": ("variable", "dual_track_execution"),
        "StateAccess": ("variable", "access_list_prefetch"),
        "StateStorage": ("fixed", "hash_state_storage"),
        "Commit": ("variable", "hot_update_aggregation_commit"),
        "MetricsReport": ("output", "basic_metrics"),
    }
    return V3ComposerDraftRequest(
        template_id="metatrack_ablation",
        modules={
            module_id: V3ComposerDraftModule(module_id=module_id, status=status, plugin=plugin)
            for module_id, (status, plugin) in plugins.items()
        },
    )


def test_run_draft_smoke_writes_single_draft_artifacts(monkeypatch, tmp_path: Path) -> None:
    calls: list[dict] = []

    def fake_run_go_v3_runtime(**kwargs):
        output_dir = kwargs["output_dir"]
        calls.append(kwargs)
        output_dir.mkdir(parents=True, exist_ok=True)
        (output_dir / "summary.csv").write_text("tx_count,success_count\n1,1\n", encoding="utf-8")
        (output_dir / "latency.csv").write_text("tx_id,latency_ms\n0,1\n", encoding="utf-8")
        (output_dir / "runtime.log").write_text("draft smoke complete\n", encoding="utf-8")
        for name in ("node_topology.csv", "node_log.csv", "network_log.csv", "consensus_message_log.csv"):
            (output_dir / name).write_text("id\n1\n", encoding="utf-8")
        summary = {
            "tx_count": 1,
            "success_count": 1,
            "shard_count": 4,
            "validators_per_shard": 4,
            "logical_node_count": 25,
            "validator_node_count": 16,
            "executor_node_count": 4,
            "storage_node_count": 4,
            "supervisor_node_count": 1,
            "message_count": 8,
            "network_message_count": 4,
            "consensus_message_count": 4,
            "node_event_count": 29,
        }
        (output_dir / "summary.json").write_text('{"tx_count": 1, "success_count": 1, "logical_node_count": 25}\n', encoding="utf-8")
        return SimpleNamespace(output_dir=output_dir, summary=summary)

    monkeypatch.setattr(draft_runner, "run_go_v3_runtime", fake_run_go_v3_runtime)

    result = draft_runner.run_v3_composer_draft_smoke(valid_draft(), root=tmp_path)

    assert result["status"] == "completed"
    assert result["stage"] == "V3.5.1"
    assert result["current_stage"] == "V3.5.1"
    assert result["latest_runtime_stage"] == "V3.5.1 logical node topology runtime"
    assert result["runtime_truth"] == "single_process_logical_node_topology_runtime"
    assert result["run_mode"] == "draft_smoke"
    assert result["topology_summary"]["logical_node_count"] == 25
    assert len(calls) == 1
    assert calls[0]["plugin_profile_id"] == draft_runner.DRAFT_PLUGIN_PROFILE_ID
    run_dir = Path(result["output_dir"])
    for name in (
        "composer_draft.json",
        "normalized_draft.json",
        "draft_validation.json",
        "generated_experiment_profile.json",
        "generated_experiment_profile.yaml",
        "generated_plugin_profile.yaml",
        "summary.csv",
        "latency.csv",
        "runtime.log",
        "node_topology.csv",
        "node_log.csv",
        "network_log.csv",
        "consensus_message_log.csv",
    ):
        assert (run_dir / name).is_file()
    artifact_names = {artifact["name"] for artifact in result["artifacts"]}
    assert {"composer_draft.json", "normalized_draft.json", "generated_experiment_profile.json", "summary.csv", "runtime.log", "node_topology.csv", "node_log.csv", "network_log.csv", "consensus_message_log.csv"} <= artifact_names


def test_run_draft_smoke_invalid_draft_does_not_start_runner(monkeypatch, tmp_path: Path) -> None:
    started = False

    def fake_run_go_v3_runtime(**_kwargs):
        nonlocal started
        started = True
        raise AssertionError("runner should not start")

    monkeypatch.setattr(draft_runner, "run_go_v3_runtime", fake_run_go_v3_runtime)
    invalid = valid_draft()
    invalid.modules["Consensus"] = V3ComposerDraftModule(module_id="Consensus", status="disabled", plugin="simple_leader")

    with pytest.raises(draft_runner.DraftSmokeNotRunnable):
        draft_runner.run_v3_composer_draft_smoke(invalid, root=tmp_path)

    assert started is False
