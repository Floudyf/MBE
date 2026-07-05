from __future__ import annotations

from types import SimpleNamespace

from backend.app.models.v3_composer_draft import V3ComposerDraftModule, V3ComposerDraftRequest, V3RuntimeTopology
from backend.app.models.v3_metatrack_formal_benchmark import V3FormalMetatrackBenchmarkRequest
from backend.app.services import v3_metatrack_formal_benchmark_runner as runner


def draft(topology: V3RuntimeTopology | None = None) -> V3ComposerDraftRequest:
    plugins = {
        "Workload": ("fixed", "synthetic_hotspot"),
        "TxPool": ("fixed", "fifo_pool"),
        "BlockProducer": ("fixed", "time_or_count_block_producer"),
        "Consensus": ("fixed", "simple_leader"),
        "CommitteeEpoch": ("disabled", "disabled"),
        "Routing": ("variable", "metatrack_coaccess_routing"),
        "Execution": ("variable", "metatrack_dual_track_execution"),
        "StateAccess": ("variable", "access_list_prefetch"),
        "StateStorage": ("fixed", "hash_state_storage"),
        "Commit": ("variable", "constraint_checked_aggregation"),
        "MetricsReport": ("output", "basic_metrics"),
    }
    return V3ComposerDraftRequest(
        template_id="metatrack_ablation",
        preset_id="metatrack_full_smoke",
        topology=topology or V3RuntimeTopology(),
        modules={module_id: V3ComposerDraftModule(module_id=module_id, status=status, plugin=plugin) for module_id, (status, plugin) in plugins.items()},
    )


def request(**overrides) -> V3FormalMetatrackBenchmarkRequest:
    payload = {
        "draft": draft(),
        "formal_tx_count": 10000,
        "seed_count": 2,
        "baseline_ids": ["baseline_hash_serial", "metatrack_full"],
    }
    payload.update(overrides)
    return V3FormalMetatrackBenchmarkRequest(**payload)


def test_workload_comparison_matrix_contains_scenarios() -> None:
    req = request(experiment_type="workload_comparison", workload_scenario_points=["scene_hotspot", "cross_scene_migration"])
    preview = runner.preview_formal_metatrack_benchmark(req)

    assert preview["is_runnable"] is True
    assert preview["run_count"] == 8
    assert {row["scan_variable"] for row in preview["matrix"]} == {"workload_scenario"}
    assert {row["workload_scenario"] for row in preview["matrix"]} == {"scene_hotspot", "cross_scene_migration"}


def test_workload_comparison_profile_sets_metaverse_workload() -> None:
    req = request(experiment_type="workload_comparison", workload_scenario_points=["mixed_metaverse"])
    row = runner.preview_formal_metatrack_benchmark(req)["matrix"][0]
    profile = runner.build_formal_experiment_profile(req, row)

    assert profile["workload_source"] == "metaverse"
    assert profile["metaverse_scenario"] == "mixed_metaverse"


def test_formal_profile_inherits_topology_details() -> None:
    topology = V3RuntimeTopology(
        node_runtime_mode="local_multi_process",
        process_runtime_mode="dry_run",
        network_adapter="localhost_tcp_preview",
        cross_shard_protocol="relay_mvp",
        state_backend="merkle_trie_mvp",
        max_local_processes=3,
        enable_committee_epoch=True,
        epoch_count=3,
        shard_count=8,
        metaverse_scenario="scene_hotspot",
        workload_source="metaverse",
    )
    req = request(draft=draft(topology))
    row = runner.preview_formal_metatrack_benchmark(req)["matrix"][0]
    profile = runner.build_formal_experiment_profile(req, row)

    assert profile["network_adapter"] == "localhost_tcp_preview"
    assert profile["cross_shard_protocol"] == "relay_mvp"
    assert profile["state_backend"] == "merkle_trie_mvp"
    assert profile["max_local_processes"] == 3
    assert profile["enable_committee_epoch"] is True
    assert profile["epoch_count"] == 3


def test_saved_method_config_can_replace_baselines(monkeypatch) -> None:
    saved = {
        "v3cfg_method": {
            "config_id": "v3cfg_method",
            "config_kind": "method",
            "name": "Saved MetaTrack full",
            "payload": {"draft": runner.model_dump(draft())},
        }
    }
    monkeypatch.setattr(runner, "get_saved_config", lambda config_id: saved[config_id])
    req = request(baseline_ids=[], method_config_ids=["v3cfg_method"], seed_count=1)
    preview = runner.preview_formal_metatrack_benchmark(req)

    assert preview["is_runnable"] is True
    assert preview["run_count"] == 1
    assert preview["matrix"][0]["method_config_id"] == "v3cfg_method"
    assert preview["matrix"][0]["method_config_name"] == "Saved MetaTrack full"


def test_run_writes_progress_and_child_index(monkeypatch, tmp_path) -> None:
    def fake_run_go_v3_runtime(**kwargs):
        output_dir = kwargs["output_dir"]
        output_dir.mkdir(parents=True, exist_ok=True)
        (output_dir / "summary.json").write_text("{}\n", encoding="utf-8")
        return SimpleNamespace(summary={"throughput_tps": 10.0})

    monkeypatch.setattr(runner, "run_go_v3_runtime", fake_run_go_v3_runtime)
    result = runner.run_formal_metatrack_benchmark(request(seed_count=1), root=tmp_path)
    names = {artifact["name"] for artifact in result["artifacts"]}

    assert "formal_run_manifest.json" in names
    assert "formal_progress.json" in names
    assert "formal_failed_runs.csv" in names
    assert "formal_child_artifact_index.csv" in names


def test_existing_trace_preview_is_not_formal_default() -> None:
    req = request(draft=draft(V3RuntimeTopology(workload_source="existing_trace_preview")))
    preview = runner.preview_formal_metatrack_benchmark(req)

    assert preview["is_runnable"] is False
    assert any("existing_trace_preview" in error for error in preview["errors"])
