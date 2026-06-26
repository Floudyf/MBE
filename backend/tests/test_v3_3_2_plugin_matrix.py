from backend.app.services.v3_profile_preview import preview_profile


def test_metatrack_plugin_matrix_contains_four_methods():
    matrix = preview_profile("experiment_profile", "metatrack_go_backed_ablation_smoke")["plugin_matrix"]
    by_id = {row["method_id"]: row for row in matrix}

    assert set(by_id) == {"baseline_hash_only", "co_access_only", "co_access_dual_track", "full_MetaTrack"}
    assert by_id["baseline_hash_only"]["module_plugins"] == {
        "Routing": "hash_sharding",
        "Execution": "serial_execution",
        "StateAccess": "direct_fetch",
        "Commit": "normal_commit",
    }
    assert by_id["full_MetaTrack"]["module_plugins"] == {
        "Routing": "co_access_sharding",
        "Execution": "dual_track_execution",
        "StateAccess": "access_list_prefetch",
        "Commit": "hot_update_aggregation_commit",
    }
    assert "baseline" in by_id["baseline_hash_only"]["tags"]
    assert {"proposed", "MetaTrack"} <= set(by_id["full_MetaTrack"]["tags"])


def test_composer_preview_does_not_generate_fabric_or_metaflow_artifacts():
    preview = preview_profile("experiment_profile", "single_chain_composer_preview")

    assert preview["runnable"] is False
    assert preview["expected_outputs"] == ["composer_preview.json"]
    assert "fabric_validation_summary.csv" not in preview["expected_outputs"]
    assert "metaflow_events.csv" not in preview["expected_outputs"]
