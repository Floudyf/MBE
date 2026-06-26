from backend.app.services.v3_profile_preview import preview_all_profiles, preview_profile


def test_preview_includes_blocking_reasons_and_expected_outputs():
    preview = preview_profile("experiment_profile", "metatrack_ablation_profile_preview")

    assert preview["profile_id"] == "metatrack_ablation_profile_preview"
    assert preview["status"] == "planned"
    assert preview["runnable"] is False
    assert preview["blocking_reasons"]
    assert "metatrack_summary.csv" in preview["expected_outputs"]
    assert preview["plugin_summary"]


def test_preview_does_not_create_run_artifacts():
    preview = preview_profile("experiment_profile", "metaflow_dual_chain_profile_preview")

    assert preview["execution"] == {
        "creates_run_id": False,
        "writes_runtime_artifacts": False,
        "starts_fabric": False,
        "starts_docker": False,
        "calls_go_executor": False,
    }


def test_preview_all_profiles_lists_inventory():
    result = preview_all_profiles()

    assert result["stage"] == "V3.3"
    assert "chain_x_default" in result["inventory"]["chain_profiles"]
    assert "metaflow_afs_fda" in result["inventory"]["plugin_profiles"]
    assert any(item["profile_id"] == "fabric_validation_profile_preview" for item in result["items"])


def test_preview_marks_metaflow_as_planned_not_runnable():
    preview = preview_profile("plugin_profile", "metaflow_afs_fda")

    assert preview["status"] == "planned"
    assert preview["runnable"] is False
    assert any("requires v3.6" in warning.lower() for warning in preview["warnings"])
