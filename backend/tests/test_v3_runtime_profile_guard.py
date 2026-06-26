from backend.app.services.v3_profile_loader import load_profile_store
from backend.app.services.v3_profile_preview import preview_profile
from backend.app.services.v3_profile_validator import validate_experiment_profile


def test_v32_smoke_profile_loads_and_is_runnable():
    store = load_profile_store()
    profile = store.experiments["single_chain_runtime_smoke"]

    result = validate_experiment_profile(profile, store)

    assert profile["experiment"]["stage"] == "v3.2"
    assert profile["experiment"]["truth_label"] == "modular_runtime"
    assert profile["experiment"]["backend_type"] == "modular_research_chain"
    assert result["valid"] is True
    assert result["runnable"] is True


def test_future_v3_profiles_remain_planned_not_runnable():
    store = load_profile_store()

    for profile_id in (
        "metatrack_ablation_profile_preview",
        "fabric_validation_profile_preview",
        "metaflow_dual_chain_profile_preview",
    ):
        result = validate_experiment_profile(store.experiments[profile_id], store)
        assert result["valid"] is True
        assert result["status"] == "planned"
        assert result["runnable"] is False


def test_v33_go_backed_metatrack_smoke_validates_runnable():
    store = load_profile_store()
    profile = store.experiments["metatrack_go_backed_ablation_smoke"]

    result = validate_experiment_profile(profile, store)

    assert profile["experiment"]["stage"] == "v3.3"
    assert profile["experiment"]["runtime_mode"] == "go_backed"
    assert result["valid"] is True
    assert result["runnable"] is True


def test_metaflow_preview_remains_planned():
    preview = preview_profile("experiment_profile", "metaflow_dual_chain_profile_preview")

    assert preview["status"] == "planned"
    assert preview["runnable"] is False
    assert "control_decisions.csv" in preview["expected_outputs"]
