from copy import deepcopy

from backend.app.services.v3_profile_loader import load_profile_store
from backend.app.services.v3_profile_validator import validate_experiment_profile


def test_metatrack_fairness_passes_when_only_allowed_plugins_differ():
    store = load_profile_store()

    result = validate_experiment_profile(store.experiments["metatrack_ablation_profile_preview"], store)

    assert result["valid"] is True
    assert result["runnable"] is False


def test_metatrack_fairness_fails_when_seed_differs():
    store = load_profile_store()
    profile = deepcopy(store.experiments["metatrack_ablation_profile_preview"])
    profile["fairness"]["same_seed"] = False

    result = validate_experiment_profile(profile, store)

    assert result["valid"] is False
    assert "MetaTrack fairness requires same_seed=true" in result["errors"]


def test_metatrack_fairness_fails_when_block_config_differs():
    store = load_profile_store()
    profile = deepcopy(store.experiments["metatrack_ablation_profile_preview"])
    profile["fairness"]["same_block_config"] = False

    result = validate_experiment_profile(profile, store)

    assert result["valid"] is False
    assert "MetaTrack fairness requires same_block_config=true" in result["errors"]


def test_metatrack_fairness_fails_when_chain_profile_differs():
    store = load_profile_store()
    profile = deepcopy(store.experiments["metatrack_ablation_profile_preview"])
    profile["fairness"]["same_chain_profile"] = False

    result = validate_experiment_profile(profile, store)

    assert result["valid"] is False
    assert "MetaTrack fairness requires same_chain_profile=true" in result["errors"]


def test_metatrack_fairness_fails_with_cross_chain_protocol_variable():
    store = load_profile_store()
    profile = deepcopy(store.experiments["metatrack_ablation_profile_preview"])
    profile["plugin_profiles"]["proposed"] = ["serial_baseline"]

    result = validate_experiment_profile(profile, store)

    assert result["valid"] is False
    assert any("CrossChainProtocolPlugin" in error for error in result["errors"])


def test_metaflow_fairness_passes_when_only_protocol_control_differs():
    store = load_profile_store()

    result = validate_experiment_profile(store.experiments["metaflow_dual_chain_profile_preview"], store)

    assert result["valid"] is True
    assert result["runnable"] is False


def test_metaflow_fairness_fails_when_source_target_profile_changes_unfairly():
    store = load_profile_store()
    profile = deepcopy(store.experiments["metaflow_dual_chain_profile_preview"])
    profile["fairness"]["same_chain_profile"] = False

    result = validate_experiment_profile(profile, store)

    assert result["valid"] is False
    assert "MetaFlow fairness requires same_chain_profile=true" in result["errors"]


def test_metaflow_fairness_fails_when_target_finality_changes_only_for_metaflow():
    store = load_profile_store()
    profile = deepcopy(store.experiments["metaflow_dual_chain_profile_preview"])
    profile["fairness"]["finality_profile"] = ""

    result = validate_experiment_profile(profile, store)

    assert result["valid"] is False
    assert "MetaFlow fairness requires a shared finality_profile" in result["errors"]


def test_metaflow_fairness_fails_with_different_hardware_or_network_profile():
    store = load_profile_store()
    profile = deepcopy(store.experiments["metaflow_dual_chain_profile_preview"])
    profile["fairness"]["same_network_profile"] = False

    result = validate_experiment_profile(profile, store)

    assert result["valid"] is False
    assert "MetaFlow fairness requires same_network_profile=true" in result["errors"]
