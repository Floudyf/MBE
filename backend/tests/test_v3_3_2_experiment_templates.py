import pytest

from backend.app.services.v3_experiment_templates import STANDARD_MODULE_ORDER, V3ExperimentTemplateError, get_template, load_templates, normalize_template


def test_all_v332_templates_load_and_use_standard_order():
    templates = load_templates()

    assert {
        "consensus_only",
        "sharding_only",
        "execution_scheduler_only",
        "state_access_only",
        "commit_only",
        "metatrack_ablation",
        "committee_lifecycle_planned",
    } <= set(templates)
    for template in templates.values():
        assert template["module_order"] == STANDARD_MODULE_ORDER
        assert set(template["module_status"].values()) <= {"fixed", "variable", "disabled", "planned", "output"}


def test_template_modules_cannot_have_multiple_statuses():
    template = get_template("metatrack_ablation")
    bad = {**template, "fixed_modules": list(template["fixed_modules"]) + ["Routing"]}

    with pytest.raises(V3ExperimentTemplateError, match="Routing cannot be both"):
        normalize_template(bad)


def test_metatrack_template_runnable_and_committee_preview_only():
    metatrack = get_template("metatrack_ablation")
    committee = get_template("committee_lifecycle_planned")

    assert metatrack["runnable"] is True
    assert metatrack["preview_only"] is False
    assert committee["runnable"] is False
    assert committee["preview_only"] is True
    assert "CommitteeEpoch" in committee["planned_modules"]
