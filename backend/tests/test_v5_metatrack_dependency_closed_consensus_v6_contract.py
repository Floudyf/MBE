from pathlib import Path


def read(path: str) -> str:
    return Path(path).read_text(encoding="utf-8")


def test_metatrack_ablation_is_registered_only_in_ablation_module():
    catalog = read("frontend/src/v5FormalExperimentCatalog.ts")
    assert 'ablationTarget?: "batch_si" | "metatrack"' in catalog
    assert 'methodId: "metatrack_latest"' in catalog and 'ablationTarget: "metatrack", isFullVariant: true' in catalog
    for method_id in ("metatrack_ab_route", "metatrack_ab_track", "metatrack_ab_cons", "metatrack_ab_state"):
        pos = catalog.index(f'methodId: "{method_id}"')
        block = catalog[pos:pos+700]
        assert 'comparisonVisible: false' in block
        assert 'mainVisible: false' in block
        assert 'ablationTarget: "metatrack"' in block
    assert 'methodId: "metatrack_exp50"' not in catalog
    assert 'methodId: "metatrack_exp200"' not in catalog


def test_full_metatrack_still_enables_critical_width_closure():
    backend = read("backend/app/services/v5_formal_plan_validator.py")
    full = backend.split('"metatrack_latest": V5FormalMethod(', 1)[1].split('"metatrack_ab_route": V5FormalMethod(', 1)[0]
    assert '"dependency_closed_consensus": True' in full
    assert '"version_liveness_indexed": True' in full
    assert '"single_final_seal": True' in full
    assert '"ablation_disable_critical_width_window_v659": True' not in full


def test_ablation_analysis_labels_present():
    panel = read("frontend/src/components/v5/V5AnalysisPanel.tsx")
    mechanism = read("frontend/src/components/v5/V5MechanismAnalysis.tsx")
    for method_id in ("metatrack_ab_route", "metatrack_ab_track", "metatrack_ab_cons", "metatrack_ab_state"):
        assert method_id in panel
        assert method_id in mechanism


def test_ablation_form_validation_is_generic_not_batch_si_hardcoded():
    page = read("frontend/src/pages/V5FormalRunPage.tsx")
    assert 'method.method_id === "hash_batch_si"' not in page
    assert 'const mains = input.selected.filter((method) => method.role === "main")' in page
    assert 'const variants = input.selected.filter((method) => method.role === "ablation" || method.role === "baseline")' in page
    assert 'mains.length !== 1 || variants.length < 1' in page
    assert '消融实验至少需要一个完整版本和一个消融变体。' in page
