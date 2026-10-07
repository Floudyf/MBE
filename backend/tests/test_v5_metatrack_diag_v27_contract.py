from pathlib import Path

from backend.app.services.v5_formal_plan_validator import BUILTIN_METHODS

ROOT = Path(__file__).resolve().parents[2]


def _changed_plugins(left, right):
    keys = set(left.plugin_overrides) | set(right.plugin_overrides)
    return {key for key in keys if left.plugin_overrides.get(key) != right.plugin_overrides.get(key)}


def test_v27_diagnostic_profile_differs_from_full_only_by_execution_plugin():
    full = BUILTIN_METHODS["metatrack_latest"]
    diag = BUILTIN_METHODS["metatrack_diag_parallel"]
    assert diag.role == "custom"
    assert diag.display_name == "诊断：统一单轨并行"
    assert _changed_plugins(full, diag) == {"execution"}
    assert full.plugin_overrides["execution"] == "dual_track_execution"
    assert diag.plugin_overrides["execution"] == "metatrack_single_conservative_execution"
    assert diag.plugin_config_overrides == full.plugin_config_overrides


def test_v27_diagnostic_is_comparison_only_not_official_ablation():
    catalog = (ROOT / "frontend/src/v5FormalExperimentCatalog.ts").read_text(encoding="utf-8")
    ids = catalog.split("export const METATRACK_ABLATION_METHOD_IDS = [", 1)[1].split("] as const;", 1)[0]
    assert '"metatrack_diag_parallel"' not in ids
    line = next(line for line in catalog.splitlines() if 'methodId: "metatrack_diag_parallel"' in line)
    assert "comparisonVisible: false" in line
    assert "mainVisible: false" in line
    assert "ablationTarget" not in line
    assert "历史诊断" in line
    assert "统一单轨并行" in line


def test_v27_frontend_profile_matches_backend_diagnostic_boundary():
    front = (ROOT / "frontend/src/v5MethodProfile.ts").read_text(encoding="utf-8")
    start = front.index('method_id: "metatrack_diag_parallel"')
    block = front[start: front.index("\n  },", start) + 5]
    assert 'execution: "metatrack_single_conservative_execution"' in block
    assert 'block_producer: "metatrack_nl_window_v669"' in block
    assert 'state_access: "metatrack_local_exact_access"' in block
    for token in (
        "batch_entry_state_prefetch: true",
        "batch_remote_writeback: true",
        "version_liveness: true",
        "version_liveness_indexed: true",
        "single_final_seal: true",
    ):
        assert token in block
