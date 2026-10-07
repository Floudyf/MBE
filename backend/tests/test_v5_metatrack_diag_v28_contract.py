from pathlib import Path

from backend.app.services.v5_formal_plan_validator import BUILTIN_METHODS
from backend.app.services.v5_plugin_manifest_store import STORE

ROOT = Path(__file__).resolve().parents[2]


def _plugin_diff(left, right):
    keys = set(left.plugin_overrides) | set(right.plugin_overrides)
    return {key for key in keys if left.plugin_overrides.get(key) != right.plugin_overrides.get(key)}


def test_v28_replacement_diagnostic_differs_from_full_only_by_single_business_flag():
    full = BUILTIN_METHODS["metatrack_latest"]
    diag = BUILTIN_METHODS["metatrack_diag_serial"]
    assert diag.role == "custom"
    assert diag.display_name == "诊断：双轨单业务执行"
    assert _plugin_diff(full, diag) == set()
    assert diag.plugin_overrides == full.plugin_overrides

    assert set(diag.plugin_config_overrides) == set(full.plugin_config_overrides)
    assert diag.plugin_config_overrides["routing"] == full.plugin_config_overrides["routing"]
    full_executor = dict(full.plugin_config_overrides["block_executor"])
    diag_executor = dict(diag.plugin_config_overrides["block_executor"])
    assert diag_executor.pop("diagnostic_single_business_execution_v28") is True
    assert diag_executor == full_executor


def test_v28_new_card_replaces_old_visible_diagnostic():
    catalog = (ROOT / "frontend/src/v5FormalExperimentCatalog.ts").read_text(encoding="utf-8")
    ids = catalog.split("export const METATRACK_ABLATION_METHOD_IDS = [", 1)[1].split("] as const;", 1)[0]
    assert '"metatrack_diag_serial"' not in ids
    assert '"metatrack_diag_parallel"' not in ids

    new_line = next(line for line in catalog.splitlines() if 'methodId: "metatrack_diag_serial"' in line)
    old_line = next(line for line in catalog.splitlines() if 'methodId: "metatrack_diag_parallel"' in line)
    assert "comparisonVisible: true" in new_line
    assert "mainVisible: false" in new_line
    assert "ablationTarget" not in new_line
    assert "双轨单业务执行" in new_line
    assert "comparisonVisible: false" in old_line
    assert "历史诊断" in old_line


def test_v28_frontend_profile_keeps_full_dual_track_and_adds_only_runtime_flag():
    front = (ROOT / "frontend/src/v5MethodProfile.ts").read_text(encoding="utf-8")
    start = front.index('method_id: "metatrack_diag_serial"')
    block = front[start: front.index("\n  },", start) + 5]
    assert 'execution: "dual_track_execution"' in block
    assert 'scheduler: "fast_first_scheduler"' in block
    assert 'block_producer: "metatrack_nl_window_v669"' in block
    assert 'state_access: "metatrack_local_exact_access"' in block
    assert "diagnostic_single_business_execution_v28: true" in block


def test_v28_manifest_exposes_fail_closed_boolean_runtime_flag():
    manifest = STORE.get("metatrack_block_executor")
    schema = manifest.config_schema
    properties = schema.get("properties", {})
    flag = properties["diagnostic_single_business_execution_v28"]
    assert flag["type"] == "boolean"
    assert flag["default"] is False
    assert manifest.default_config["diagnostic_single_business_execution_v28"] is False


def test_v28_stale_v27_diagnostic_selection_is_migrated():
    page = (ROOT / "frontend/src/pages/V5FormalRunPage.tsx").read_text(encoding="utf-8")
    assert "MBE_METATRACK_DIAG_V28_UI_MIGRATION" in page
    assert 'item === "metatrack_diag_parallel" ? "metatrack_diag_serial" : item' in page
    assert "migratedDiagnosticMethods" in page
    assert "diagnosticMigratedSelectedMethodIds" in page
