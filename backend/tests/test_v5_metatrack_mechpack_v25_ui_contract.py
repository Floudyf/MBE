from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]

def read(rel: str) -> str:
    return (ROOT / rel).read_text(encoding="utf-8")

def test_v25_stale_metatrack_draft_is_migrated():
    page = read("frontend/src/pages/V5FormalRunPage.tsx")
    assert "MBE_METATRACK_MECHPACK_V25_UI_MIGRATION" in page
    assert "const metaTrackDraft = methods.some" in page
    assert 'const migratedMethods = suite === "ablation_experiment" && metaTrackDraft ? [...METATRACK_ABLATION_METHOD_IDS] : methods;' in page
    # v2.8 adds a second, orthogonal migration step for the historical diagnostic
    # card.  The v2.5 contract is the canonical four-method MetaTrack ablation
    # migration, not the name of the final temporary variable used by later UI
    # migrations.
    assert 'const migratedDiagnosticMethods = migratedMethods.map((item) => item === "metatrack_diag_parallel" ? "metatrack_diag_serial" : item);' in page
    assert 'selectedMethods: migratedDiagnosticMethods.length ? migratedDiagnosticMethods : [...V5_DEFAULT_METHOD_IDS]' in page

def test_v25_request_time_selection_is_canonical():
    page = read("frontend/src/pages/V5FormalRunPage.tsx")
    assert 'const effectiveSelectedMethodIds = selectedSuite === "ablation_experiment"' in page
    assert '? [...METATRACK_ABLATION_METHOD_IDS]' in page
    # v2.8 maps only the historical comparison diagnostic after the v2.5 formal
    # ablation canonicalization.  Request filtering must consume that final list.
    assert 'const diagnosticMigratedSelectedMethodIds = effectiveSelectedMethodIds.map((item) => item === "metatrack_diag_parallel" ? "metatrack_diag_serial" : item);' in page
    assert 'methods.filter((method) => diagnosticMigratedSelectedMethodIds.includes(method.method_id))' in page

def test_v25_historical_route_subablation_is_hidden_from_formal_cards():
    catalog = read("frontend/src/v5FormalExperimentCatalog.ts")
    route_line = next(line for line in catalog.splitlines() if 'methodId: "metatrack_ab_route"' in line)
    assert 'ablationTarget: "metatrack"' not in route_line
    ids = catalog.split("export const METATRACK_ABLATION_METHOD_IDS = [", 1)[1].split("] as const;", 1)[0]
    assert '"metatrack_ab_route"' not in ids
    for method_id in ("metatrack_latest", "metatrack_ab_track", "metatrack_ab_state", "metatrack_ab_cons"):
        assert f'"{method_id}"' in ids
