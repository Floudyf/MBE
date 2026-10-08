"""Official four-member ablation draft migration remains independent of diagnostics."""
from pathlib import Path
ROOT=Path(__file__).resolve().parents[2]
def read(rel): return (ROOT/rel).read_text(encoding='utf-8')

def test_ablation_draft_migration_still_uses_formal_four_group():
    page=read('frontend/src/pages/V5FormalRunPage.tsx')
    assert 'MBE_METATRACK_MECHPACK_V25_UI_MIGRATION' in page
    assert 'const migratedMethods = suite === "ablation_experiment" && metaTrackDraft ? [...METATRACK_ABLATION_METHOD_IDS] : methods;' in page
    assert 'selectedMethods: migratedDiagnosticMethods.length ? migratedDiagnosticMethods : [...V5_DEFAULT_METHOD_IDS]' in page
    assert 'methods.filter((method) => diagnosticMigratedSelectedMethodIds.includes(method.method_id))' in page

def test_formal_ablation_ids_frozen():
    catalog=read('frontend/src/v5FormalExperimentCatalog.ts')
    segment=catalog.split('export const METATRACK_ABLATION_METHOD_IDS = [',1)[1].split('] as const;',1)[0]
    for item in ('metatrack_latest','metatrack_ab_track','metatrack_ab_state','metatrack_ab_cons'):
        assert f'"{item}"' in segment
    assert 'metatrack_diag_' not in segment
