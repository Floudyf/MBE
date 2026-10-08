"""Retired v2.8 card remains historical-only; the current variant replaces it."""
from pathlib import Path
from backend.app.services.v5_formal_plan_validator import BUILTIN_METHODS
ROOT=Path(__file__).resolve().parents[2]

def test_retired_v28_not_visible_and_migration_keeps_existing_drafts():
    assert 'metatrack_diag_serial' in BUILTIN_METHODS
    assert 'metatrack_diag_dual1' in BUILTIN_METHODS
    catalog=(ROOT/'frontend/src/v5FormalExperimentCatalog.ts').read_text(encoding='utf-8')
    page=(ROOT/'frontend/src/pages/V5FormalRunPage.tsx').read_text(encoding='utf-8')
    assert 'methodId: "metatrack_diag_serial"' not in catalog
    assert 'metatrack_diag_serial" ? "metatrack_diag_dual1"' in page
