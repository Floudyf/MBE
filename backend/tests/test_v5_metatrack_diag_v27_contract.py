"""Retired v2.7 card stays resolvable on the backend for old artifacts."""
from pathlib import Path
from backend.app.services.v5_formal_plan_validator import BUILTIN_METHODS
ROOT=Path(__file__).resolve().parents[2]

def test_retired_v27_not_a_visible_card_and_current_four_way_replacement_exists():
    assert 'metatrack_diag_parallel' in BUILTIN_METHODS
    assert 'metatrack_diag_ready8' in BUILTIN_METHODS
    front=(ROOT/'frontend/src/v5MethodProfile.ts').read_text(encoding='utf-8')
    catalog=(ROOT/'frontend/src/v5FormalExperimentCatalog.ts').read_text(encoding='utf-8')
    assert 'method_id: "metatrack_diag_parallel"' not in front
    assert 'methodId: "metatrack_diag_parallel"' not in catalog
