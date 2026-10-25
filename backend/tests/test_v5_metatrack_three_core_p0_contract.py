"""P0 contract: three-core formal matrix; no new state-lifetime runtime claim."""
from pathlib import Path
import re

ROOT = Path(__file__).resolve().parents[2]
PRIMARY = ('metatrack_latest', 'metatrack_ab_flow', 'metatrack_ab_supply', 'metatrack_ab_cons')
LEGACY = ('metatrack_ab_exact', 'metatrack_ab_track', 'metatrack_sh_noco')


def text(rel):
    return (ROOT / rel).read_text(encoding='utf-8')


def test_primary_frontend_backend_aligned_and_history_registered():
    cat = text('frontend/src/v5FormalExperimentCatalog.ts')
    ids = cat.split('export const METATRACK_ABLATION_METHOD_IDS = [', 1)[1].split('] as const;', 1)[0]
    assert tuple(re.findall(r'"(metatrack_[^"]+)"', ids)) == PRIMARY
    validator = text('backend/app/services/v5_formal_plan_validator.py')
    allow = validator.split('official_metatrack_ids = {', 1)[1].split('}', 1)[0]
    assert set(re.findall(r'"(metatrack_[^"]+)"', allow)) == set(PRIMARY)
    profile = text('frontend/src/v5MethodProfile.ts')
    for method in PRIMARY + LEGACY:
        assert f'methodId: "{method}"' in cat
        assert f'method_id: "{method}"' in profile
        assert f'"{method}": V5FormalMethod(' in validator
    for method in LEGACY + ("metatrack_ab_state",):
        assert method not in ids


def test_state_treatment_is_named_narrowly_and_retains_reliable_writeback():
    cat = text('frontend/src/v5FormalExperimentCatalog.ts')
    assert '状态供应子消融' in cat and '去状态供应组合优化' in cat
    for rel, start, end in (
        ('frontend/src/v5MethodProfile.ts', 'method_id: "metatrack_ab_state"', 'method_id: "metatrack_ab_handoff"'),
        ('backend/app/services/v5_formal_plan_validator.py', '"metatrack_ab_state": V5FormalMethod(', '"metatrack_ab_handoff": V5FormalMethod('),
    ):
        source = text(rel)
        section = source.split(start, 1)[1].split(end, 1)[0]
        compact = re.sub(r'\s+', '', section).lower()
        for token in ('metatrack_home_exact_access', 'batch_entry_state_prefetch', 'ablation_on_demand_state_fetch_v661',
                      'batch_remote_writeback', 'final_version_batch_writeback', 'version_liveness'):
            assert token in section
        assert 'batch_entry_state_prefetch:false' in compact or '"batch_entry_state_prefetch":false' in compact
        assert 'version_liveness:true' in compact or '"version_liveness":true' in compact
        assert 'final_version_batch_writeback:true' in compact or '"final_version_batch_writeback":true' in compact


def test_export_additive_not_relabelled_as_full_liveness_removal():
    exporter = text('backend/app/services/v5_paper_exporter.py')
    assert 'metatrack_three_core_observed_summary.csv' in exporter
    assert 'metatrack_three_core_paper_valid_raw.csv' in exporter
    assert 'ablation_summary.csv' in exporter
    assert 'paper_figure_data.csv' in exporter
    assert 'supply-combination P1' in exporter
