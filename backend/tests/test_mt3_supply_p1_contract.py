"""P1 formal-selection contract: normal-execution promotion, retained runtime safety boundaries."""
from pathlib import Path
import ast
import re

ROOT = Path(__file__).resolve().parents[2]
PRIMARY = ('metatrack_latest', 'metatrack_ab_flow', 'metatrack_ab_supply', 'metatrack_ab_cons')


def read(rel):
    return (ROOT / rel).read_text(encoding='utf-8')


def test_p1_registered_as_primary_and_old_state_retained():
    cat = read('frontend/src/v5FormalExperimentCatalog.ts')
    primary = cat.split('export const METATRACK_ABLATION_METHOD_IDS = [', 1)[1].split('] as const;', 1)[0]
    assert tuple(re.findall(r'"(metatrack_[^"]+)"', primary)) == PRIMARY
    assert '"metatrack_ab_state"' not in primary
    assert 'methodId: "metatrack_ab_state"' in cat
    assert 'methodId: "metatrack_ab_supply"' in cat
    backend = read('backend/app/services/v5_formal_plan_validator.py')
    ast.parse(backend)
    assert '"metatrack_ab_supply": V5FormalMethod(' in backend
    allow = backend.split('official_metatrack_ids = {', 1)[1].split('}', 1)[0]
    assert '"metatrack_ab_supply"' in allow
    assert '"metatrack_ab_state"' not in allow


def test_candidate_frontend_backend_flags():
    front = read('frontend/src/v5MethodProfile.ts')
    back = read('backend/app/services/v5_formal_plan_validator.py')
    for whole, begin in ((front, 'method_id: "metatrack_ab_supply"'),
                         (back, '"metatrack_ab_supply": V5FormalMethod(')):
        section = whole.split(begin, 1)[1].split('metatrack_ab_handoff', 1)[0]
        normalized = re.sub(r'\s+', '', section).lower()
        normalized = re.sub(r'["\']([a-z_][a-z0-9_]*)["\']:', r'\1:', normalized)
        for flag in (
            'batch_entry_state_prefetch:false', 'batch_remote_writeback:false',
            'final_version_batch_writeback:false', 'version_liveness:true',
            'dependency_closed_consensus:true', 'ablation_conservative_home_publish_v1:true',
            'ablation_on_demand_state_fetch_v661:true',
        ):
            assert flag in normalized, (begin, flag)
        assert 'metatrack_home_exact_access' in section
    assert '去状态供应组合优化' in front and '去状态供应组合优化' in back


def test_publisher_preserves_acked_home_path_and_signed_metadata():
    source = read('executor/v5/metatrack_version_liveness_v5.go')
    assert 'func metaTrackConservativeHomeClassV1(' in source
    assert 'ablation_conservative_home_publish_v1' in source
    assert 'r.applyRemoteStateDelta(ctx, block, versionItem' in source
    assert 'validateMetaTrackVersionLivenessMetadata(item)' in source
    assert 'metatrack_ab_supply_forced_home_publish_count' in source
    assert 'r.metaTrackBlockExecutorFlag("dependency_closed_consensus")' in source
    assert 'r.metaTrackBlockExecutorFlag("batch_remote_writeback")' in source
