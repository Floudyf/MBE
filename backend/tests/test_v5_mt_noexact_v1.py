"""Routing-only fifth ablation: no safety/runtime or original four changes."""
from pathlib import Path
from backend.app.services.v5_formal_plan_validator import BUILTIN_METHODS

ROOT=Path(__file__).resolve().parents[2]
MID='metatrack_ab_exact'
FLAG='ablation_no_exact_version_routing_v1'


def test_noexact_is_full_plus_only_routing_flag():
    full=BUILTIN_METHODS['metatrack_latest']
    cut=BUILTIN_METHODS[MID]
    assert cut.role=='ablation'
    assert cut.plugin_overrides==full.plugin_overrides
    assert cut.plugin_config_overrides['block_executor']==full.plugin_config_overrides['block_executor']
    assert set(cut.plugin_config_overrides)==set(full.plugin_config_overrides)
    assert cut.plugin_config_overrides['routing']=={**full.plugin_config_overrides['routing'],FLAG:True}
    assert 'ablation_hash_routing_v662' not in cut.plugin_config_overrides['routing']
    assert 'ablation_ignore_coaccess_routing_v661' not in cut.plugin_config_overrides['routing']


def test_catalog_contains_exact_six_and_legacy_four():
    catalog=(ROOT/'frontend/src/v5FormalExperimentCatalog.ts').read_text('utf-8-sig')
    s=catalog.split('export const METATRACK_ABLATION_METHOD_IDS = [',1)[1].split('] as const;',1)[0]
    import re
    assert re.findall(r'"(metatrack_[^"]+)"',s)==[
       'metatrack_latest','metatrack_ab_flow',MID,
       'metatrack_ab_track','metatrack_ab_state','metatrack_ab_cons']
    for expected in ('metatrack_ab_flow','metatrack_ab_track','metatrack_ab_state','metatrack_ab_cons'):
        assert expected in s
    assert 'metatrack_sh_noco' not in s


def test_noexact_branch_is_really_three_ranks():
    src=(ROOT/'executor/v5/metatrack_incremental_routing_v650.go').read_text('utf-8-sig')
    body=src.split('func metaTrackRankWithoutExactV1(',1)[1].split('func metaTrackIncrementalCandidateLessV650(',1)[0]
    assert 'RemoteCost < b.RemoteCost' in body
    assert 'CoaccessLocality > b.CoaccessLocality' in body
    assert 'Load < b.Load' in body
    assert 'ReadyRank' not in body.split('func metaTrackRankWithoutExactV1(',1)[-1].split('return ranked',1)[0]
    assert 'ExactCross' not in body.split('func metaTrackRankWithoutExactV1(',1)[-1].split('return ranked',1)[0]
    assert 'selector := metaTrackIncrementalRankCandidatesV651' in src
    assert 'selector = metaTrackRankWithoutExactV1' in src
    assert 'filtered = selector(filtered)' in src
    assert 'unconstrained := selector(candidates)' in src
    assert 'plan.IncrementalExactCrossShardStateEdgeCount++' in src
    manifest=(ROOT/'backend/app/services/v5_plugin_manifest_store.py').read_text('utf-8-sig')
    assert f'"{FLAG}": {{"type": "boolean", "default": False}}' in manifest
