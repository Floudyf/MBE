"""MetaTrack 4-way execution-sharding comparison: only routing changes."""
from pathlib import Path
from backend.app.services.v5_formal_plan_validator import BUILTIN_METHODS
from backend.app.services.v5_plugin_manifest_store import STORE

ROOT = Path(__file__).resolve().parents[2]
FULL = 'metatrack_latest'
HASH = 'metatrack_sh_hash'
COACC = 'metatrack_sh_coacc'
NOCOACC = 'metatrack_sh_noco'
IDS = (FULL,HASH,COACC,NOCOACC)
LEGACY = ('metatrack_diag_ready8','metatrack_diag_dual1','metatrack_diag_fifo1')


def test_sharding_controls_clone_full_without_execution_consensus_or_state_changes():
    full=BUILTIN_METHODS[FULL]
    expected=((HASH,'metatrack_hash_routing',False,False),
              (COACC,'metatrack_coaccess_routing',False,False),
              (NOCOACC,'metatrack_coaccess_routing',True,True))
    for mid,routing,incremental,remove_coacc in expected:
        m=BUILTIN_METHODS[mid]
        assert m.role=='custom'
        assert m.plugin_overrides == {**full.plugin_overrides,'routing':routing}
        assert m.plugin_config_overrides['block_executor'] == full.plugin_config_overrides['block_executor']
        assert set(m.plugin_config_overrides) == set(full.plugin_config_overrides)
        opts=dict(m.plugin_config_overrides['routing'])
        assert opts.pop('incremental_exact_continuity_routing_v65') is incremental
        assert opts.pop('ablation_ignore_coaccess_routing_v661',False) is remove_coacc
        full_opts=dict(full.plugin_config_overrides['routing'])
        full_opts.pop('incremental_exact_continuity_routing_v65')
        assert opts==full_opts
    assert BUILTIN_METHODS[FULL].plugin_config_overrides['routing']['incremental_exact_continuity_routing_v65'] is True


def test_existing_plugins_and_branches_reused_instead_of_new_algorithms():
    assert STORE.get('metatrack_hash_routing') is not None
    assert STORE.get('metatrack_coaccess_routing') is not None
    schema=STORE.get('metatrack_coaccess_routing').config_schema['properties']
    assert schema['ablation_ignore_coaccess_routing_v661']['type']=='boolean'
    go=(ROOT/'executor/v5/registry.go').read_text(encoding='utf-8')
    assert 'func (p *metaTrackHashRouting) PlanBatch(' in go
    assert 'return p.planIncrementalExactContinuityV650(input)' in go
    assert 'PlacementPolicy: "frequency_coaccess_admissible_v2"' in go
    assert 'ablation_no_coaccess_sharding_v675' in go
    assert 'if p != nil && boolFromAny(p.config["ablation_no_coaccess_sharding_v675"])' in go
    opts=BUILTIN_METHODS[NOCOACC].plugin_config_overrides['routing']
    assert 'ablation_no_coaccess_sharding_v675' not in opts


def test_only_four_comparison_cards_are_visible_and_preset_is_updated():
    catalog=(ROOT/'frontend/src/v5FormalExperimentCatalog.ts').read_text(encoding='utf-8')
    page=(ROOT/'frontend/src/pages/V5FormalRunPage.tsx').read_text(encoding='utf-8')
    ts=(ROOT/'frontend/src/v5MethodProfile.ts').read_text(encoding='utf-8')
    for mid in IDS:
        matches=[row for row in catalog.splitlines() if f'methodId: "{mid}"' in row]
        assert len(matches)==1
        assert 'comparisonVisible: true' in matches[0]
        assert f'method_id: "{mid}"' in ts
        assert f'"{mid}": V5FormalMethod(' in (ROOT/'backend/app/services/v5_formal_plan_validator.py').read_text(encoding='utf-8')
    for mid in LEGACY:
        matches=[row for row in catalog.splitlines() if f'methodId: "{mid}"' in row]
        assert len(matches)==1
        assert 'comparisonVisible: false' in matches[0]
        assert mid in BUILTIN_METHODS  # old artifact compatibility
    method_set=catalog.split('export const METATRACK_SHARD4_METHOD_IDS = [',1)[1].split('] as const;',1)[0]
    assert all(f'"{mid}"' in method_set for mid in IDS)
    assert 'MetaTrack 四组执行分片' in page
    assert 'v5-metatrack-shard4-preset' in page
    assert 'METATRACK_SHARD4_METHOD_IDS' in page
    assert all(mid in page for mid in IDS[1:])
    official=catalog.split('export const METATRACK_ABLATION_METHOD_IDS = [',1)[1].split('] as const;',1)[0]
    assert all(mid not in official for mid in IDS[1:])


def test_full_profile_remains_the_control_and_old_scheduler_flags_are_absent_in_new_routes():
    front=(ROOT/'frontend/src/v5MethodProfile.ts').read_text(encoding='utf-8')
    full=front.split('method_id: "metatrack_latest"',1)[1].split('method_id: "metatrack_unified"',1)[0]
    assert 'routing: "metatrack_coaccess_routing"' in full
    assert 'incremental_exact_continuity_routing_v65: true' in full
    assert 'diagnostic_unified_ready_v1: true' not in full
    assert 'diagnostic_single_business_execution_v1: true' not in full
    for mid in IDS[1:]:
        section=front.split('method_id: "'+mid+'"',1)[1].split('\n  },',1)[0]
        assert 'diagnostic_unified_ready_v1: true' not in section
        assert 'diagnostic_single_business_execution_v1: true' not in section
