"""Retained historical MetaTrack scheduler diagnostic contracts.

The visible four variants are not added to the official ablation suite.
"""
from pathlib import Path
from backend.app.services.v5_formal_plan_validator import BUILTIN_METHODS
from backend.app.services.v5_plugin_manifest_store import STORE

ROOT = Path(__file__).resolve().parents[2]
IDS = (
    'metatrack_latest',
    'metatrack_diag_ready8',
    'metatrack_diag_dual1',
    'metatrack_diag_fifo1',
)


def test_current_full_clone_and_isolated_execution_control():
    full = BUILTIN_METHODS[IDS[0]]
    expected = (
        ('metatrack_diag_ready8', 'diagnostic_unified_ready_v1', None),
        ('metatrack_diag_dual1', 'diagnostic_single_business_execution_v1', None),
        ('metatrack_diag_fifo1', None, 'metatrack_single_execution'),
    )
    for method_id, flag, execution in expected:
        item = BUILTIN_METHODS[method_id]
        assert item.role == 'custom'
        assert item.plugin_overrides == {**full.plugin_overrides, **({'execution': execution} if execution else {})}
        assert item.plugin_config_overrides['routing'] == full.plugin_config_overrides['routing']
        current = dict(item.plugin_config_overrides['block_executor'])
        if flag:
            assert current.pop(flag, False) is True
        assert current == full.plugin_config_overrides['block_executor']
        assert set(item.plugin_config_overrides) == set(full.plugin_config_overrides)


def test_strict_fifo_matches_registered_official_a2_without_changing_a2():
    strict = BUILTIN_METHODS['metatrack_diag_fifo1']
    official = BUILTIN_METHODS['metatrack_ab_track']
    assert strict.plugin_overrides == official.plugin_overrides
    assert strict.plugin_config_overrides == official.plugin_config_overrides


def test_current_boolean_flags_fail_closed_in_plugin_manifest():
    manifest = STORE.get('metatrack_block_executor')
    for flag in ('diagnostic_unified_ready_v1', 'diagnostic_single_business_execution_v1'):
        assert manifest.default_config[flag] is False
        assert manifest.config_schema['properties'][flag] == {'type': 'boolean', 'default': False}


def test_four_cards_only_in_metatrack_comparison_and_old_diagnostics_gone():
    catalog = (ROOT/'frontend/src/v5FormalExperimentCatalog.ts').read_text(encoding='utf-8')
    profile = (ROOT/'frontend/src/v5MethodProfile.ts').read_text(encoding='utf-8')
    page = (ROOT/'frontend/src/pages/V5FormalRunPage.tsx').read_text(encoding='utf-8')
    ablations = catalog.split('export const METATRACK_ABLATION_METHOD_IDS = [', 1)[1].split('] as const;', 1)[0]
    for method_id in IDS:
        assert method_id in catalog and method_id in profile
    for method_id in IDS[1:]:
        row = next(row for row in catalog.splitlines() if f'methodId: "{method_id}"' in row)
        assert 'comparisonVisible: false' in row
        assert 'mainVisible: false' in row
        assert 'ablationTarget' not in row
        assert method_id not in ablations
    for retired_id in ('metatrack_diag_parallel','metatrack_diag_serial'):
        assert f'methodId: "{retired_id}"' not in catalog
        assert f'method_id: "{retired_id}"' not in profile
    assert 'METATRACK_SHARD4_METHOD_IDS' in page
    assert 'v5-metatrack-shard4-preset' in page
    assert 'metatrack_diag_parallel" ? "metatrack_diag_ready8"' in page
    assert 'metatrack_diag_serial" ? "metatrack_diag_dual1"' in page


def test_runtime_semantics_not_just_four_frontend_titles():
    runtime = (ROOT/'executor/v5/registry.go').read_text(encoding='utf-8')
    assert 'decision.Track = "conservative"' in runtime
    assert 'singleReadyQueueV661 := singleConservativeSerialV675 || diagnosticUnifiedReadyV1' in runtime
    assert 'singleConservativeSerialV675 || diagnosticSingleBusinessV28' in runtime
    assert 'metatrack_diag_unified_ready_v1' in runtime
    assert 'metatrack_diag_single_business_execution_v1' in runtime
    assert 'metatrack_single_fifo_hol_enabled' in runtime
