from pathlib import Path


def test_retired_stream_card_is_hidden_and_50_200_are_deleted():
    catalog = Path("frontend/src/v5FormalExperimentCatalog.ts").read_text(encoding="utf-8")
    assert 'methodId: "metatrack_exp50"' not in catalog
    assert 'methodId: "metatrack_exp200"' not in catalog
    if 'methodId: "metatrack_exp"' in catalog:
        pos = catalog.index('methodId: "metatrack_exp"')
        assert 'comparisonVisible: false' in catalog[pos:pos+450]
