from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]


def test_retired_batch_sensitivity_cards_are_deleted() -> None:
    profile = (ROOT / "frontend/src/v5MethodProfile.ts").read_text(encoding="utf-8")
    catalog = (ROOT / "frontend/src/v5FormalExperimentCatalog.ts").read_text(encoding="utf-8")
    backend = (ROOT / "backend/app/services/v5_formal_plan_validator.py").read_text(encoding="utf-8")
    for method_id in ("metatrack_exp50", "metatrack_exp200"):
        assert f'method_id: "{method_id}"' not in profile
        assert f'methodId: "{method_id}"' not in catalog
        assert f'"{method_id}": V5FormalMethod(' not in backend
    assert 'natural_producer_window_v657: true' not in profile
    assert 'stream_partition_invariant_v658: true' not in profile
    assert 'partition_invariant_consensus_v658: true' not in profile
