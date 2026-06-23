from pathlib import Path

import yaml


ROOT = Path(__file__).resolve().parents[1]
REQUIRED_FIELDS = {"name", "stage", "fixed_components", "replaceable_components", "required_trace_fields", "output_metrics", "runnable", "description"}


def test_v1_templates_are_parseable_declarations() -> None:
    paths = sorted((ROOT / "configs" / "templates").glob("v1_*.yaml"))

    assert len(paths) == 6
    for path in paths:
        document = yaml.safe_load(path.read_text(encoding="utf-8"))
        assert REQUIRED_FIELDS <= document.keys(), path
        assert document["stage"] == "v1.1"
