from pathlib import Path

import yaml


ROOT = Path(__file__).resolve().parents[1]
REQUIRED_FIELDS = {"name", "type", "version", "maturity", "description", "default_params", "inputs", "outputs", "metrics"}


def test_v0_plugin_manifests_declare_required_fields() -> None:
    manifests = list((ROOT / "configs" / "plugins").glob("*/plugin.yaml"))

    assert manifests
    for manifest in manifests:
        with manifest.open(encoding="utf-8") as stream:
            declaration = yaml.safe_load(stream)
        assert REQUIRED_FIELDS <= declaration.keys(), manifest
