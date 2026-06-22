import gzip, hashlib
from pathlib import Path
import yaml
from workload.asset_hotspot.generator import generate_from_config

ROOT = Path(__file__).resolve().parents[2]
def test_same_seed_is_reproducible(tmp_path):
    config = yaml.safe_load((ROOT / "configs/experiments/v0_default_asset_hotspot.yaml").read_text())
    a, _ = generate_from_config(config, tmp_path / "a"); b, _ = generate_from_config(config, tmp_path / "b")
    assert hashlib.sha256(a.read_bytes()).digest() == hashlib.sha256(b.read_bytes()).digest()
    with gzip.open(a, "rt") as stream: assert sum(1 for _ in stream) == config["workload"]["tx_count"]
