from pathlib import Path

import yaml


ROOT = Path(__file__).resolve().parents[1]
REQUIRED_FIELDS = {"experiment", "template", "workload", "trace", "runtime", "routing", "execution", "commit", "metrics", "output"}
EXPECTED = {
    "v1_baseline_hash_serial", "v1_baseline_blockstm_like", "v1_baseline_calvin_like", "v1_baseline_porygon_like",
    "v1_ours_metatrack", "v1_ablation_no_routing", "v1_ablation_no_dual_track", "v1_ablation_no_hot_aggregation",
    "v1_fabric_chain_backed_asset",
}


def test_v1_experiment_declarations_are_complete_and_safe() -> None:
    paths = sorted((ROOT / "configs" / "experiments").glob("v1_*.yaml"))
    assert {path.stem for path in paths} == EXPECTED
    for path in paths:
        document = yaml.safe_load(path.read_text(encoding="utf-8"))
        assert REQUIRED_FIELDS <= document.keys(), path
        experiment = document["experiment"]
        assert {"name", "version", "stage", "seed", "runnable", "implemented", "description"} <= experiment.keys(), path
        if path.stem == "v1_baseline_hash_serial":
            assert experiment["runnable"] is True
            assert experiment["implemented"] is True
        else:
            assert experiment["runnable"] is False
            assert experiment["implemented"] is False
