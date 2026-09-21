from pathlib import Path


def test_v20_metric_patcher_materializes_both_timing_truth_regions() -> None:
    root = Path(__file__).resolve().parents[2]
    source = (root / "backend" / "app" / "services" / "v5_metric_extractor.py").read_text(encoding="utf-8")
    for marker in (
        "execution_cpu_sum_ms",
        "execution_critical_path_ms",
        "business_execution_critical_path_ms",
        "porygon_local_business_execution_count_by_node",
        "porygon_esc_ownership_verified",
    ):
        assert marker in source
