from __future__ import annotations

from backend.app.services import v3_metatrack_formal_benchmark_runner as runner


def _long_row(scan_value: str = "scene_hotspot") -> dict:
    return {
        "run_index": 0,
        "experiment_type": "workload_comparison",
        "method_config_id": "",
        "baseline_id": "baseline_hash_serial",
        "seed": 42,
        "scan_variable": "workload_scenario",
        "scan_value": scan_value,
    }


def test_child_dir_name_bounds_long_formal_matrix_points_for_windows():
    name = runner._child_dir_name(_long_row())
    assert len(name) <= runner._MAX_CHILD_DIR_NAME_CHARS
    assert name.startswith("run_000_workload_comparison_")
    assert len(name.rsplit("_", 1)[-1]) == 16


def test_child_dir_name_is_deterministic_and_distinguishes_long_points():
    first = runner._child_dir_name(_long_row("scene_hotspot"))
    again = runner._child_dir_name(_long_row("scene_hotspot"))
    other = runner._child_dir_name(_long_row("scene_hotspot_variant_with_another_long_value"))
    assert first == again
    assert first != other


def test_child_dir_name_preserves_historical_short_names():
    row = {
        "run_index": 1,
        "experiment_type": "comparison",
        "method_config_id": "serial",
        "baseline_id": "",
        "seed": 1,
        "scan_variable": "x",
        "scan_value": "1",
    }
    expected = "run_001_comparison_serial_seed_1_x_1"
    assert len(expected) <= runner._MAX_CHILD_DIR_NAME_CHARS
    assert runner._child_dir_name(row) == expected


def test_failed_windows_case_keeps_generated_profile_path_below_max_path_budget():
    observed_root_chars = 103
    run_id_chars = len("v2run_20260817_170511_873cea")
    separator_chars = 3
    filename_chars = len("generated_experiment_profile.json")
    total = (
        observed_root_chars
        + run_id_chars
        + len(runner._child_dir_name(_long_row()))
        + filename_chars
        + separator_chars
    )
    assert total < 240
