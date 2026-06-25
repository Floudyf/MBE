from backend.app.services.calibration_report_v2 import generate_calibration_report


def test_calibration_report_contains_required_sections_and_boundaries() -> None:
    report = generate_calibration_report({
        "summary": {
            "data_truth_label": "synthetic_replay",
            "backend_type": "local_virtual",
            "calibration_truth": "synthetic_observation_sample",
            "observed_record_count": 2,
            "matched_record_count": 2,
        },
        "config": {"calibration": {}, "input": {"source_type": "synthetic", "trace_file": "trace/samples/v2_cross_trace_small.jsonl", "meta_file": "trace/samples/v2_multi_chain_trace_meta.json"}},
        "warnings": ["Synthetic calibration sample is not real chain execution."],
    })

    assert "# V2.9 Realism Bridge Report" in report
    assert "## Scope" in report
    assert "## Data Truth" in report
    assert "## Non-goals" in report
    assert "not a V3 live backend" in report
    assert "not real chain execution" in report
    assert "not web control of Fabric" in report
