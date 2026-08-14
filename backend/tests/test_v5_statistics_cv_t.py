import math

from backend.app.services.v5_statistics_service import summarize


def test_student_t_ci_and_cv_for_small_repeat_sample():
    result = summarize([100.0, 110.0, 90.0], completed_count=3, failed_count=0, missing_count=0)
    assert result["count"] == 3
    assert result["mean"] == 100.0
    assert result["std"] == 10.0
    assert math.isclose(result["cv"], 0.1)
    assert math.isclose(result["cv_percent"], 10.0)
    assert math.isclose(result["ci95_critical_value"], 4.302652729911275)
    margin = 4.302652729911275 * 10.0 / math.sqrt(3)
    assert math.isclose(result["ci95_low"], 100.0 - margin)
    assert math.isclose(result["ci95_high"], 100.0 + margin)
    assert result["ci95_method"] == "student_t_95"


def test_single_sample_has_no_variance_cv_or_ci():
    result = summarize([42.0], completed_count=1, failed_count=0, missing_count=0)
    assert result["std"] is None
    assert result["cv"] is None
    assert result["ci95_low"] is None
    assert result["ci95_high"] is None


def test_zero_mean_cv_is_not_reported():
    result = summarize([-1.0, 1.0], completed_count=2, failed_count=0, missing_count=0)
    assert result["mean"] == 0.0
    assert result["cv"] is None
    assert result["cv_percent"] is None
