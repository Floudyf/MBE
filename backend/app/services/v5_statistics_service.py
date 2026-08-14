from __future__ import annotations

import math
import statistics


# Two-sided 95% Student-t critical values for df=1..30. Formal repeats are
# capped well below this range; values above 30 use the normal approximation.
_T95 = {
    1: 12.706204736432095, 2: 4.302652729911275, 3: 3.182446305284263,
    4: 2.7764451051977987, 5: 2.5705818366147395, 6: 2.4469118487916806,
    7: 2.3646242510102993, 8: 2.306004135204166, 9: 2.2621571627409915,
    10: 2.2281388519649385, 11: 2.200985160091638, 12: 2.1788128296634177,
    13: 2.1603686564610127, 14: 2.1447866879169273, 15: 2.131449545559323,
    16: 2.1199052992210112, 17: 2.1098155778331806, 18: 2.10092204024096,
    19: 2.093024054408263, 20: 2.0859634472658364, 21: 2.079613844727662,
    22: 2.0738730679040147, 23: 2.0686576104190406, 24: 2.0638985616280205,
    25: 2.059538552753294, 26: 2.055529438642871, 27: 2.051830516480289,
    28: 2.048407141795244, 29: 2.045229642132703, 30: 2.0422724563012373,
}


def summarize(values: list[float], *, completed_count: int, failed_count: int, missing_count: int) -> dict:
    if not values:
        return {
            "count": 0,
            "mean": None,
            "median": None,
            "std": None,
            "min": None,
            "max": None,
            "cv": None,
            "cv_percent": None,
            "ci95_low": None,
            "ci95_high": None,
            "ci95_critical_value": None,
            "ci95_method": "unavailable",
            "completed_count": completed_count,
            "failed_count": failed_count,
            "missing_count": missing_count,
        }

    numeric = [float(value) for value in values]
    mean = statistics.fmean(numeric)
    std = statistics.stdev(numeric) if len(numeric) > 1 else None
    cv = None
    if std is not None and abs(mean) > 1e-12:
        cv = std / abs(mean)

    critical = None
    margin = None
    ci_method = "single_sample_no_ci"
    if std is not None:
        df = len(numeric) - 1
        critical = _T95.get(df, 1.96)
        margin = critical * std / math.sqrt(len(numeric))
        ci_method = "student_t_95" if df in _T95 else "normal_approx_95"

    return {
        "count": len(numeric),
        "mean": mean,
        "median": statistics.median(numeric),
        "std": std,
        "min": min(numeric),
        "max": max(numeric),
        "cv": cv,
        "cv_percent": cv * 100.0 if cv is not None else None,
        "ci95_low": mean - margin if margin is not None else None,
        "ci95_high": mean + margin if margin is not None else None,
        "ci95_critical_value": critical,
        "ci95_method": ci_method,
        "completed_count": completed_count,
        "failed_count": failed_count,
        "missing_count": missing_count,
    }
