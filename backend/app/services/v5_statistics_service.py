from __future__ import annotations

import math
import statistics


def summarize(values: list[float], *, completed_count: int, failed_count: int, missing_count: int) -> dict:
    if not values:
        return {"count": 0, "mean": None, "median": None, "std": None, "min": None, "max": None, "ci95_low": None, "ci95_high": None, "completed_count": completed_count, "failed_count": failed_count, "missing_count": missing_count}
    mean = statistics.fmean(values)
    std = statistics.stdev(values) if len(values) > 1 else None
    margin = 1.96 * std / math.sqrt(len(values)) if std is not None else None
    return {"count": len(values), "mean": mean, "median": statistics.median(values), "std": std, "min": min(values), "max": max(values), "ci95_low": mean - margin if margin is not None else None, "ci95_high": mean + margin if margin is not None else None, "completed_count": completed_count, "failed_count": failed_count, "missing_count": missing_count}
