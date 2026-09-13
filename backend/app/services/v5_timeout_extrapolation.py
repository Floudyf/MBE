from __future__ import annotations

import csv
import json
import math
import statistics
from collections import defaultdict
from pathlib import Path
from typing import Any

SCHEMA_VERSION = "mbe_v5_timeout_extrapolation_v1"
MEASUREMENT_TIMEOUT_EXTRAPOLATED = "timeout_extrapolated"
MEASUREMENT_TIMEOUT_UNSTABLE = "timeout_unstable"

TAIL_WINDOW_MS = 20 * 60 * 1000
BUCKET_WINDOW_MS = 5 * 60 * 1000
MIN_OBSERVATION_MS = 55 * 60 * 1000
MAX_CV = 0.15
MAX_ABS_DRIFT = 0.15
MAX_POSITIVE_RECOVERY = 0.10
MIN_TERMINAL_R2 = 0.995


def _read_json(path: Path) -> dict:
    if not path.is_file():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8-sig"))
    except (OSError, UnicodeError, json.JSONDecodeError):
        return {}
    return payload if isinstance(payload, dict) else {}


def _number(value: Any) -> float | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        try:
            return float(value.strip())
        except ValueError:
            return None
    return None


def _int_value(value: Any) -> int | None:
    number = _number(value)
    return int(number) if number is not None else None


def _load_progress(path: Path) -> tuple[list[tuple[int, int]], int | None]:
    if not path.is_file():
        return [], None
    points: list[tuple[int, int]] = []
    submitted: int | None = None
    try:
        with path.open(newline="", encoding="utf-8-sig") as handle:
            for row in csv.DictReader(handle):
                timestamp = _int_value(row.get("timestamp"))
                terminal = _int_value(row.get("terminal"))
                current_submitted = _int_value(row.get("submitted"))
                if timestamp is None or terminal is None:
                    continue
                if points and timestamp < points[-1][0]:
                    continue
                points.append((timestamp, terminal))
                if current_submitted is not None:
                    submitted = current_submitted
    except (OSError, UnicodeError, csv.Error):
        return [], None
    return points, submitted


def _latest_at_or_before(points: list[tuple[int, int]], boundary_ms: int) -> tuple[int, int] | None:
    result: tuple[int, int] | None = None
    for point in points:
        if point[0] > boundary_ms:
            break
        result = point
    return result


def _mean(values: list[float]) -> float:
    return sum(values) / len(values) if values else 0.0


def _sample_cv(values: list[float]) -> float:
    mean = _mean(values)
    if len(values) < 2 or mean <= 0:
        return 0.0
    return statistics.stdev(values) / mean


def _linear_regression(points: list[tuple[int, int]]) -> tuple[float, float]:
    if len(points) < 2:
        return 0.0, 0.0
    origin = points[0][0] / 1000.0
    xs = [timestamp / 1000.0 - origin for timestamp, _ in points]
    ys = [float(terminal) for _, terminal in points]
    mean_x = _mean(xs)
    mean_y = _mean(ys)
    sxx = sum((x - mean_x) ** 2 for x in xs)
    if sxx <= 0:
        return 0.0, 0.0
    sxy = sum((x - mean_x) * (y - mean_y) for x, y in zip(xs, ys))
    slope = sxy / sxx
    intercept = mean_y - slope * mean_x
    sst = sum((y - mean_y) ** 2 for y in ys)
    ssr = sum((y - (intercept + slope * x)) ** 2 for x, y in zip(xs, ys))
    r2 = 1.0 if sst <= 0 else 1.0 - ssr / sst
    return slope, r2


def _first_submitted_ms(run_dir: Path) -> int | None:
    payload = _read_json(run_dir / "client" / "client_submission_complete.json")
    for key in ("first_submitted_at", "first_submitted_at_ms"):
        value = _int_value(payload.get(key))
        if value is not None and value > 0:
            return value
    return None


def analyze_run_timeout_extrapolation(run_dir: Path) -> dict:
    """Analyze an already-terminated supervisor hard timeout.

    The function is diagnostic-only. It never changes execution status and only
    returns an estimate when the final twenty minutes are demonstrably stable.
    """
    run_dir = Path(run_dir)
    drain = _read_json(run_dir / "drain_status.json")
    stalled = _read_json(run_dir / "stalled_runtime_report.json")
    completion_reason = str(drain.get("completion_reason") or "").strip().lower()
    stalled_reason = str(stalled.get("reason") or "").strip().lower()
    if completion_reason != "hard_timeout" and "drain hard timeout" not in stalled_reason:
        return {}

    reasons: list[str] = []
    points, submitted_from_progress = _load_progress(run_dir / "drain_progress.csv")
    if len(points) < 2:
        reasons.append("drain_progress_unavailable")

    submitted = _int_value(drain.get("submitted")) or submitted_from_progress
    terminal = _int_value(drain.get("terminal"))
    if terminal is None and points:
        terminal = points[-1][1]
    first_submitted_ms = _first_submitted_ms(run_dir)
    if first_submitted_ms is None:
        reasons.append("first_submitted_at_missing")

    fresh_status_complete = drain.get("fresh_status_complete") is True
    if not fresh_status_complete:
        reasons.append("status_snapshot_incomplete")

    snapshot = stalled.get("last_snapshot") if isinstance(stalled.get("last_snapshot"), dict) else {}
    min_height = _int_value(snapshot.get("min_height"))
    max_height = _int_value(snapshot.get("max_height"))
    validator_aligned = min_height is not None and max_height is not None and min_height == max_height
    if not validator_aligned:
        reasons.append("validator_height_not_aligned")

    if submitted is None or terminal is None or submitted <= 0 or terminal <= 0 or terminal >= submitted:
        reasons.append("timeout_not_partial_positive_workload")

    if not points:
        end_ms = None
        observed_duration_ms = None
    else:
        end_ms = points[-1][0]
        observed_duration_ms = end_ms - first_submitted_ms if first_submitted_ms is not None else None
    if observed_duration_ms is None or observed_duration_ms < MIN_OBSERVATION_MS:
        reasons.append("observation_shorter_than_55_minutes")

    bucket_tps: list[float] = []
    tail_points: list[tuple[int, int]] = []
    tail_start_ms: int | None = None
    if end_ms is not None:
        tail_start_ms = end_ms - TAIL_WINDOW_MS
        tail_points = [point for point in points if point[0] >= tail_start_ms]
        if len(tail_points) < 2 or points[0][0] > tail_start_ms:
            reasons.append("insufficient_last_20_minute_history")
        else:
            boundaries: list[tuple[int, int]] = []
            for index in range(5):
                boundary = tail_start_ms + index * BUCKET_WINDOW_MS
                point = _latest_at_or_before(points, boundary)
                if point is None:
                    reasons.append("tail_bucket_boundary_missing")
                    break
                boundaries.append(point)
            if len(boundaries) == 5:
                for left, right in zip(boundaries, boundaries[1:]):
                    duration_seconds = (right[0] - left[0]) / 1000.0
                    terminal_delta = right[1] - left[1]
                    if duration_seconds <= 0 or terminal_delta <= 0:
                        reasons.append("non_positive_tail_bucket_progress")
                        break
                    bucket_tps.append(terminal_delta / duration_seconds)

    bucket_mean_tps = _mean(bucket_tps)
    tail_cv = _sample_cv(bucket_tps)
    early_mean_tps = 0.0
    late_mean_tps = 0.0
    relative_drift = 0.0
    if len(bucket_tps) == 4 and bucket_mean_tps > 0:
        early_mean_tps = _mean(bucket_tps[:2])
        late_mean_tps = _mean(bucket_tps[2:])
        relative_drift = (late_mean_tps - early_mean_tps) / bucket_mean_tps
        if tail_cv > MAX_CV:
            reasons.append("tail_cv_above_15_percent")
        if abs(relative_drift) > MAX_ABS_DRIFT:
            reasons.append("tail_absolute_drift_above_15_percent")
        if relative_drift > MAX_POSITIVE_RECOVERY:
            reasons.append("tail_recovery_above_10_percent")
    else:
        reasons.append("four_tail_buckets_not_available")

    regression_tps, terminal_r2 = _linear_regression(tail_points)
    if regression_tps <= 0:
        reasons.append("tail_regression_tps_not_positive")
    if terminal_r2 < MIN_TERMINAL_R2:
        reasons.append("tail_terminal_r2_below_0_995")

    reasons = list(dict.fromkeys(reasons))
    qualified = not reasons
    estimated_remaining_seconds = None
    estimated_total_duration_seconds = None
    estimated_end_to_end_tps = None
    empirical_low = None
    empirical_high = None

    if qualified and submitted is not None and terminal is not None and observed_duration_ms is not None:
        remaining = submitted - terminal
        estimated_remaining_seconds = remaining / regression_tps
        estimated_total_duration_seconds = observed_duration_ms / 1000.0 + estimated_remaining_seconds
        estimated_end_to_end_tps = submitted / estimated_total_duration_seconds
        minimum_rate = min(bucket_tps)
        maximum_rate = max(bucket_tps)
        empirical_low = submitted / (observed_duration_ms / 1000.0 + remaining / minimum_rate)
        empirical_high = submitted / (observed_duration_ms / 1000.0 + remaining / maximum_rate)
    else:
        remaining = submitted - terminal if submitted is not None and terminal is not None else None

    return {
        "schema_version": SCHEMA_VERSION,
        "measurement_type": MEASUREMENT_TIMEOUT_EXTRAPOLATED if qualified else MEASUREMENT_TIMEOUT_UNSTABLE,
        "qualified_for_tps_estimate": qualified,
        "source_completion_reason": "hard_timeout",
        "full_workload_completed": False,
        "estimated_tps_is_observed": False,
        "latency_estimate_eligible": False,
        "diagnostic_only": True,
        "reasons": reasons,
        "extrapolation_assumption": "last_20_minute_terminal_rate_remains_stationary_until_completion",
        "first_submitted_at_ms": first_submitted_ms,
        "tail_observation_start_ms": tail_start_ms,
        "tail_observation_end_ms": end_ms,
        "observed_duration_ms": observed_duration_ms,
        "tail_window_ms": TAIL_WINDOW_MS,
        "tail_bucket_ms": BUCKET_WINDOW_MS,
        "submitted": submitted,
        "terminal_at_timeout": terminal,
        "incomplete_at_timeout": remaining,
        "completion_ratio_at_timeout": (terminal / submitted) if submitted and terminal is not None else None,
        "fresh_status_complete": fresh_status_complete,
        "validator_height_aligned": validator_aligned,
        "tail_bucket_tps": bucket_tps,
        "tail_bucket_mean_tps": bucket_mean_tps,
        "tail_cv": tail_cv,
        "tail_early_10min_mean_tps": early_mean_tps,
        "tail_late_10min_mean_tps": late_mean_tps,
        "tail_relative_drift": relative_drift,
        "tail_regression_tps": regression_tps,
        "tail_terminal_r2": terminal_r2,
        "estimated_remaining_seconds": estimated_remaining_seconds,
        "estimated_total_duration_seconds": estimated_total_duration_seconds,
        "estimated_end_to_end_tps": estimated_end_to_end_tps,
        "empirical_estimated_tps_low": empirical_low,
        "empirical_estimated_tps_high": empirical_high,
        "thresholds": {
            "minimum_observation_ms": MIN_OBSERVATION_MS,
            "maximum_cv": MAX_CV,
            "maximum_absolute_drift": MAX_ABS_DRIFT,
            "maximum_positive_recovery_drift": MAX_POSITIVE_RECOVERY,
            "minimum_terminal_r2": MIN_TERMINAL_R2,
        },
    }


def _timeout_evidence(child: dict) -> dict:
    result = child.get("result") if isinstance(child.get("result"), dict) else {}
    summary = result.get("summary") if isinstance(result.get("summary"), dict) else {}
    evidence = summary.get("timeout_extrapolation")
    return evidence if isinstance(evidence, dict) else {}


def _observed_tps(child: dict) -> float | None:
    if child.get("status") != "completed" or child.get("execution_status", child.get("status")) != "completed":
        return None
    suite = str(child.get("suite_type") or "")
    if suite == "workload_sensitivity":
        if child.get("formal_eligibility") is not True:
            return None
    elif child.get("paper_candidate") is not True:
        return None

    metrics = child.get("metrics") if isinstance(child.get("metrics"), dict) else {}
    value = _number(metrics.get("end_to_end_tps"))
    if value is not None and value > 0:
        return value
    result = child.get("result") if isinstance(child.get("result"), dict) else {}
    summary = result.get("summary") if isinstance(result.get("summary"), dict) else {}
    finality = summary.get("finality_evidence") if isinstance(summary.get("finality_evidence"), dict) else {}
    value = _number(finality.get("end_to_end_tps"))
    return value if value is not None and value > 0 else None


def _estimated_tps(child: dict) -> float | None:
    evidence = _timeout_evidence(child)
    if evidence.get("qualified_for_tps_estimate") is not True:
        return None
    if evidence.get("measurement_type") != MEASUREMENT_TIMEOUT_EXTRAPOLATED:
        return None
    value = _number(evidence.get("estimated_end_to_end_tps"))
    return value if value is not None and value > 0 else None


def _student_t_975(df: int) -> float:
    table = {
        1: 12.706, 2: 4.303, 3: 3.182, 4: 2.776, 5: 2.571, 6: 2.447,
        7: 2.365, 8: 2.306, 9: 2.262, 10: 2.228, 11: 2.201, 12: 2.179,
        13: 2.160, 14: 2.145, 15: 2.131, 16: 2.120, 17: 2.110, 18: 2.101,
        19: 2.093, 20: 2.086, 21: 2.080, 22: 2.074, 23: 2.069, 24: 2.064,
        25: 2.060, 26: 2.056, 27: 2.052, 28: 2.048, 29: 2.045, 30: 2.042,
    }
    return table.get(df, 1.960)


def _summary(values: list[float]) -> dict:
    if not values:
        return {"count": 0, "mean": None, "std": None, "ci95_low": None, "ci95_high": None, "min": None, "max": None}
    mean = statistics.mean(values)
    if len(values) == 1:
        return {"count": 1, "mean": mean, "std": None, "ci95_low": None, "ci95_high": None, "min": mean, "max": mean}
    std = statistics.stdev(values)
    half = _student_t_975(len(values) - 1) * std / math.sqrt(len(values))
    return {
        "count": len(values), "mean": mean, "std": std,
        "ci95_low": mean - half, "ci95_high": mean + half,
        "min": min(values), "max": max(values),
    }


def _group_key(child: dict) -> tuple:
    method = child.get("method") if isinstance(child.get("method"), dict) else {}
    return (
        str(child.get("suite_type") or ""),
        str(child.get("method_config_id") or ""),
        str(method.get("display_name") or child.get("method_config_id") or ""),
        str(child.get("seed") if child.get("seed") is not None else ""),
        str(child.get("scan_variable") or ""),
        str(child.get("scan_value") if child.get("scan_value") is not None else ""),
    )


def _write_csv(path: Path, rows: list[dict], fields: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def write_timeout_tps_exports(group_dir: Path, group: dict, children: list[dict]) -> dict:
    """Write additive timeout-TPS exports without changing existing paper statistics."""
    timeout_rows: list[dict] = []
    buckets: dict[tuple, list[dict]] = defaultdict(list)

    for child in children:
        observed = _observed_tps(child)
        estimated = _estimated_tps(child)
        if observed is not None:
            buckets[_group_key(child)].append({"value": observed, "mode": "observed_complete", "child_id": child.get("child_run_id")})
        if estimated is not None:
            evidence = _timeout_evidence(child)
            buckets[_group_key(child)].append({"value": estimated, "mode": MEASUREMENT_TIMEOUT_EXTRAPOLATED, "child_id": child.get("child_run_id")})
            timeout_rows.append({
                "child_run_id": child.get("child_run_id"),
                "method_config_id": child.get("method_config_id"),
                "method_name": (child.get("method") or {}).get("display_name"),
                "seed": child.get("seed"),
                "repeat_index": child.get("repeat_index"),
                "scan_variable": child.get("scan_variable"),
                "scan_value": child.get("scan_value"),
                "measurement_type": MEASUREMENT_TIMEOUT_EXTRAPOLATED,
                "observed_duration_ms": evidence.get("observed_duration_ms"),
                "terminal_at_timeout": evidence.get("terminal_at_timeout"),
                "incomplete_at_timeout": evidence.get("incomplete_at_timeout"),
                "tail_regression_tps": evidence.get("tail_regression_tps"),
                "tail_cv": evidence.get("tail_cv"),
                "tail_relative_drift": evidence.get("tail_relative_drift"),
                "tail_terminal_r2": evidence.get("tail_terminal_r2"),
                "estimated_remaining_seconds": evidence.get("estimated_remaining_seconds"),
                "estimated_total_duration_seconds": evidence.get("estimated_total_duration_seconds"),
                "estimated_end_to_end_tps": estimated,
                "empirical_estimated_tps_low": evidence.get("empirical_estimated_tps_low"),
                "empirical_estimated_tps_high": evidence.get("empirical_estimated_tps_high"),
                "note": "estimated after hard timeout; not an observed completed sample",
            })

    timeout_fields = [
        "child_run_id", "method_config_id", "method_name", "seed", "repeat_index",
        "scan_variable", "scan_value", "measurement_type", "observed_duration_ms",
        "terminal_at_timeout", "incomplete_at_timeout", "tail_regression_tps", "tail_cv",
        "tail_relative_drift", "tail_terminal_r2", "estimated_remaining_seconds",
        "estimated_total_duration_seconds", "estimated_end_to_end_tps",
        "empirical_estimated_tps_low", "empirical_estimated_tps_high", "note",
    ]
    _write_csv(Path(group_dir) / "timeout_extrapolated_tps.csv", timeout_rows, timeout_fields)

    figure_rows: list[dict] = []
    for key in sorted(buckets):
        entries = buckets[key]
        values = [float(entry["value"]) for entry in entries]
        stats = _summary(values)
        suite, method_id, method_name, seed, scan_variable, scan_value = key
        observed_count = sum(entry["mode"] == "observed_complete" for entry in entries)
        estimated_count = sum(entry["mode"] == MEASUREMENT_TIMEOUT_EXTRAPOLATED for entry in entries)
        modes = []
        if observed_count:
            modes.append("observed_complete")
        if estimated_count:
            modes.append(MEASUREMENT_TIMEOUT_EXTRAPOLATED)
        figure_rows.append({
            "suite_type": suite,
            "method_config_id": method_id,
            "series": method_name,
            "seed": seed,
            "x_variable": scan_variable or "method",
            "x_value": scan_value or method_name,
            "value": stats["mean"],
            "std": stats["std"],
            "ci95_low": stats["ci95_low"],
            "ci95_high": stats["ci95_high"],
            "min": stats["min"],
            "max": stats["max"],
            "sample_count": stats["count"],
            "observed_complete_count": observed_count,
            "timeout_extrapolated_count": estimated_count,
            "measurement_modes": ",".join(modes),
            "marker": "*" if estimated_count else "",
            "raw_values": json.dumps(values, separators=(",", ":")),
            "source_child_ids": json.dumps([entry["child_id"] for entry in entries], separators=(",", ":")),
            "note": (
                "* contains timeout-extrapolated fixed-workload TPS; estimates are explicitly marked"
                if estimated_count else "observed full-workload completion TPS"
            ),
        })

    figure_fields = [
        "suite_type", "method_config_id", "series", "seed", "x_variable", "x_value",
        "value", "std", "ci95_low", "ci95_high", "min", "max", "sample_count",
        "observed_complete_count", "timeout_extrapolated_count", "measurement_modes",
        "marker", "raw_values", "source_child_ids", "note",
    ]
    _write_csv(Path(group_dir) / "paper_figure_tps_with_timeout_extrapolation.csv", figure_rows, figure_fields)
    return {
        "schema_version": "mbe_v5_timeout_tps_export_v1",
        "run_group_id": group.get("run_group_id"),
        "timeout_extrapolated_count": len(timeout_rows),
        "figure_group_count": len(figure_rows),
    }
