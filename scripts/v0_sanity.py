"""Run the reproducible V0 asset-hotspot pipeline and validate its artifacts."""

from __future__ import annotations

import csv
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
EXPERIMENT_ID = "v0_default_asset_hotspot"
CONFIG = ROOT / "configs" / "experiments" / f"{EXPERIMENT_ID}.yaml"
RUN_DIR = ROOT / "experiments" / "runs" / EXPERIMENT_ID


def run_command(label: str, command: list[str], cwd: Path) -> None:
    print(f"[v0-sanity] {label}")
    try:
        result = subprocess.run(command, cwd=cwd, text=True, capture_output=True, check=True)
    except FileNotFoundError as error:
        raise RuntimeError(f"{label} 无法启动：找不到命令 {command[0]!r}") from error
    except subprocess.CalledProcessError as error:
        output = "\n".join(part for part in (error.stdout, error.stderr) if part).strip()
        raise RuntimeError(f"{label} 失败（退出码 {error.returncode}）：\n{output}") from error
    if result.stdout.strip():
        print(result.stdout.strip())
    if result.stderr.strip():
        print(result.stderr.strip(), file=sys.stderr)


def require_file(path: Path) -> None:
    if not path.is_file():
        raise RuntimeError(f"缺少预期产物：{path.relative_to(ROOT)}")


def validate_summary(path: Path) -> None:
    require_file(path)
    with path.open(encoding="utf-8", newline="") as stream:
        rows = list(csv.DictReader(stream))
    if len(rows) != 1:
        raise RuntimeError(f"summary.csv 应只有一行指标，实际为 {len(rows)} 行")
    summary = rows[0]
    expected_counts = {"tx_count": "10000", "success_count": "10000", "failed_count": "0"}
    for key, expected in expected_counts.items():
        actual = summary.get(key)
        if actual != expected:
            raise RuntimeError(f"summary.csv 字段 {key} 应为 {expected}，实际为 {actual!r}")
    for key in ("throughput_tps", "cross_shard_ratio", "remote_fetch_count"):
        try:
            value = float(summary[key])
        except (KeyError, ValueError) as error:
            raise RuntimeError(f"summary.csv 缺少有效的 {key} 指标") from error
        if value <= 0:
            raise RuntimeError(f"summary.csv 字段 {key} 应大于 0，实际为 {value}")


def validate_latency(path: Path) -> None:
    require_file(path)
    with path.open(encoding="utf-8", newline="") as stream:
        rows = list(csv.DictReader(stream))
    if len(rows) < 10_000:
        raise RuntimeError(f"latency.csv 应包含至少 10000 条交易，实际为 {len(rows)}")
    for index, row in enumerate(rows[:3], start=1):
        if row.get("status") != "success":
            raise RuntimeError(f"latency.csv 第 {index} 条交易 status 应为 success，实际为 {row.get('status')!r}")


def validate_runtime_log(path: Path) -> None:
    require_file(path)
    content = path.read_text(encoding="utf-8")
    for marker in ("replay start", "replay done"):
        if marker not in content:
            raise RuntimeError(f"runtime.log 缺少标记：{marker}")


def main() -> None:
    if not CONFIG.is_file():
        raise RuntimeError(f"缺少 V0 配置：{CONFIG.relative_to(ROOT)}")

    run_command(
        "生成 asset_hotspot trace",
        [sys.executable, "-m", "workload.asset_hotspot.cli", "--config", str(CONFIG), "--output", str(RUN_DIR)],
        ROOT,
    )
    require_file(RUN_DIR / "trace.jsonl.gz")
    require_file(RUN_DIR / "trace_meta.json")

    run_command(
        "执行 Go replay",
        [
            "go",
            "run",
            "./cmd/replay",
            "--config",
            "../configs/experiments/v0_default_asset_hotspot.yaml",
            "--trace",
            "../experiments/runs/v0_default_asset_hotspot/trace.jsonl.gz",
            "--output",
            "../experiments/runs/v0_default_asset_hotspot",
        ],
        ROOT / "executor",
    )
    validate_summary(RUN_DIR / "summary.csv")
    validate_latency(RUN_DIR / "latency.csv")
    validate_runtime_log(RUN_DIR / "runtime.log")
    print("[v0-sanity] 通过：V0 workload、trace、replay 与指标产物均符合预期。")


if __name__ == "__main__":
    try:
        main()
    except RuntimeError as error:
        print(f"[v0-sanity] 失败：{error}", file=sys.stderr)
        raise SystemExit(1)
