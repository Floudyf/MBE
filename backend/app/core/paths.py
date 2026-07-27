from __future__ import annotations

from pathlib import Path
import os

ROOT = Path(__file__).resolve().parents[3]


def _env_path(name: str, fallback: Path) -> Path:
    value = os.environ.get(name, "").strip()
    if not value:
        return fallback
    return Path(value).expanduser()


CACHE_ROOT = ROOT / ".cache"
RUNTIME_ROOT = _env_path("MBE_RUNTIME_ROOT", CACHE_ROOT)
WORKLOAD_CACHE_ROOT = _env_path("MBE_WORKLOAD_CACHE_ROOT", CACHE_ROOT / "workloads")
FORMAL_RUN_ROOT = _env_path("MBE_FORMAL_RUN_ROOT", CACHE_ROOT / "v5_formal_runs")
V5_REAL_CLUSTER_RUNS_ROOT = RUNTIME_ROOT / "v5_real_cluster_runs"
V4_REALISM_RUNS_ROOT = CACHE_ROOT / "v4_realism_runs"
