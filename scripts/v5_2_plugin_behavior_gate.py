from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
FORBIDDEN = (
    "metatrack_coaccess_routing", "hash_routing_baseline", "dual_track_execution",
    "serial_execution_baseline", "commutative_hot_update_aggregation", "normal_commit",
)
CORE_FILES = (
    ROOT / "executor" / "v5" / "runtime.go",
    ROOT / "executor" / "v5" / "client.go",
    ROOT / "executor" / "cmd" / "mbe-supervisor" / "main.go",
    ROOT / "backend" / "app" / "services" / "v5_formal_scheduler.py",
)


def checked(command: list[str], cwd: Path) -> None:
    result = subprocess.run(command, cwd=cwd, text=True, capture_output=True, timeout=600)
    if result.returncode:
        raise RuntimeError(f"{' '.join(command)} failed:\n{result.stdout}\n{result.stderr}")


def audit() -> list[str]:
    findings: list[str] = []
    for path in CORE_FILES:
        text = path.read_text(encoding="utf-8")
        for plugin_id in FORBIDDEN:
            if plugin_id in text:
                findings.append(f"core plugin-id branch audit: {path.relative_to(ROOT)} contains {plugin_id}")
    return findings


def main() -> int:
    parser = argparse.ArgumentParser(description="Close V5.2 Gate A with behavioral and real-cluster evidence.")
    parser.add_argument("--output-root", default=str(ROOT / ".cache" / "v5_2_plugin_behavior_gate"))
    args = parser.parse_args()
    try:
        checked(["go", "test", "./v5"], ROOT / "executor")
        checked([sys.executable, "scripts/v5_1_plugin_difference_acceptance.py", "--output-root", str(Path(args.output_root) / "four_methods")], ROOT)
    except RuntimeError as exc:
        print(str(exc), file=sys.stderr)
        return 1
    findings = audit()
    if findings:
        print("\n".join(findings), file=sys.stderr)
        return 1
    print('{"gate":"A","closed":true,"four_method_real_cluster":true,"core_plugin_id_audit":"passed"}')
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
