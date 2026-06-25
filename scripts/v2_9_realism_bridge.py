from __future__ import annotations

import argparse
import json
from pathlib import Path

from backend.app.services.calibration_runner_v2 import CalibrationBlocked, ROOT, run_calibration


def main() -> None:
    parser = argparse.ArgumentParser(description="Run V2.9 chain-backed calibration without starting live chains.")
    parser.add_argument("--config", required=True, help="Calibration config path inside the workspace.")
    parser.add_argument("--out", required=True, help="Output directory for calibration artifacts.")
    args = parser.parse_args()

    try:
        result = run_calibration(Path(args.config), Path(args.out), root=ROOT)
    except CalibrationBlocked as exc:
        print(json.dumps(exc.payload, indent=2))
        raise SystemExit(2)
    print(json.dumps({"status": result["status"], "summary": result["summary"], "output_dir": str(Path(args.out))}, indent=2))


if __name__ == "__main__":
    main()
