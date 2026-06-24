from __future__ import annotations

import argparse
from pathlib import Path

import yaml

from .generator import generate_from_config


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", required=True)
    parser.add_argument("--output", required=True)
    args = parser.parse_args()
    with Path(args.config).open(encoding="utf-8") as stream:
        config = yaml.safe_load(stream)
    trace, meta = generate_from_config(config, args.output)
    print(f"wrote {trace}\nwrote {meta}")


if __name__ == "__main__":
    main()
