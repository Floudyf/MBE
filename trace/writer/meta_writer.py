"""Writer for V0 trace metadata."""

from __future__ import annotations

import json
from collections.abc import Mapping
from pathlib import Path

REQUIRED_META_FIELDS = ("tx_count", "actual_tx_mix", "actual_hot_key_ratio", "actual_cross_shard_ratio", "avg_read_set_size", "avg_write_set_size", "seed", "trace_format", "compression", "schema_version")


def write_trace_meta(metadata: Mapping[str, object], path: str | Path) -> None:
    missing = [field for field in REQUIRED_META_FIELDS if field not in metadata or metadata[field] is None]
    if missing:
        raise ValueError(f"trace metadata is missing required fields: {', '.join(missing)}")
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(json.dumps(metadata, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
