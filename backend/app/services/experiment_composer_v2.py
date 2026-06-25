from __future__ import annotations

from typing import Any

from backend.app.services.config_validator_v2 import validate_selection
from backend.app.services.plugin_registry import load_registry


def preview_experiment(payload: dict[str, Any]) -> dict[str, Any]:
    registry = load_registry()
    return validate_selection(payload, registry)
