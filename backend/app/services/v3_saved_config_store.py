from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from backend.app.models.v3_saved_config import V3SavedConfig, V3SavedConfigCreateRequest, V3SavedConfigKind, V3SavedConfigUpdateRequest


ROOT = Path(__file__).resolve().parents[3]
SAVED_CONFIG_ROOT = ROOT / ".cache" / "v3_saved_configs"
TRUTH_BOUNDARY = "local_emulator_config_not_production_chain"


class SavedConfigNotFound(FileNotFoundError):
    pass


class SavedConfigStoreError(ValueError):
    pass


def list_saved_configs(kind: V3SavedConfigKind | None = None, root: Path = SAVED_CONFIG_ROOT) -> list[dict[str, Any]]:
    root.mkdir(parents=True, exist_ok=True)
    configs = [_read(path) for path in sorted(root.glob("v3cfg_*.json"))]
    if kind:
        configs = [config for config in configs if config["config_kind"] == kind]
    configs.sort(key=lambda item: (item.get("updated_at", ""), item.get("created_at", "")), reverse=True)
    return configs


def get_saved_config(config_id: str, root: Path = SAVED_CONFIG_ROOT) -> dict[str, Any]:
    path = _path_for(config_id, root)
    if not path.is_file():
        raise SavedConfigNotFound(f"unknown saved config: {config_id}")
    return _read(path)


def create_saved_config(request: V3SavedConfigCreateRequest, root: Path = SAVED_CONFIG_ROOT) -> dict[str, Any]:
    root.mkdir(parents=True, exist_ok=True)
    now = _now()
    config_id = _new_config_id(request.name, request.config_kind, now)
    config = V3SavedConfig(
        config_id=config_id,
        config_kind=request.config_kind,
        name=request.name.strip(),
        description=request.description,
        owner_label=request.owner_label,
        tags=_clean_tags(request.tags),
        created_at=now,
        updated_at=now,
        payload=request.payload,
        validation_status=request.validation_status,
        last_validation=request.last_validation,
        last_smoke_run_id=request.last_smoke_run_id,
        source=request.source,
        truth_boundary=TRUTH_BOUNDARY,
    )
    _write(_path_for(config_id, root), _dump(config))
    return _dump(config)


def update_saved_config(config_id: str, request: V3SavedConfigUpdateRequest, root: Path = SAVED_CONFIG_ROOT) -> dict[str, Any]:
    current = get_saved_config(config_id, root)
    updates = request.model_dump(exclude_unset=True) if hasattr(request, "model_dump") else request.dict(exclude_unset=True)
    for key, value in updates.items():
        if value is not None:
            current[key] = _clean_tags(value) if key == "tags" else value
    current["updated_at"] = _now()
    current["version"] = int(current.get("version", 1)) + 1
    current["truth_boundary"] = TRUTH_BOUNDARY
    config = V3SavedConfig(**current)
    _write(_path_for(config_id, root), _dump(config))
    return _dump(config)


def delete_saved_config(config_id: str, root: Path = SAVED_CONFIG_ROOT) -> dict[str, Any]:
    path = _path_for(config_id, root)
    if not path.is_file():
        raise SavedConfigNotFound(f"unknown saved config: {config_id}")
    path.unlink()
    return {"deleted": True, "config_id": config_id}


def _new_config_id(name: str, kind: str, now: str) -> str:
    digest = hashlib.sha256(f"{kind}:{name}:{now}".encode("utf-8")).hexdigest()[:8]
    stamp = datetime.now(UTC).strftime("%Y%m%d%H%M%S%f")
    return f"v3cfg_{stamp}_{digest}"


def _path_for(config_id: str, root: Path) -> Path:
    if not config_id.startswith("v3cfg_") or "/" in config_id or "\\" in config_id or config_id in {".", ".."}:
        raise SavedConfigStoreError("invalid saved config id")
    return root / f"{config_id}.json"


def _read(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _write(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")


def _dump(model: V3SavedConfig) -> dict[str, Any]:
    return model.model_dump() if hasattr(model, "model_dump") else model.dict()


def _now() -> str:
    return datetime.now(UTC).replace(tzinfo=None).isoformat(timespec="microseconds")


def _clean_tags(tags: list[str]) -> list[str]:
    return [tag.strip() for tag in tags if tag and tag.strip()][:32]
