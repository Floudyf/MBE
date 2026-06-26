from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml

ROOT = Path(__file__).resolve().parents[3]
V3_CONFIG_ROOT = ROOT / "configs/v3"
PROFILE_DIRS = {
    "chain_profile": V3_CONFIG_ROOT / "chains",
    "plugin_profile": V3_CONFIG_ROOT / "plugins",
    "experiment_profile": V3_CONFIG_ROOT / "experiments",
}


class V3ProfileLoadError(ValueError):
    """Raised when V3 profile files cannot be loaded."""


@dataclass(frozen=True)
class V3ProfileStore:
    chains: dict[str, dict[str, Any]]
    plugins: dict[str, dict[str, Any]]
    experiments: dict[str, dict[str, Any]]


def load_yaml_file(path: Path) -> dict[str, Any]:
    try:
        document = yaml.safe_load(path.read_text(encoding="utf-8"))
    except OSError as exc:
        raise V3ProfileLoadError(f"cannot read profile file: {path}") from exc
    if not isinstance(document, dict):
        raise V3ProfileLoadError(f"profile file must be a mapping: {path}")
    return document


def load_profiles_from_dir(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        raise V3ProfileLoadError(f"profile directory does not exist: {path}")
    profiles: list[dict[str, Any]] = []
    for file_path in sorted(path.glob("*.yaml")):
        document = load_yaml_file(file_path)
        if document.get("profile_type") == "plugin_profile_collection":
            for item in document.get("profiles", []):
                if not isinstance(item, dict):
                    raise V3ProfileLoadError(f"plugin profile item must be a mapping in {file_path}")
                profiles.append({**item, "profile_type": "plugin_profile", "source_path": str(file_path)})
        else:
            profiles.append({**document, "source_path": str(file_path)})
    return profiles


def load_profile_store(root: Path = V3_CONFIG_ROOT) -> V3ProfileStore:
    chains = _index_profiles(load_profiles_from_dir(root / "chains"), "profile_id")
    plugins = _index_profiles(load_profiles_from_dir(root / "plugins"), "plugin_profile_id")
    experiments = _index_profiles(load_profiles_from_dir(root / "experiments"), "profile_id")
    return V3ProfileStore(chains=chains, plugins=plugins, experiments=experiments)


def get_profile(profile_type: str, profile_id: str, store: V3ProfileStore | None = None) -> dict[str, Any]:
    store = store or load_profile_store()
    table = {
        "chain_profile": store.chains,
        "plugin_profile": store.plugins,
        "experiment_profile": store.experiments,
    }.get(profile_type)
    if table is None:
        raise KeyError(f"unknown profile type: {profile_type}")
    if profile_id not in table:
        raise KeyError(f"unknown {profile_type}: {profile_id}")
    return table[profile_id]


def profile_inventory(store: V3ProfileStore | None = None) -> dict[str, list[str]]:
    store = store or load_profile_store()
    return {
        "chain_profiles": sorted(store.chains),
        "plugin_profiles": sorted(store.plugins),
        "experiment_profiles": sorted(store.experiments),
    }


def _index_profiles(profiles: list[dict[str, Any]], id_field: str) -> dict[str, dict[str, Any]]:
    indexed: dict[str, dict[str, Any]] = {}
    for profile in profiles:
        profile_id = str(profile.get(id_field, ""))
        if not profile_id:
            raise V3ProfileLoadError(f"profile missing id field {id_field}")
        if profile_id in indexed:
            raise V3ProfileLoadError(f"duplicate profile id: {profile_id}")
        indexed[profile_id] = profile
    return indexed
