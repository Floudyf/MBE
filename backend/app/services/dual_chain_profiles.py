from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml

from backend.app.services.chain_backend import ChainProfile

ROOT = Path(__file__).resolve().parents[3]
RUNNABLE_BACKENDS = {"local_virtual", "trace_replay"}


class DualChainConfigError(ValueError):
    """Raised when a V2.5 dual-chain config is not runnable."""


def resolve_workspace_path(path_text: str, root: Path = ROOT) -> Path:
    path = Path(path_text)
    if not path.is_absolute():
        path = root / path
    resolved = path.resolve()
    try:
        resolved.relative_to(root.resolve())
    except ValueError as exc:
        raise DualChainConfigError(f"path must stay inside workspace: {path_text}") from exc
    return resolved


def load_dual_chain_config(config_path: Path) -> dict[str, Any]:
    document = yaml.safe_load(config_path.read_text(encoding="utf-8"))
    if not isinstance(document, dict):
        raise DualChainConfigError("dual-chain config must be a mapping")
    if document.get("status") == "planned" or document.get("runnable") is False:
        raise DualChainConfigError("planned dual-chain configs are not executable")
    if document.get("stage") != "V2.5":
        raise DualChainConfigError("dual-chain replay config must declare stage: V2.5")
    if document.get("topology") != "dual_chain":
        raise DualChainConfigError("dual-chain replay config must declare topology: dual_chain")
    chains = document.get("chains")
    if not isinstance(chains, dict) or len(chains) != 2:
        raise DualChainConfigError("V2.5 sample replay requires exactly two chains")
    return document


def build_chain_profiles(config: dict[str, Any]) -> dict[str, ChainProfile]:
    data_truth_label = str(config.get("data_truth_label", "synthetic_replay"))
    profiles: dict[str, ChainProfile] = {}
    for chain_key, chain in config["chains"].items():
        chain_id = str(chain.get("chain_id", chain_key))
        backend_type = str(chain.get("backend_type", "local_virtual"))
        if backend_type not in RUNNABLE_BACKENDS:
            raise DualChainConfigError(f"backend_type {backend_type} is not runnable in V2.5")
        profiles[chain_id] = ChainProfile(
            chain_id=chain_id,
            role=str(chain["role"]),
            backend_type=backend_type,
            backend=str(chain.get("backend", "mock_chain")),
            block_interval_ms=int(chain["block_interval_ms"]),
            finality_depth=int(chain["finality_depth"]),
            data_truth_label=data_truth_label,
            capabilities=list(chain.get("capabilities", [])),
        )
    roles = {profile.role for profile in profiles.values()}
    if not {"source", "target"}.issubset(roles):
        raise DualChainConfigError("V2.5 replay requires one source and one target chain")
    return profiles


def source_target_profiles(profiles: dict[str, ChainProfile]) -> tuple[ChainProfile, ChainProfile]:
    source = next(profile for profile in profiles.values() if profile.role == "source")
    target = next(profile for profile in profiles.values() if profile.role == "target")
    return source, target
