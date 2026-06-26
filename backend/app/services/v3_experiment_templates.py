from __future__ import annotations

from copy import deepcopy
from pathlib import Path
from typing import Any

import yaml

from backend.app.services.v3_profile_loader import V3_CONFIG_ROOT

TEMPLATE_ROOT = V3_CONFIG_ROOT / "templates"
STANDARD_MODULE_ORDER = [
    "Workload",
    "TxPool",
    "BlockProducer",
    "Consensus",
    "CommitteeEpoch",
    "Routing",
    "Execution",
    "StateAccess",
    "StateStorage",
    "Commit",
    "MetricsReport",
]
STATUS_FIELDS = {
    "variable_modules": "variable",
    "fixed_modules": "fixed",
    "disabled_modules": "disabled",
    "planned_modules": "planned",
    "output_modules": "output",
}
ALLOWED_STATUSES = set(STATUS_FIELDS.values())
DEFAULT_TEMPLATE_ID = "metatrack_ablation"


class V3ExperimentTemplateError(ValueError):
    """Raised when a V3 experiment template is missing or invalid."""


def load_templates(root: Path = TEMPLATE_ROOT) -> dict[str, dict[str, Any]]:
    if not root.exists():
        raise V3ExperimentTemplateError(f"template directory does not exist: {root}")
    templates: dict[str, dict[str, Any]] = {}
    for path in sorted(root.glob("*.yaml")):
        document = _load_yaml(path)
        normalized = normalize_template(document)
        template_id = normalized["template_id"]
        if template_id in templates:
            raise V3ExperimentTemplateError(f"duplicate template_id: {template_id}")
        templates[template_id] = {**normalized, "source_path": str(path)}
    return templates


def list_templates(root: Path = TEMPLATE_ROOT) -> list[dict[str, Any]]:
    return list(load_templates(root).values())


def get_template(template_id: str, root: Path = TEMPLATE_ROOT) -> dict[str, Any]:
    templates = load_templates(root)
    if template_id not in templates:
        raise V3ExperimentTemplateError(f"unknown experiment template: {template_id}")
    return templates[template_id]


def normalize_template(template: dict[str, Any]) -> dict[str, Any]:
    normalized = deepcopy(template)
    errors = validate_template(normalized)
    if errors:
        raise V3ExperimentTemplateError("; ".join(errors))
    normalized.setdefault("runnable", False)
    normalized.setdefault("preview_only", not bool(normalized.get("runnable")))
    normalized["module_status"] = _module_status(normalized)
    normalized["status_fields"] = STATUS_FIELDS.copy()
    return normalized


def template_is_runnable(template: dict[str, Any]) -> bool:
    return bool(template.get("runnable")) and not bool(template.get("preview_only"))


def template_is_preview_only(template: dict[str, Any]) -> bool:
    return bool(template.get("preview_only")) or not bool(template.get("runnable"))


def validate_template(template: dict[str, Any]) -> list[str]:
    errors: list[str] = []
    template_id = str(template.get("template_id", ""))
    if not template_id:
        errors.append("template_id is required")
    if template.get("chain_mode") != "single_chain":
        errors.append(f"{template_id}: chain_mode must be single_chain")
    module_order = template.get("module_order")
    if module_order != STANDARD_MODULE_ORDER:
        errors.append(f"{template_id}: module_order must match the standard single-chain order")
    if not isinstance(template.get("allowed_plugins", {}), dict):
        errors.append(f"{template_id}: allowed_plugins must be a mapping")
    if not isinstance(template.get("fairness", {}), dict):
        errors.append(f"{template_id}: fairness must be a mapping")
    seen: dict[str, str] = {}
    for field_name, status in STATUS_FIELDS.items():
        modules = template.get(field_name, [])
        if not isinstance(modules, list):
            errors.append(f"{template_id}: {field_name} must be a list")
            continue
        for module in modules:
            if module in seen:
                errors.append(f"{template_id}: {module} cannot be both {seen[module]} and {status}")
            seen[str(module)] = status
    for status in seen.values():
        if status not in ALLOWED_STATUSES:
            errors.append(f"{template_id}: invalid module status {status}")
    fairness = template.get("fairness", {})
    if fairness.get("planned_modules_not_runnable") is not True:
        errors.append(f"{template_id}: fairness.planned_modules_not_runnable must be true")
    if template.get("preview_only") is True and template.get("runnable") is True:
        errors.append(f"{template_id}: preview_only template must not be runnable")
    return errors


def _module_status(template: dict[str, Any]) -> dict[str, str]:
    status_by_module: dict[str, str] = {}
    for field_name, status in STATUS_FIELDS.items():
        for module in template.get(field_name, []):
            status_by_module[str(module)] = status
    return status_by_module


def _load_yaml(path: Path) -> dict[str, Any]:
    try:
        document = yaml.safe_load(path.read_text(encoding="utf-8"))
    except OSError as exc:
        raise V3ExperimentTemplateError(f"cannot read template file: {path}") from exc
    if not isinstance(document, dict):
        raise V3ExperimentTemplateError(f"template file must be a mapping: {path}")
    return document
