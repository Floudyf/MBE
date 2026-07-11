from __future__ import annotations

from backend.app.services.v5_plugin_manifest_store import CATEGORIES, STORE


def list_catalog(*, category: str | None = None, backend: str | None = None, status: str | None = None) -> list[dict]:
    if category and category not in CATEGORIES:
        raise ValueError(f"unknown category: {category}")
    items = STORE.list()
    if category:
        items = [item for item in items if item.category == category]
    if backend:
        items = [item for item in items if backend in item.supported_backends]
    if status:
        items = [item for item in items if item.implementation_status == status]
    return [item.model_dump() for item in sorted(items, key=lambda item: (item.category, item.display_name))]


def categories() -> list[str]:
    return list(CATEGORIES)
