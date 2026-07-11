from __future__ import annotations

from fastapi import APIRouter, HTTPException

from backend.app.services import v5_plugin_catalog_service


router = APIRouter(prefix="/api/v5/plugins", tags=["v5"])


@router.get("/categories")
def categories() -> dict:
    return {"items": v5_plugin_catalog_service.categories()}


@router.get("")
def catalog(category: str | None = None, backend: str | None = None, status: str | None = None) -> dict:
    try:
        return {"items": v5_plugin_catalog_service.list_catalog(category=category, backend=backend, status=status)}
    except ValueError as exc:
        raise HTTPException(400, str(exc)) from exc
