from __future__ import annotations

from backend.app.services.workload_adapters.base import DatasetAdapter
from backend.app.services.workload_adapters.canonical_csv_v1 import CanonicalCSVAdapter
from backend.app.services.workload_adapters.decentraland_sales_v1 import DecentralandSalesAdapter


_ADAPTERS: dict[str, DatasetAdapter] = {
    DecentralandSalesAdapter.adapter_id: DecentralandSalesAdapter(),
    CanonicalCSVAdapter.adapter_id: CanonicalCSVAdapter(),
}


def get_adapter(adapter_id: str) -> DatasetAdapter:
    try:
        return _ADAPTERS[adapter_id]
    except KeyError as exc:
        raise ValueError(f"unknown dataset adapter_id: {adapter_id}") from exc

