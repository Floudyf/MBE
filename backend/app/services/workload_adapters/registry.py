from __future__ import annotations

from backend.app.services.workload_adapters.base import DatasetAdapter
from backend.app.services.workload_adapters.canonical_csv_v1 import CanonicalCSVAdapter
from backend.app.services.workload_adapters.decentraland_sales_v1 import DecentralandSalesAdapter
from backend.app.services.workload_adapters.alien_worlds_rmw_v1 import AlienWorldsRMWAdapter
from backend.app.services.workload_adapters.axie_full_day_v1 import AxieFullDayAdapter
from backend.app.services.workload_adapters.tapos_exact_write_set_v1 import TaposExactWriteSetAdapter


_ADAPTERS: dict[str, DatasetAdapter] = {
    DecentralandSalesAdapter.adapter_id: DecentralandSalesAdapter(),
    CanonicalCSVAdapter.adapter_id: CanonicalCSVAdapter(),
    AlienWorldsRMWAdapter.adapter_id: AlienWorldsRMWAdapter(),
    AxieFullDayAdapter.adapter_id: AxieFullDayAdapter(),
    TaposExactWriteSetAdapter.adapter_id: TaposExactWriteSetAdapter(),
}


def get_adapter(adapter_id: str) -> DatasetAdapter:
    try:
        return _ADAPTERS[adapter_id]
    except KeyError as exc:
        raise ValueError(f"unknown dataset adapter_id: {adapter_id}") from exc

