from __future__ import annotations

from backend.app.services.workload_adapters.base import DatasetAdapter
from backend.app.services.workload_adapters.canonical_csv_v1 import CanonicalCSVAdapter
from backend.app.services.workload_adapters.decentraland_sales_v1 import DecentralandSalesAdapter
from backend.app.services.workload_adapters.alien_worlds_rmw_v1 import AlienWorldsRMWAdapter
from backend.app.services.workload_adapters.alien_worlds_layered_v2 import AlienWorldsLayeredV2Adapter
from backend.app.services.workload_adapters.axie_full_day_v1 import AxieFullDayAdapter
# MBE_MV_LAYERED_V4_V1
from backend.app.services.workload_adapters.mv_layered_v4 import AxieLayeredV4Adapter, DCLLayeredV4Adapter
from backend.app.services.workload_adapters.axie_controlled_rmw_v1 import AxieControlledRMWAdapter
from backend.app.services.workload_adapters.tapos_exact_write_set_v1 import TaposExactWriteSetAdapter
# MBE_MV_TAPOS_V4_V1
from backend.app.services.workload_adapters.tapos_layered_v4 import TaposLayeredV4Adapter


_ADAPTERS: dict[str, DatasetAdapter] = {
    DecentralandSalesAdapter.adapter_id: DecentralandSalesAdapter(),
    CanonicalCSVAdapter.adapter_id: CanonicalCSVAdapter(),
    AlienWorldsRMWAdapter.adapter_id: AlienWorldsRMWAdapter(),
    AlienWorldsLayeredV2Adapter.adapter_id: AlienWorldsLayeredV2Adapter(),
    AxieControlledRMWAdapter.adapter_id: AxieControlledRMWAdapter(),
    AxieFullDayAdapter.adapter_id: AxieFullDayAdapter(),
    AxieLayeredV4Adapter.adapter_id: AxieLayeredV4Adapter(),
    DCLLayeredV4Adapter.adapter_id: DCLLayeredV4Adapter(),
    TaposExactWriteSetAdapter.adapter_id: TaposExactWriteSetAdapter(),
    TaposLayeredV4Adapter.adapter_id: TaposLayeredV4Adapter(),
}


def get_adapter(adapter_id: str) -> DatasetAdapter:
    try:
        return _ADAPTERS[adapter_id]
    except KeyError as exc:
        raise ValueError(f"unknown dataset adapter_id: {adapter_id}") from exc

