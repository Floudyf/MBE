from __future__ import annotations

from datetime import datetime
from uuid import uuid4


def new_run_id(now: datetime | None = None) -> str:
    timestamp = (now or datetime.now()).strftime("%Y%m%d_%H%M%S")
    return f"v2run_{timestamp}_{uuid4().hex[:6]}"
