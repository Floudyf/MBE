from __future__ import annotations

from backend.app.services.v3_runtime.models import StateCommit, TxResult
from backend.app.services.v3_runtime.state_access import DirectFetchState


class NormalCommit:
    plugin_id = "normal_commit"

    def commit(self, state: DirectFetchState, results: list[TxResult]) -> list[StateCommit]:
        commits: list[StateCommit] = []
        for result in results:
            for key, (_old_value, delta, _new_value) in result.deltas.items():
                old_value = state.values.get(key, 0)
                new_value = old_value + delta
                state.values[key] = new_value
                commits.append(
                    StateCommit(
                        block_height=result.block_height,
                        tx_id=result.tx_id,
                        state_key=key,
                        old_value=old_value,
                        delta=delta,
                        new_value=new_value,
                        commit_plugin=self.plugin_id,
                        commit_time_ms=result.commit_time_ms,
                        status=result.status,
                    )
                )
        return commits
