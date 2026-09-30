"""Compatibility shim for the superseded v15/v15.1 truth-closure API.

v16 deliberately separates TxAllo account allocation, transaction placement,
routing and state-home evidence.  Old imports remain valid, but all semantics are
implemented by the v16 fidelity closure.
"""
from backend.app.services.v5_optme_txallo_fidelity_closure import (  # noqa: F401
    CLOSURE_VERSION,
    LEADER_ONLY_REJECTION,
    WRITEBACK_REPLICATION_CONTRACT,
    apply_fairness_fidelity_gate,
    apply_group_fidelity_gate,
    apply_truth_closure_to_state_equivalence,
    compute_txallo_evidence,
    compute_txallo_placement_evidence,
    compute_writeback_fanout,
    enrich_extracted_metrics,
    enrich_metrics,
)

TRUTH_CLOSURE_VERSION = CLOSURE_VERSION
