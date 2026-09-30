# OptME source lock

Primary paper: Donghyeon Ryu and Chanik Park, *Toward High-Performance Blockchain System by Blurring the Line between Ordering and Execution*, SC 2024.

Author repository: `https://github.com/Dong-Hyeon-Yu/optme`

Reviewed source commit: `4cac103bd98440670d71219dfa185b8516ea6512`.

MBE source-of-truth files:

- `crates/sslab-execution/optme/src/optme_core.rs`
- `crates/sslab-execution/optme/src/address_based_conflict_graph.rs`
- `crates/sslab-execution/optme/src/types.rs`
- `crates/sslab-execution/optme/src/tests/optme_tests.rs`

The implementation boundary is post-consensus. OptME consumes consensus output, performs simulation to obtain observed read/write sets, constructs its AddressBasedConflictGraph, runs hierarchical ordering and First-Updater-Wins early detection, reorders eligible write-only transactions, extracts sequences, commits the main schedule, and re-executes remaining aborted groups.

MBE does not replace the observed simulation read/write sets with the signed AccessList. In Stateless-OptME the AccessList only bounds which state values may be fetched to construct the simulation snapshot.

The author source has the further serial/fallback path after a failed second optimistic validation commented out. MBE therefore does not silently add Block-STM or an unbounded serial retry. A second-pass transaction that remains invalid receives an explicit terminal failed receipt with the MBE integration reason `optme_second_pass_invalidated_no_source_fallback`.
