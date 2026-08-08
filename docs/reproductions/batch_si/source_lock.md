# Batch-SI source lock

## Primary paper

- Guoqiong Liao et al., **Batch-SI: Semi-Parallel Concurrency Scheme for Permissioned Blockchains Exploiting Batch Snapshot Isolation**.
- Implementation target: AWRT, WRBP, OFAS, and batch snapshot isolation as specified in Sections 3.2–3.5 and Algorithms 1–2.
- The user-supplied PDF is the normative source for this reproduction.

## Source-code availability

No official Batch-SI implementation repository or source archive was supplied with the paper, and no official source code is used by this integration. The MBE implementation is therefore a paper-level independent reimplementation.

## Locked semantic decisions

1. Transactions are ranked deterministically by lexical `TxID`; this provides a stable total order for MBE identifiers that are not paper-style integers.
2. AWRT rows use ascending transaction priority, matching the paper's AWRT definition and Figure 7 worked result.
3. The WRBP prose says transactions are processed in descending ID order, while the AWRT definition, Figure 7 result, and Algorithm 1 input traversal are inconsistent with that sentence. MBE follows the worked result and records this deviation explicitly.
4. OFAS follows Algorithm 2 state (`tRNum`, `kRNum`, `kMaxR`, transaction sort labels, and Rule 2 abort checks). A deterministic reader-to-writer topological validation is applied after the Algorithm 2 pass to resolve equal sort labels and guarantee the serializable order required by Rule 1.
5. Batches execute sequentially. Every transaction in one batch reads the same immutable snapshot created from the preceding batch's committed state.

## Isolation boundary

Batch-SI does not call Serial, Aria, Block-STM, Groundhog, MetaTrack, CG, ACG, or BSX algorithm functions. It shares only MBE platform contracts: transaction/block types, state snapshots, receipt/result structures, PBFT transport, persistence, plugin registration, and metric export.
