# Batch-SI deviations and limitations

## Documented source ambiguities

- Section 3.3 prose says WRBP processes transactions in descending transaction-ID order. The AWRT definition gives smaller IDs priority, Algorithm 1 does not reverse its input, and Figure 7's exact three-batch result is reproduced by ascending priority. MBE follows the definition and worked result.
- Algorithm 2 assigns integer sort labels that can tie. The paper states the required reader-before-writer outcome but does not define a complete tie-breaking data structure. MBE performs a deterministic topological validation using OFAS-derived priorities and defers a lowest-priority cycle participant when necessary.

## MBE adaptation

- MBE transaction IDs are strings/hashes. A lexical ID rank replaces the paper's small integer IDs.
- The paper's Java role simulation is not copied. MBE runs its existing independent-node, TCP, PBFT-style real-cluster path.
- The transaction business semantics are independently implemented inside Batch-SI to prevent cross-scheme code coupling, while preserving MBE's common transaction/access-list contract.
- Multi-shard Batch-SI represents independent shard-local execution only; cross-shard Batch-SI is not claimed.
- Timing fields are wall-clock measurements and may be zero milliseconds for very small unit-test blocks.

## Claims not made

- no claim of official-source-code equivalence;
- no claim that this implementation reproduces the paper's Java simulator architecture;
- no claim of cross-shard Batch-SI semantics;
- no claim that worker count equals node or process count.
