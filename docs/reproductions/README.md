# Reproduction Dossiers

Every future reproduction of a paper mechanism or real system mechanism must lock the evidence before implementation.

Required source lock fields:

- formal paper version
- DOI or arXiv identifier when available
- official or author source repository when available
- source commit SHA
- license
- core mechanism checklist
- correctness invariants
- MBE adaptation boundary
- deleted or modified mechanisms
- impact on experiment claims
- formal-name activation conditions

No MBE stage may claim a formal external algorithm name until the source lock and mechanism checklist are complete and the implementation has passed the matching acceptance matrix.

The Serial dossier in this directory is an internal reference. It is not a reproduction of an external paper algorithm.

## Active External Mechanism Dossier

`block_stm/` locks the Block-STM paper and Aptos source commit for the next
block-execution reproduction. Its current status is source lock and mechanism
mapping only. It must not be cited as an implemented `block_stm` executor until
MVMemory, incarnation, validation, abort/re-execution, ESTIMATE, dependency
waiting, Serial equivalence, plugin integration, and real-cluster acceptance all
pass.
