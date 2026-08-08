# Groundhog source lock

## Paper

- Title: *Groundhog: A Parallel, Deterministic, Composable, and Efficient Transaction Execution Engine for Blockchains*
- arXiv: 2404.03201
- Source: https://arxiv.org/abs/2404.03201

## Official implementation

- Repository: https://github.com/scslab/smart-contract-scalability
- Locked commit: `6b357bc206b73ece39fd61fe7dba655352200c0a`
- License: Apache-2.0

Primary source paths used for this reimplementation:

```text
block_assembly/assembly_worker.cc
transaction_context/transaction_context.cc
transaction_context/execution_context.cc
state_db/groundhog_persistent_state_db.cc
object/revertable_object.h
object/revertable_object.cc
vm/base_vm.cc
vm/groundhog_vm.cc
cpp_contracts/sdk/replay_cache.h
```

The MBE implementation is a clean-room Go reimplementation of the paper's core mechanism over MBE transaction semantics. It does not copy the original C++ implementation and does not embed the original WASM VM or persistent trie.
