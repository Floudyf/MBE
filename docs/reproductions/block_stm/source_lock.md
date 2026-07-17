# Block-STM Source Lock

Status: source-locked for mechanism review. This document does not claim an implementation is complete.

## Paper

- Title: Block-STM: Scaling Blockchain Execution by Turning Ordering Curse to a Performance Blessing
- Authors: Rati Gelashvili, Alexander Spiegelman, Zhuolun Xiang, George Danezis, Zekun Li, Dahlia Malkhi, Yu Xia, Runtian Zhou
- Version locked: arXiv v3, last revised 2022-08-25
- DOI: 10.48550/arXiv.2203.06871
- arXiv: https://arxiv.org/abs/2203.06871
- Truth label target: paper_faithful_mechanism_reimplementation

## Implementation Source

- Repository: https://github.com/aptos-labs/aptos-core
- Branch observed: main
- Source commit locked: 20f9379515358add43f4042693462aaedd654826
- License observed at locked commit: Innovation-Enabling Source Code License
- Citation date: 2026-07-17

## Source Paths

- aptos-move/block-executor/
- aptos-move/block-executor/src/scheduler.rs
- aptos-move/block-executor/src/executor.rs
- aptos-move/block-executor/src/view.rs
- aptos-move/block-executor/src/txn_last_input_output.rs
- aptos-move/mvhashmap/
- aptos-move/mvhashmap/src/lib.rs
- aptos-move/mvhashmap/src/versioned_data.rs
- aptos-move/mvhashmap/src/registered_dependencies.rs

## License Boundary

MBE must not copy Aptos source code verbatim. The MBE implementation is a Go reimplementation of the paper mechanism over MBE transfer semantics and the existing V5 block executor interface.

The Aptos license is recorded because it constrains reuse of source text and production/commercial use. This stage uses it as a research reference only.
