# ACG / Nezha final truth boundary — 2026-08-10

Primary publication: Jiang Xiao, Shijie Zhang, Zhiwei Zhang, Bo Li, Xiaohai Dai, Hai Jin, “Nezha: Exploiting Concurrency for Transaction Processing in DAG-based Blockchains,” ICDCS 2022, pp. 269–279, DOI 10.1109/ICDCS54860.2022.00034.

The publicly verifiable description states that Nezha constructs an address-based conflict graph (ACG) incorporating address dependencies, then uses hierarchical sorting (HS) to derive address sorting ranks and a transaction total order.

At package-manufacturing time, the IEEE full text was closed and no author implementation containing the paper's exact ACG/HS pseudocode was found in the reviewed public sources. MBE therefore keeps the current `nezha_acg_hs_paper_description_v1` implementation and its existing explicit source type `paper_description_reimplementation`. This package does not invent an undocumented HS control flow merely to claim a source-identical Nezha reproduction.

The current implementation remains deterministic, address-indexed, conflict-safe and independently implemented. It must be described as a paper-description ACG/Nezha baseline until the exact primary HS algorithm or author code is source-locked. This boundary is intentional scientific evidence, not a performance tuning choice.
