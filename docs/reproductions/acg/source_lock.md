# ACG / Nezha source lock

Primary source identity:
- Jiang Xiao, Shijie Zhang, Zhiwei Zhang, Bo Li, Xiaohai Dai, Hai Jin.
- **Nezha: Exploiting Concurrency for Transaction Processing in DAG-based Blockchains**.
- ICDCS 2022, DOI `10.1109/ICDCS54860.2022.00034`.

Publicly verifiable description: Nezha constructs an address-based conflict graph (ACG), incorporates address dependencies to capture conflicting transactions, and uses hierarchical sorting (HS) to derive ordering ranks.

No author implementation source was available to this reproduction. MBE therefore labels this path `nezha_acg_hs_paper_description_v1` and does **not** claim source-identical Nezha reproduction.

Implementation boundary:
- index reads/writes by logical address;
- emit original-order conflict precedence without all-pairs conflict tests;
- derive deterministic hierarchy waves from that address-indexed graph;
- execute one hierarchy wave in parallel;
- no fallback and no reuse of CG/BSX scheduling functions.

If author source or full algorithm pseudocode becomes available later, this path must be re-audited before upgrading the truth label.
