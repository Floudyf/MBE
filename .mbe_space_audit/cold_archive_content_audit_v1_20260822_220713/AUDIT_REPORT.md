# MBE Cold Archive Content Audit

- CURRENT child records scanned: **954**
- CURRENT physical run IDs discovered: **954**
- CURRENT cold archives selected: **332**
- Non-current cold archives excluded: **0**
- Successful archive scans: **332**
- Failed archive scans: **0**
- Compressed CURRENT archive size: **299.67 GiB**
- Decompressed logical file bytes: **828.56 GiB**

## Exact duplicate content

- Total exact duplicate logical bytes: **329.52 GiB** (39.77%)
- Cross-run exact duplicate logical bytes: **5.61 GiB** (0.68%)
- Rough compressed-equivalent indicator: **2.03 GiB**
- **Warning:** the compressed-equivalent value is an indicator only, not measured reclaimable space.

## Largest top-level areas

| Top level | Files | Logical size |
|---|---:|---:|
| `nodes` | 86,856 | 728.37 GiB |
| `transaction_lifecycle.jsonl` | 326 | 59.84 GiB |
| `transaction_lifecycle.csv` | 326 | 31.77 GiB |
| `bin` | 664 | 4.26 GiB |
| `client` | 3,360 | 3.21 GiB |
| `transaction_finality.csv` | 326 | 372.37 MiB |
| `client_receipt_log.csv` | 326 | 313.46 MiB |
| `workload_manifest_snapshot.json` | 314 | 126.41 MiB |
| `height_root_matrix.csv` | 326 | 91.48 MiB |
| `artifact_catalog.json` | 332 | 49.29 MiB |
| `physical_remote_state_operations.csv` | 326 | 22.66 MiB |
| `mbe_archive_manifest.json` | 332 | 17.46 MiB |
| `real_cluster_summary.json` | 332 | 15.95 MiB |
| `node_config_n0.json` | 332 | 12.45 MiB |
| `node_config_n1.json` | 332 | 12.45 MiB |
| `node_config_n2.json` | 332 | 12.45 MiB |
| `node_config_n3.json` | 332 | 12.45 MiB |
| `node_config_n4.json` | 328 | 12.36 MiB |
| `node_config_n5.json` | 328 | 12.36 MiB |
| `node_config_n6.json` | 328 | 12.36 MiB |

## Semantic categories

| Category | Files | Logical size |
|---|---:|---:|
| `node_trace` | 54,040 | 704.67 GiB |
| `other` | 10,790 | 96.82 GiB |
| `node_other` | 16,056 | 19.03 GiB |
| `metrics_summary` | 19,674 | 3.95 GiB |
| `client_artifacts` | 2,028 | 2.64 GiB |
| `state_storage` | 704 | 781.84 MiB |
| `workload_dataset` | 646 | 707.93 MiB |
| `logs` | 3,304 | 133.23 KiB |

## Largest exact cross-run duplicate groups

| Size each | Copies | Runs | Cross-run duplicate logical | Example |
|---:|---:|---:|---:|---|
| 7.95 MiB | 311 | 311 | 2.41 GiB | `v5_20260820_164552_f7c973d8:bin/mbe-node.exe \| v5_20260820_164650_68606798:bin/mbe-node.exe \| v5_20260820_164733_7b75f737:bin/mbe-node.exe \| v5_20260820_1648` |
| 5.18 MiB | 311 | 311 | 1.57 GiB | `v5_20260820_164552_f7c973d8:bin/mbe-client.exe \| v5_20260820_164650_68606798:bin/mbe-client.exe \| v5_20260820_164733_7b75f737:bin/mbe-client.exe \| v5_2026082` |
| 412.24 KiB | 314 | 314 | 126.01 MiB | `v5_20260820_164552_f7c973d8:workload_manifest_snapshot.json \| v5_20260820_164650_68606798:workload_manifest_snapshot.json \| v5_20260820_164733_7b75f737:worklo` |
| 7.94 MiB | 10 | 10 | 71.49 MiB | `v5_20260817_094551_eded5bc6:bin/mbe-node.exe \| v5_20260817_094606_0dc5593a:bin/mbe-node.exe \| v5_20260817_094951_9e8ef0aa:bin/mbe-node.exe \| v5_20260817_0950` |
| 7.94 MiB | 8 | 8 | 55.60 MiB | `v5_20260817_091240_ed0c0ab6:bin/mbe-node.exe \| v5_20260817_091250_12c3b04d:bin/mbe-node.exe \| v5_20260817_091259_2b1f36f8:bin/mbe-node.exe \| v5_20260817_0913` |
| 2.39 MiB | 24 | 24 | 55.06 MiB | `v5_20260820_193403_4e359de5:client/transaction_lifecycle.jsonl \| v5_20260820_193447_af782bb2:client/transaction_lifecycle.jsonl \| v5_20260820_193531_6a9e73e3:` |
| 2.39 MiB | 24 | 24 | 55.06 MiB | `v5_20260820_172326_9009fb1e:client/transaction_lifecycle.jsonl \| v5_20260820_172417_b4c97a06:client/transaction_lifecycle.jsonl \| v5_20260820_172503_9a0d09a6:` |
| 2.39 MiB | 24 | 24 | 55.06 MiB | `v5_20260820_164552_f7c973d8:client/transaction_lifecycle.jsonl \| v5_20260820_164650_68606798:client/transaction_lifecycle.jsonl \| v5_20260820_164733_7b75f737:` |
| 2.39 MiB | 24 | 24 | 55.06 MiB | `v5_20260820_231846_e279112a:client/transaction_lifecycle.jsonl \| v5_20260820_231936_51447433:client/transaction_lifecycle.jsonl \| v5_20260820_232019_b76f3673:` |
| 2.39 MiB | 24 | 24 | 55.06 MiB | `v5_20260820_183942_eed299e2:client/transaction_lifecycle.jsonl \| v5_20260820_184031_1d9aa3ae:client/transaction_lifecycle.jsonl \| v5_20260820_184116_42344020:` |
| 2.39 MiB | 24 | 24 | 55.06 MiB | `v5_20260820_201858_b1f1f01a:client/transaction_lifecycle.jsonl \| v5_20260820_201947_99276fc0:client/transaction_lifecycle.jsonl \| v5_20260820_202031_92868db3:` |
| 2.39 MiB | 24 | 24 | 55.06 MiB | `v5_20260821_023315_77ab50b6:client/transaction_lifecycle.jsonl \| v5_20260821_023405_7ff32322:client/transaction_lifecycle.jsonl \| v5_20260821_023450_f69653bf:` |
| 2.39 MiB | 24 | 24 | 55.06 MiB | `v5_20260820_170430_75937342:client/transaction_lifecycle.jsonl \| v5_20260820_170520_d8adf040:client/transaction_lifecycle.jsonl \| v5_20260820_170605_6df30709:` |
| 2.39 MiB | 24 | 24 | 55.06 MiB | `v5_20260820_180032_fc5b2ce4:client/transaction_lifecycle.jsonl \| v5_20260820_180122_15acd2c4:client/transaction_lifecycle.jsonl \| v5_20260820_180206_ebf2406c:` |
| 2.39 MiB | 24 | 24 | 55.06 MiB | `v5_20260820_181928_503f14ca:client/transaction_lifecycle.jsonl \| v5_20260820_182018_c753703f:client/transaction_lifecycle.jsonl \| v5_20260820_182101_92d4c745:` |
| 2.39 MiB | 24 | 24 | 55.06 MiB | `v5_20260820_174200_acd68e8d:client/transaction_lifecycle.jsonl \| v5_20260820_174248_c2ea3097:client/transaction_lifecycle.jsonl \| v5_20260820_174333_c421f83d:` |
| 2.39 MiB | 24 | 24 | 55.06 MiB | `v5_20260820_190309_740a740c:client/transaction_lifecycle.jsonl \| v5_20260820_190357_fb9081b6:client/transaction_lifecycle.jsonl \| v5_20260820_190440_c7188062:` |
| 2.39 MiB | 24 | 24 | 55.06 MiB | `v5_20260820_212852_258d2507:client/transaction_lifecycle.jsonl \| v5_20260820_212941_fb30363a:client/transaction_lifecycle.jsonl \| v5_20260820_213024_ae919486:` |
| 5.18 MiB | 10 | 10 | 46.61 MiB | `v5_20260817_094551_eded5bc6:bin/mbe-client.exe \| v5_20260817_094606_0dc5593a:bin/mbe-client.exe \| v5_20260817_094951_9e8ef0aa:bin/mbe-client.exe \| v5_2026081` |
| 1.92 MiB | 24 | 24 | 44.09 MiB | `v5_20260820_172326_9009fb1e:client/resolved_access_lists.jsonl.gz \| v5_20260820_172417_b4c97a06:client/resolved_access_lists.jsonl.gz \| v5_20260820_172503_9a0` |
| 1.92 MiB | 24 | 24 | 44.08 MiB | `v5_20260820_170430_75937342:client/resolved_access_lists.jsonl.gz \| v5_20260820_170520_d8adf040:client/resolved_access_lists.jsonl.gz \| v5_20260820_170605_6df` |
| 1.92 MiB | 24 | 24 | 44.07 MiB | `v5_20260820_174200_acd68e8d:client/resolved_access_lists.jsonl.gz \| v5_20260820_174248_c2ea3097:client/resolved_access_lists.jsonl.gz \| v5_20260820_174333_c42` |
| 1.92 MiB | 24 | 24 | 44.07 MiB | `v5_20260820_180032_fc5b2ce4:client/resolved_access_lists.jsonl.gz \| v5_20260820_180122_15acd2c4:client/resolved_access_lists.jsonl.gz \| v5_20260820_180206_ebf` |
| 1.92 MiB | 24 | 24 | 44.06 MiB | `v5_20260820_164552_f7c973d8:client/resolved_access_lists.jsonl.gz \| v5_20260820_164650_68606798:client/resolved_access_lists.jsonl.gz \| v5_20260820_164733_7b7` |
| 1.91 MiB | 24 | 24 | 44.03 MiB | `v5_20260820_181928_503f14ca:client/resolved_access_lists.jsonl.gz \| v5_20260820_182018_c753703f:client/resolved_access_lists.jsonl.gz \| v5_20260820_182101_92d` |
| 1.91 MiB | 24 | 24 | 43.93 MiB | `v5_20260820_183942_eed299e2:client/resolved_access_lists.jsonl.gz \| v5_20260820_184031_1d9aa3ae:client/resolved_access_lists.jsonl.gz \| v5_20260820_184116_423` |
| 1.90 MiB | 24 | 24 | 43.63 MiB | `v5_20260820_190309_740a740c:client/resolved_access_lists.jsonl.gz \| v5_20260820_190357_fb9081b6:client/resolved_access_lists.jsonl.gz \| v5_20260820_190440_c71` |
| 1.87 MiB | 24 | 24 | 43.00 MiB | `v5_20260820_193403_4e359de5:client/resolved_access_lists.jsonl.gz \| v5_20260820_193447_af782bb2:client/resolved_access_lists.jsonl.gz \| v5_20260820_193531_6a9` |
| 1.82 MiB | 24 | 24 | 41.90 MiB | `v5_20260820_201858_b1f1f01a:client/resolved_access_lists.jsonl.gz \| v5_20260820_201947_99276fc0:client/resolved_access_lists.jsonl.gz \| v5_20260820_202031_928` |
| 1.76 MiB | 24 | 24 | 40.45 MiB | `v5_20260820_212852_258d2507:client/resolved_access_lists.jsonl.gz \| v5_20260820_212941_fb30363a:client/resolved_access_lists.jsonl.gz \| v5_20260820_213024_ae9` |

## Safety

The audit opened source `.tar.zst` archives read-only and never extracted, replaced, renamed, recompressed, or deleted them.
The SQLite inventory and report files are newly written only under `.mbe_space_audit`.
