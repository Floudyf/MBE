# MBE Run-Internal Exact Dedup Benchmark V1

- Successful samples: **3**
- Failed samples: **0**
- Zstd level for both comparison outputs: **10**
- Source CURRENT archives: **read-only; never replaced/deleted/renamed**

## Aggregate

| Measure | Value |
|---|---:|
| Original archives | 46.85 GiB |
| Plain level10 | 43.66 GiB |
| Dedup level10 | 10.95 GiB |
| Plain level10 saving | 3.19 GiB (6.81%) |
| Additional dedup saving vs plain level10 | 32.72 GiB (74.93%) |
| Total saving vs original | 35.90 GiB (76.63%) |
| Exact within-run duplicate logical bytes | 47.47 GiB (74.84%) |

## Samples

| Run | Original | Plain L10 | Dedup L10 | Extra dedup vs L10 | Total saving | Verified |
|---|---:|---:|---:|---:|---:|---|
| `v5_20260821_023450_f69653bf` | 15.67 GiB | 14.60 GiB | 3.66 GiB | 10.94 GiB (74.95%) | 12.01 GiB (76.65%) | True |
| `v5_20260821_051621_00fdd0f7` | 15.64 GiB | 14.58 GiB | 5.47 GiB | 9.11 GiB (62.48%) | 10.17 GiB (65.03%) | True |
| `v5_20260821_035541_033cebaf` | 15.54 GiB | 14.48 GiB | 1.82 GiB | 12.66 GiB (87.43%) | 13.72 GiB (88.29%) | True |

## Verification contract

- Plain level10 output must decompress to the exact same TAR byte count and TAR SHA256 as the source archive.
- Dedup archive stores one object per unique regular-file SHA256+size and a complete TAR-member manifest.
- Every content object is re-hashed from the dedup archive.
- The source archive is independently rescanned; its full member-manifest SHA256 must equal the dedup manifest SHA256.
- Source compressed archive SHA256 is checked before and after the sample.
- Temporary GB-scale benchmark archives are deleted after each sample.

## Important

This benchmark does **not** change the MBE production archive format. It only measures whether an exact, self-contained within-run dedup format is worthwhile.
