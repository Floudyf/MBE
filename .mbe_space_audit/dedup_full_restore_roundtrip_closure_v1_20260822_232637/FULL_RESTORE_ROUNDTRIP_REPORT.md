# MBE Dedup Full Restore Round-Trip Closure V1

- Successful samples: **3**
- Failed samples: **0**
- All samples passed: **True**
- Production source archives: **read-only and identity-checked before/after**

## Closure gates

Each sample must pass all of the following:

1. Rebuild the same exact within-run SHA256 dedup archive format used by the successful benchmark.
2. Verify every dedup object and dedup manifest.
3. Prove the dedup source TAR contract equals the current production external manifest plus the reserved embedded manifest.
4. Restore a complete isolated run tree from the dedup archive.
5. Verify every production artifact path, size, and SHA256 against the original production manifest.
6. Run the repository's current `v5_artifact_storage.archive_run(..., delete_raw=False, compression_level=10)` on that restored tree.
7. Require the regenerated current-MBE manifest content contract to equal the original production manifest.
8. Run the repository's current `verify_archive()`.
9. Delete one non-preserved online artifact as a restore probe.
10. Run the repository's current `restore_run(..., reapply_ntfs=False)` and require the probe to be recreated.
11. Re-verify every original production artifact path, size, and SHA256.
12. Re-hash the original production cold archive and require its size/mtime/SHA256 to be unchanged.

## Results

| Run | Source | Dedup | Dedup restore | Current MBE archive | Current MBE restore | Source unchanged |
|---|---:|---:|---|---|---|---|
| `v5_20260821_023450_f69653bf` | 15.67 GiB | 3.66 GiB | True | True | True | True |
| `v5_20260821_051621_00fdd0f7` | 15.64 GiB | 5.47 GiB | True | True | True | True |
| `v5_20260821_035541_033cebaf` | 15.54 GiB | 1.82 GiB | True | True | True | True |

## Contract note

The current MBE production archive manifest freezes artifact **path, size, and SHA256**. The current restore implementation writes verified file bytes into a temporary restore tree and does not restore POSIX mode/mtime as part of its formal artifact contract. This closure therefore uses path/size/SHA256 as the blocking equality contract and retains TAR metadata only as audit metadata.

## Safety

- No production archive is replaced, renamed, deleted, or restored in place.
- All restore and current-MBE archive/restore operations occur under the audit directory.
- Large temporary files and restored trees are deleted after each sample, including failures.
- Results are saved after every sample.
