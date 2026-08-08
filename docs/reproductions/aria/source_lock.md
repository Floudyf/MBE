# Aria Source Lock

Status: source-locked for the MBE baseline reimplementation.

## Paper

- Title: Aria: A Fast and Practical Deterministic OLTP Database
- Authors: Yi Lu, Xiangyao Yu, Lei Cao, Samuel Madden
- Venue: PVLDB 13(12), 2020
- DOI: 10.14778/3407790.3407808
- Paper: https://www.vldb.org/pvldb/vol13/p2047-lu.pdf

## Official implementation source

- Repository: https://github.com/luyi0619/aria
- Locked commit: `d0508c393ec084582c12e6f3abadab63501eaedd`
- Core paths:
  - `protocol/Aria/AriaExecutor.h`
  - `protocol/Aria/AriaHelper.h`
  - `protocol/Aria/Aria.h`
  - `core/Context.h`

## Locked mechanism

The MBE baseline follows the official implementation defaults:

- read-only optimization enabled;
- deterministic reordering enabled;
- snapshot-isolation-only mode disabled;
- fallback locking excluded from the Aria baseline.

The official source reserves the minimum transaction ID for reads and writes,
computes RAW, WAR, and WAW dependencies, rejects every WAW transaction, and with
reordering enabled commits a transaction unless it has both RAW and WAR.

No source code is copied verbatim. MBE contains an independent Go
reimplementation over its existing transaction and block-executor contracts.
