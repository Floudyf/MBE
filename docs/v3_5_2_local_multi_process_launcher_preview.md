# V3.5.2 Local Multi-process Launcher Preview

## 1. Scope

This stage generates local multi-process launcher preview artifacts from the V3.5.1 logical node topology. It does not implement real TCP, a real multi-process runtime, or real PBFT.

## 2. Why This Stage

V3.5.2 moves MBE closer to BlockEmulator-style local node startup ergonomics by making node addresses and launch scripts explicit. It remains a preview layer over logical topology and does not claim full BlockEmulator behavior.

## 3. Implemented Artifacts

- node_address_table.csv
- topology.json
- launch_nodes_windows.bat
- launch_nodes_linux.sh
- launcher_readme.md

## 4. Runtime Truth

The runtime truth is launcher preview only. The generated scripts are derived from logical node topology. They are not proof of real TCP networking, not real PBFT/HotStuff/Raft, not a real multi-process runtime, and not a BlockEmulator backend.

## 5. Summary Metrics

- launcher_mode
- launcher_script_count
- launchable_node_count
- node_address_count
- windows_launcher_available
- linux_launcher_available
- launcher_preview_only

Default topology should produce 25 launchable nodes and 25 preview address entries.

## 6. Frontend Alignment

The V3 Composer result panel includes a Launcher Preview summary section. Artifact grouping includes Local launcher artifacts for the address table, topology JSON, Windows script, Linux script, and launcher README.

## 7. Validation

V3.5.2 validation commands:

```powershell
C:\Users\飛\AppData\Local\Programs\Python\Python312\python.exe -m pytest backend/tests -q
C:\Users\飛\AppData\Local\Programs\Python\Python312\python.exe -m pytest tests -q
cd executor
go test ./...
cd ..
cd frontend
npm.cmd run build
cd ..
C:\Users\飛\AppData\Local\Programs\Python\Python312\python.exe scripts/v0_sanity.py
git diff --check
git status --short
```

Recorded V3.5.2 validation results:

- `pytest backend/tests -q`: passed, 275 passed, 1 Starlette/httpx2 deprecation warning.
- `pytest tests -q`: passed, 24 passed.
- `go test ./...`: passed.
- `npm.cmd run build`: passed, with the existing Vite CJS Node API deprecation warning.
- `scripts/v0_sanity.py`: passed.
- `git diff --check`: passed with line-ending conversion warnings only.
- `git status --short`: expected V3.5.2 modified files before commit.

## 8. Next Step

V3.5.3 Local Node Process Runtime.
