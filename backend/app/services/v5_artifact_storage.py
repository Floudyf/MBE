from __future__ import annotations

import ctypes
import hashlib
import json
import os
import shutil
import subprocess
import tarfile
import tempfile
import threading
import time
from contextlib import contextmanager
from datetime import UTC, datetime
from pathlib import Path, PurePosixPath
from typing import Callable, Iterator
from uuid import uuid4

SCHEMA_VERSION = "mbe_v5_artifact_storage_v2"
ARCHIVE_MANIFEST_SCHEMA = "mbe_v5_tar_zst_archive_manifest_v1"
STORAGE_SUMMARY_NAME = "artifact_storage_summary.json"
ARCHIVE_DIR_NAME = "_cold_archive"
ARCHIVE_MEMBER_MANIFEST = ".mbe_archive_manifest.json"
DEFAULT_ZSTD_LEVEL = 3
_NATIVE_TAR_ENV = "MBE_V5_NATIVE_TAR_ARCHIVE"
_ARCHIVE_CHUNK_BYTES = 8 * 1024 * 1024
ProgressCallback = Callable[[dict], None]
_PRESERVED_SHELL = {
    "real_cluster_summary.json",
    "artifact_catalog.json",
    STORAGE_SUMMARY_NAME,
    # Keep compact research diagnostics online after the heavy raw tree moves
    # to cold storage.  These files are tiny relative to node logs/WALs and let
    # Formal bundles explain timed-out, failed, and completed-invalid children
    # without restoring multi-gigabyte tar.zst archives.
    "finality_summary.json",
    "drain_status.json",
    "drain_progress.csv",
    "formal_timeout_summary.json",
    "resource_sampler_summary.json",
    "resource_usage_summary.json",
    "resource_usage_timeseries.csv",
    "network_metrics_summary.json",
    "network_message_summary.csv",
    "throughput_windows.csv",
    "latency_distribution.csv",
    "client_submission_complete.json",
    "workload_replay_summary.json",
    "client/client_submission_complete.json",
    "client/workload_replay_summary.json",
    "stalled_runtime_report.json",
    "shutdown_status.json",
    "aggregate/block_production_summary.json",
    "aggregate/mechanism_metrics_summary.json",
}

_PRESERVED_NODE_SHELL_NAMES = {
    "node_runtime_status.json",
    "node_summary.json",
    "runtime_metrics.json",
    "commit_summary.json",
    "commit_log.csv",
    "committed_chain.csv",
    "block_execution_summary.json",
}


def _is_preserved_shell_member(name: str) -> bool:
    normalized = _safe_relative_name(name)
    if normalized in _PRESERVED_SHELL:
        return True
    parts = PurePosixPath(normalized).parts
    return len(parts) == 3 and parts[0] == "nodes" and parts[2] in _PRESERVED_NODE_SHELL_NAMES


class ArtifactStorageError(RuntimeError):
    pass


_LOCKS_GUARD = threading.Lock()
_RUN_LOCKS: dict[str, threading.RLock] = {}


@contextmanager
def _run_storage_lock(run_dir: Path):
    key = str(run_dir.resolve())
    with _LOCKS_GUARD:
        lock = _RUN_LOCKS.get(key)
        if lock is None:
            lock = threading.RLock()
            _RUN_LOCKS[key] = lock
    with lock:
        yield


def _now() -> str:
    return datetime.now(UTC).isoformat()


def _atomic_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(f".{path.name}.{uuid4().hex}.tmp")
    try:
        tmp.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
        os.replace(tmp, path)
    finally:
        try:
            tmp.unlink()
        except FileNotFoundError:
            pass


def _safe_relative_name(value: str) -> str:
    text = str(value).replace("\\", "/").strip()
    raw_parts = text.split("/")
    path = PurePosixPath(text)
    if not text or path.is_absolute() or ":" in text or any(part in {"", ".", ".."} for part in raw_parts):
        raise ArtifactStorageError(f"unsafe archive member path: {value!r}")
    return path.as_posix()


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(4 * 1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _is_windows() -> bool:
    return os.name == "nt"


def _physical_file_bytes(path: Path) -> int:
    if _is_windows():
        try:
            from ctypes import wintypes
            high = wintypes.DWORD(0)
            get_size = ctypes.windll.kernel32.GetCompressedFileSizeW
            get_size.argtypes = [wintypes.LPCWSTR, ctypes.POINTER(wintypes.DWORD)]
            get_size.restype = wintypes.DWORD
            ctypes.set_last_error(0)
            low = int(get_size(str(path), ctypes.byref(high)))
            error = ctypes.get_last_error()
            if low == 0xFFFFFFFF and error:
                raise OSError(error, "GetCompressedFileSizeW failed")
            return (int(high.value) << 32) | low
        except (AttributeError, OSError, ValueError):
            pass
    try:
        stat = path.stat()
        blocks = getattr(stat, "st_blocks", None)
        if isinstance(blocks, int) and blocks >= 0:
            return blocks * 512
        return int(stat.st_size)
    except OSError:
        return 0


def _iter_online_files(run_dir: Path) -> Iterator[Path]:
    archive_dir = run_dir / ARCHIVE_DIR_NAME
    for path in sorted(run_dir.rglob("*")):
        if not path.is_file():
            continue
        try:
            path.relative_to(archive_dir)
            continue
        except ValueError:
            pass
        yield path


def measure_online_tree(run_dir: Path) -> dict:
    logical = 0
    physical = 0
    count = 0
    for path in _iter_online_files(run_dir):
        try:
            logical += int(path.stat().st_size)
            physical += _physical_file_bytes(path)
            count += 1
        except OSError:
            continue
    return {"file_count": count, "logical_bytes": logical, "physical_bytes": physical}


def read_storage_summary(run_dir: Path) -> dict:
    path = run_dir / STORAGE_SUMMARY_NAME
    if not path.is_file():
        return {}
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return value if isinstance(value, dict) else {}


def _write_storage_summary(run_dir: Path, payload: dict) -> dict:
    value = {
        "schema_version": SCHEMA_VERSION,
        "formal_eligibility_affected": False,
        **payload,
        "updated_at": _now(),
    }
    _atomic_json(run_dir / STORAGE_SUMMARY_NAME, value)
    return value


def _ntfs_auto_enabled() -> bool:
    return os.environ.get("MBE_V5_AUTO_NTFS_COMPRESSION", "1").strip().lower() not in {"0", "false", "no", "off"}


def _state_after_ntfs(previous_state: object, succeeded: bool) -> str:
    state = str(previous_state or "")
    # Cold-archive identity must survive a later manual NTFS compaction.
    if state in {
        "archived",
        "archive_online",
        "archive_verified_pending_raw_cleanup",
        "archive_verified_raw_cleanup_incomplete",
    }:
        return state
    if state.startswith("online_restored"):
        return "online_restored_ntfs_compressed" if succeeded else "online_restored"
    return "online_ntfs_compressed" if succeeded else "online_uncompressed"


def _run_compact(run_dir: Path) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        ["compact.exe", "/C", "/S", "/I", "/Q", "/A"],
        cwd=run_dir,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        timeout=3600,
        check=False,
    )


def _compact_online_run_unlocked(run_dir: Path, *, run_id: str, force: bool = False) -> dict:
    """Apply transparent NTFS compression after measurement.

    This operation is deliberately failure-open: protocol/formal validity is never
    changed when the filesystem does not support NTFS compression.
    """
    run_dir = run_dir.resolve()
    before = measure_online_tree(run_dir)
    previous = read_storage_summary(run_dir)
    if not _is_windows():
        return _write_storage_summary(run_dir, {
            **previous,
            "run_id": run_id,
            "storage_state": previous.get("storage_state") or "online_uncompressed",
            "online_compression": "unsupported_non_windows",
            "ntfs_compression_attempted": False,
            "ntfs_compression_succeeded": False,
            "original_file_count": int(previous.get("original_file_count") or before["file_count"]),
            "original_logical_bytes": int(previous.get("original_logical_bytes") or before["logical_bytes"]),
            "online_logical_bytes": before["logical_bytes"],
            "online_physical_bytes": before["physical_bytes"],
            "saved_bytes": max(0, before["logical_bytes"] - before["physical_bytes"]),
            "saving_ratio": (1.0 - before["physical_bytes"] / before["logical_bytes"]) if before["logical_bytes"] else 0.0,
        })
    if not force and not _ntfs_auto_enabled():
        return _write_storage_summary(run_dir, {
            **previous,
            "run_id": run_id,
            "storage_state": previous.get("storage_state") or "online_uncompressed",
            "online_compression": "disabled_by_environment",
            "ntfs_compression_attempted": False,
            "ntfs_compression_succeeded": False,
            "original_file_count": int(previous.get("original_file_count") or before["file_count"]),
            "original_logical_bytes": int(previous.get("original_logical_bytes") or before["logical_bytes"]),
            "online_logical_bytes": before["logical_bytes"],
            "online_physical_bytes": before["physical_bytes"],
        })

    try:
        completed = _run_compact(run_dir)
        succeeded = completed.returncode == 0
        error = "" if succeeded else (completed.stderr or completed.stdout or f"compact.exe exit {completed.returncode}").strip()[-4000:]
    except (OSError, subprocess.SubprocessError) as exc:
        succeeded = False
        error = str(exc)

    after = measure_online_tree(run_dir)
    original_logical = int(previous.get("original_logical_bytes") or before["logical_bytes"])
    saved = max(0, original_logical - after["physical_bytes"])
    return _write_storage_summary(run_dir, {
        **previous,
        "run_id": run_id,
        "storage_state": _state_after_ntfs(previous.get("storage_state"), succeeded),
        "online_compression": "windows_ntfs" if succeeded else "windows_ntfs_failed",
        "ntfs_compression_attempted": True,
        "ntfs_compression_succeeded": succeeded,
        "ntfs_compression_error": error,
        "original_file_count": int(previous.get("original_file_count") or before["file_count"]),
        "original_logical_bytes": original_logical,
        "online_logical_bytes": after["logical_bytes"],
        "online_physical_bytes": after["physical_bytes"],
        "saved_bytes": saved,
        "saving_ratio": (saved / original_logical) if original_logical else 0.0,
        "ntfs_compressed_at": _now() if succeeded else previous.get("ntfs_compressed_at"),
    })


def compact_online_run(run_dir: Path, *, run_id: str, force: bool = False) -> dict:
    with _run_storage_lock(run_dir):
        return _compact_online_run_unlocked(run_dir, run_id=run_id, force=force)


def finalize_online_storage(run_dir: Path, *, run_id: str) -> dict:
    try:
        return compact_online_run(run_dir, run_id=run_id, force=False)
    except Exception as exc:  # storage must never invalidate an otherwise valid experiment
        before = measure_online_tree(run_dir)
        return _write_storage_summary(run_dir, {
            "run_id": run_id,
            "storage_state": "online_uncompressed",
            "online_compression": "failure_open",
            "ntfs_compression_attempted": _is_windows(),
            "ntfs_compression_succeeded": False,
            "ntfs_compression_error": str(exc),
            "original_file_count": before["file_count"],
            "original_logical_bytes": before["logical_bytes"],
            "online_logical_bytes": before["logical_bytes"],
            "online_physical_bytes": before["physical_bytes"],
        })


def _zstandard():
    try:
        import zstandard  # type: ignore
    except ImportError as exc:
        raise ArtifactStorageError("zstandard Python package is required for tar.zst cold archive") from exc
    return zstandard


def _emit_progress(callback: ProgressCallback | None, phase: str, **payload) -> None:
    if callback is None:
        return
    try:
        callback({"phase": phase, "at": _now(), **payload})
    except Exception:
        # Progress reporting is observational only and must never invalidate storage.
        pass


def _native_tar_enabled() -> bool:
    return os.environ.get(_NATIVE_TAR_ENV, "1").strip().lower() not in {"0", "false", "no", "off"}


def _native_tar_executable() -> str | None:
    if not _is_windows() or not _native_tar_enabled():
        return None
    return shutil.which("tar.exe") or shutil.which("tar")


def _native_tar_compatible(manifest: dict) -> bool:
    # bsdtar -T is line-oriented. Rare control/newline-leading names fall back
    # to the Python tar writer so archive semantics remain safe and deterministic.
    for item in manifest.get("files", []):
        name = str(item.get("name") or "")
        if "\n" in name or "\r" in name or name.startswith("-"):
            return False
    return True


def _archive_paths(run_dir: Path, run_id: str) -> tuple[Path, Path, Path]:
    archive_dir = run_dir / ARCHIVE_DIR_NAME
    archive_dir.mkdir(parents=True, exist_ok=True)
    archive = archive_dir / f"{run_id}.tar.zst"
    manifest = archive_dir / f"{run_id}.manifest.json"
    checksum = archive_dir / f"{run_id}.tar.zst.sha256"
    return archive, manifest, checksum


def _snapshot(run_dir: Path, run_id: str, *, progress: ProgressCallback | None = None) -> dict:
    entries: list[dict] = []
    source_files = [path for path in _iter_online_files(run_dir) if path.relative_to(run_dir).as_posix() != STORAGE_SUMMARY_NAME]
    total_bytes = sum(int(path.stat().st_size) for path in source_files)
    processed = 0
    _emit_progress(progress, "snapshotting", source_file_count=len(source_files), source_logical_bytes=total_bytes, processed_source_bytes=0)
    for index, path in enumerate(source_files, start=1):
        rel = path.relative_to(run_dir).as_posix()
        safe = _safe_relative_name(rel)
        size = int(path.stat().st_size)
        entries.append({"name": safe, "size_bytes": size, "sha256": _sha256_file(path)})
        processed += size
        _emit_progress(progress, "snapshotting", source_file_count=len(source_files), processed_file_count=index, source_logical_bytes=total_bytes, processed_source_bytes=processed)
    return {
        "schema_version": ARCHIVE_MANIFEST_SCHEMA,
        "run_id": run_id,
        "created_at": _now(),
        "file_count": len(entries),
        "logical_bytes": sum(int(item["size_bytes"]) for item in entries),
        "files": entries,
    }


def _manifest_bytes(manifest: dict) -> bytes:
    return (json.dumps(manifest, indent=2, sort_keys=True) + "\n").encode("utf-8")


def _create_archive_python(run_dir: Path, archive: Path, manifest: dict, level: int, *, progress: ProgressCallback | None = None) -> dict:
    zstd = _zstandard()
    tmp = archive.with_name(f".{archive.name}.{uuid4().hex}.tmp")
    started = time.monotonic()
    _emit_progress(progress, "packing", archive_engine="python_tarfile_zstd_multithread", source_logical_bytes=int(manifest.get("logical_bytes") or 0))
    try:
        with tmp.open("wb") as raw:
            # threads=-1 asks python-zstandard to use all detected logical CPUs.
            compressor = zstd.ZstdCompressor(level=level, threads=-1, write_checksum=True)
            with compressor.stream_writer(raw, closefd=False) as compressed:
                with tarfile.open(fileobj=compressed, mode="w|") as tar:
                    payload = _manifest_bytes(manifest)
                    info = tarfile.TarInfo(ARCHIVE_MEMBER_MANIFEST)
                    info.size = len(payload)
                    info.mtime = 0
                    import io
                    tar.addfile(info, io.BytesIO(payload))
                    for index, item in enumerate(manifest["files"], start=1):
                        name = _safe_relative_name(item["name"])
                        path = (run_dir / Path(*PurePosixPath(name).parts)).resolve()
                        try:
                            path.relative_to(run_dir.resolve())
                        except ValueError as exc:
                            raise ArtifactStorageError(f"source escapes run directory: {name}") from exc
                        if not path.is_file():
                            raise ArtifactStorageError(f"source artifact disappeared during archive: {name}")
                        tar.add(path, arcname=name, recursive=False)
                        _emit_progress(progress, "compressing", archive_engine="python_tarfile_zstd_multithread", processed_file_count=index, source_file_count=int(manifest.get("file_count") or 0), source_logical_bytes=int(manifest.get("logical_bytes") or 0), archive_bytes_written=int(tmp.stat().st_size) if tmp.exists() else 0)
        os.replace(tmp, archive)
        return {"archive_engine": "python_tarfile_zstd_multithread", "native_tar_used": False, "zstd_threads": -1, "archive_seconds": max(0.0, time.monotonic() - started)}
    finally:
        try:
            tmp.unlink()
        except FileNotFoundError:
            pass


def _create_archive_native_tar(run_dir: Path, archive: Path, manifest: dict, level: int, tar_executable: str, *, progress: ProgressCallback | None = None) -> dict:
    zstd = _zstandard()
    tmp = archive.with_name(f".{archive.name}.{uuid4().hex}.tmp")
    embedded_manifest = run_dir / ARCHIVE_MEMBER_MANIFEST
    list_path = archive.parent / f".{archive.name}.{uuid4().hex}.files.txt"
    started = time.monotonic()
    if embedded_manifest.exists():
        # This path is reserved by the archive format and is never a real artifact.
        try:
            embedded_manifest.unlink()
        except OSError as exc:
            raise ArtifactStorageError(f"cannot prepare embedded archive manifest: {exc}") from exc
    try:
        embedded_manifest.write_bytes(_manifest_bytes(manifest))
        names = [ARCHIVE_MEMBER_MANIFEST] + [_safe_relative_name(str(item["name"])) for item in manifest.get("files", [])]
        list_path.write_text("\n".join(names) + "\n", encoding="utf-8", newline="\n")
        _emit_progress(progress, "packing", archive_engine="windows_tar_zstd_multithread", native_tar_executable=tar_executable, source_file_count=int(manifest.get("file_count") or 0), source_logical_bytes=int(manifest.get("logical_bytes") or 0))
        with tempfile.TemporaryFile() as stderr_file:
            process = subprocess.Popen(
                [tar_executable, "-cf", "-", "-C", str(run_dir), "-T", str(list_path)],
                stdout=subprocess.PIPE,
                stderr=stderr_file,
                stdin=subprocess.DEVNULL,
            )
            if process.stdout is None:
                process.kill()
                raise ArtifactStorageError("native tar stdout pipe is unavailable")
            tar_bytes = 0
            try:
                with tmp.open("wb") as raw:
                    compressor = zstd.ZstdCompressor(level=level, threads=-1, write_checksum=True)
                    with compressor.stream_writer(raw, closefd=False) as compressed:
                        while True:
                            chunk = process.stdout.read(_ARCHIVE_CHUNK_BYTES)
                            if not chunk:
                                break
                            compressed.write(chunk)
                            tar_bytes += len(chunk)
                            _emit_progress(progress, "compressing", archive_engine="windows_tar_zstd_multithread", processed_tar_bytes=tar_bytes, source_logical_bytes=int(manifest.get("logical_bytes") or 0), archive_bytes_written=int(tmp.stat().st_size) if tmp.exists() else 0)
                returncode = process.wait(timeout=120)
            except Exception:
                process.kill()
                process.wait(timeout=30)
                raise
            if returncode != 0:
                stderr_file.seek(0)
                error = stderr_file.read().decode("utf-8", errors="replace")[-8000:]
                raise ArtifactStorageError(f"native tar failed with exit {returncode}: {error.strip()}")
        os.replace(tmp, archive)
        return {"archive_engine": "windows_tar_zstd_multithread", "native_tar_used": True, "native_tar_executable": tar_executable, "zstd_threads": -1, "archive_seconds": max(0.0, time.monotonic() - started)}
    finally:
        for path in (embedded_manifest, list_path, tmp):
            try:
                path.unlink()
            except FileNotFoundError:
                pass
            except OSError:
                pass


def _create_archive(run_dir: Path, archive: Path, manifest: dict, level: int, *, progress: ProgressCallback | None = None) -> dict:
    # A hard process stop may leave a previous temporary archive. It is never
    # part of the verified archive identity and is safe to remove on retry.
    for stale in archive.parent.glob(f".{archive.name}.*.tmp"):
        try:
            stale.unlink()
        except OSError:
            pass
    tar_executable = _native_tar_executable()
    if tar_executable and _native_tar_compatible(manifest):
        try:
            return _create_archive_native_tar(run_dir, archive, manifest, level, tar_executable, progress=progress)
        except Exception as exc:
            _emit_progress(progress, "native_tar_fallback", archive_engine="python_tarfile_zstd_multithread", native_tar_error=str(exc))
    return _create_archive_python(run_dir, archive, manifest, level, progress=progress)

def _read_external_manifest(path: Path) -> dict:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise ArtifactStorageError(f"invalid archive manifest: {path.name}") from exc
    if not isinstance(value, dict) or value.get("schema_version") != ARCHIVE_MANIFEST_SCHEMA:
        raise ArtifactStorageError("archive manifest schema mismatch")
    return value


def verify_archive(archive: Path, manifest: dict, *, expected_archive_sha256: str | None = None, progress: ProgressCallback | None = None) -> dict:
    if not archive.is_file():
        raise ArtifactStorageError("archive file is missing")
    if expected_archive_sha256 and _sha256_file(archive) != expected_archive_sha256:
        raise ArtifactStorageError("tar.zst archive SHA-256 mismatch")
    zstd = _zstandard()
    expected = {str(item["name"]): item for item in manifest.get("files", [])}
    _emit_progress(progress, "verifying", source_file_count=len(expected), verified_file_count=0, archive_bytes=int(archive.stat().st_size))
    observed: set[str] = set()
    embedded: dict | None = None
    with archive.open("rb") as raw:
        with zstd.ZstdDecompressor().stream_reader(raw) as reader:
            with tarfile.open(fileobj=reader, mode="r|") as tar:
                for member in tar:
                    if member.name == ARCHIVE_MEMBER_MANIFEST:
                        handle = tar.extractfile(member)
                        if handle is None:
                            raise ArtifactStorageError("embedded archive manifest is unreadable")
                        try:
                            embedded = json.loads(handle.read().decode("utf-8"))
                        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
                            raise ArtifactStorageError("embedded archive manifest is invalid") from exc
                        continue
                    name = _safe_relative_name(member.name)
                    if not member.isfile() or name not in expected:
                        raise ArtifactStorageError(f"unexpected archive member: {member.name}")
                    handle = tar.extractfile(member)
                    if handle is None:
                        raise ArtifactStorageError(f"unreadable archive member: {name}")
                    digest = hashlib.sha256()
                    size = 0
                    for chunk in iter(lambda: handle.read(4 * 1024 * 1024), b""):
                        size += len(chunk)
                        digest.update(chunk)
                    if size != int(expected[name]["size_bytes"]) or digest.hexdigest() != str(expected[name]["sha256"]):
                        raise ArtifactStorageError(f"archive member verification failed: {name}")
                    if name in observed:
                        raise ArtifactStorageError(f"duplicate archive member: {name}")
                    observed.add(name)
                    _emit_progress(progress, "verifying", source_file_count=len(expected), verified_file_count=len(observed), archive_bytes=int(archive.stat().st_size))
    if embedded != manifest:
        raise ArtifactStorageError("embedded and external archive manifests differ")
    missing = sorted(set(expected) - observed)
    if missing:
        raise ArtifactStorageError(f"archive is missing {len(missing)} expected files")
    return {"verified": True, "verified_file_count": len(observed), "verified_at": _now()}


def _delete_archived_raw(run_dir: Path, manifest: dict, *, progress: ProgressCallback | None = None) -> None:
    root = run_dir.resolve()
    errors: list[str] = []
    source_items = list(manifest.get("files", []))
    _emit_progress(progress, "releasing_raw", source_file_count=len(source_items), released_file_count=0)
    released = 0
    for item in source_items:
        name = _safe_relative_name(str(item["name"]))
        if _is_preserved_shell_member(name):
            continue
        path = (root / Path(*PurePosixPath(name).parts)).resolve()
        try:
            path.relative_to(root)
        except ValueError as exc:
            raise ArtifactStorageError(f"delete target escapes run directory: {name}") from exc
        if path.is_file():
            try:
                path.unlink()
                released += 1
                _emit_progress(progress, "releasing_raw", source_file_count=len(source_items), released_file_count=released)
            except OSError as exc:
                errors.append(f"{name}: {exc}")
    archive_root = (root / ARCHIVE_DIR_NAME).resolve()
    directories = sorted((path for path in root.rglob("*") if path.is_dir()), key=lambda p: len(p.parts), reverse=True)
    for directory in directories:
        if directory == archive_root or archive_root in directory.parents:
            continue
        try:
            directory.rmdir()
        except OSError:
            pass
    if errors:
        raise ArtifactStorageError("raw cleanup incomplete after verified archive: " + "; ".join(errors[:20]))


def _archive_run_unlocked(run_dir: Path, *, run_id: str, delete_raw: bool = True, compression_level: int = DEFAULT_ZSTD_LEVEL, progress: ProgressCallback | None = None) -> dict:
    run_dir = run_dir.resolve()
    level = max(1, min(19, int(compression_level)))
    previous = read_storage_summary(run_dir)
    archive, manifest_path, checksum_path = _archive_paths(run_dir, run_id)

    resumable_states = {
        "archived",
        "archive_online",
        "archive_verified_pending_raw_cleanup",
        "archive_verified_raw_cleanup_incomplete",
    }
    if previous.get("storage_state") in resumable_states and archive.is_file() and manifest_path.is_file():
        manifest = _read_external_manifest(manifest_path)
        expected_size = int(previous.get("archive_bytes") or -1)
        expected_sha = str(previous.get("archive_sha256") or "")
        if int(archive.stat().st_size) != expected_size:
            raise ArtifactStorageError("existing archive size differs from frozen storage metadata")
        if previous.get("storage_state") == "archived" and delete_raw:
            return previous
        if previous.get("storage_state") == "archive_online" and not delete_raw:
            return previous
        # Resume only from the already-verified immutable archive. Never rebuild
        # from a partially deleted raw tree.
        verify_archive(archive, manifest, expected_archive_sha256=expected_sha, progress=progress)
        if delete_raw:
            try:
                if progress is None:
                    _delete_archived_raw(run_dir, manifest)
                else:
                    _delete_archived_raw(run_dir, manifest, progress=progress)
                current = measure_online_tree(run_dir)
                original = int(previous.get("original_logical_bytes") or manifest.get("logical_bytes") or 0)
                effective = current["physical_bytes"] + expected_size
                saved = max(0, original - effective)
                return _write_storage_summary(run_dir, {
                    **previous,
                    "storage_state": "archived",
                    "raw_deleted_after_verification": True,
                    "raw_cleanup_error": "",
                    "online_logical_bytes": current["logical_bytes"],
                    "online_physical_bytes": current["physical_bytes"],
                    "current_effective_bytes": effective,
                    "saved_bytes": saved,
                    "saving_ratio": (saved / original) if original else 0.0,
                    "archived_at": previous.get("archived_at") or _now(),
                })
            except ArtifactStorageError as exc:
                return _write_storage_summary(run_dir, {
                    **previous,
                    "storage_state": "archive_verified_raw_cleanup_incomplete",
                    "raw_deleted_after_verification": False,
                    "raw_cleanup_error": str(exc),
                })
        return previous

    manifest = _snapshot(run_dir, run_id, progress=progress)
    if not manifest["files"]:
        raise ArtifactStorageError("no online artifacts are available for cold archive")
    engine = _create_archive(run_dir, archive, manifest, level, progress=progress)
    _emit_progress(progress, "checksumming_archive", archive_bytes=int(archive.stat().st_size))
    archive_sha = _sha256_file(archive)
    verification = verify_archive(archive, manifest, progress=progress)
    _atomic_json(manifest_path, manifest)
    checksum_path.write_text(f"{archive_sha}  {archive.name}\n", encoding="ascii")

    original = int(previous.get("original_logical_bytes") or manifest["logical_bytes"])
    archive_bytes = int(archive.stat().st_size)
    before_cleanup = measure_online_tree(run_dir)
    before_effective = before_cleanup["physical_bytes"] + archive_bytes
    before_saved = max(0, original - before_effective)
    base_payload = {
        **previous,
        "run_id": run_id,
        "storage_state": "archive_online" if not delete_raw else "archive_verified_pending_raw_cleanup",
        "archive_format": "tar.zst",
        "archive_compression": "zstd",
        "archive_compression_level": level,
        "archive_engine": engine.get("archive_engine"),
        "native_tar_used": engine.get("native_tar_used"),
        "native_tar_executable": engine.get("native_tar_executable"),
        "zstd_threads": engine.get("zstd_threads", -1),
        "archive_seconds": engine.get("archive_seconds"),
        "archive_relative_path": f"{ARCHIVE_DIR_NAME}/{archive.name}",
        "archive_manifest_relative_path": f"{ARCHIVE_DIR_NAME}/{manifest_path.name}",
        "archive_checksum_relative_path": f"{ARCHIVE_DIR_NAME}/{checksum_path.name}",
        "archive_bytes": archive_bytes,
        "archive_sha256": archive_sha,
        "archive_verified": True,
        "archive_verified_at": verification["verified_at"],
        "archived_file_count": int(manifest["file_count"]),
        "original_file_count": int(previous.get("original_file_count") or manifest["file_count"]),
        "original_logical_bytes": original,
        "online_logical_bytes": before_cleanup["logical_bytes"],
        "online_physical_bytes": before_cleanup["physical_bytes"],
        "current_effective_bytes": before_effective,
        "saved_bytes": before_saved,
        "saving_ratio": (before_saved / original) if original else 0.0,
        "raw_deleted_after_verification": False,
        "raw_cleanup_error": "",
        "archived_at": _now(),
    }
    # Persist the verified archive identity before any raw deletion. A crash or
    # locked-file cleanup failure can then resume from this archive without ever
    # rebuilding from a partially removed source tree.
    payload = _write_storage_summary(run_dir, base_payload)

    if delete_raw:
        try:
            if progress is None:
                _delete_archived_raw(run_dir, manifest)
            else:
                _delete_archived_raw(run_dir, manifest, progress=progress)
            state = "archived"
            cleanup_error = ""
            raw_deleted = True
        except ArtifactStorageError as exc:
            state = "archive_verified_raw_cleanup_incomplete"
            cleanup_error = str(exc)
            raw_deleted = False
    else:
        state = "archive_online"
        cleanup_error = ""
        raw_deleted = False

    current = measure_online_tree(run_dir)
    effective = current["physical_bytes"] + archive_bytes
    saved = max(0, original - effective)
    _emit_progress(progress, "completed", archive_bytes=archive_bytes, raw_deleted_after_verification=raw_deleted, archive_engine=engine.get("archive_engine"))
    return _write_storage_summary(run_dir, {
        **payload,
        "storage_state": state,
        "online_logical_bytes": current["logical_bytes"],
        "online_physical_bytes": current["physical_bytes"],
        "current_effective_bytes": effective,
        "raw_deleted_after_verification": raw_deleted,
        "raw_cleanup_error": cleanup_error,
        "saved_bytes": saved,
        "saving_ratio": (saved / original) if original else 0.0,
    })


def archive_run(run_dir: Path, *, run_id: str, delete_raw: bool = True, compression_level: int = DEFAULT_ZSTD_LEVEL, progress: ProgressCallback | None = None) -> dict:
    with _run_storage_lock(run_dir):
        return _archive_run_unlocked(
            run_dir, run_id=run_id, delete_raw=delete_raw, compression_level=compression_level, progress=progress
        )


def _archive_metadata(run_dir: Path) -> tuple[dict, Path, dict]:
    storage = read_storage_summary(run_dir)
    relative = storage.get("archive_relative_path")
    manifest_relative = storage.get("archive_manifest_relative_path")
    if not isinstance(relative, str) or not isinstance(manifest_relative, str):
        raise ArtifactStorageError("archive metadata is incomplete")
    archive = (run_dir / Path(*PurePosixPath(_safe_relative_name(relative)).parts)).resolve()
    manifest_path = (run_dir / Path(*PurePosixPath(_safe_relative_name(manifest_relative)).parts)).resolve()
    root = run_dir.resolve()
    for path in (archive, manifest_path):
        try:
            path.relative_to(root)
        except ValueError as exc:
            raise ArtifactStorageError("archive metadata escapes run directory") from exc
    manifest = _read_external_manifest(manifest_path)
    expected_size = int(storage.get("archive_bytes") or -1)
    if not archive.is_file() or int(archive.stat().st_size) != expected_size:
        raise ArtifactStorageError("archive file is missing or its size changed")
    return storage, archive, manifest


def archived_artifact_info(run_dir: Path, name: str) -> dict:
    storage, archive, manifest = _archive_metadata(run_dir)
    safe = _safe_relative_name(name)
    item = next((value for value in manifest.get("files", []) if value.get("name") == safe), None)
    if not isinstance(item, dict):
        raise FileNotFoundError(name)
    return {**item, "archive_path": archive, "archive_sha256": storage.get("archive_sha256")}


def stream_archived_artifact(run_dir: Path, name: str) -> Iterator[bytes]:
    info = archived_artifact_info(run_dir, name)
    archive = Path(info["archive_path"])
    safe = _safe_relative_name(name)
    zstd = _zstandard()
    with archive.open("rb") as raw:
        with zstd.ZstdDecompressor().stream_reader(raw) as reader:
            with tarfile.open(fileobj=reader, mode="r|") as tar:
                for member in tar:
                    if member.name != safe:
                        continue
                    handle = tar.extractfile(member)
                    if handle is None:
                        break
                    digest = hashlib.sha256()
                    size = 0
                    for chunk in iter(lambda: handle.read(1024 * 1024), b""):
                        size += len(chunk)
                        digest.update(chunk)
                        yield chunk
                    if size != int(info["size_bytes"]) or digest.hexdigest() != str(info["sha256"]):
                        raise ArtifactStorageError(f"streamed archive member failed integrity check: {safe}")
                    return
    raise FileNotFoundError(name)


def archive_path_for_download(run_dir: Path) -> Path:
    _storage, archive, _manifest = _archive_metadata(run_dir)
    return archive


def _restore_run_unlocked(run_dir: Path, *, run_id: str, reapply_ntfs: bool = True) -> dict:
    run_dir = run_dir.resolve()
    storage, archive, manifest = _archive_metadata(run_dir)
    verify_archive(archive, manifest, expected_archive_sha256=str(storage.get("archive_sha256") or ""))
    zstd = _zstandard()
    restore_prefix = f".r_{hashlib.sha256(run_id.encode('utf-8')).hexdigest()[:8]}_"
    for stale in run_dir.parent.glob(f"{restore_prefix}*"):
        if stale.is_dir():
            shutil.rmtree(stale, ignore_errors=True)
    temp_root = Path(tempfile.mkdtemp(prefix=restore_prefix, dir=str(run_dir.parent))).resolve()
    expected = {str(item["name"]): item for item in manifest.get("files", [])}
    try:
        with archive.open("rb") as raw:
            with zstd.ZstdDecompressor().stream_reader(raw) as reader:
                with tarfile.open(fileobj=reader, mode="r|") as tar:
                    for member in tar:
                        if member.name == ARCHIVE_MEMBER_MANIFEST:
                            continue
                        name = _safe_relative_name(member.name)
                        if name not in expected or not member.isfile():
                            raise ArtifactStorageError(f"unexpected restore member: {member.name}")
                        handle = tar.extractfile(member)
                        if handle is None:
                            raise ArtifactStorageError(f"restore member unreadable: {name}")
                        target = (temp_root / Path(*PurePosixPath(name).parts)).resolve()
                        try:
                            target.relative_to(temp_root)
                        except ValueError as exc:
                            raise ArtifactStorageError(f"restore target escapes temporary root: {name}") from exc
                        target.parent.mkdir(parents=True, exist_ok=True)
                        digest = hashlib.sha256()
                        size = 0
                        with target.open("wb") as out:
                            for chunk in iter(lambda: handle.read(4 * 1024 * 1024), b""):
                                out.write(chunk)
                                size += len(chunk)
                                digest.update(chunk)
                        if size != int(expected[name]["size_bytes"]) or digest.hexdigest() != str(expected[name]["sha256"]):
                            raise ArtifactStorageError(f"restored artifact verification failed: {name}")
        for name in expected:
            if _is_preserved_shell_member(name):
                continue
            source = (temp_root / Path(*PurePosixPath(name).parts)).resolve()
            if not source.is_file():
                raise ArtifactStorageError(f"verified restore file missing from temporary root: {name}")
            target = (run_dir / Path(*PurePosixPath(name).parts)).resolve()
            target.parent.mkdir(parents=True, exist_ok=True)
            if target.exists():
                target.unlink()
            os.replace(source, target)
    finally:
        shutil.rmtree(temp_root, ignore_errors=True)

    restored = measure_online_tree(run_dir)
    payload = _write_storage_summary(run_dir, {
        **storage,
        "run_id": run_id,
        "storage_state": "online_restored",
        "raw_deleted_after_verification": False,
        "online_logical_bytes": restored["logical_bytes"],
        "online_physical_bytes": restored["physical_bytes"],
        "current_effective_bytes": restored["physical_bytes"] + int(storage.get("archive_bytes") or 0),
        "restored_at": _now(),
    })
    if reapply_ntfs and _is_windows():
        payload = _compact_online_run_unlocked(run_dir, run_id=run_id, force=True)
        archive_bytes = int(storage.get("archive_bytes") or 0)
        online_physical = int(payload.get("online_physical_bytes") or 0)
        original = int(payload.get("original_logical_bytes") or storage.get("original_logical_bytes") or 0)
        effective = online_physical + archive_bytes
        saved = max(0, original - effective)
        payload = _write_storage_summary(run_dir, {
            **payload,
            "storage_state": "online_restored_ntfs_compressed",
            "archive_bytes": archive_bytes,
            "archive_sha256": storage.get("archive_sha256"),
            "archive_relative_path": storage.get("archive_relative_path"),
            "archive_manifest_relative_path": storage.get("archive_manifest_relative_path"),
            "archive_checksum_relative_path": storage.get("archive_checksum_relative_path"),
            "current_effective_bytes": effective,
            "saved_bytes": saved,
            "saving_ratio": (saved / original) if original else 0.0,
            "restored_at": payload.get("restored_at") or _now(),
        })
    return payload


def restore_run(run_dir: Path, *, run_id: str, reapply_ntfs: bool = True) -> dict:
    with _run_storage_lock(run_dir):
        return _restore_run_unlocked(run_dir, run_id=run_id, reapply_ntfs=reapply_ntfs)
