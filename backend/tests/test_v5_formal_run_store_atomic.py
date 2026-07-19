from __future__ import annotations

import threading
from pathlib import Path

from backend.app.services import v5_formal_run_store as store


def test_write_group_is_atomic_under_concurrent_readers(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setattr(store, "ROOT_DIR", tmp_path)
    group_id = "v5grp_atomic_group"
    store.write_group({"run_group_id": group_id, "status": "queued", "sequence": -1})
    errors: list[BaseException] = []
    stop = threading.Event()

    def writer() -> None:
        try:
            for sequence in range(200):
                store.write_group({"run_group_id": group_id, "status": "running", "sequence": sequence, "payload": "x" * 1024})
        except BaseException as exc:  # pragma: no cover - asserted through shared error list
            errors.append(exc)
        finally:
            stop.set()

    def reader() -> None:
        try:
            while not stop.is_set():
                record = store.read_group(group_id)
                assert record["run_group_id"] == group_id
                assert isinstance(record["sequence"], int)
        except BaseException as exc:  # pragma: no cover - asserted through shared error list
            errors.append(exc)

    threads = [threading.Thread(target=reader) for _ in range(4)]
    threads.append(threading.Thread(target=writer))
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=5)

    assert errors == []
    final = store.read_group(group_id)
    assert final["status"] == "running"
    assert isinstance(final["sequence"], int)


def test_child_and_attempt_writes_are_atomic_under_concurrent_readers(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setattr(store, "ROOT_DIR", tmp_path)
    group_id = "v5grp_atomic_child"
    child_id = "v5child_atomic"
    store.write_group({"run_group_id": group_id, "status": "queued"})
    store.write_child(group_id, {"run_group_id": group_id, "child_run_id": child_id, "status": "queued", "sequence": -1})
    store.write_attempt(group_id, child_id, {"attempt_number": 1, "status": "queued", "sequence": -1})
    attempt_path = store.group_dir(group_id) / "children" / child_id / "attempt_1.json"
    errors: list[BaseException] = []
    stop = threading.Event()

    def writer() -> None:
        try:
            for sequence in range(200):
                store.write_child(group_id, {"run_group_id": group_id, "child_run_id": child_id, "status": "running", "sequence": sequence, "payload": "y" * 1024})
                store.write_attempt(group_id, child_id, {"attempt_number": 1, "status": "running", "sequence": sequence, "payload": "z" * 1024})
        except BaseException as exc:  # pragma: no cover - asserted through shared error list
            errors.append(exc)
        finally:
            stop.set()

    def reader() -> None:
        try:
            while not stop.is_set():
                child = store.read_child(group_id, child_id)
                attempt = store._read_json(attempt_path)
                assert child["child_run_id"] == child_id
                assert attempt["attempt_number"] == 1
                assert isinstance(child["sequence"], int)
                assert isinstance(attempt["sequence"], int)
        except BaseException as exc:  # pragma: no cover - asserted through shared error list
            errors.append(exc)

    threads = [threading.Thread(target=reader) for _ in range(4)]
    threads.append(threading.Thread(target=writer))
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=5)

    assert errors == []
    assert store.read_child(group_id, child_id)["status"] == "running"
    assert store._read_json(attempt_path)["status"] == "running"
