from fastapi.testclient import TestClient

from backend.app import main


def test_download_rejects_unknown_filename(tmp_path, monkeypatch) -> None:
    monkeypatch.setattr(main, "RUN", tmp_path)

    response = TestClient(main.app).get("/api/v0/experiments/v0_default_asset_hotspot/files/not-allowed.txt")

    assert response.status_code == 403


def test_download_rejects_path_separator(tmp_path, monkeypatch) -> None:
    monkeypatch.setattr(main, "RUN", tmp_path)

    response = TestClient(main.app).get("/api/v0/experiments/v0_default_asset_hotspot/files/summary.csv%5Csecret.txt")

    assert response.status_code == 400


def test_download_returns_404_for_missing_output(tmp_path, monkeypatch) -> None:
    monkeypatch.setattr(main, "RUN", tmp_path)

    response = TestClient(main.app).get("/api/v0/experiments/v0_default_asset_hotspot/files/summary.csv")

    assert response.status_code == 404


def test_downloads_allowed_existing_output(tmp_path, monkeypatch) -> None:
    monkeypatch.setattr(main, "RUN", tmp_path)
    expected = b"tx_count\n10000\n"
    (tmp_path / "summary.csv").write_bytes(expected)

    response = TestClient(main.app).get("/api/v0/experiments/v0_default_asset_hotspot/files/summary.csv")

    assert response.status_code == 200
    assert response.content == expected
    assert 'attachment; filename="summary.csv"' in response.headers["content-disposition"]
