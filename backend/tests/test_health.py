from unittest.mock import AsyncMock

from fastapi.testclient import TestClient

from app import main


def test_health_contract_and_security_headers() -> None:
    with TestClient(main.create_app()) as client:
        response = client.get("/health")

    assert response.status_code == 200
    assert response.json() == {"status": "ok", "version": "0.1.0"}
    assert response.headers["x-content-type-options"] == "nosniff"
    assert response.headers["x-frame-options"] == "DENY"


def test_ready_contract_and_correlation_id_propagation(monkeypatch) -> None:
    correlation_id = "test-correlation-id"
    monkeypatch.setattr(main, "check_database_connection", AsyncMock(return_value=True))
    with TestClient(main.create_app()) as client:
        response = client.get("/ready", headers={"X-Correlation-ID": correlation_id})

    assert response.status_code == 200
    assert response.json() == {"status": "ready"}
    assert response.headers["x-correlation-id"] == correlation_id


def test_openapi_document_is_available() -> None:
    with TestClient(main.create_app()) as client:
        response = client.get("/api/v1/openapi.json")

    assert response.status_code == 200
    assert response.json()["info"]["title"] == "AegisOps"


def test_ready_returns_service_unavailable_when_database_is_unavailable(monkeypatch) -> None:
    monkeypatch.setattr(main, "check_database_connection", AsyncMock(return_value=False))
    with TestClient(main.create_app()) as client:
        response = client.get("/ready")

    assert response.status_code == 503
    assert response.json() == {"status": "not_ready"}
