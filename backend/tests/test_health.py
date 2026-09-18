from fastapi.testclient import TestClient

from app.main import create_app


def test_health_contract_and_security_headers() -> None:
    with TestClient(create_app()) as client:
        response = client.get("/health")

    assert response.status_code == 200
    assert response.json() == {"status": "ok", "version": "0.1.0"}
    assert response.headers["x-content-type-options"] == "nosniff"
    assert response.headers["x-frame-options"] == "DENY"


def test_ready_contract_and_correlation_id_propagation() -> None:
    correlation_id = "test-correlation-id"
    with TestClient(create_app()) as client:
        response = client.get("/ready", headers={"X-Correlation-ID": correlation_id})

    assert response.status_code == 200
    assert response.json() == {"status": "ready"}
    assert response.headers["x-correlation-id"] == correlation_id


def test_openapi_document_is_available() -> None:
    with TestClient(create_app()) as client:
        response = client.get("/api/v1/openapi.json")

    assert response.status_code == 200
    assert response.json()["info"]["title"] == "AegisOps"
