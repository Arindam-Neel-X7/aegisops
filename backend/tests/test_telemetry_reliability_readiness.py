import asyncio
from unittest.mock import AsyncMock, patch

import pytest

from app.main import app
from app.telemetry.reliability.readiness import check_telemetry_readiness
import httpx


@pytest.mark.asyncio
async def test_check_telemetry_readiness_all_healthy() -> None:
    is_ready, data = await check_telemetry_readiness(
        kafka_probe=asyncio.sleep(0, result=True),
        vm_probe=asyncio.sleep(0, result=True),
        os_probe=asyncio.sleep(0, result=(True, "green")),
        metric_query_probe=asyncio.sleep(0, result=True),
        evidence_query_probe=asyncio.sleep(0, result=True),
    )
    assert is_ready is True
    assert data["status"] == "ready"
    assert data["components"]["kafka"]["ready"] is True
    assert data["components"]["victoriametrics"]["ready"] is True
    assert data["components"]["opensearch"]["ready"] is True
    assert data["components"]["opensearch"]["cluster_status"] == "green"
    assert data["components"]["metric_query"]["ready"] is True
    assert data["components"]["evidence_query"]["ready"] is True


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "failed_component,k,vm,os,mq,eq",
    [
        ("kafka", False, True, (True, "green"), True, True),
        ("victoriametrics", True, False, (True, "green"), True, True),
        ("opensearch", True, True, (False, "unreachable"), True, True),
        ("metric_query", True, True, (True, "green"), False, True),
        ("evidence_query", True, True, (True, "green"), True, False),
    ],
)
async def test_check_telemetry_readiness_subsystem_failures(
    failed_component: str,
    k: bool,
    vm: bool,
    os: tuple[bool, str],
    mq: bool,
    eq: bool,
) -> None:
    is_ready, data = await check_telemetry_readiness(
        kafka_probe=asyncio.sleep(0, result=k),
        vm_probe=asyncio.sleep(0, result=vm),
        os_probe=asyncio.sleep(0, result=os),
        metric_query_probe=asyncio.sleep(0, result=mq),
        evidence_query_probe=asyncio.sleep(0, result=eq),
    )
    assert is_ready is False
    assert data["status"] == "not_ready"
    assert data["components"][failed_component]["ready"] is False


@pytest.mark.asyncio
async def test_fastapi_telemetry_readiness_endpoint_200_and_503() -> None:
    transport = httpx.ASGITransport(app=app)  # type: ignore[arg-type]
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        # 1. Mock healthy readiness
        with patch(
            "app.telemetry.reliability.readiness.check_telemetry_readiness",
            new_callable=AsyncMock,
        ) as mock_probe:
            mock_probe.return_value = (True, {"status": "ready", "components": {}})
            res = await client.get("/ready/telemetry")
            assert res.status_code == 200
            assert res.json()["status"] == "ready"

        # 2. Mock unhealthy readiness
        with patch(
            "app.telemetry.reliability.readiness.check_telemetry_readiness",
            new_callable=AsyncMock,
        ) as mock_probe:
            mock_probe.return_value = (False, {"status": "not_ready", "components": {"kafka": {"ready": False}}})
            res = await client.get("/ready/telemetry")
            assert res.status_code == 503
            assert res.json()["status"] == "not_ready"


@pytest.mark.asyncio
async def test_fastapi_telemetry_metrics_endpoint() -> None:
    transport = httpx.ASGITransport(app=app)  # type: ignore[arg-type]
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        res = await client.get("/telemetry/metrics")
        assert res.status_code == 200
        body = res.json()
        assert "published_count" in body
        assert "persisted_count" in body
        assert "quarantined_count" in body
        assert "throughput_per_second" in body
