from datetime import datetime, timedelta, timezone
import uuid

import httpx
import pytest

from app.telemetry.query.errors import (
    MetricQueryBackendError,
    QueryResponseValidationError,
)
from app.telemetry.query.metrics import (
    MetricQueryAdapter,
    build_metric_selector,
    parse_metric_response,
)
from app.telemetry.query.models import MetricQuery
from app.telemetry.transport.serialization import MAX_UINT64


# --- 1. Metric Selector Generation Tests ---


def test_build_metric_selector_metric_only() -> None:
    q = MetricQuery(metric="http_requests_total")
    selector = build_metric_selector(q)
    assert selector == "http_requests_total"


def test_build_metric_selector_all_filters_and_escaping() -> None:
    tenant_id = uuid.uuid4()
    run_id = uuid.uuid4()
    event_id = uuid.uuid4()

    q = MetricQuery(
        metric="http_request_duration_seconds",
        service='order-"service"\\v1',
        tenant_id=tenant_id,
        environment="simulation",
        run_id=run_id,
        seed=MAX_UINT64,
        event_id=event_id,
    )

    selector = build_metric_selector(q)
    assert selector.startswith("http_request_duration_seconds{")
    assert 'service="order-\\"service\\"\\\\v1"' in selector
    assert f'tenant_id="{tenant_id}"' in selector
    assert 'environment="simulation"' in selector
    assert f'run_id="{run_id}"' in selector
    assert 'seed="18446744073709551615"' in selector
    assert f'event_id="{event_id}"' in selector


# --- 2. Response Parsing Unit Tests ---


def test_parse_metric_response_vector_success_and_deterministic_order() -> None:
    event_id1 = uuid.uuid4()
    event_id2 = uuid.uuid4()
    run_id = uuid.uuid4()
    tenant_id = uuid.uuid4()

    data = {
        "status": "success",
        "data": {
            "resultType": "vector",
            "result": [
                {
                    "metric": {
                        "__name__": "cpu_usage",
                        "service": "order-service",
                        "tenant_id": str(tenant_id),
                        "environment": "simulation",
                        "run_id": str(run_id),
                        "seed": "42",
                        "event_id": str(event_id2),
                        "scenario_id": "sc-1",
                    },
                    "value": [1774612350.0, "85.5"],
                },
                {
                    "metric": {
                        "__name__": "cpu_usage",
                        "service": "order-service",
                        "tenant_id": str(tenant_id),
                        "environment": "simulation",
                        "run_id": str(run_id),
                        "seed": "42",
                        "event_id": str(event_id1),
                        "scenario_id": "sc-1",
                    },
                    "value": [1774612340.0, "50.0"],
                },
            ],
        },
    }

    samples = parse_metric_response(data, "cpu_usage")
    assert len(samples) == 2

    # Deterministic order: timestamp ASC
    assert samples[0].event_id == event_id1
    assert samples[0].value == 50.0
    assert samples[0].seed == 42
    assert samples[0].timestamp.timestamp() == 1774612340.0

    assert samples[1].event_id == event_id2
    assert samples[1].value == 85.5


def test_parse_metric_response_matrix_success() -> None:
    event_id = uuid.uuid4()
    run_id = uuid.uuid4()
    tenant_id = uuid.uuid4()

    data = {
        "status": "success",
        "data": {
            "resultType": "matrix",
            "result": [
                {
                    "metric": {
                        "__name__": "latency_ms",
                        "service": "api-gateway",
                        "tenant_id": str(tenant_id),
                        "environment": "simulation",
                        "run_id": str(run_id),
                        "seed": "18446744073709551615",
                        "event_id": str(event_id),
                    },
                    "values": [
                        [1774612300.0, "10.5"],
                        [1774612310.0, "15.2"],
                    ],
                }
            ],
        },
    }

    samples = parse_metric_response(data, "latency_ms")
    assert len(samples) == 2
    assert samples[0].seed == MAX_UINT64
    assert samples[0].value == 10.5
    assert samples[1].value == 15.2


def test_parse_metric_response_backend_error_status() -> None:
    data = {"status": "error", "error": "syntax error near unexpected token"}
    with pytest.raises(MetricQueryBackendError, match="syntax error"):
        parse_metric_response(data, "cpu")


def test_parse_metric_response_unsupported_result_type() -> None:
    data = {"status": "success", "data": {"resultType": "scalar", "result": []}}
    with pytest.raises(QueryResponseValidationError, match="Unsupported resultType"):
        parse_metric_response(data, "cpu")


def test_parse_metric_response_missing_labels() -> None:
    data = {
        "status": "success",
        "data": {
            "resultType": "vector",
            "result": [{"metric": {"__name__": "cpu"}, "value": [1774612340.0, "50.0"]}],
        },
    }
    with pytest.raises(QueryResponseValidationError, match="Failed to parse required metric sample labels"):
        parse_metric_response(data, "cpu")


# --- 3. HTTP Client & Lifecycle Unit Tests ---


@pytest.mark.asyncio
async def test_metric_query_adapter_instant_query_http() -> None:
    tenant_id = uuid.uuid4()
    run_id = uuid.uuid4()
    event_id = uuid.uuid4()

    def mock_handler(request: httpx.Request) -> httpx.Response:
        assert request.url.path == "/api/v1/query"
        assert "http_requests" in request.url.params["query"]
        return httpx.Response(
            200,
            json={
                "status": "success",
                "data": {
                    "resultType": "vector",
                    "result": [
                        {
                            "metric": {
                                "__name__": "http_requests",
                                "service": "order-service",
                                "tenant_id": str(tenant_id),
                                "environment": "simulation",
                                "run_id": str(run_id),
                                "seed": "100",
                                "event_id": str(event_id),
                            },
                            "value": [1774612345.0, "42.0"],
                        }
                    ],
                },
            },
        )

    transport = httpx.MockTransport(mock_handler)
    async with httpx.AsyncClient(transport=transport) as client:
        adapter = MetricQueryAdapter(client=client)
        res = await adapter.query(MetricQuery(metric="http_requests"))
        assert res.count == 1
        assert res.samples[0].value == 42.0


@pytest.mark.asyncio
async def test_metric_query_adapter_range_query_http() -> None:
    now = datetime.now(timezone.utc)
    captured_requests: list[httpx.Request] = []

    def mock_handler(request: httpx.Request) -> httpx.Response:
        captured_requests.append(request)
        return httpx.Response(
            200,
            json={"status": "success", "data": {"resultType": "matrix", "result": []}},
        )

    transport = httpx.MockTransport(mock_handler)
    async with httpx.AsyncClient(transport=transport) as client:
        adapter = MetricQueryAdapter(client=client)
        q = MetricQuery(
            metric="cpu",
            start_time=now - timedelta(minutes=5),
            end_time=now,
            step=timedelta(seconds=15),
        )
        res = await adapter.query(q)
        assert res.count == 0

        assert len(captured_requests) == 1
        req = captured_requests[0]
        assert req.url.path == "/api/v1/query_range"
        assert "start" in req.url.params
        assert "end" in req.url.params
        assert req.url.params["step"] == "15s"


@pytest.mark.asyncio
async def test_metric_query_adapter_backend_500_raises_error() -> None:
    def mock_handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(500, text="Internal Server Error")

    transport = httpx.MockTransport(mock_handler)
    async with httpx.AsyncClient(transport=transport) as client:
        adapter = MetricQueryAdapter(client=client)
        with pytest.raises(MetricQueryBackendError, match="HTTP 500"):
            await adapter.query(MetricQuery(metric="cpu"))
