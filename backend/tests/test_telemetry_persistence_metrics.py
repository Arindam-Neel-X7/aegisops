from datetime import datetime, timezone, timedelta
from typing import Any
from unittest.mock import AsyncMock, MagicMock
import uuid

import aiokafka
from aiokafka import TopicPartition
from aiokafka.structs import ConsumerRecord
import httpx
import pytest

from app.telemetry.persistence.errors import (
    InvalidMetricError,
    VictoriaMetricsPersistenceError,
    VictoriaMetricsRetryExhaustedError,
)
from app.telemetry.persistence.metrics import (
    VictoriaMetricsPersistenceAdapter,
    check_victoriametrics_health,
    extract_metric_data,
)
from app.telemetry.schemas import EventSeverity, EventType, TelemetryEvent
from app.telemetry.topics import KafkaTopic
from app.telemetry.transport.consumer import (
    MetricsConsumer,
    TelemetryEnvelope,
)
from app.telemetry.transport.errors import ConsumerHandlerError
from app.telemetry.transport.serialization import (
    MAX_UINT64,
    TelemetryExecutionContext,
    construct_kafka_headers,
    construct_kafka_key,
    serialize_event,
)


def _create_sample_envelope(
    metric_name: str = "cpu_usage_percent",
    value: Any = 42.5,
    event_time: datetime | None = None,
    tenant_id: uuid.UUID | None = None,
    event_id: uuid.UUID | None = None,
    run_id: uuid.UUID | None = None,
    seed: int = 12345,
    extra_payload: dict[str, Any] | None = None,
    service: str = "order-service",
    environment: str = "simulation",
) -> TelemetryEnvelope:
    payload: dict[str, Any] = {}
    if metric_name is not None:
        payload["metric_name"] = metric_name
    if value is not None:
        payload["value"] = value
    if extra_payload:
        payload.update(extra_payload)

    event = TelemetryEvent(
        schema_version="1.0",
        event_id=event_id or uuid.uuid4(),
        event_time=event_time or datetime(2026, 9, 26, 14, 0, 0, tzinfo=timezone.utc),
        tenant_id=tenant_id or uuid.uuid4(),
        environment=environment,
        service=service,
        event_type=EventType.METRIC,
        severity=EventSeverity.INFO,
        trace_id="trace-abc-123",
        payload=payload,
    )

    context = TelemetryExecutionContext(
        run_id=run_id or uuid.uuid4(),
        scenario_id="scenario-cpu-saturation",
        scenario_version="1.0",
        reproducibility_key="rep-key-456",
        seed=seed,
    )

    return TelemetryEnvelope(
        event=event,
        context=context,
        kafka_timestamp_ms=1770000000000,
        topic=KafkaTopic.METRICS.value,
        partition=0,
        offset=10,
        key=b"test-key",
        producer_version="step2.4",
    )


# --- 1. Metric Validation Tests ---


def test_metric_validation_valid_integer() -> None:
    env = _create_sample_envelope(value=100)
    name, val, labels, ts = extract_metric_data(env)
    assert name == "cpu_usage_percent"
    assert val == 100.0
    assert isinstance(val, float)


def test_metric_validation_valid_float() -> None:
    env = _create_sample_envelope(value=95.75)
    name, val, labels, ts = extract_metric_data(env)
    assert name == "cpu_usage_percent"
    assert val == 95.75


def test_metric_validation_boolean_mappings() -> None:
    env_true = _create_sample_envelope(value=True)
    _, val_true, _, _ = extract_metric_data(env_true)
    assert val_true == 1.0

    env_false = _create_sample_envelope(value=False)
    _, val_false, _, _ = extract_metric_data(env_false)
    assert val_false == 0.0


def test_metric_validation_missing_metric_name_rejected() -> None:
    env = _create_sample_envelope()
    env.event.payload.pop("metric_name")
    with pytest.raises(InvalidMetricError, match="Missing required 'metric_name'"):
        extract_metric_data(env)


def test_metric_validation_empty_metric_name_rejected() -> None:
    env = _create_sample_envelope(metric_name="   ")
    with pytest.raises(InvalidMetricError, match="must be a non-empty string"):
        extract_metric_data(env)


def test_metric_validation_missing_value_rejected() -> None:
    env = _create_sample_envelope()
    env.event.payload.pop("value")
    with pytest.raises(InvalidMetricError, match="Missing required 'value'"):
        extract_metric_data(env)


def test_metric_validation_none_value_rejected() -> None:
    env = _create_sample_envelope(value=None)
    with pytest.raises(InvalidMetricError, match="Missing required 'value'"):
        extract_metric_data(env)


def test_metric_validation_string_value_rejected() -> None:
    env = _create_sample_envelope(value="42.5")
    with pytest.raises(InvalidMetricError, match="Invalid metric value type: 'str'"):
        extract_metric_data(env)


def test_metric_validation_nan_rejected() -> None:
    env = _create_sample_envelope(value=float("nan"))
    with pytest.raises(InvalidMetricError, match="cannot be NaN or Infinity"):
        extract_metric_data(env)


def test_metric_validation_pos_infinity_rejected() -> None:
    env = _create_sample_envelope(value=float("inf"))
    with pytest.raises(InvalidMetricError, match="cannot be NaN or Infinity"):
        extract_metric_data(env)


def test_metric_validation_neg_infinity_rejected() -> None:
    env = _create_sample_envelope(value=float("-inf"))
    with pytest.raises(InvalidMetricError, match="cannot be NaN or Infinity"):
        extract_metric_data(env)


def test_metric_validation_list_rejected() -> None:
    env = _create_sample_envelope(value=[1, 2, 3])
    with pytest.raises(InvalidMetricError, match="Invalid metric value type: 'list'"):
        extract_metric_data(env)


def test_metric_validation_dict_rejected() -> None:
    env = _create_sample_envelope(value={"nested": 1.0})
    with pytest.raises(InvalidMetricError, match="Invalid metric value type: 'dict'"):
        extract_metric_data(env)


# --- 2. Label Mapping Tests ---


def test_label_mapping_required_and_forbidden_labels() -> None:
    event_id = uuid.uuid4()
    run_id = uuid.uuid4()
    tenant_id = uuid.uuid4()

    env = _create_sample_envelope(
        metric_name="http_request_duration_seconds",
        value=0.123,
        event_id=event_id,
        run_id=run_id,
        tenant_id=tenant_id,
        seed=MAX_UINT64,
        service="payment-service",
        environment="production-simulation",
        extra_payload={
            "status_code": 200,
            "outcome": "success",
            "trace_id": "forbidden-trace",
            "request_id": "forbidden-req",
            "arbitrary_nested": {"a": 1},
        },
    )

    name, val, labels, ts = extract_metric_data(env)

    # Required labels
    assert labels["__name__"] == "http_request_duration_seconds"
    assert labels["service"] == "payment-service"
    assert labels["tenant_id"] == str(tenant_id)
    assert labels["environment"] == "production-simulation"
    assert labels["run_id"] == str(run_id)
    assert labels["scenario_id"] == "scenario-cpu-saturation"
    assert labels["seed"] == "18446744073709551615"
    assert labels["event_id"] == str(event_id)

    # Optional labels
    assert labels["status_code"] == "200"
    assert labels["outcome"] == "success"

    # Forbidden labels verified absent
    assert "trace_id" not in labels
    assert "request_id" not in labels
    assert "scenario_version" not in labels
    assert "reproducibility_key" not in labels
    assert "arbitrary_nested" not in labels


# --- 3. Timestamp Mapping Tests ---


def test_timestamp_mapping_utc_and_non_utc() -> None:
    # 1. UTC time
    dt_utc = datetime(2026, 9, 26, 15, 30, 0, 0, tzinfo=timezone.utc)
    env_utc = _create_sample_envelope(event_time=dt_utc)
    _, _, _, ts_utc = extract_metric_data(env_utc)
    expected_ms = int(dt_utc.timestamp() * 1000)
    assert ts_utc == expected_ms

    # 2. Non-UTC time pointing to the same instant (+05:30)
    tz_offset = timezone(timedelta(hours=5, minutes=30))
    dt_offset = datetime(2026, 9, 26, 21, 0, 0, 0, tzinfo=tz_offset)
    env_offset = _create_sample_envelope(event_time=dt_offset)
    _, _, _, ts_offset = extract_metric_data(env_offset)

    assert ts_offset == expected_ms
    assert ts_offset == ts_utc


# --- 4. HTTP Success & Retry Bounds Tests ---


@pytest.mark.asyncio
async def test_http_persistence_200_success() -> None:
    captured_requests: list[httpx.Request] = []

    def mock_handler(request: httpx.Request) -> httpx.Response:
        captured_requests.append(request)
        return httpx.Response(200, json={"status": "ok"})

    transport = httpx.MockTransport(mock_handler)
    async with httpx.AsyncClient(transport=transport) as client:
        adapter = VictoriaMetricsPersistenceAdapter(client=client)
        env = _create_sample_envelope(metric_name="requests_total", value=50)

        result = await adapter.persist(env)

        assert len(captured_requests) == 1
        req = captured_requests[0]
        assert req.url.path == "/api/v1/import"
        assert req.headers["Content-Type"] == "application/json"
        assert result.status_code == 200
        assert result.attempts == 1
        assert result.metric_name == "requests_total"
        assert result.event_id == env.event.event_id


@pytest.mark.asyncio
async def test_http_persistence_204_success() -> None:
    def mock_handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(204)

    transport = httpx.MockTransport(mock_handler)
    async with httpx.AsyncClient(transport=transport) as client:
        adapter = VictoriaMetricsPersistenceAdapter(client=client)
        env = _create_sample_envelope()
        result = await adapter.persist(env)
        assert result.status_code == 204
        assert result.attempts == 1


@pytest.mark.asyncio
async def test_http_transient_retry_success_on_attempt_2() -> None:
    call_count = 0

    def mock_handler(request: httpx.Request) -> httpx.Response:
        nonlocal call_count
        call_count += 1
        if call_count == 1:
            return httpx.Response(503, text="Service Temporarily Unavailable", headers={"Retry-After": "0.01"})
        return httpx.Response(200, json={"status": "ok"})

    transport = httpx.MockTransport(mock_handler)
    async with httpx.AsyncClient(transport=transport) as client:
        adapter = VictoriaMetricsPersistenceAdapter(client=client)
        env = _create_sample_envelope()
        result = await adapter.persist(env)
        assert call_count == 2
        assert result.attempts == 2
        assert result.status_code == 200


@pytest.mark.asyncio
async def test_http_transient_retry_exhaustion_after_3_attempts() -> None:
    call_count = 0

    def mock_handler(request: httpx.Request) -> httpx.Response:
        nonlocal call_count
        call_count += 1
        return httpx.Response(500, text="Internal Server Error")

    transport = httpx.MockTransport(mock_handler)
    async with httpx.AsyncClient(transport=transport) as client:
        adapter = VictoriaMetricsPersistenceAdapter(client=client)
        env = _create_sample_envelope()
        with pytest.raises(VictoriaMetricsRetryExhaustedError, match="exhausted 3 attempts"):
            await adapter.persist(env)
        assert call_count == 3


@pytest.mark.asyncio
async def test_http_permanent_400_not_retried() -> None:
    call_count = 0

    def mock_handler(request: httpx.Request) -> httpx.Response:
        nonlocal call_count
        call_count += 1
        return httpx.Response(400, text="Bad Request: invalid metric format")

    transport = httpx.MockTransport(mock_handler)
    async with httpx.AsyncClient(transport=transport) as client:
        adapter = VictoriaMetricsPersistenceAdapter(client=client)
        env = _create_sample_envelope()
        with pytest.raises(VictoriaMetricsPersistenceError, match="permanent HTTP error 400"):
            await adapter.persist(env)
        assert call_count == 1


@pytest.mark.asyncio
async def test_http_permanent_403_not_retried() -> None:
    call_count = 0

    def mock_handler(request: httpx.Request) -> httpx.Response:
        nonlocal call_count
        call_count += 1
        return httpx.Response(403, text="Forbidden")

    transport = httpx.MockTransport(mock_handler)
    async with httpx.AsyncClient(transport=transport) as client:
        adapter = VictoriaMetricsPersistenceAdapter(client=client)
        env = _create_sample_envelope()
        with pytest.raises(VictoriaMetricsPersistenceError, match="permanent HTTP error 403"):
            await adapter.persist(env)
        assert call_count == 1


@pytest.mark.asyncio
async def test_http_network_timeout_retried() -> None:
    call_count = 0

    def mock_handler(request: httpx.Request) -> httpx.Response:
        nonlocal call_count
        call_count += 1
        if call_count < 3:
            raise httpx.ReadTimeout("Socket read timed out")
        return httpx.Response(200, json={"status": "ok"})

    transport = httpx.MockTransport(mock_handler)
    async with httpx.AsyncClient(transport=transport) as client:
        adapter = VictoriaMetricsPersistenceAdapter(client=client)
        env = _create_sample_envelope()
        result = await adapter.persist(env)
        assert call_count == 3
        assert result.attempts == 3


@pytest.mark.asyncio
async def test_malformed_metric_causes_zero_http_calls() -> None:
    call_count = 0

    def mock_handler(request: httpx.Request) -> httpx.Response:
        nonlocal call_count
        call_count += 1
        return httpx.Response(200)

    transport = httpx.MockTransport(mock_handler)
    async with httpx.AsyncClient(transport=transport) as client:
        adapter = VictoriaMetricsPersistenceAdapter(client=client)
        env = _create_sample_envelope(value="non-numeric-string")
        with pytest.raises(InvalidMetricError):
            await adapter.persist(env)
        assert call_count == 0


@pytest.mark.asyncio
async def test_check_victoriametrics_health_helper() -> None:
    def mock_handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/health":
            return httpx.Response(200, text="OK")
        return httpx.Response(404)

    transport = httpx.MockTransport(mock_handler)
    async with httpx.AsyncClient(transport=transport) as client:
        is_healthy = await check_victoriametrics_health(client=client)
        assert is_healthy is True


# --- 5. Consumer + Adapter Integration Unit Tests ---


def _create_mock_consumer_record(
    topic: str,
    event: TelemetryEvent,
    context: TelemetryExecutionContext,
    offset: int = 10,
) -> ConsumerRecord:
    value = serialize_event(event)
    headers = construct_kafka_headers(context, producer_version="step2.4")
    key = construct_kafka_key(event)
    return ConsumerRecord(
        topic=topic,
        partition=0,
        offset=offset,
        timestamp=1770000000000,
        timestamp_type=0,
        key=key,
        value=value,
        checksum=None,
        serialized_key_size=len(key),
        serialized_value_size=len(value),
        headers=headers,
    )


@pytest.mark.asyncio
async def test_metrics_consumer_with_adapter_success_commits_offset() -> None:
    def mock_handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={"status": "ok"})

    transport = httpx.MockTransport(mock_handler)
    async with httpx.AsyncClient(transport=transport) as client:
        adapter = VictoriaMetricsPersistenceAdapter(client=client)

        mock_aiokafka = MagicMock(spec=aiokafka.AIOKafkaConsumer)
        mock_aiokafka.start = AsyncMock()
        mock_aiokafka.stop = AsyncMock()
        mock_aiokafka.commit = AsyncMock()

        consumer = MetricsConsumer(
            handler=adapter,
            consumer_factory=lambda *args, **kwargs: mock_aiokafka,
        )
        await consumer.start()

        event = TelemetryEvent(
            schema_version="1.0",
            event_id=uuid.uuid4(),
            event_time=datetime(2026, 9, 26, 12, 0, 0, tzinfo=timezone.utc),
            tenant_id=uuid.uuid4(),
            environment="simulation",
            service="order-service",
            event_type=EventType.METRIC,
            severity=EventSeverity.INFO,
            trace_id="trace-123",
            payload={"metric_name": "http_requests", "value": 42.0},
        )
        context = TelemetryExecutionContext(
            run_id=uuid.uuid4(),
            scenario_id="scenario-test",
            scenario_version="1.0",
            reproducibility_key="rep-key",
            seed=12345,
        )
        record = _create_mock_consumer_record(KafkaTopic.METRICS.value, event, context, offset=50)

        envelope = await consumer.process_record(record)
        assert envelope.event.event_id == event.event_id
        mock_aiokafka.commit.assert_awaited_once_with({TopicPartition(KafkaTopic.METRICS.value, 0): 51})


@pytest.mark.asyncio
async def test_metrics_consumer_with_adapter_failure_does_not_commit() -> None:
    def mock_handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(500, text="Internal Server Error")

    transport = httpx.MockTransport(mock_handler)
    async with httpx.AsyncClient(transport=transport) as client:
        adapter = VictoriaMetricsPersistenceAdapter(client=client)

        mock_aiokafka = MagicMock(spec=aiokafka.AIOKafkaConsumer)
        mock_aiokafka.start = AsyncMock()
        mock_aiokafka.stop = AsyncMock()
        mock_aiokafka.commit = AsyncMock()

        consumer = MetricsConsumer(
            handler=adapter,
            consumer_factory=lambda *args, **kwargs: mock_aiokafka,
        )
        await consumer.start()

        event = TelemetryEvent(
            schema_version="1.0",
            event_id=uuid.uuid4(),
            event_time=datetime(2026, 9, 26, 12, 0, 0, tzinfo=timezone.utc),
            tenant_id=uuid.uuid4(),
            environment="simulation",
            service="order-service",
            event_type=EventType.METRIC,
            severity=EventSeverity.INFO,
            trace_id="trace-123",
            payload={"metric_name": "http_requests", "value": 42.0},
        )
        context = TelemetryExecutionContext(
            run_id=uuid.uuid4(),
            scenario_id="scenario-test",
            scenario_version="1.0",
            reproducibility_key="rep-key",
            seed=12345,
        )
        record = _create_mock_consumer_record(KafkaTopic.METRICS.value, event, context, offset=50)

        with pytest.raises(ConsumerHandlerError, match="Downstream handler failed"):
            await consumer.process_record(record)

        mock_aiokafka.commit.assert_not_called()
