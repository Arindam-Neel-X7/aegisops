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
    EvidenceIndexBootstrapError,
    InvalidEvidenceEventError,
    OpenSearchPersistenceError,
    OpenSearchRetryExhaustedError,
)
from app.telemetry.persistence.search import (
    CONCRETE_INDEX_V1,
    EVIDENCE_ALIAS,
    EVIDENCE_INDEX_TEMPLATE,
    TEMPLATE_NAME,
    OpenSearchPersistenceAdapter,
    build_evidence_document,
    check_opensearch_health,
)
from app.telemetry.schemas import EventSeverity, EventType, TelemetryEvent
from app.telemetry.topics import KafkaTopic
from app.telemetry.transport.consumer import (
    EvidenceConsumer,
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


def _create_sample_evidence_envelope(
    event_type: EventType = EventType.LOG,
    service: str = "payment-service",
    event_id: uuid.UUID | None = None,
    run_id: uuid.UUID | None = None,
    tenant_id: uuid.UUID | None = None,
    event_time: datetime | None = None,
    trace_id: str | None = "trace-log-123",
    seed: int = 12345,
    payload: dict[str, Any] | None = None,
    topic: str | None = None,
    offset: int = 10,
) -> TelemetryEnvelope:
    chosen_topic = topic or (
        KafkaTopic.LOGS.value if event_type == EventType.LOG else KafkaTopic.SYSTEM_EVENTS.value
    )
    event = TelemetryEvent(
        schema_version="1.0",
        event_id=event_id or uuid.uuid4(),
        event_time=event_time or datetime(2026, 9, 26, 12, 0, 0, tzinfo=timezone.utc),
        tenant_id=tenant_id or uuid.uuid4(),
        environment="simulation",
        service=service,
        event_type=event_type,
        severity=EventSeverity.WARNING if event_type == EventType.LOG else EventSeverity.CRITICAL,
        trace_id=trace_id,
        payload=payload if payload is not None else {"message": "Payment timeout", "status_code": 504},
    )

    context = TelemetryExecutionContext(
        run_id=run_id or uuid.uuid4(),
        scenario_id="scenario-test",
        scenario_version="1.0",
        reproducibility_key="rep-key-log",
        seed=seed,
    )

    return TelemetryEnvelope(
        event=event,
        context=context,
        kafka_timestamp_ms=1770000000000,
        topic=chosen_topic,
        partition=0,
        offset=offset,
        key=b"test-key",
        producer_version="step2.4",
    )


# --- 1. Document Building Unit Tests ---


def test_build_evidence_document_log_accepted() -> None:
    env = _create_sample_evidence_envelope(event_type=EventType.LOG)
    doc_id, doc = build_evidence_document(env)
    assert doc_id == f"{env.context.run_id}:{env.event.event_id}"
    assert doc["event_type"] == "log"
    assert doc["service"] == "payment-service"
    assert doc["payload"] == {"message": "Payment timeout", "status_code": 504}


def test_build_evidence_document_system_accepted() -> None:
    env = _create_sample_evidence_envelope(
        event_type=EventType.SYSTEM,
        payload={"marker": "fault_injected", "fault_type": "latency", "latency_ms": 150.0},
    )
    doc_id, doc = build_evidence_document(env)
    assert doc_id == f"{env.context.run_id}:{env.event.event_id}"
    assert doc["event_type"] == "system"
    assert doc["payload"]["marker"] == "fault_injected"


@pytest.mark.parametrize(
    "forbidden_type",
    [
        EventType.METRIC,
        EventType.ANOMALY,
        EventType.INCIDENT,
        EventType.AGENT,
    ],
)
def test_build_evidence_document_rejected_types(forbidden_type: EventType) -> None:
    env = _create_sample_evidence_envelope(event_type=forbidden_type, topic=KafkaTopic.METRICS.value)
    with pytest.raises(InvalidEvidenceEventError, match="not allowed for evidence persistence"):
        build_evidence_document(env)


def test_build_evidence_document_preserves_all_fields_and_metadata() -> None:
    event_id = uuid.uuid4()
    run_id = uuid.uuid4()
    tenant_id = uuid.uuid4()
    event_time = datetime(2026, 9, 26, 15, 45, 0, tzinfo=timezone.utc)
    custom_payload = {
        "message": "Custom message",
        "nested_extra": {"key": "val"},
        "status_code": 200,
    }

    env = _create_sample_evidence_envelope(
        event_type=EventType.LOG,
        event_id=event_id,
        run_id=run_id,
        tenant_id=tenant_id,
        event_time=event_time,
        trace_id=None,
        seed=MAX_UINT64,
        payload=custom_payload,
    )

    doc_id, doc = build_evidence_document(env)

    assert doc_id == f"{run_id}:{event_id}"
    assert doc["schema_version"] == "1.0"
    assert doc["event_id"] == str(event_id)
    assert doc["event_time"] == event_time.isoformat()
    assert doc["tenant_id"] == str(tenant_id)
    assert doc["environment"] == "simulation"
    assert doc["service"] == "payment-service"
    assert doc["event_type"] == "log"
    assert doc["severity"] == "warning"
    assert doc["trace_id"] is None
    assert doc["run_id"] == str(run_id)
    assert doc["scenario_id"] == "scenario-test"
    assert doc["scenario_version"] == "1.0"
    assert doc["reproducibility_key"] == "rep-key-log"
    assert doc["seed"] == "18446744073709551615"
    assert doc["payload"] == custom_payload

    # Validate ingested_at is ISO-8601 UTC
    ingested_at = datetime.fromisoformat(doc["ingested_at"])
    assert ingested_at.tzinfo is not None


def test_build_evidence_document_non_utc_event_time_preserves_instant() -> None:
    tz_offset = timezone(timedelta(hours=5, minutes=30))
    dt_offset = datetime(2026, 9, 26, 21, 0, 0, tzinfo=tz_offset)
    env = _create_sample_evidence_envelope(event_time=dt_offset)

    doc_id, doc = build_evidence_document(env)
    parsed_dt = datetime.fromisoformat(doc["event_time"])
    assert parsed_dt.timestamp() == dt_offset.timestamp()


# --- 2. Document ID Unit Tests ---


def test_document_id_determinism_and_isolation() -> None:
    event_id1 = uuid.uuid4()
    event_id2 = uuid.uuid4()
    run_id1 = uuid.uuid4()
    run_id2 = uuid.uuid4()

    env1 = _create_sample_evidence_envelope(event_id=event_id1, run_id=run_id1)
    env2 = _create_sample_evidence_envelope(event_id=event_id1, run_id=run_id1)
    env3 = _create_sample_evidence_envelope(event_id=event_id1, run_id=run_id2)
    env4 = _create_sample_evidence_envelope(event_id=event_id2, run_id=run_id1)

    id1, _ = build_evidence_document(env1)
    id2, _ = build_evidence_document(env2)
    id3, _ = build_evidence_document(env3)
    id4, _ = build_evidence_document(env4)

    assert id1 == f"{run_id1}:{event_id1}"
    assert id1 == id2  # same run, same event -> identical ID
    assert id1 != id3  # different run -> different ID
    assert id1 != id4  # different event -> different ID


# --- 3. Index Template / Mapping Unit Tests ---


def test_index_template_contract() -> None:
    template = EVIDENCE_INDEX_TEMPLATE
    assert template["index_patterns"] == ["aegis-evidence*"]

    props = template["template"]["mappings"]["properties"]
    assert props["schema_version"]["type"] == "keyword"
    assert props["event_id"]["type"] == "keyword"
    assert props["event_time"]["type"] == "date"
    assert props["tenant_id"]["type"] == "keyword"
    assert props["environment"]["type"] == "keyword"
    assert props["service"]["type"] == "keyword"
    assert props["event_type"]["type"] == "keyword"
    assert props["severity"]["type"] == "keyword"
    assert props["trace_id"]["type"] == "keyword"
    assert props["run_id"]["type"] == "keyword"
    assert props["scenario_id"]["type"] == "keyword"
    assert props["scenario_version"]["type"] == "keyword"
    assert props["reproducibility_key"]["type"] == "keyword"
    assert props["seed"]["type"] == "keyword"
    assert props["ingested_at"]["type"] == "date"

    payload_prop = props["payload"]
    assert payload_prop["type"] == "object"
    assert payload_prop["dynamic"] is False

    known_payload = payload_prop["properties"]
    assert known_payload["marker"]["type"] == "keyword"
    assert known_payload["message"]["type"] == "text"
    assert known_payload["message"]["fields"]["keyword"]["type"] == "keyword"
    assert known_payload["status_code"]["type"] == "integer"
    assert known_payload["outcome"]["type"] == "keyword"
    assert known_payload["latency_ms"]["type"] == "float"
    assert known_payload["request_id"]["type"] == "keyword"
    assert known_payload["fault_id"]["type"] == "keyword"
    assert known_payload["fault_type"]["type"] == "keyword"
    assert known_payload["target_service_id"]["type"] == "keyword"
    assert known_payload["deployment_version"]["type"] == "keyword"
    assert known_payload["route"]["type"] == "keyword"
    assert known_payload["error"]["type"] == "keyword"

    assert "research_unknown_field" not in known_payload


# --- 4. HTTP Success & Bootstrap Unit Tests ---


@pytest.mark.asyncio
async def test_opensearch_bootstrap_idempotent() -> None:
    routes_hit: list[tuple[str, str]] = []

    def mock_handler(request: httpx.Request) -> httpx.Response:
        routes_hit.append((request.method, request.url.path))
        if request.url.path == f"/_index_template/{TEMPLATE_NAME}":
            return httpx.Response(200, json={"acknowledged": True})
        if request.url.path == f"/{CONCRETE_INDEX_V1}":
            if request.method == "HEAD":
                return httpx.Response(404)
            if request.method == "PUT":
                return httpx.Response(200, json={"acknowledged": True})
        if request.url.path == f"/_alias/{EVIDENCE_ALIAS}":
            return httpx.Response(404)
        if request.url.path == "/_aliases":
            return httpx.Response(200, json={"acknowledged": True})
        return httpx.Response(404)

    transport = httpx.MockTransport(mock_handler)
    async with httpx.AsyncClient(transport=transport) as client:
        adapter = OpenSearchPersistenceAdapter(client=client)
        await adapter.bootstrap()

        # Check expected bootstrap sequence
        assert ("PUT", f"/_index_template/{TEMPLATE_NAME}") in routes_hit
        assert ("PUT", f"/{CONCRETE_INDEX_V1}") in routes_hit
        assert ("POST", "/_aliases") in routes_hit


@pytest.mark.asyncio
async def test_opensearch_bootstrap_alias_mismatch_raises_error() -> None:
    def mock_handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == f"/_index_template/{TEMPLATE_NAME}":
            return httpx.Response(200, json={"acknowledged": True})
        if request.url.path == f"/{CONCRETE_INDEX_V1}":
            return httpx.Response(200)
        if request.url.path == f"/_alias/{EVIDENCE_ALIAS}":
            # Points to unexpected other index
            return httpx.Response(200, json={"other-index-v99": {"aliases": {EVIDENCE_ALIAS: {}}}})
        return httpx.Response(404)

    transport = httpx.MockTransport(mock_handler)
    async with httpx.AsyncClient(transport=transport) as client:
        adapter = OpenSearchPersistenceAdapter(client=client)
        with pytest.raises(EvidenceIndexBootstrapError, match="points to unexpected target indices"):
            await adapter.bootstrap()


@pytest.mark.asyncio
async def test_opensearch_persist_201_created_success() -> None:
    captured_requests: list[httpx.Request] = []

    def mock_handler(request: httpx.Request) -> httpx.Response:
        captured_requests.append(request)
        return httpx.Response(201, json={"result": "created", "_id": request.url.path.split("/")[-1]})

    transport = httpx.MockTransport(mock_handler)
    async with httpx.AsyncClient(transport=transport) as client:
        adapter = OpenSearchPersistenceAdapter(client=client)
        env = _create_sample_evidence_envelope(event_type=EventType.LOG)

        res = await adapter.persist(env)

        assert len(captured_requests) == 1
        req = captured_requests[0]
        assert req.method == "PUT"
        expected_path = f"/{CONCRETE_INDEX_V1}/_doc/{env.context.run_id}:{env.event.event_id}"
        assert req.url.path == expected_path
        assert res.status_code == 201
        assert res.result == "created"
        assert res.document_id == f"{env.context.run_id}:{env.event.event_id}"
        assert res.attempts == 1


@pytest.mark.asyncio
async def test_opensearch_persist_200_updated_success() -> None:
    def mock_handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={"result": "updated"})

    transport = httpx.MockTransport(mock_handler)
    async with httpx.AsyncClient(transport=transport) as client:
        adapter = OpenSearchPersistenceAdapter(client=client)
        env = _create_sample_evidence_envelope(event_type=EventType.SYSTEM)
        res = await adapter.persist(env)
        assert res.status_code == 200
        assert res.result == "updated"


# --- 5. Retry / Failure Unit Tests ---


@pytest.mark.asyncio
async def test_opensearch_transient_retry_success_on_attempt_2() -> None:
    call_count = 0

    def mock_handler(request: httpx.Request) -> httpx.Response:
        nonlocal call_count
        call_count += 1
        if call_count == 1:
            return httpx.Response(503, text="Service Unavailable", headers={"Retry-After": "0.01"})
        return httpx.Response(201, json={"result": "created"})

    transport = httpx.MockTransport(mock_handler)
    async with httpx.AsyncClient(transport=transport) as client:
        adapter = OpenSearchPersistenceAdapter(client=client)
        env = _create_sample_evidence_envelope()
        res = await adapter.persist(env)
        assert call_count == 2
        assert res.attempts == 2
        assert res.status_code == 201


@pytest.mark.asyncio
async def test_opensearch_transient_retry_exhaustion_after_3_attempts() -> None:
    call_count = 0

    def mock_handler(request: httpx.Request) -> httpx.Response:
        nonlocal call_count
        call_count += 1
        return httpx.Response(500, text="Internal Cluster Error")

    transport = httpx.MockTransport(mock_handler)
    async with httpx.AsyncClient(transport=transport) as client:
        adapter = OpenSearchPersistenceAdapter(client=client)
        env = _create_sample_evidence_envelope()
        with pytest.raises(OpenSearchRetryExhaustedError, match="exhausted 3 attempts"):
            await adapter.persist(env)
        assert call_count == 3


@pytest.mark.asyncio
async def test_opensearch_permanent_400_not_retried() -> None:
    call_count = 0

    def mock_handler(request: httpx.Request) -> httpx.Response:
        nonlocal call_count
        call_count += 1
        return httpx.Response(400, text="Bad Request: mapping error")

    transport = httpx.MockTransport(mock_handler)
    async with httpx.AsyncClient(transport=transport) as client:
        adapter = OpenSearchPersistenceAdapter(client=client)
        env = _create_sample_evidence_envelope()
        with pytest.raises(OpenSearchPersistenceError, match="permanent HTTP error 400"):
            await adapter.persist(env)
        assert call_count == 1


@pytest.mark.asyncio
async def test_opensearch_invalid_event_type_causes_zero_http_calls() -> None:
    call_count = 0

    def mock_handler(request: httpx.Request) -> httpx.Response:
        nonlocal call_count
        call_count += 1
        return httpx.Response(201)

    transport = httpx.MockTransport(mock_handler)
    async with httpx.AsyncClient(transport=transport) as client:
        adapter = OpenSearchPersistenceAdapter(client=client)
        env = _create_sample_evidence_envelope(event_type=EventType.METRIC)
        with pytest.raises(InvalidEvidenceEventError):
            await adapter.persist(env)
        assert call_count == 0


@pytest.mark.asyncio
async def test_check_opensearch_health_helper() -> None:
    def mock_handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/_cluster/health":
            return httpx.Response(200, json={"status": "green"})
        return httpx.Response(404)

    transport = httpx.MockTransport(mock_handler)
    async with httpx.AsyncClient(transport=transport) as client:
        is_healthy, status = await check_opensearch_health(client=client)
        assert is_healthy is True
        assert status == "green"


# --- 6. EvidenceConsumer + Adapter Integration Unit Tests ---


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
async def test_evidence_consumer_with_adapter_success_commits_offset() -> None:
    def mock_handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(201, json={"result": "created"})

    transport = httpx.MockTransport(mock_handler)
    async with httpx.AsyncClient(transport=transport) as client:
        adapter = OpenSearchPersistenceAdapter(client=client)

        mock_aiokafka = MagicMock(spec=aiokafka.AIOKafkaConsumer)
        mock_aiokafka.start = AsyncMock()
        mock_aiokafka.stop = AsyncMock()
        mock_aiokafka.commit = AsyncMock()

        consumer = EvidenceConsumer(
            handler=adapter,
            consumer_factory=lambda *args, **kwargs: mock_aiokafka,
        )
        await consumer.start()

        # LOG event on aegis.telemetry.logs
        log_event = TelemetryEvent(
            schema_version="1.0",
            event_id=uuid.uuid4(),
            event_time=datetime(2026, 9, 26, 12, 0, 0, tzinfo=timezone.utc),
            tenant_id=uuid.uuid4(),
            environment="simulation",
            service="payment-service",
            event_type=EventType.LOG,
            severity=EventSeverity.WARNING,
            trace_id="trace-123",
            payload={"message": "Payment processing slow", "latency_ms": 350.0},
        )
        context = TelemetryExecutionContext(
            run_id=uuid.uuid4(),
            scenario_id="scenario-test",
            scenario_version="1.0",
            reproducibility_key="rep-key",
            seed=12345,
        )
        record = _create_mock_consumer_record(KafkaTopic.LOGS.value, log_event, context, offset=30)

        envelope = await consumer.process_record(record)
        assert envelope.event.event_id == log_event.event_id
        mock_aiokafka.commit.assert_awaited_once_with({TopicPartition(KafkaTopic.LOGS.value, 0): 31})


@pytest.mark.asyncio
async def test_evidence_consumer_with_adapter_failure_does_not_commit() -> None:
    def mock_handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(500, text="Internal Error")

    transport = httpx.MockTransport(mock_handler)
    async with httpx.AsyncClient(transport=transport) as client:
        adapter = OpenSearchPersistenceAdapter(client=client)

        mock_aiokafka = MagicMock(spec=aiokafka.AIOKafkaConsumer)
        mock_aiokafka.start = AsyncMock()
        mock_aiokafka.stop = AsyncMock()
        mock_aiokafka.commit = AsyncMock()

        consumer = EvidenceConsumer(
            handler=adapter,
            consumer_factory=lambda *args, **kwargs: mock_aiokafka,
        )
        await consumer.start()

        sys_event = TelemetryEvent(
            schema_version="1.0",
            event_id=uuid.uuid4(),
            event_time=datetime(2026, 9, 26, 12, 0, 0, tzinfo=timezone.utc),
            tenant_id=uuid.uuid4(),
            environment="simulation",
            service="database",
            event_type=EventType.SYSTEM,
            severity=EventSeverity.CRITICAL,
            trace_id="trace-sys",
            payload={"marker": "pool_exhausted"},
        )
        context = TelemetryExecutionContext(
            run_id=uuid.uuid4(),
            scenario_id="scenario-test",
            scenario_version="1.0",
            reproducibility_key="rep-key",
            seed=12345,
        )
        record = _create_mock_consumer_record(KafkaTopic.SYSTEM_EVENTS.value, sys_event, context, offset=40)

        with pytest.raises(ConsumerHandlerError, match="Downstream handler failed"):
            await consumer.process_record(record)

        mock_aiokafka.commit.assert_not_called()
