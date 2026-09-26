from datetime import datetime, timedelta, timezone
import uuid

import httpx
import pytest

from app.telemetry.persistence.search import EVIDENCE_ALIAS
from app.telemetry.query.errors import (
    EvidenceQueryBackendError,
    QueryResponseValidationError,
)
from app.telemetry.query.evidence import (
    EvidenceQueryAdapter,
    build_evidence_dsl,
    parse_evidence_response,
)
from app.telemetry.query.models import EvidenceQuery
from app.telemetry.schemas import EventSeverity, EventType
from app.telemetry.transport.serialization import MAX_UINT64


# --- 1. DSL Construction Tests ---


def test_build_evidence_dsl_match_all_when_empty() -> None:
    q = EvidenceQuery()
    dsl = build_evidence_dsl(q)
    assert dsl["size"] == 100
    assert dsl["query"] == {"match_all": {}}
    assert dsl["sort"] == [
        {"event_time": {"order": "asc"}},
        {"event_id": {"order": "asc"}},
    ]


def test_build_evidence_dsl_all_filters() -> None:
    run_id = uuid.uuid4()
    event_id = uuid.uuid4()
    now = datetime(2026, 9, 26, 12, 0, 0, tzinfo=timezone.utc)

    q = EvidenceQuery(
        run_id=run_id,
        seed=MAX_UINT64,
        start_time=now - timedelta(minutes=10),
        end_time=now,
        service="order-service",
        event_type=EventType.LOG,
        severity=EventSeverity.ERROR,
        trace_id="trace-abc",
        event_id=event_id,
        marker="pool_exhausted",
        limit=50,
    )

    dsl = build_evidence_dsl(q)
    assert dsl["size"] == 50
    assert dsl["sort"] == [
        {"event_time": {"order": "asc"}},
        {"event_id": {"order": "asc"}},
    ]

    filters = dsl["query"]["bool"]["filter"]
    assert {"term": {"run_id": str(run_id)}} in filters
    assert {"term": {"seed": "18446744073709551615"}} in filters
    assert {"term": {"service": "order-service"}} in filters
    assert {"term": {"event_type": "log"}} in filters
    assert {"term": {"severity": "error"}} in filters
    assert {"term": {"trace_id": "trace-abc"}} in filters
    assert {"term": {"event_id": str(event_id)}} in filters
    assert {"term": {"payload.marker": "pool_exhausted"}} in filters
    assert {
        "range": {
            "event_time": {
                "gte": (now - timedelta(minutes=10)).isoformat(),
                "lte": now.isoformat(),
            }
        }
    } in filters


# --- 2. Response Parsing Unit Tests ---


def test_parse_evidence_response_success_and_deterministic_sorting() -> None:
    event_id1 = uuid.uuid4()
    event_id2 = uuid.uuid4()
    run_id = uuid.uuid4()
    tenant_id = uuid.uuid4()

    body = {
        "hits": {
            "hits": [
                {
                    "_source": {
                        "schema_version": "1.0",
                        "event_id": str(event_id2),
                        "event_time": "2026-09-26T12:05:00+00:00",
                        "tenant_id": str(tenant_id),
                        "environment": "simulation",
                        "service": "database",
                        "event_type": "system",
                        "severity": "critical",
                        "trace_id": None,
                        "payload": {"marker": "crash"},
                        "run_id": str(run_id),
                        "scenario_id": "sc-1",
                        "scenario_version": "1.0",
                        "reproducibility_key": "rk-1",
                        "seed": "18446744073709551615",
                        "ingested_at": "2026-09-26T12:05:01+00:00",
                    }
                },
                {
                    "_source": {
                        "schema_version": "1.0",
                        "event_id": str(event_id1),
                        "event_time": "2026-09-26T12:00:00+00:00",
                        "tenant_id": str(tenant_id),
                        "environment": "simulation",
                        "service": "payment-service",
                        "event_type": "log",
                        "severity": "warning",
                        "trace_id": "trace-123",
                        "payload": {"message": "slow query", "unknown_extra": 99},
                        "run_id": str(run_id),
                        "scenario_id": "sc-1",
                        "scenario_version": "1.0",
                        "reproducibility_key": "rk-1",
                        "seed": "42",
                        "ingested_at": "2026-09-26T12:00:01+00:00",
                    }
                },
            ]
        }
    }

    records = parse_evidence_response(body)
    assert len(records) == 2

    # Deterministic sorting: event_time ASC
    assert records[0].event_id == event_id1
    assert records[0].event_type == EventType.LOG
    assert records[0].severity == EventSeverity.WARNING
    assert records[0].seed == 42
    assert records[0].payload["unknown_extra"] == 99

    assert records[1].event_id == event_id2
    assert records[1].event_type == EventType.SYSTEM
    assert records[1].seed == MAX_UINT64
    assert records[1].trace_id is None


def test_parse_evidence_response_missing_hits() -> None:
    body = {"took": 5}
    with pytest.raises(QueryResponseValidationError, match="Missing 'hits' object"):
        parse_evidence_response(body)


def test_parse_evidence_response_malformed_fields() -> None:
    body = {
        "hits": {
            "hits": [
                {
                    "_source": {
                        "event_id": "not-a-uuid",
                        "event_time": "2026-09-26T12:00:00+00:00",
                    }
                }
            ]
        }
    }
    with pytest.raises(QueryResponseValidationError, match="Failed to parse required fields"):
        parse_evidence_response(body)


# --- 3. HTTP Client & Lifecycle Unit Tests ---


@pytest.mark.asyncio
async def test_evidence_query_adapter_search_http() -> None:
    captured_requests: list[httpx.Request] = []

    def mock_handler(request: httpx.Request) -> httpx.Response:
        captured_requests.append(request)
        assert request.url.path == f"/{EVIDENCE_ALIAS}/_search"
        return httpx.Response(
            200,
            json={"hits": {"hits": []}},
        )

    transport = httpx.MockTransport(mock_handler)
    async with httpx.AsyncClient(transport=transport) as client:
        adapter = EvidenceQueryAdapter(client=client)
        res = await adapter.query(EvidenceQuery(service="order-service"))
        assert res.count == 0
        assert len(captured_requests) == 1


@pytest.mark.asyncio
async def test_evidence_query_adapter_backend_errors() -> None:
    def mock_handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(400, text="Bad Request: illegal_argument_exception")

    transport = httpx.MockTransport(mock_handler)
    async with httpx.AsyncClient(transport=transport) as client:
        adapter = EvidenceQueryAdapter(client=client)
        with pytest.raises(EvidenceQueryBackendError, match="HTTP 400"):
            await adapter.query(EvidenceQuery())
