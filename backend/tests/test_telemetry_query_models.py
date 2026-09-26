from datetime import datetime, timedelta, timezone
import uuid

import pytest

from app.telemetry.query.errors import (
    InvalidEvidenceQueryError,
    InvalidMetricQueryError,
)
from app.telemetry.query.models import (
    EvidenceQuery,
    MetricQuery,
)
from app.telemetry.schemas import EventType
from app.telemetry.transport.serialization import MAX_UINT64


# --- 1. MetricQuery Validation Tests ---


def test_metric_query_valid() -> None:
    now = datetime.now(timezone.utc)
    q = MetricQuery(
        metric="http_requests_total",
        service="order-service",
        tenant_id=uuid.uuid4(),
        environment="simulation",
        run_id=uuid.uuid4(),
        seed=42,
        event_id=uuid.uuid4(),
        start_time=now - timedelta(minutes=5),
        end_time=now,
        step="10s",
    )
    assert q.metric == "http_requests_total"
    assert q.seed == 42


def test_metric_query_blank_metric_rejected() -> None:
    with pytest.raises(InvalidMetricQueryError, match="must be a non-empty string"):
        MetricQuery(metric="   ")


@pytest.mark.parametrize("invalid_name", ["http-requests", "http.requests", "metric@123", "123metric", "metric$"])
def test_metric_query_invalid_name_characters_rejected(invalid_name: str) -> None:
    with pytest.raises(InvalidMetricQueryError, match="contains invalid characters"):
        MetricQuery(metric=invalid_name)


def test_metric_query_seed_bounds() -> None:
    # 0 accepted
    q0 = MetricQuery(metric="cpu", seed=0)
    assert q0.seed == 0

    # MAX_UINT64 accepted
    q_max = MetricQuery(metric="cpu", seed=MAX_UINT64)
    assert q_max.seed == MAX_UINT64

    # Negative rejected
    with pytest.raises(InvalidMetricQueryError, match="must be in range"):
        MetricQuery(metric="cpu", seed=-1)

    # Overflow rejected
    with pytest.raises(InvalidMetricQueryError, match="must be in range"):
        MetricQuery(metric="cpu", seed=MAX_UINT64 + 1)


def test_metric_query_time_validation() -> None:
    now = datetime.now(timezone.utc)

    # Naive start_time rejected by Pydantic AwareDatetime
    with pytest.raises(Exception):
        MetricQuery(metric="cpu", start_time=datetime(2026, 9, 26, 12, 0, 0))  # type: ignore[arg-type]

    # Naive end_time rejected by Pydantic AwareDatetime
    with pytest.raises(Exception):
        MetricQuery(metric="cpu", end_time=datetime(2026, 9, 26, 12, 0, 0))  # type: ignore[arg-type]

    # start_time > end_time rejected
    with pytest.raises(InvalidMetricQueryError, match="cannot be greater than end_time"):
        MetricQuery(metric="cpu", start_time=now, end_time=now - timedelta(seconds=10))


def test_metric_query_step_validation() -> None:
    # Numeric positive accepted
    q1 = MetricQuery(metric="cpu", step=10)
    assert q1.step == 10

    q2 = MetricQuery(metric="cpu", step=0.5)
    assert q2.step == 0.5

    q3 = MetricQuery(metric="cpu", step=timedelta(seconds=15))
    assert q3.step == timedelta(seconds=15)

    # Non-positive rejected
    with pytest.raises(InvalidMetricQueryError, match="step must be positive"):
        MetricQuery(metric="cpu", step=0)

    with pytest.raises(InvalidMetricQueryError, match="step must be positive"):
        MetricQuery(metric="cpu", step=-5)

    with pytest.raises(InvalidMetricQueryError, match="step timedelta must be positive"):
        MetricQuery(metric="cpu", step=timedelta(seconds=0))

    with pytest.raises(InvalidMetricQueryError, match="step string cannot be empty"):
        MetricQuery(metric="cpu", step="  ")


def test_metric_query_immutability() -> None:
    q = MetricQuery(metric="cpu")
    with pytest.raises(Exception):
        q.metric = "memory"  # type: ignore[misc]


# --- 2. EvidenceQuery Validation Tests ---


def test_evidence_query_valid_and_defaults() -> None:
    q = EvidenceQuery()
    assert q.limit == 100
    assert q.run_id is None
    assert q.event_type is None


def test_evidence_query_seed_bounds() -> None:
    q0 = EvidenceQuery(seed=0)
    assert q0.seed == 0

    q_max = EvidenceQuery(seed=MAX_UINT64)
    assert q_max.seed == MAX_UINT64

    with pytest.raises(InvalidEvidenceQueryError, match="must be in range"):
        EvidenceQuery(seed=-1)

    with pytest.raises(InvalidEvidenceQueryError, match="must be in range"):
        EvidenceQuery(seed=MAX_UINT64 + 1)


def test_evidence_query_time_validation() -> None:
    now = datetime.now(timezone.utc)

    # Reversed time range
    with pytest.raises(InvalidEvidenceQueryError, match="cannot be greater than end_time"):
        EvidenceQuery(start_time=now, end_time=now - timedelta(seconds=1))


def test_evidence_query_event_types() -> None:
    # LOG and SYSTEM accepted
    q_log = EvidenceQuery(event_type=EventType.LOG)
    assert q_log.event_type == EventType.LOG

    q_sys = EvidenceQuery(event_type=EventType.SYSTEM)
    assert q_sys.event_type == EventType.SYSTEM

    # METRIC, ANOMALY, INCIDENT, AGENT rejected
    for forbidden in (EventType.METRIC, EventType.ANOMALY, EventType.INCIDENT, EventType.AGENT):
        with pytest.raises(InvalidEvidenceQueryError, match="not allowed for evidence queries"):
            EvidenceQuery(event_type=forbidden)


def test_evidence_query_string_filters_non_empty() -> None:
    with pytest.raises(InvalidEvidenceQueryError, match="cannot be empty string"):
        EvidenceQuery(service="   ")

    with pytest.raises(InvalidEvidenceQueryError, match="cannot be empty string"):
        EvidenceQuery(trace_id="   ")

    with pytest.raises(InvalidEvidenceQueryError, match="cannot be empty string"):
        EvidenceQuery(marker="   ")


def test_evidence_query_limit_bounds() -> None:
    # Valid bounds
    q1 = EvidenceQuery(limit=1)
    assert q1.limit == 1

    q1000 = EvidenceQuery(limit=1000)
    assert q1000.limit == 1000

    # Invalid bounds
    with pytest.raises(Exception):
        EvidenceQuery(limit=0)

    with pytest.raises(Exception):
        EvidenceQuery(limit=1001)


def test_evidence_query_immutability() -> None:
    q = EvidenceQuery(service="auth-service")
    with pytest.raises(Exception):
        q.limit = 50  # type: ignore[misc]
