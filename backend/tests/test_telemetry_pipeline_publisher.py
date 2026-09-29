from datetime import datetime, timezone
import uuid

import pytest

from app.telemetry.pipeline.errors import TelemetryPipelinePublishError
from app.telemetry.pipeline.publisher import TelemetryPipelinePublisher
from app.telemetry.schemas import EventType, TelemetryEvent, EventSeverity
from app.telemetry.transport.producer import PublishResult
from app.telemetry.transport.errors import ProducerError


def make_fake_publish_result() -> PublishResult:
    return PublishResult(
        topic="aegis.telemetry.metrics",
        partition=0,
        offset=0,
        timestamp_ms=0,
        latency_ms=0.0,
        serialized_bytes=0,
    )


def make_fake_event(event_type=EventType.METRIC, service="api-gateway") -> TelemetryEvent:
    return TelemetryEvent(
        event_id=uuid.uuid4(),
        event_time=datetime.now(timezone.utc),
        tenant_id=uuid.uuid4(),
        environment="simulation",
        service=service,
        event_type=event_type,
        severity=EventSeverity.INFO,
        trace_id=str(uuid.uuid4()),
        payload={"metric_name": "request_count", "value": 1},
    )


class FakePublisher:
    def __init__(self, fail_at_index=-1):
        self.fail_at_index = fail_at_index
        self.call_count = 0

    async def publish(self, event, context) -> PublishResult:
        current = self.call_count
        self.call_count += 1
        if current == self.fail_at_index:
            raise ProducerError(f"simulated failure at index {current}")
        return make_fake_publish_result()


@pytest.mark.asyncio
async def test_detached_run_success() -> None:
    from app.simulator.runtime.runner import ScenarioRunner
    runner = ScenarioRunner()
    result = await runner.run("cpu-saturation", run_id=uuid.uuid4(),
                               run_start_time=datetime.now(timezone.utc), seed=42)
    fake = FakePublisher()
    pub = TelemetryPipelinePublisher(producer=fake)
    norm_before = result.model_dump()
    out = await pub.publish_run(result)
    assert result.model_dump() == norm_before
    assert out.published_events == len(result.telemetry_events)
    assert out.run_id == result.run_id
    assert out.scenario_id == result.scenario_id


@pytest.mark.asyncio
async def test_failure_preserves_result() -> None:
    from app.simulator.runtime.runner import ScenarioRunner
    runner = ScenarioRunner()
    result = await runner.run("cpu-saturation", run_id=uuid.uuid4(),
                               run_start_time=datetime.now(timezone.utc), seed=42)
    fake = FakePublisher(fail_at_index=1)
    pub = TelemetryPipelinePublisher(producer=fake)
    with pytest.raises(TelemetryPipelinePublishError) as exc:
        await pub.publish_run(result)
    assert exc.value.published_count == 1
    assert exc.value.failing_event_id is not None
    assert exc.value.run_id == result.run_id
    assert exc.value.__cause__ is not None


@pytest.mark.asyncio
async def test_future_event_rejected() -> None:
    from app.simulator.runtime.runner import ScenarioRunner
    runner = ScenarioRunner()
    result = await runner.run("cpu-saturation", run_id=uuid.uuid4(),
                               run_start_time=datetime.now(timezone.utc), seed=42)
    # Monkeypatch telemetry_events to inject ANOMALY without constructing a new
    # frozen ScenarioRunResult — this reaches the publisher's guard directly.
    original_events = result.telemetry_events
    bad_event = TelemetryEvent(
        event_id=uuid.uuid4(),
        event_time=datetime.now(timezone.utc),
        tenant_id=uuid.uuid4(),
        environment="simulation",
        service="test",
        event_type=EventType.ANOMALY,
        severity=EventSeverity.INFO,
        payload={"anomaly": "test"},
    )
    # Insert bad event at index 2 (between METRIC and LOG in real result)
    monkey_events = original_events[:2] + (bad_event,) + original_events[2:]

    object.__setattr__(result, "telemetry_events", monkey_events)

    fake = FakePublisher()
    pub = TelemetryPipelinePublisher(producer=fake)
    with pytest.raises(TelemetryPipelinePublishError) as exc:
        await pub.publish_run(result)
    assert "reserved" in str(exc.value)
    assert exc.value.failing_event_id == bad_event.event_id
    assert exc.value.published_count == 2  # first 2 METRIC events published
    # Verify fake never received reserved event (would raise if passed to FakePublisher)
    assert fake.call_count == 2
