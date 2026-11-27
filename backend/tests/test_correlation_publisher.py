"""P1 transport-only tests; broker responses are simulated, no live services."""

import asyncio
from dataclasses import fields
import hashlib
import math
from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock

from aiokafka import errors
import pytest
import test_correlation_publication as c1

from app.correlation.publication import (
    DEFAULT_PUBLICATION_PRODUCER_VERSION,
    IncidentPublicationRecord,
    build_incident_publication,
)
from app.correlation.publisher import (
    IncidentPublishAmbiguousError,
    IncidentPublishContractError,
    IncidentPublisher,
    IncidentPublishOutcome,
    IncidentPublishRetriableError,
    IncidentPublishStatus as Status,
    IncidentPublishUnavailableError,
)
from app.telemetry.transport.errors import ProducerError, ProducerNotStartedError
from app.telemetry.transport.producer import KafkaTelemetryProducer, PublishResult

fix01_bundle = c1.fix01_bundle


@pytest.fixture(scope="module")
def publication(fix01_bundle: dict[str, Any]) -> IncidentPublicationRecord:
    return make_record(fix01_bundle)


def make_record(bundle, revision="rev1", version=DEFAULT_PUBLICATION_PRODUCER_VERSION):
    return build_incident_publication(
        incident=bundle[revision],
        snapshot=bundle["snapshot"],
        expected_topology=bundle["topology_ref"],
        blast_radius_policy=bundle["policy"],
        correlation_config=bundle["config"],
        execution_context=bundle["context"],
        producer_version=version,
    )


def transport(publication):
    producer = KafkaTelemetryProducer(producer_version=publication.producer_version)
    broker = SimpleNamespace(
        send_and_wait=AsyncMock(
            return_value=SimpleNamespace(
                topic="aegis.incidents", partition=2, offset=37, timestamp=1795737600000
            )
        ),
        stop=AsyncMock(),
        flush=AsyncMock(),
    )
    producer._producer = broker
    producer._started = True
    return producer, broker


def ack(publication):
    return PublishResult(
        topic="aegis.incidents",
        partition=2,
        offset=37,
        timestamp_ms=None,
        latency_ms=0.0,
        serialized_bytes=len(publication.kafka_value),
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "name,revision,version",
    [
        ("FIX-01-REV1", "rev1", DEFAULT_PUBLICATION_PRODUCER_VERSION),
        ("FIX-01-REV1-NO-PRODUCER-VERSION", "rev1", None),
        ("FIX-01-REV2", "rev2", DEFAULT_PUBLICATION_PRODUCER_VERSION),
    ],
)
async def test_actual_transport_exact_wire_and_ack(
    fix01_bundle, name, revision, version
):
    record = make_record(fix01_bundle, revision, version)
    c1._assert_matches_oracle(record, c1.WIRE_ORACLES[name])
    before = (
        record.kafka_value,
        record.payload.model_dump(),
        record.execution_context.model_dump(),
    )
    producer, broker = transport(record)
    publisher = IncidentPublisher(producer)
    outcome = await publisher.publish(record)
    assert outcome.status is Status.ACKNOWLEDGED and outcome.error is None
    assert outcome.result.topic == "aegis.incidents"
    assert outcome.result.partition == 2 and outcome.result.offset == 37
    assert outcome.result.timestamp_ms == 1795737600000
    assert outcome.result.serialized_bytes == len(record.kafka_value)
    assert math.isfinite(outcome.result.latency_ms) and outcome.result.latency_ms >= 0
    sent = broker.send_and_wait.call_args.kwargs
    assert set(sent) == {"topic", "key", "value", "headers", "timestamp_ms"}
    assert sent["topic"] == record.topic.value
    assert sent["key"] == record.kafka_key
    assert sent["value"] == record.kafka_value
    assert tuple(sent["headers"]) == record.kafka_headers
    assert type(sent["timestamp_ms"]) is int  # noqa: E721
    assert (
        hashlib.sha256(sent["value"]).hexdigest()
        == hashlib.sha256(before[0]).hexdigest()
    )
    broker.send_and_wait.assert_awaited_once()
    broker.stop.assert_not_awaited()
    broker.flush.assert_not_awaited()
    assert producer.producer_version == version
    assert before == (
        record.kafka_value,
        record.payload.model_dump(),
        record.execution_context.model_dump(),
    )
    assert publisher.pending_attempt_count == 0


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "field,value",
    [
        ("kafka_value", b"{}"),
        ("kafka_key", b"wrong"),
        ("kafka_headers", ()),
        ("event_id", None),
    ],
)
async def test_forged_record_rejected_before_send(publication, field, value):
    forged = object.__new__(IncidentPublicationRecord)
    for item in fields(publication):
        object.__setattr__(forged, item.name, getattr(publication, item.name))
    object.__setattr__(forged, field, value)
    producer, broker = transport(publication)
    outcome = await IncidentPublisher(producer).publish(forged)
    assert outcome.status is Status.FAILED_CONTRACT
    assert isinstance(outcome.error, IncidentPublishContractError)
    assert outcome.error.__cause__ is not None
    broker.send_and_wait.assert_not_called()


@pytest.mark.asyncio
@pytest.mark.parametrize("value", [None, {}, "invalid"])
async def test_wrong_input_type_is_safe_outcome(publication, value):
    producer, broker = transport(publication)
    outcome = await IncidentPublisher(producer).publish(value)
    assert outcome.status is Status.FAILED_CONTRACT
    broker.send_and_wait.assert_not_called()


@pytest.mark.asyncio
@pytest.mark.parametrize("version", [None, "wrong-version"])
async def test_producer_version_mismatch_never_changes_config(publication, version):
    producer, broker = transport(publication)
    producer.producer_version = version
    outcome = await IncidentPublisher(producer).publish(publication)
    assert outcome.status is Status.FAILED_CONTRACT
    assert producer.producer_version == version
    broker.send_and_wait.assert_not_called()


@pytest.mark.asyncio
@pytest.mark.parametrize("state", ["not_started", "closed", "missing_native"])
async def test_unavailable_is_not_sent(publication, state):
    producer, broker = transport(publication)
    if state == "not_started":
        producer._started = False
    elif state == "closed":
        producer._closed = True
    else:
        producer._producer = None
    outcome = await IncidentPublisher(producer).publish(publication)
    assert outcome.status is Status.FAILED_UNAVAILABLE
    assert isinstance(outcome.error, IncidentPublishUnavailableError)
    assert isinstance(outcome.error.__cause__, ProducerNotStartedError)
    broker.send_and_wait.assert_not_called()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "error_type",
    [
        errors.LeaderNotAvailableError,
        errors.NotEnoughReplicasError,
        errors.NotLeaderForPartitionError,
        errors.UnknownTopicOrPartitionError,
    ],
)
async def test_explicit_retryable_rejection_one_attempt_safe_cause(
    publication, error_type
):
    producer, broker = transport(publication)
    cause = error_type("password=secret broker.internal:9092 raw-driver-diagnostics")
    broker.send_and_wait.side_effect = cause
    outcome = await IncidentPublisher(producer).publish(publication)
    assert outcome.status is Status.FAILED_RETRIABLE
    assert isinstance(outcome.error, IncidentPublishRetriableError)
    assert isinstance(outcome.error.__cause__, ProducerError)
    assert outcome.error.__cause__.__cause__ is cause
    assert "secret" not in str(outcome.error) + repr(outcome)
    assert "broker.internal" not in str(outcome.error)
    broker.send_and_wait.assert_awaited_once()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "error_type",
    [
        errors.NotEnoughReplicasAfterAppendError,
        errors.RequestTimedOutError,
        errors.KafkaTimeoutError,
        errors.KafkaConnectionError,
        errors.TopicAuthorizationFailedError,
        ProducerError,
        RuntimeError,
    ],
)
async def test_unconfirmed_errors_remain_ambiguous(publication, error_type):
    producer, broker = transport(publication)
    cause = error_type("postgresql://user:secret@host/db SQL parameters")
    broker.send_and_wait.side_effect = cause
    outcome = await IncidentPublisher(producer).publish(publication)
    assert outcome.status is Status.FAILED_AMBIGUOUS
    assert isinstance(outcome.error, IncidentPublishAmbiguousError)
    assert outcome.error.__cause__.__cause__ is cause
    assert "secret" not in str(outcome.error) + repr(outcome)
    broker.send_and_wait.assert_awaited_once()


@pytest.mark.asyncio
async def test_timeout_is_bounded_without_retry(publication):
    producer, broker = transport(publication)
    cancelled = asyncio.Event()

    async def block(**kwargs):
        try:
            await asyncio.Event().wait()
        finally:
            cancelled.set()

    broker.send_and_wait.side_effect = block
    publisher = IncidentPublisher(producer, publish_timeout_seconds=0.1)
    outcome = await asyncio.wait_for(publisher.publish(publication), 2)
    assert outcome.status is Status.FAILED_AMBIGUOUS
    assert isinstance(outcome.error.__cause__, TimeoutError)
    await asyncio.wait_for(cancelled.wait(), 2)
    broker.send_and_wait.assert_awaited_once()


@pytest.mark.asyncio
@pytest.mark.parametrize("late_failure", [False, True])
async def test_cancel_resistant_send_cannot_turn_timeout_into_ack(
    publication, late_failure
):
    producer, broker = transport(publication)
    release = asyncio.Event()
    cancelled = asyncio.Event()
    settled = asyncio.Event()
    loop = asyncio.get_running_loop()
    unhandled = []
    previous = loop.get_exception_handler()
    loop.set_exception_handler(lambda _, context: unhandled.append(context))

    async def late(**kwargs):
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            cancelled.set()
            await release.wait()
        finally:
            settled.set()
        if late_failure:
            raise RuntimeError("late secret driver failure")
        return SimpleNamespace(
            topic="aegis.incidents", partition=0, offset=38, timestamp=None
        )

    broker.send_and_wait.side_effect = late
    publisher = IncidentPublisher(producer, publish_timeout_seconds=0.1)
    try:
        outcome = await asyncio.wait_for(publisher.publish(publication), 2)
        await asyncio.wait_for(cancelled.wait(), 2)
        assert outcome.status is Status.FAILED_AMBIGUOUS
        assert publisher.pending_attempt_count == 1
        release.set()
        await asyncio.wait_for(settled.wait(), 2)
        await asyncio.sleep(0)
        await asyncio.sleep(0)
        assert publisher.pending_attempt_count == 0
        assert not unhandled
        assert outcome.status is Status.FAILED_AMBIGUOUS
        broker.send_and_wait.assert_awaited_once()
    finally:
        release.set()
        loop.set_exception_handler(previous)


@pytest.mark.asyncio
async def test_caller_cancellation_propagates(publication):
    producer, broker = transport(publication)
    entered = asyncio.Event()
    stopped = asyncio.Event()

    async def block(**kwargs):
        entered.set()
        try:
            await asyncio.Event().wait()
        finally:
            stopped.set()

    broker.send_and_wait.side_effect = block
    publisher = IncidentPublisher(producer)
    attempt = asyncio.create_task(publisher.publish(publication))
    await asyncio.wait_for(entered.wait(), 2)
    attempt.cancel()
    with pytest.raises(asyncio.CancelledError):
        await attempt
    await asyncio.wait_for(stopped.wait(), 2)
    broker.send_and_wait.assert_awaited_once()


@pytest.mark.asyncio
async def test_child_cancellation_propagates(publication):
    producer, broker = transport(publication)
    broker.send_and_wait.side_effect = asyncio.CancelledError()
    with pytest.raises(asyncio.CancelledError):
        await IncidentPublisher(producer).publish(publication)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "field,value",
    [
        ("topic", "aegis.anomalies"),
        ("partition", True),
        ("partition", -1),
        ("partition", 2**31),
        ("offset", True),
        ("offset", -1),
        ("offset", 2**63),
        ("timestamp_ms", True),
        ("timestamp_ms", 2**63),
        ("latency_ms", float("nan")),
        ("latency_ms", float("inf")),
        ("latency_ms", -1.0),
        ("latency_ms", True),
        ("serialized_bytes", True),
        ("serialized_bytes", 1),
    ],
)
async def test_malformed_ack_is_never_confirmed(publication, field, value):
    producer, _ = transport(publication)
    producer.publish = AsyncMock(
        return_value=ack(publication).model_copy(update={field: value})
    )
    outcome = await IncidentPublisher(producer).publish(publication)
    assert outcome.status is Status.FAILED_AMBIGUOUS
    producer.publish.assert_awaited_once()


@pytest.mark.asyncio
@pytest.mark.parametrize("result", [None, {}, "ack"])
async def test_wrong_ack_type_is_ambiguous(publication, result):
    producer, _ = transport(publication)
    producer.publish = AsyncMock(return_value=result)
    outcome = await IncidentPublisher(producer).publish(publication)
    assert outcome.status is Status.FAILED_AMBIGUOUS


@pytest.mark.asyncio
async def test_producer_receives_copies_not_mutable_input_objects(publication):
    producer, _ = transport(publication)
    original_payload = publication.payload.model_dump()
    original_context = publication.execution_context.model_dump()

    async def mutate(event, context):
        assert context is not publication.execution_context
        event.payload.clear()
        object.__setattr__(context, "scenario_id", "changed")
        return ack(publication)

    producer.publish = AsyncMock(side_effect=mutate)
    outcome = await IncidentPublisher(producer).publish(publication)
    assert outcome.status is Status.ACKNOWLEDGED
    assert publication.payload.model_dump() == original_payload
    assert publication.execution_context.model_dump() == original_context


@pytest.mark.parametrize(
    "timeout",
    [True, False, 0, 0.09, 60.1, float("nan"), float("inf"), -float("inf"), "5", None],
)
def test_invalid_timeouts_rejected(timeout):
    with pytest.raises(IncidentPublishContractError):
        IncidentPublisher(KafkaTelemetryProducer(), publish_timeout_seconds=timeout)


@pytest.mark.parametrize("timeout", [0.1, 5, 60.0])
def test_timeout_boundaries(timeout):
    IncidentPublisher(KafkaTelemetryProducer(), publish_timeout_seconds=timeout)


def test_constructor_requires_existing_transport():
    with pytest.raises(IncidentPublishContractError):
        IncidentPublisher(None)


@pytest.mark.parametrize(
    "kwargs",
    [
        {"status": "ACKNOWLEDGED"},
        {"status": Status.ACKNOWLEDGED},
        {"status": Status.FAILED_AMBIGUOUS},
        {
            "status": Status.FAILED_UNAVAILABLE,
            "error": IncidentPublishAmbiguousError("safe"),
        },
    ],
)
def test_outcome_invariants(kwargs):
    with pytest.raises(IncidentPublishContractError):
        IncidentPublishOutcome(**kwargs)


@pytest.mark.asyncio
async def test_version_checked_at_actual_dispatch(publication):
    producer, broker = transport(publication)
    publisher = IncidentPublisher(producer)
    loop = asyncio.get_running_loop()
    # This callback runs after validation but before the scheduled send starts.
    loop.call_soon(setattr, producer, "producer_version", "changed-at-dispatch")
    outcome = await publisher.publish(publication)
    assert outcome.status is Status.FAILED_CONTRACT
    broker.send_and_wait.assert_not_called()


@pytest.mark.asyncio
async def test_extra_ack_field_is_not_silently_discarded(publication):
    producer, _ = transport(publication)
    producer.publish = AsyncMock(
        return_value=ack(publication).model_copy(update={"extra": "ignored"})
    )
    assert (
        await IncidentPublisher(producer).publish(publication)
    ).status is Status.FAILED_AMBIGUOUS


@pytest.mark.asyncio
async def test_repeated_explicit_calls_keep_logical_wire_identity(publication):
    producer, broker = transport(publication)
    publisher = IncidentPublisher(producer)
    for _ in range(2):
        assert (await publisher.publish(publication)).status is Status.ACKNOWLEDGED
    assert broker.send_and_wait.await_count == 2
    first, second = [call.kwargs for call in broker.send_and_wait.call_args_list]
    for field in ["key", "value", "headers", "topic"]:
        assert first[field] == second[field]
