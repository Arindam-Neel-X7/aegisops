from __future__ import annotations

import pydantic

from app.anomaly.errors import AnomalyHandoffError
from app.anomaly.models import AnomalySignal
from app.anomaly.serialization import (
    ANOMALY_EVENT_TYPE,
    ANOMALY_KAFKA_TOPIC,
    construct_anomaly_kafka_key,
)
from app.telemetry.transport.consumer import TelemetryEnvelope


def anomaly_signal_from_envelope(envelope: TelemetryEnvelope) -> AnomalySignal:
    """Extract and strictly validate an AnomalySignal from an anomaly TelemetryEnvelope."""
    if not isinstance(envelope, TelemetryEnvelope):
        raise TypeError("Expected TelemetryEnvelope")

    if (
        envelope.topic != ANOMALY_KAFKA_TOPIC.value
        or envelope.event.event_type != ANOMALY_EVENT_TYPE
    ):
        raise AnomalyHandoffError("Invalid anomaly handoff route")

    try:
        signal = AnomalySignal.model_validate(envelope.event.payload)
    except pydantic.ValidationError as exc:
        raise AnomalyHandoffError("Invalid anomaly handoff payload") from exc

    event = envelope.event
    if (
        event.event_id != signal.signal_id
        or event.event_time != signal.event_time
        or event.tenant_id != signal.tenant_id
        or event.environment != signal.environment
        or event.service != signal.service
        or event.severity != signal.severity
        or event.trace_id != signal.trace_id
    ):
        raise AnomalyHandoffError("Anomaly handoff envelope does not match payload")

    context = envelope.context
    if (
        context.run_id != signal.run_id
        or context.scenario_id != signal.scenario_id
        or context.scenario_version != signal.scenario_version
        or context.reproducibility_key != signal.reproducibility_key
        or context.seed != signal.seed
    ):
        raise AnomalyHandoffError("Anomaly handoff context does not match payload")

    if envelope.key != construct_anomaly_kafka_key(signal):
        raise AnomalyHandoffError("Anomaly handoff key does not match payload")

    return signal
