from enum import StrEnum

from .schemas import EventType


class KafkaTopic(StrEnum):
    METRICS = "aegis.telemetry.metrics"
    LOGS = "aegis.telemetry.logs"
    SYSTEM_EVENTS = "aegis.system.events"
    ANOMALIES = "aegis.ml.anomalies"
    INCIDENTS = "aegis.incidents"
    AGENT_EVENTS = "aegis.agent.events"


EVENT_TYPE_TO_TOPIC: dict[EventType, KafkaTopic] = {
    EventType.METRIC: KafkaTopic.METRICS,
    EventType.LOG: KafkaTopic.LOGS,
    EventType.SYSTEM: KafkaTopic.SYSTEM_EVENTS,
    EventType.ANOMALY: KafkaTopic.ANOMALIES,
    EventType.INCIDENT: KafkaTopic.INCIDENTS,
    EventType.AGENT: KafkaTopic.AGENT_EVENTS,
}
