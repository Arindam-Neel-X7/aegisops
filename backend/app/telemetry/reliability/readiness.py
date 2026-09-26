import asyncio
from typing import Any

import aiokafka
import structlog

from app.core.config import settings
from app.telemetry.topics import KafkaTopic

logger = structlog.get_logger(__name__)


async def check_kafka_readiness(
    bootstrap_servers: str = settings.KAFKA_BOOTSTRAP_SERVERS,
    timeout_seconds: float = 3.0,
) -> bool:
    """Non-destructive readiness probe checking Kafka broker connectivity and active topics."""
    consumer = aiokafka.AIOKafkaConsumer(
        bootstrap_servers=bootstrap_servers,
        request_timeout_ms=int(timeout_seconds * 1000),
    )
    try:
        await asyncio.wait_for(consumer.start(), timeout=timeout_seconds)
        try:
            # Fetch cluster metadata and inspect active canonical topics
            topics = await asyncio.wait_for(
                consumer.topics(),
                timeout=timeout_seconds,
            )
            required_topics = {
                KafkaTopic.METRICS.value,
                KafkaTopic.LOGS.value,
                KafkaTopic.SYSTEM_EVENTS.value,
            }
            return required_topics.issubset(topics)
        finally:
            await consumer.stop()
    except Exception as exc:
        logger.debug("Kafka readiness probe failed", error=str(exc))
        return False


async def check_metric_query_readiness(
    timeout_seconds: float = 3.0,
    adapter: Any = None,
) -> bool:
    """Readiness probe verifying VictoriaMetrics query execution capability."""
    from app.telemetry.query.metrics import MetricQueryAdapter
    from app.telemetry.query.models import MetricQuery

    own_adapter = adapter is None
    ad = adapter or MetricQueryAdapter(timeout_seconds=timeout_seconds)
    try:
        res = await ad.query(MetricQuery(metric="http_requests_total"))
        return res is not None and isinstance(res.count, int)
    except Exception as exc:
        logger.debug("Metric query readiness probe failed", error=str(exc))
        return False
    finally:
        if own_adapter:
            await ad.close()


async def check_evidence_query_readiness(
    timeout_seconds: float = 3.0,
    adapter: Any = None,
) -> bool:
    """Readiness probe verifying OpenSearch alias query execution capability."""
    from app.telemetry.query.evidence import EvidenceQueryAdapter
    from app.telemetry.query.models import EvidenceQuery

    own_adapter = adapter is None
    ad = adapter or EvidenceQueryAdapter(timeout_seconds=timeout_seconds)
    try:
        res = await ad.query(EvidenceQuery(limit=1))
        return res is not None and isinstance(res.count, int)
    except Exception as exc:
        logger.debug("Evidence query readiness probe failed", error=str(exc))
        return False
    finally:
        if own_adapter:
            await ad.close()


async def check_telemetry_readiness(
    timeout_seconds: float = 5.0,
    kafka_probe: Any = None,
    vm_probe: Any = None,
    os_probe: Any = None,
    metric_query_probe: Any = None,
    evidence_query_probe: Any = None,
) -> tuple[bool, dict[str, Any]]:
    """Compose readiness across Kafka, VictoriaMetrics, OpenSearch, and Query adapters."""
    from app.telemetry.persistence.metrics import check_victoriametrics_health
    from app.telemetry.persistence.search import check_opensearch_health

    _kafka = kafka_probe or check_kafka_readiness(timeout_seconds=min(3.0, timeout_seconds))
    _vm = vm_probe or check_victoriametrics_health(timeout_seconds=min(3.0, timeout_seconds))
    _os = os_probe or check_opensearch_health(timeout_seconds=min(3.0, timeout_seconds))
    _mq = metric_query_probe or check_metric_query_readiness(timeout_seconds=min(3.0, timeout_seconds))
    _eq = evidence_query_probe or check_evidence_query_readiness(timeout_seconds=min(3.0, timeout_seconds))

    results = await asyncio.gather(_kafka, _vm, _os, _mq, _eq, return_exceptions=True)

    kafka_ready = results[0] is True
    vm_ready = results[1] is True

    os_res = results[2]
    os_ready = False
    os_status = "unhealthy"
    if isinstance(os_res, tuple) and len(os_res) == 2:
        os_ready = os_res[0] is True
        os_status = os_res[1]

    mq_ready = results[3] is True
    eq_ready = results[4] is True

    all_ready = kafka_ready and vm_ready and os_ready and mq_ready and eq_ready

    components = {
        "kafka": {"ready": kafka_ready},
        "victoriametrics": {"ready": vm_ready},
        "opensearch": {"ready": os_ready, "cluster_status": os_status},
        "metric_query": {"ready": mq_ready},
        "evidence_query": {"ready": eq_ready},
    }

    response_body = {
        "status": "ready" if all_ready else "not_ready",
        "components": components,
    }

    return all_ready, response_body
