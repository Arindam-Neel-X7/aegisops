import asyncio
from datetime import datetime, timezone
import random
import time
from typing import Any
import uuid

import httpx
from pydantic import AwareDatetime, BaseModel, ConfigDict
import structlog

from app.core.config import settings
from app.telemetry.persistence.errors import (
    EvidenceIndexBootstrapError,
    InvalidEvidenceEventError,
    OpenSearchPersistenceError,
    OpenSearchRetryExhaustedError,
)
from app.telemetry.schemas import EventType
from app.telemetry.transport.consumer import TelemetryEnvelope

logger = structlog.get_logger(__name__)

TEMPLATE_NAME = "aegis-evidence-template-v1"
CONCRETE_INDEX_V1 = "aegis-evidence-v1"
EVIDENCE_ALIAS = "aegis-evidence"

EVIDENCE_INDEX_TEMPLATE: dict[str, Any] = {
    "index_patterns": ["aegis-evidence*"],
    "template": {
        "settings": {
            "index": {
                "number_of_shards": 1,
                "number_of_replicas": 0,
            }
        },
        "mappings": {
            "properties": {
                "schema_version": {"type": "keyword"},
                "event_id": {"type": "keyword"},
                "event_time": {"type": "date"},
                "tenant_id": {"type": "keyword"},
                "environment": {"type": "keyword"},
                "service": {"type": "keyword"},
                "event_type": {"type": "keyword"},
                "severity": {"type": "keyword"},
                "trace_id": {"type": "keyword"},
                "run_id": {"type": "keyword"},
                "scenario_id": {"type": "keyword"},
                "scenario_version": {"type": "keyword"},
                "reproducibility_key": {"type": "keyword"},
                "seed": {"type": "keyword"},
                "ingested_at": {"type": "date"},
                "payload": {
                    "type": "object",
                    "dynamic": False,
                    "properties": {
                        "marker": {"type": "keyword"},
                        "message": {
                            "type": "text",
                            "fields": {
                                "keyword": {
                                    "type": "keyword",
                                    "ignore_above": 256,
                                }
                            },
                        },
                        "status_code": {"type": "integer"},
                        "outcome": {"type": "keyword"},
                        "latency_ms": {"type": "float"},
                        "request_id": {"type": "keyword"},
                        "fault_id": {"type": "keyword"},
                        "fault_type": {"type": "keyword"},
                        "target_service_id": {"type": "keyword"},
                        "deployment_version": {"type": "keyword"},
                        "route": {"type": "keyword"},
                        "error": {"type": "keyword"},
                    },
                },
            }
        },
    },
}


class EvidencePersistenceResult(BaseModel):
    """Immutable result metadata returned following confirmed OpenSearch document indexing."""

    model_config = ConfigDict(frozen=True)

    event_id: uuid.UUID
    run_id: uuid.UUID
    document_id: str
    index_name: str
    status_code: int
    result: str
    attempts: int
    latency_ms: float
    ingested_at: AwareDatetime


def build_evidence_document(
    envelope: TelemetryEnvelope,
    ingested_at: datetime | None = None,
) -> tuple[str, dict[str, Any]]:
    """Build deterministic document _id and body dictionary from a TelemetryEnvelope.

    Allowed event types:
    - EventType.LOG
    - EventType.SYSTEM

    Rejected:
    - EventType.METRIC, ANOMALY, INCIDENT, AGENT

    Deterministic Document ID:
    _id = f"{envelope.context.run_id}:{envelope.event.event_id}"
    """
    event = envelope.event
    context = envelope.context

    if event.event_type not in (EventType.LOG, EventType.SYSTEM):
        raise InvalidEvidenceEventError(
            f"Event type '{event.event_type}' is not allowed for evidence persistence. "
            "Only LOG and SYSTEM events are supported."
        )

    doc_id = f"{context.run_id}:{event.event_id}"
    ingested_at_dt = ingested_at or datetime.now(timezone.utc)

    doc: dict[str, Any] = {
        "schema_version": event.schema_version,
        "event_id": str(event.event_id),
        "event_time": event.event_time.isoformat(),
        "tenant_id": str(event.tenant_id),
        "environment": event.environment,
        "service": event.service,
        "event_type": event.event_type.value,
        "severity": event.severity.value,
        "trace_id": event.trace_id,
        "payload": dict(event.payload),
        "run_id": str(context.run_id),
        "scenario_id": context.scenario_id,
        "scenario_version": context.scenario_version,
        "reproducibility_key": context.reproducibility_key,
        "seed": str(context.seed),
        "ingested_at": ingested_at_dt.isoformat(),
    }

    return doc_id, doc


class OpenSearchPersistenceAdapter:
    """Async persistence adapter indexing LOG and SYSTEM telemetry evidence into OpenSearch."""

    def __init__(
        self,
        base_url: str = settings.OPENSEARCH_URL,
        user: str | None = settings.OPENSEARCH_USER,
        password: str | None = settings.OPENSEARCH_PASSWORD,
        timeout_seconds: float = settings.OPENSEARCH_TIMEOUT_SECONDS,
        max_attempts: int = settings.TELEMETRY_PERSISTENCE_RETRY_MAX_ATTEMPTS,
        client: httpx.AsyncClient | None = None,
        index_alias: str = EVIDENCE_ALIAS,
        concrete_index: str = CONCRETE_INDEX_V1,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.user = user
        self.password = password
        self.timeout_seconds = timeout_seconds
        self.max_attempts = max_attempts
        self.index_alias = index_alias
        self.concrete_index = concrete_index
        self._external_client = client
        self._client: httpx.AsyncClient | None = client

    def _get_auth(self) -> httpx.BasicAuth | None:
        if self.user and self.password:
            return httpx.BasicAuth(self.user, self.password)
        return None

    async def _get_client(self) -> httpx.AsyncClient:
        if self._client is None:
            self._client = httpx.AsyncClient(
                timeout=self.timeout_seconds,
                auth=self._get_auth(),
            )
        return self._client

    async def close(self) -> None:
        """Close underlying HTTP client if created internally."""
        if self._client is not None and self._client is not self._external_client:
            await self._client.aclose()
            self._client = None

    async def __aenter__(self) -> "OpenSearchPersistenceAdapter":
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: Any,
    ) -> None:
        await self.close()

    async def bootstrap(self) -> None:
        """Idempotently bootstrap index template, concrete index, and alias."""
        client = await self._get_client()

        # 1. Put index template
        template_url = f"{self.base_url}/_index_template/{TEMPLATE_NAME}"
        try:
            tpl_res = await client.put(
                template_url,
                json=EVIDENCE_INDEX_TEMPLATE,
                headers={"Content-Type": "application/json"},
            )
            if tpl_res.status_code not in (200, 201):
                raise EvidenceIndexBootstrapError(
                    f"Failed to create index template {TEMPLATE_NAME}: HTTP {tpl_res.status_code} - {tpl_res.text}"
                )
        except (httpx.TimeoutException, httpx.NetworkError) as exc:
            raise EvidenceIndexBootstrapError(
                f"Network error while creating index template {TEMPLATE_NAME}: {exc}"
            ) from exc

        # 2. Check and create concrete index if absent
        index_url = f"{self.base_url}/{self.concrete_index}"
        try:
            idx_check = await client.head(index_url)
            if idx_check.status_code == 404:
                idx_res = await client.put(index_url)
                if idx_res.status_code not in (200, 201):
                    raise EvidenceIndexBootstrapError(
                        f"Failed to create concrete index {self.concrete_index}: HTTP {idx_res.status_code} - {idx_res.text}"
                    )
        except (httpx.TimeoutException, httpx.NetworkError) as exc:
            raise EvidenceIndexBootstrapError(
                f"Network error while verifying concrete index {self.concrete_index}: {exc}"
            ) from exc

        # 3. Verify / create alias
        alias_url = f"{self.base_url}/_alias/{self.index_alias}"
        try:
            alias_res = await client.get(alias_url)
            if alias_res.status_code == 404:
                # Add alias pointing to concrete index
                alias_actions = {
                    "actions": [
                        {
                            "add": {
                                "index": self.concrete_index,
                                "alias": self.index_alias,
                            }
                        }
                    ]
                }
                add_res = await client.post(
                    f"{self.base_url}/_aliases",
                    json=alias_actions,
                    headers={"Content-Type": "application/json"},
                )
                if add_res.status_code not in (200, 201):
                    raise EvidenceIndexBootstrapError(
                        f"Failed to create alias {self.index_alias}: HTTP {add_res.status_code} - {add_res.text}"
                    )
            elif alias_res.status_code == 200:
                alias_data = alias_res.json()
                if self.concrete_index not in alias_data:
                    raise EvidenceIndexBootstrapError(
                        f"Alias '{self.index_alias}' exists but points to unexpected target indices: {list(alias_data.keys())}. "
                        f"Expected target: '{self.concrete_index}'."
                    )
            else:
                raise EvidenceIndexBootstrapError(
                    f"Unexpected status checking alias {self.index_alias}: HTTP {alias_res.status_code} - {alias_res.text}"
                )
        except (httpx.TimeoutException, httpx.NetworkError) as exc:
            raise EvidenceIndexBootstrapError(
                f"Network error while checking alias {self.index_alias}: {exc}"
            ) from exc

        logger.info(
            "OpenSearch evidence storage bootstrapped successfully",
            template=TEMPLATE_NAME,
            concrete_index=self.concrete_index,
            alias=self.index_alias,
        )

    async def handle(self, envelope: TelemetryEnvelope) -> None:
        """Satisfy the TelemetryEnvelopeHandler protocol boundary for EvidenceConsumer."""
        await self.persist(envelope)

    async def persist(self, envelope: TelemetryEnvelope) -> EvidencePersistenceResult:
        """Persist a TelemetryEnvelope containing a LOG or SYSTEM event to OpenSearch."""
        ingested_at = datetime.now(timezone.utc)
        doc_id, doc = build_evidence_document(envelope, ingested_at=ingested_at)

        target_url = f"{self.base_url}/{self.concrete_index}/_doc/{doc_id}"

        client = await self._get_client()
        attempts = 0
        backoff_schedule = [0.1, 0.2, 0.4]

        start_time = time.perf_counter()

        while attempts < self.max_attempts:
            attempts += 1
            try:
                response = await client.put(
                    target_url,
                    json=doc,
                    headers={"Content-Type": "application/json"},
                )

                if 200 <= response.status_code < 300:
                    latency_ms = (time.perf_counter() - start_time) * 1000.0
                    body = response.json()
                    result_action = body.get("result", "created" if response.status_code == 201 else "updated")

                    logger.debug(
                        "Persisted evidence document to OpenSearch",
                        event_id=str(envelope.event.event_id),
                        run_id=str(envelope.context.run_id),
                        document_id=doc_id,
                        service=envelope.event.service,
                        event_type=envelope.event.event_type.value,
                        index=self.concrete_index,
                        status_code=response.status_code,
                        result=result_action,
                        attempts=attempts,
                        latency_ms=round(latency_ms, 3),
                    )
                    return EvidencePersistenceResult(
                        event_id=envelope.event.event_id,
                        run_id=envelope.context.run_id,
                        document_id=doc_id,
                        index_name=self.concrete_index,
                        status_code=response.status_code,
                        result=result_action,
                        attempts=attempts,
                        latency_ms=latency_ms,
                        ingested_at=ingested_at,
                    )

                status = response.status_code
                if status in (408, 429, 500, 502, 503, 504):
                    if attempts >= self.max_attempts:
                        raise OpenSearchRetryExhaustedError(
                            f"OpenSearch transient HTTP error {status} exhausted {attempts} attempts: {response.text}"
                        )

                    sleep_duration = self._calculate_backoff(response, attempts, backoff_schedule)
                    await asyncio.sleep(sleep_duration)
                    continue

                # Permanent HTTP error
                raise OpenSearchPersistenceError(
                    f"OpenSearch permanent HTTP error {status}: {response.text}"
                )

            except (httpx.TimeoutException, httpx.NetworkError) as exc:
                if attempts >= self.max_attempts:
                    raise OpenSearchRetryExhaustedError(
                        f"OpenSearch network error exhausted {attempts} attempts: {exc}"
                    ) from exc
                sleep_duration = self._calculate_backoff(None, attempts, backoff_schedule)
                await asyncio.sleep(sleep_duration)
                continue

        raise OpenSearchRetryExhaustedError(
            f"OpenSearch persistence failed after {attempts} attempts"
        )

    def _calculate_backoff(
        self,
        response: httpx.Response | None,
        attempt: int,
        schedule: list[float],
    ) -> float:
        if response is not None and response.status_code in (429, 503):
            retry_after_hdr = response.headers.get("Retry-After")
            if retry_after_hdr:
                try:
                    retry_sec = float(retry_after_hdr)
                    if 0 < retry_sec <= 2.0:
                        return retry_sec
                except (ValueError, TypeError):
                    pass

        idx = min(attempt - 1, len(schedule) - 1)
        base = schedule[idx]
        return random.uniform(0, base)


async def check_opensearch_health(
    base_url: str = settings.OPENSEARCH_URL,
    user: str | None = settings.OPENSEARCH_USER,
    password: str | None = settings.OPENSEARCH_PASSWORD,
    timeout_seconds: float = settings.OPENSEARCH_TIMEOUT_SECONDS,
    client: httpx.AsyncClient | None = None,
) -> tuple[bool, str]:
    """Check reachability and cluster health of the OpenSearch subsystem."""
    url = f"{base_url.rstrip('/')}/_cluster/health"
    auth = httpx.BasicAuth(user, password) if user and password else None
    try:
        if client is not None:
            res = await client.get(url, timeout=timeout_seconds, auth=auth)
            if res.status_code == 200:
                cluster_status = res.json().get("status", "unknown")
                return (cluster_status in ("green", "yellow"), cluster_status)
            return (False, f"http_{res.status_code}")

        async with httpx.AsyncClient(timeout=timeout_seconds, auth=auth) as temp_client:
            res = await temp_client.get(url)
            if res.status_code == 200:
                cluster_status = res.json().get("status", "unknown")
                return (cluster_status in ("green", "yellow"), cluster_status)
            return (False, f"http_{res.status_code}")
    except Exception:
        return (False, "unreachable")
