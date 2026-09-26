import asyncio
import math
import random
import time
from typing import Any
import uuid

import httpx
from pydantic import BaseModel, ConfigDict
import structlog

from app.core.config import settings
from app.telemetry.persistence.errors import (
    InvalidMetricError,
    VictoriaMetricsPersistenceError,
    VictoriaMetricsRetryExhaustedError,
)
from app.telemetry.transport.consumer import TelemetryEnvelope

logger = structlog.get_logger(__name__)


class MetricPersistenceResult(BaseModel):
    """Immutable result metadata returned following successful VictoriaMetrics metric ingestion."""

    model_config = ConfigDict(frozen=True)

    event_id: uuid.UUID
    run_id: uuid.UUID
    metric_name: str
    timestamp_ms: int
    attempts: int
    status_code: int
    latency_ms: float


def extract_metric_data(
    envelope: TelemetryEnvelope,
) -> tuple[str, float, dict[str, str], int]:
    """Validate and extract metric data and label dictionary from a TelemetryEnvelope.

    Required fields:
    - payload['metric_name']: non-empty string
    - payload['value']: int, float, or bool (mapped True->1.0, False->0.0)

    Required labels:
    - __name__, service, tenant_id, environment, run_id, scenario_id, seed, event_id

    Optional labels (only when present):
    - status_code, outcome

    Forbidden as labels:
    - trace_id, request_id, scenario_version, reproducibility_key, arbitrary payload objects
    """
    event = envelope.event
    context = envelope.context
    payload = event.payload

    if "metric_name" not in payload:
        raise InvalidMetricError("Missing required 'metric_name' in metric payload")
    metric_name = payload["metric_name"]
    if not isinstance(metric_name, str) or not metric_name.strip():
        raise InvalidMetricError("Field 'metric_name' must be a non-empty string")

    if "value" not in payload:
        raise InvalidMetricError("Missing required 'value' in metric payload")
    raw_val = payload["value"]

    if isinstance(raw_val, bool):
        val = 1.0 if raw_val else 0.0
    elif isinstance(raw_val, (int, float)):
        if math.isnan(raw_val) or math.isinf(raw_val):
            raise InvalidMetricError(f"Metric value cannot be NaN or Infinity, got {raw_val}")
        val = float(raw_val)
    else:
        raise InvalidMetricError(
            f"Invalid metric value type: '{type(raw_val).__name__}'. Expected int, float, or bool."
        )

    # Convert event.event_time to epoch milliseconds preserving exact domain instant
    timestamp_ms = int(event.event_time.timestamp() * 1000)

    labels: dict[str, str] = {
        "__name__": metric_name,
        "service": event.service,
        "tenant_id": str(event.tenant_id),
        "environment": event.environment,
        "run_id": str(context.run_id),
        "scenario_id": context.scenario_id,
        "seed": str(context.seed),
        "event_id": str(event.event_id),
    }

    if "status_code" in payload and payload["status_code"] is not None:
        labels["status_code"] = str(payload["status_code"])
    if "outcome" in payload and payload["outcome"] is not None:
        labels["outcome"] = str(payload["outcome"])

    return metric_name, val, labels, timestamp_ms


class VictoriaMetricsPersistenceAdapter:
    """Async persistence adapter ingesting canonical metrics into VictoriaMetrics."""

    def __init__(
        self,
        base_url: str = settings.VICTORIAMETRICS_URL,
        timeout_seconds: float = settings.VICTORIAMETRICS_TIMEOUT_SECONDS,
        max_attempts: int = settings.TELEMETRY_PERSISTENCE_RETRY_MAX_ATTEMPTS,
        client: httpx.AsyncClient | None = None,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.import_url = f"{self.base_url}/api/v1/import"
        self.timeout_seconds = timeout_seconds
        self.max_attempts = max_attempts
        self._external_client = client
        self._client: httpx.AsyncClient | None = client

    async def _get_client(self) -> httpx.AsyncClient:
        if self._client is None:
            self._client = httpx.AsyncClient(timeout=self.timeout_seconds)
        return self._client

    async def close(self) -> None:
        """Close underlying HTTP client if created internally."""
        if self._client is not None and self._client is not self._external_client:
            await self._client.aclose()
            self._client = None

    async def __aenter__(self) -> "VictoriaMetricsPersistenceAdapter":
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: Any,
    ) -> None:
        await self.close()

    async def handle(self, envelope: TelemetryEnvelope) -> None:
        """Satisfy the TelemetryEnvelopeHandler protocol boundary for MetricsConsumer."""
        await self.persist(envelope)

    async def persist(self, envelope: TelemetryEnvelope) -> MetricPersistenceResult:
        """Validate, format, and transmit a metric sample to VictoriaMetrics /api/v1/import."""
        metric_name, val, labels, timestamp_ms = extract_metric_data(envelope)

        import_payload = {
            "metric": labels,
            "values": [val],
            "timestamps": [timestamp_ms],
        }

        client = await self._get_client()
        attempts = 0
        backoff_schedule = [0.1, 0.2, 0.4]

        start_time = time.perf_counter()

        while attempts < self.max_attempts:
            attempts += 1
            try:
                response = await client.post(
                    self.import_url,
                    json=import_payload,
                    headers={"Content-Type": "application/json"},
                )

                if 200 <= response.status_code < 300:
                    latency_ms = (time.perf_counter() - start_time) * 1000.0
                    logger.debug(
                        "Persisted metric event to VictoriaMetrics",
                        event_id=str(envelope.event.event_id),
                        run_id=str(envelope.context.run_id),
                        metric_name=metric_name,
                        status_code=response.status_code,
                        attempts=attempts,
                        latency_ms=round(latency_ms, 3),
                    )
                    return MetricPersistenceResult(
                        event_id=envelope.event.event_id,
                        run_id=envelope.context.run_id,
                        metric_name=metric_name,
                        timestamp_ms=timestamp_ms,
                        attempts=attempts,
                        status_code=response.status_code,
                        latency_ms=latency_ms,
                    )

                status = response.status_code
                if status in (408, 429, 500, 502, 503, 504):
                    if attempts >= self.max_attempts:
                        raise VictoriaMetricsRetryExhaustedError(
                            f"VictoriaMetrics transient HTTP error {status} exhausted {attempts} attempts: {response.text}"
                        )

                    sleep_duration = self._calculate_backoff(response, attempts, backoff_schedule)
                    await asyncio.sleep(sleep_duration)
                    continue

                # Permanent HTTP error
                raise VictoriaMetricsPersistenceError(
                    f"VictoriaMetrics permanent HTTP error {status}: {response.text}"
                )

            except (httpx.TimeoutException, httpx.NetworkError) as exc:
                if attempts >= self.max_attempts:
                    raise VictoriaMetricsRetryExhaustedError(
                        f"VictoriaMetrics network error exhausted {attempts} attempts: {exc}"
                    ) from exc
                sleep_duration = self._calculate_backoff(None, attempts, backoff_schedule)
                await asyncio.sleep(sleep_duration)
                continue

        raise VictoriaMetricsRetryExhaustedError(
            f"VictoriaMetrics persistence failed after {attempts} attempts"
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


async def check_victoriametrics_health(
    base_url: str = settings.VICTORIAMETRICS_URL,
    timeout_seconds: float = settings.VICTORIAMETRICS_TIMEOUT_SECONDS,
    client: httpx.AsyncClient | None = None,
) -> bool:
    """Check reachability and health of the VictoriaMetrics subsystem."""
    url = f"{base_url.rstrip('/')}/health"
    try:
        if client is not None:
            res = await client.get(url, timeout=timeout_seconds)
            return res.status_code == 200
        async with httpx.AsyncClient(timeout=timeout_seconds) as temp_client:
            res = await temp_client.get(url)
            return res.status_code == 200
    except Exception:
        return False
