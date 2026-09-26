from datetime import datetime, timedelta, timezone
import math
import time
from typing import Any
import uuid

import httpx
import structlog

from app.core.config import settings
from app.telemetry.query.errors import (
    MetricQueryBackendError,
    QueryResponseValidationError,
)
from app.telemetry.query.models import (
    MetricQuery,
    MetricQueryResult,
    MetricSample,
)

logger = structlog.get_logger(__name__)


def escape_label_value(val: str) -> str:
    """Safely escape characters for PromQL/MetricQL label value literals."""
    return val.replace("\\", "\\\\").replace('"', '\\"').replace("\n", "\\n")


def build_metric_selector(query: MetricQuery) -> str:
    """Construct safe MetricQL/PromQL selector string from MetricQuery filters."""
    labels: list[str] = []
    if query.service is not None:
        labels.append(f'service="{escape_label_value(query.service)}"')
    if query.tenant_id is not None:
        labels.append(f'tenant_id="{str(query.tenant_id)}"')
    if query.environment is not None:
        labels.append(f'environment="{escape_label_value(query.environment)}"')
    if query.run_id is not None:
        labels.append(f'run_id="{str(query.run_id)}"')
    if query.seed is not None:
        labels.append(f'seed="{str(query.seed)}"')
    if query.event_id is not None:
        labels.append(f'event_id="{str(query.event_id)}"')

    if labels:
        return f"{query.metric}{{{','.join(labels)}}}"
    return query.metric


def _build_sample_from_labels(
    labels: dict[str, Any],
    ts_sec: float,
    val: float,
    query_metric: str,
) -> MetricSample:
    try:
        metric_name = labels.get("__name__", query_metric)
        event_id = uuid.UUID(labels["event_id"])
        run_id = uuid.UUID(labels["run_id"])
        service = labels["service"]
        tenant_id = uuid.UUID(labels["tenant_id"])
        environment = labels["environment"]
        seed = int(labels["seed"])
        scenario_id = labels.get("scenario_id")
        status_code = labels.get("status_code")
        outcome = labels.get("outcome")
        timestamp = datetime.fromtimestamp(ts_sec, tz=timezone.utc)
    except (KeyError, ValueError, TypeError) as exc:
        raise QueryResponseValidationError(
            f"Failed to parse required metric sample labels: {exc}"
        ) from exc

    return MetricSample(
        metric=metric_name,
        event_id=event_id,
        run_id=run_id,
        service=service,
        tenant_id=tenant_id,
        environment=environment,
        seed=seed,
        timestamp=timestamp,
        value=val,
        scenario_id=scenario_id,
        status_code=status_code,
        outcome=outcome,
    )


def parse_metric_response(data: dict[str, Any], query_metric: str) -> list[MetricSample]:
    """Validate and normalize VictoriaMetrics Prometheus JSON response into MetricSamples."""
    if not isinstance(data, dict):
        raise QueryResponseValidationError(
            f"Expected JSON object from VictoriaMetrics, got {type(data).__name__}"
        )

    status = data.get("status")
    if status != "success":
        error_msg = data.get("error", "Unknown VictoriaMetrics error")
        raise MetricQueryBackendError(
            f"VictoriaMetrics query returned error status '{status}': {error_msg}"
        )

    data_block = data.get("data")
    if not isinstance(data_block, dict):
        raise QueryResponseValidationError("Missing 'data' block in VictoriaMetrics response")

    result_type = data_block.get("resultType")
    results = data_block.get("result")
    if not isinstance(results, list):
        raise QueryResponseValidationError(
            "Missing or invalid 'result' list in VictoriaMetrics response"
        )

    samples: list[MetricSample] = []

    if result_type == "vector":
        for item in results:
            metric_labels = item.get("metric", {})
            value_pair = item.get("value")
            if not isinstance(value_pair, list) or len(value_pair) < 2:
                raise QueryResponseValidationError(
                    f"Malformed value pair in vector result: {value_pair}"
                )
            try:
                ts_sec = float(value_pair[0])
                val_raw = float(value_pair[1])
            except (ValueError, TypeError) as exc:
                raise QueryResponseValidationError(f"Invalid timestamp or value in result: {exc}") from exc

            if math.isnan(val_raw) or math.isinf(val_raw):
                raise QueryResponseValidationError(f"Non-finite float value in result: {val_raw}")
            sample = _build_sample_from_labels(metric_labels, ts_sec, val_raw, query_metric)
            samples.append(sample)

    elif result_type == "matrix":
        for item in results:
            metric_labels = item.get("metric", {})
            values_list = item.get("values", [])
            for value_pair in values_list:
                if not isinstance(value_pair, list) or len(value_pair) < 2:
                    raise QueryResponseValidationError(
                        f"Malformed value pair in matrix result: {value_pair}"
                    )
                try:
                    ts_sec = float(value_pair[0])
                    val_raw = float(value_pair[1])
                except (ValueError, TypeError) as exc:
                    raise QueryResponseValidationError(f"Invalid timestamp or value in result: {exc}") from exc

                if math.isnan(val_raw) or math.isinf(val_raw):
                    raise QueryResponseValidationError(f"Non-finite float value in result: {val_raw}")
                sample = _build_sample_from_labels(metric_labels, ts_sec, val_raw, query_metric)
                samples.append(sample)
    else:
        raise QueryResponseValidationError(
            f"Unsupported resultType from VictoriaMetrics: '{result_type}'"
        )

    # Deterministic sorting: timestamp ASC, then event_id ASC
    samples.sort(key=lambda s: (s.timestamp, s.event_id))
    return samples


class MetricQueryAdapter:
    """Async query adapter retrieving normalized metric data from VictoriaMetrics."""

    def __init__(
        self,
        base_url: str = settings.VICTORIAMETRICS_URL,
        timeout_seconds: float = settings.VICTORIAMETRICS_TIMEOUT_SECONDS,
        client: httpx.AsyncClient | None = None,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.timeout_seconds = timeout_seconds
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

    async def __aenter__(self) -> "MetricQueryAdapter":
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: Any,
    ) -> None:
        await self.close()

    async def query(self, query: MetricQuery) -> MetricQueryResult:
        """Execute a MetricQuery against VictoriaMetrics and return normalized MetricQueryResult."""
        selector = build_metric_selector(query)
        client = await self._get_client()

        params: dict[str, str] = {"query": selector}
        endpoint = "/api/v1/query"

        if query.start_time is not None and query.end_time is not None:
            endpoint = "/api/v1/query_range"
            params["start"] = str(query.start_time.timestamp())
            params["end"] = str(query.end_time.timestamp())
            if query.step is not None:
                if isinstance(query.step, timedelta):
                    params["step"] = f"{int(query.step.total_seconds())}s"
                elif isinstance(query.step, (int, float)):
                    params["step"] = f"{query.step}s"
                else:
                    params["step"] = str(query.step)
            else:
                params["step"] = "1s"
        elif query.end_time is not None:
            params["time"] = str(query.end_time.timestamp())
        elif query.start_time is not None:
            params["time"] = str(query.start_time.timestamp())

        target_url = f"{self.base_url}{endpoint}"
        start_t = time.perf_counter()

        try:
            res = await client.get(target_url, params=params)
        except (httpx.TimeoutException, httpx.NetworkError) as exc:
            raise MetricQueryBackendError(
                f"Failed to query VictoriaMetrics at {target_url}: {exc}"
            ) from exc

        latency_ms = (time.perf_counter() - start_t) * 1000.0

        if res.status_code != 200:
            raise MetricQueryBackendError(
                f"VictoriaMetrics query returned HTTP {res.status_code}: {res.text}"
            )

        try:
            body = res.json()
        except Exception as exc:
            raise QueryResponseValidationError(
                f"Invalid JSON response from VictoriaMetrics: {exc}"
            ) from exc

        samples = parse_metric_response(body, query.metric)

        logger.debug(
            "Executed metric query",
            metric=query.metric,
            run_id=str(query.run_id) if query.run_id else None,
            service=query.service,
            result_count=len(samples),
            latency_ms=round(latency_ms, 3),
        )

        return MetricQueryResult(samples=samples, count=len(samples))
