from datetime import datetime
import time
from typing import Any
import uuid

import httpx
import structlog

from app.core.config import settings
from app.telemetry.persistence.search import EVIDENCE_ALIAS
from app.telemetry.query.errors import (
    EvidenceQueryBackendError,
    QueryResponseValidationError,
)
from app.telemetry.query.models import (
    EvidenceQuery,
    EvidenceQueryResult,
    EvidenceRecord,
)
from app.telemetry.schemas import EventSeverity, EventType

logger = structlog.get_logger(__name__)


def build_evidence_dsl(query: EvidenceQuery) -> dict[str, Any]:
    """Construct OpenSearch Query DSL dictionary from EvidenceQuery filters."""
    filter_clauses: list[dict[str, Any]] = []

    if query.run_id is not None:
        filter_clauses.append({"term": {"run_id": str(query.run_id)}})

    if query.seed is not None:
        filter_clauses.append({"term": {"seed": str(query.seed)}})

    if query.service is not None:
        filter_clauses.append({"term": {"service": query.service}})

    if query.event_type is not None:
        filter_clauses.append({"term": {"event_type": query.event_type.value}})

    if query.severity is not None:
        filter_clauses.append({"term": {"severity": query.severity.value}})

    if query.trace_id is not None:
        filter_clauses.append({"term": {"trace_id": query.trace_id}})

    if query.event_id is not None:
        filter_clauses.append({"term": {"event_id": str(query.event_id)}})

    if query.marker is not None:
        filter_clauses.append({"term": {"payload.marker": query.marker}})

    if query.start_time is not None or query.end_time is not None:
        range_clause: dict[str, str] = {}
        if query.start_time is not None:
            range_clause["gte"] = query.start_time.isoformat()
        if query.end_time is not None:
            range_clause["lte"] = query.end_time.isoformat()
        filter_clauses.append({"range": {"event_time": range_clause}})

    query_body: dict[str, Any] = {
        "size": query.limit,
        "sort": [
            {"event_time": {"order": "asc"}},
            {"event_id": {"order": "asc"}},
        ],
    }

    if filter_clauses:
        query_body["query"] = {"bool": {"filter": filter_clauses}}
    else:
        query_body["query"] = {"match_all": {}}

    return query_body


def parse_evidence_response(body: dict[str, Any]) -> list[EvidenceRecord]:
    """Validate and normalize OpenSearch search response into EvidenceRecords."""
    if not isinstance(body, dict):
        raise QueryResponseValidationError(
            f"Expected JSON object from OpenSearch, got {type(body).__name__}"
        )

    hits_block = body.get("hits")
    if not isinstance(hits_block, dict):
        raise QueryResponseValidationError("Missing 'hits' object in OpenSearch response")

    hits_list = hits_block.get("hits")
    if not isinstance(hits_list, list):
        raise QueryResponseValidationError("Missing 'hits.hits' list in OpenSearch response")

    records: list[EvidenceRecord] = []
    for hit in hits_list:
        if not isinstance(hit, dict):
            raise QueryResponseValidationError("Malformed hit entry in OpenSearch response")
        source = hit.get("_source")
        if not isinstance(source, dict):
            raise QueryResponseValidationError("Missing or invalid '_source' in OpenSearch hit")

        try:
            schema_version = str(source.get("schema_version", "1.0"))
            event_id = uuid.UUID(source["event_id"])
            event_time = datetime.fromisoformat(source["event_time"])
            tenant_id = uuid.UUID(source["tenant_id"])
            environment = source["environment"]
            service = source["service"]
            event_type = EventType(source["event_type"])
            severity = EventSeverity(source["severity"])
            trace_id = source.get("trace_id")
            payload = dict(source.get("payload", {}))
            run_id = uuid.UUID(source["run_id"])
            scenario_id = source["scenario_id"]
            scenario_version = source["scenario_version"]
            reproducibility_key = source["reproducibility_key"]
            seed = int(source["seed"])
            ingested_at = datetime.fromisoformat(source["ingested_at"])
        except (KeyError, ValueError, TypeError) as exc:
            raise QueryResponseValidationError(
                f"Failed to parse required fields from OpenSearch _source: {exc}"
            ) from exc

        record = EvidenceRecord(
            schema_version=schema_version,
            event_id=event_id,
            event_time=event_time,
            tenant_id=tenant_id,
            environment=environment,
            service=service,
            event_type=event_type,
            severity=severity,
            trace_id=trace_id,
            payload=payload,
            run_id=run_id,
            scenario_id=scenario_id,
            scenario_version=scenario_version,
            reproducibility_key=reproducibility_key,
            seed=seed,
            ingested_at=ingested_at,
        )
        records.append(record)

    # Sort deterministically: event_time ASC, then event_id ASC
    records.sort(key=lambda r: (r.event_time, r.event_id))
    return records


class EvidenceQueryAdapter:
    """Async query adapter retrieving normalized LOG and SYSTEM evidence records from OpenSearch."""

    def __init__(
        self,
        base_url: str = settings.OPENSEARCH_URL,
        user: str | None = settings.OPENSEARCH_USER,
        password: str | None = settings.OPENSEARCH_PASSWORD,
        timeout_seconds: float = settings.OPENSEARCH_TIMEOUT_SECONDS,
        index_alias: str = EVIDENCE_ALIAS,
        client: httpx.AsyncClient | None = None,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.user = user
        self.password = password
        self.timeout_seconds = timeout_seconds
        self.index_alias = index_alias
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

    async def __aenter__(self) -> "EvidenceQueryAdapter":
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: Any,
    ) -> None:
        await self.close()

    async def query(self, query: EvidenceQuery) -> EvidenceQueryResult:
        """Execute an EvidenceQuery against OpenSearch alias and return normalized EvidenceQueryResult."""
        dsl = build_evidence_dsl(query)
        client = await self._get_client()

        target_url = f"{self.base_url}/{self.index_alias}/_search"
        start_t = time.perf_counter()

        try:
            res = await client.post(
                target_url,
                json=dsl,
                headers={"Content-Type": "application/json"},
            )
        except (httpx.TimeoutException, httpx.NetworkError) as exc:
            raise EvidenceQueryBackendError(
                f"Failed to query OpenSearch at {target_url}: {exc}"
            ) from exc

        latency_ms = (time.perf_counter() - start_t) * 1000.0

        if res.status_code != 200:
            raise EvidenceQueryBackendError(
                f"OpenSearch search query returned HTTP {res.status_code}: {res.text}"
            )

        try:
            body = res.json()
        except Exception as exc:
            raise QueryResponseValidationError(
                f"Invalid JSON response from OpenSearch: {exc}"
            ) from exc

        records = parse_evidence_response(body)

        logger.debug(
            "Executed evidence query",
            run_id=str(query.run_id) if query.run_id else None,
            service=query.service,
            event_type=query.event_type.value if query.event_type else None,
            result_count=len(records),
            latency_ms=round(latency_ms, 3),
        )

        return EvidenceQueryResult(records=records, count=len(records))
