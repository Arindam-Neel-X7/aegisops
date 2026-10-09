from __future__ import annotations

import inspect
import json
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any
from unittest.mock import MagicMock

import pytest
from pydantic import ValidationError

from app.anomaly.models import MAX_UINT64
from app.anomaly.observability import (
    SUPPORTED_OBSERVABILITY_SCHEMA_VERSION,
    FailureCategory,
    FailureRecord,
    ObservabilityCollector,
    ObservabilityContext,
    OperationalStage,
    OperationRecord,
    OperationStatus,
)


def _now() -> datetime:
    return datetime(2026, 10, 9, 12, 0, tzinfo=timezone.utc)


def _context() -> ObservabilityContext:
    return ObservabilityContext(
        run_id=uuid.UUID("11111111-1111-4111-8111-111111111111"),
        scenario_id="cpu-saturation",
        scenario_version="1.0.0",
        seed=42,
        reproducibility_key="phase3-cpu-42",
        package_id="phase3-task-3-9-comparison",
        package_version="1.0.0",
        code_revision="d9b8c9e",
        model_name="prophet",
        model_version="1.4.0",
        signal_id=uuid.UUID("22222222-2222-4222-8222-222222222222"),
        trace_id="trace-42",
    )


def _operation(**updates: object) -> OperationRecord:
    values: dict[str, object] = {
        "stage": OperationalStage.EVALUATION,
        "status": OperationStatus.SUCCESS,
        "started_at": _now(),
        "completed_at": _now() + timedelta(milliseconds=5),
        "latency_ms": 5.0,
        "context": _context(),
    }
    values.update(updates)
    return OperationRecord.model_validate(values)


def _failure(**updates: object) -> FailureRecord:
    values: dict[str, object] = {
        "stage": OperationalStage.EVALUATION,
        "category": FailureCategory.EVALUATION_FAILURE,
        "failure_type": "RuntimeError",
        "failure_message": "Evaluation failed",
        "observed_at": _now(),
        "context": _context(),
    }
    values.update(updates)
    return FailureRecord.model_validate(values)


def test_exact_schema_version_and_enum_members() -> None:
    assert SUPPORTED_OBSERVABILITY_SCHEMA_VERSION == "1.0"
    assert [value.value for value in OperationalStage] == [
        "feature_extraction",
        "model_execution",
        "calibration",
        "evaluation",
        "publication",
    ]
    assert [value.value for value in OperationStatus] == ["success", "failure"]
    assert [value.value for value in FailureCategory] == [
        "model_failure",
        "malformed_output",
        "missing_window",
        "publication_failure",
        "feature_extraction_failure",
        "calibration_failure",
        "evaluation_failure",
    ]


@pytest.mark.parametrize(
    ("model", "field", "replacement"),
    [
        (_context(), "scenario_id", "changed"),
        (_operation(), "stage", OperationalStage.PUBLICATION),
        (_failure(), "category", FailureCategory.MODEL_FAILURE),
    ],
)
def test_models_are_frozen_and_unchanged(
    model: ObservabilityContext | OperationRecord | FailureRecord,
    field: str,
    replacement: object,
) -> None:
    original = getattr(model, field)
    with pytest.raises(ValidationError):
        setattr(model, field, replacement)
    assert getattr(model, field) == original


def test_context_complete_json_round_trip() -> None:
    original = _context()
    assert (
        ObservabilityContext.model_validate_json(original.model_dump_json()) == original
    )


def test_operation_complete_json_round_trip() -> None:
    original = _operation()
    assert OperationRecord.model_validate_json(original.model_dump_json()) == original


def test_failure_complete_json_round_trip() -> None:
    original = _failure()
    assert FailureRecord.model_validate_json(original.model_dump_json()) == original


@pytest.mark.parametrize("field", ["run_id", "signal_id"])
def test_python_uuid_strings_are_rejected(field: str) -> None:
    with pytest.raises(ValidationError):
        ObservabilityContext.model_validate(
            {field: "11111111-1111-4111-8111-111111111111"}
        )


def test_json_uuid_strings_are_parsed() -> None:
    value = uuid.UUID("11111111-1111-4111-8111-111111111111")
    restored = ObservabilityContext.model_validate_json(
        json.dumps({"run_id": str(value), "signal_id": str(value)})
    )
    assert restored.run_id == value
    assert restored.signal_id == value


@pytest.mark.parametrize("field", ["started_at", "completed_at"])
def test_operation_python_timestamp_strings_are_rejected(field: str) -> None:
    values = _operation().model_dump()
    values[field] = "2026-10-09T12:00:00Z"
    with pytest.raises(ValidationError):
        OperationRecord.model_validate(values)


def test_failure_python_timestamp_string_is_rejected() -> None:
    values = _failure().model_dump()
    values["observed_at"] = "2026-10-09T12:00:00Z"
    with pytest.raises(ValidationError):
        FailureRecord.model_validate(values)


@pytest.mark.parametrize("field", ["started_at", "completed_at"])
def test_operation_naive_timestamps_are_rejected(field: str) -> None:
    values = _operation().model_dump()
    values[field] = datetime(2026, 10, 9, 12, 0)
    with pytest.raises(ValidationError):
        OperationRecord.model_validate(values)


def test_failure_naive_timestamp_is_rejected() -> None:
    values = _failure().model_dump()
    values["observed_at"] = datetime(2026, 10, 9, 12, 0)
    with pytest.raises(ValidationError):
        FailureRecord.model_validate(values)


@pytest.mark.parametrize(
    ("model_type", "values"),
    [
        (
            OperationRecord,
            {
                "stage": "evaluation",
                "status": OperationStatus.SUCCESS,
                "started_at": _now(),
                "completed_at": _now(),
                "latency_ms": 0.0,
            },
        ),
        (
            OperationRecord,
            {
                "stage": OperationalStage.EVALUATION,
                "status": "success",
                "started_at": _now(),
                "completed_at": _now(),
                "latency_ms": 0.0,
            },
        ),
        (
            FailureRecord,
            {
                "stage": "evaluation",
                "category": FailureCategory.EVALUATION_FAILURE,
                "failure_type": "RuntimeError",
                "failure_message": "failed",
                "observed_at": _now(),
            },
        ),
        (
            FailureRecord,
            {
                "stage": OperationalStage.EVALUATION,
                "category": "evaluation_failure",
                "failure_type": "RuntimeError",
                "failure_message": "failed",
                "observed_at": _now(),
            },
        ),
    ],
)
def test_python_enum_strings_are_rejected(
    model_type: type[OperationRecord] | type[FailureRecord], values: dict[str, object]
) -> None:
    with pytest.raises(ValidationError):
        model_type.model_validate(values)


def test_json_enum_strings_are_parsed() -> None:
    operation = OperationRecord.model_validate_json(_operation().model_dump_json())
    failure = FailureRecord.model_validate_json(_failure().model_dump_json())
    assert operation.stage is OperationalStage.EVALUATION
    assert operation.status is OperationStatus.SUCCESS
    assert failure.stage is OperationalStage.EVALUATION
    assert failure.category is FailureCategory.EVALUATION_FAILURE


@pytest.mark.parametrize("seed", [0, MAX_UINT64])
def test_valid_seed_boundaries(seed: int) -> None:
    assert ObservabilityContext(seed=seed).seed == seed


@pytest.mark.parametrize("seed", [-1, MAX_UINT64 + 1, True, False, 41.0, "41"])
def test_invalid_seed_boundaries(seed: object) -> None:
    with pytest.raises(ValidationError):
        ObservabilityContext(seed=seed)  # type: ignore[arg-type]


@pytest.mark.parametrize(
    "field",
    [
        "scenario_id",
        "scenario_version",
        "reproducibility_key",
        "package_id",
        "package_version",
        "code_revision",
        "model_name",
        "model_version",
        "trace_id",
    ],
)
def test_blank_context_strings_are_rejected(field: str) -> None:
    with pytest.raises(ValidationError):
        ObservabilityContext.model_validate({field: "   "})


@pytest.mark.parametrize(
    "values",
    [
        {"model_name": "prophet"},
        {"model_version": "1.4.0"},
        {"package_id": "pkg"},
        {"package_id": "pkg", "package_version": "1.0"},
        {"code_revision": "abc123"},
    ],
)
def test_incomplete_provenance_groups_are_rejected(values: dict[str, object]) -> None:
    with pytest.raises(ValidationError):
        ObservabilityContext.model_validate(values)


@pytest.mark.parametrize(
    ("model_type", "values"),
    [
        (ObservabilityContext, {"unexpected": "value"}),
        (OperationRecord, {**_operation().model_dump(), "unexpected": "value"}),
        (FailureRecord, {**_failure().model_dump(), "unexpected": "value"}),
    ],
)
def test_unknown_fields_are_rejected(
    model_type: type[ObservabilityContext]
    | type[OperationRecord]
    | type[FailureRecord],
    values: dict[str, object],
) -> None:
    with pytest.raises(ValidationError):
        model_type.model_validate(values)


@pytest.mark.parametrize("latency", [0, 0.0, 1, 1.5])
def test_valid_latency_values(latency: int | float) -> None:
    assert _operation(latency_ms=latency).latency_ms == float(latency)


@pytest.mark.parametrize(
    "latency", [True, False, "1.0", float("nan"), float("inf"), float("-inf"), -1]
)
def test_invalid_latency_values(latency: object) -> None:
    with pytest.raises(ValidationError):
        _operation(latency_ms=latency)


def test_completed_at_cannot_precede_started_at() -> None:
    with pytest.raises(ValidationError):
        _operation(completed_at=_now() - timedelta(microseconds=1))


@pytest.mark.parametrize("failure_type", ["A", "_", "A" * 128])
def test_valid_failure_type_boundaries(failure_type: str) -> None:
    assert _failure(failure_type=failure_type).failure_type == failure_type


@pytest.mark.parametrize(
    "failure_type",
    [
        "",
        "A" * 129,
        "1Error",
        "Bad Error",
        "Bad-Error",
        "Bad.Error",
        "Bad/Error",
        "Bad\\Error",
        "Bad\nError",
    ],
)
def test_invalid_failure_type_boundaries(failure_type: str) -> None:
    with pytest.raises(ValidationError):
        _failure(failure_type=failure_type)


@pytest.mark.parametrize(
    "unsafe_message",
    [
        "Traceback (most recent call last): failure",
        "password=hunter2",
        "secret value",
        "token=abc123",
        "api_key=abc123",
        "credential leaked",
        r"C:\Users\User\payload.txt",
        r"D:\AegisOps\secret.txt",
        r"\\server\share\payload.txt",
        "/etc/passwd",
        "/home/user/secret.txt",
        "/tmp/payload.json",
        "/payload.json",
        "<Widget object at 0x1234ABCD>",
        "memory address 0x1234ABCD",
        '{"raw": "payload"}',
        "[raw, payload]",
    ],
)
def test_unsafe_failure_messages_use_static_fallback(unsafe_message: str) -> None:
    assert (
        _failure(failure_message=unsafe_message).failure_message == "Operation failed"
    )


@pytest.mark.parametrize("blank_message", ["", " ", "\t", "\n", "\r\n"])
def test_blank_failure_messages_are_rejected(blank_message: str) -> None:
    with pytest.raises(ValidationError):
        _failure(failure_message=blank_message)


def test_failure_message_is_single_line_and_bounded() -> None:
    assert _failure(failure_message="safe\nmessage").failure_message == "safe message"
    message = _failure(failure_message="x" * 600).failure_message
    assert len(message) == 500
    assert message.endswith("...")


def test_measured_failure_does_not_store_exception_object() -> None:
    collector = ObservabilityCollector()
    original_exception = RuntimeError("safe failure")
    with pytest.raises(RuntimeError) as exc_info:
        collector.sync_measured_call(
            OperationalStage.MODEL_EXECUTION,
            FailureCategory.MODEL_FAILURE,
            action=lambda: (_ for _ in ()).throw(original_exception),
        )
    assert exc_info.value is original_exception
    failure = collector.snapshot_failures()[0]
    assert "exception" not in type(failure).model_fields
    assert all(value is not original_exception for value in vars(failure).values())


def test_sync_success_preserves_identity_inputs_and_cardinality() -> None:
    collector = ObservabilityCollector()
    payload = {"values": [1, 2, 3]}
    before = {"values": [1, 2, 3]}
    result = collector.sync_measured_call(
        OperationalStage.FEATURE_EXTRACTION,
        FailureCategory.FEATURE_EXTRACTION_FAILURE,
        _context(),
        action=lambda: payload,
    )
    assert result is payload
    assert payload == before
    assert len(collector.snapshot_operations()) == 1
    assert collector.snapshot_operations()[0].status is OperationStatus.SUCCESS
    assert collector.snapshot_failures() == ()


@pytest.mark.parametrize("category", list(FailureCategory))
def test_sync_failure_preserves_category_identity_and_cardinality(
    category: FailureCategory,
) -> None:
    collector = ObservabilityCollector()
    original_exception = RuntimeError("safe failure")
    with pytest.raises(RuntimeError) as exc_info:
        collector.sync_measured_call(
            OperationalStage.EVALUATION,
            category,
            action=lambda: (_ for _ in ()).throw(original_exception),
        )
    assert exc_info.value is original_exception
    assert len(collector.snapshot_operations()) == 1
    assert collector.snapshot_operations()[0].status is OperationStatus.FAILURE
    assert len(collector.snapshot_failures()) == 1
    assert collector.snapshot_failures()[0].category is category


@pytest.mark.asyncio
async def test_async_success_preserves_identity_inputs_and_cardinality() -> None:
    collector = ObservabilityCollector()
    payload = {"values": [1, 2, 3]}
    before = {"values": [1, 2, 3]}

    async def action() -> dict[str, list[int]]:
        return payload

    result = await collector.async_measured_call(
        OperationalStage.PUBLICATION,
        FailureCategory.PUBLICATION_FAILURE,
        _context(),
        action=action,
    )
    assert result is payload
    assert payload == before
    assert len(collector.snapshot_operations()) == 1
    assert collector.snapshot_operations()[0].status is OperationStatus.SUCCESS
    assert collector.snapshot_failures() == ()


@pytest.mark.parametrize("category", list(FailureCategory))
@pytest.mark.asyncio
async def test_async_failure_preserves_category_identity_and_cardinality(
    category: FailureCategory,
) -> None:
    collector = ObservabilityCollector()
    original_exception = RuntimeError("safe failure")

    async def action() -> None:
        raise original_exception

    with pytest.raises(RuntimeError) as exc_info:
        await collector.async_measured_call(
            OperationalStage.EVALUATION,
            category,
            action=action,
        )
    assert exc_info.value is original_exception
    assert len(collector.snapshot_operations()) == 1
    assert collector.snapshot_operations()[0].status is OperationStatus.FAILURE
    assert len(collector.snapshot_failures()) == 1
    assert collector.snapshot_failures()[0].category is category


def test_action_is_required_for_measured_helpers() -> None:
    assert (
        inspect.signature(ObservabilityCollector.sync_measured_call)
        .parameters["action"]
        .default
        is inspect.Parameter.empty
    )
    assert (
        inspect.signature(ObservabilityCollector.async_measured_call)
        .parameters["action"]
        .default
        is inspect.Parameter.empty
    )


def test_snapshot_mutation_cannot_change_collector_state() -> None:
    collector = ObservabilityCollector()
    collector.sync_measured_call(
        OperationalStage.EVALUATION,
        FailureCategory.EVALUATION_FAILURE,
        action=lambda: None,
    )
    snapshot = collector.snapshot_operations()
    with pytest.raises(TypeError):
        snapshot[0] = _operation()  # type: ignore[index]
    with pytest.raises(ValidationError):
        setattr(snapshot[0], "status", OperationStatus.FAILURE)
    assert collector.snapshot_operations() == snapshot


def test_structured_operation_log_has_stable_fields() -> None:
    collector = ObservabilityCollector()
    logger = MagicMock()
    collector._logger = logger
    collector.sync_measured_call(
        OperationalStage.EVALUATION,
        FailureCategory.EVALUATION_FAILURE,
        _context(),
        action=lambda: None,
    )
    logger.info.assert_called_once()
    (event,) = logger.info.call_args.args
    fields = logger.info.call_args.kwargs
    assert event == "anomaly_operation"
    assert fields["schema_version"] == "1.0"
    assert fields["stage"] == "evaluation"
    assert fields["status"] == "success"
    assert isinstance(fields["started_at"], str)
    assert isinstance(fields["completed_at"], str)
    assert fields["context"]["scenario_id"] == "cpu-saturation"


def test_structured_failure_log_is_sanitized_and_stable() -> None:
    collector = ObservabilityCollector()
    logger = MagicMock()
    collector._logger = logger
    record = _failure(failure_message=r"D:\AegisOps\token.txt")
    collector.record_failure(record)
    logger.error.assert_called_once()
    (event,) = logger.error.call_args.args
    fields = logger.error.call_args.kwargs
    assert event == "anomaly_failure"
    assert fields["stage"] == "evaluation"
    assert fields["category"] == "evaluation_failure"
    assert fields["failure_message"] == "Operation failed"
    assert "AegisOps" not in str(fields)


def test_sync_logging_failure_preserves_success_result_and_record() -> None:
    collector = ObservabilityCollector()
    collector._logger = MagicMock()
    collector._logger.info.side_effect = RuntimeError("sink failed")
    result_object: dict[str, int] = {"value": 1}
    result = collector.sync_measured_call(
        OperationalStage.EVALUATION,
        FailureCategory.EVALUATION_FAILURE,
        action=lambda: result_object,
    )
    assert result is result_object
    assert len(collector.snapshot_operations()) == 1


def test_sync_logging_failure_preserves_business_exception_and_records() -> None:
    collector = ObservabilityCollector()
    collector._logger = MagicMock()
    collector._logger.info.side_effect = RuntimeError("sink failed")
    collector._logger.error.side_effect = RuntimeError("sink failed")
    original_exception = RuntimeError("business failed")
    with pytest.raises(RuntimeError) as exc_info:
        collector.sync_measured_call(
            OperationalStage.EVALUATION,
            FailureCategory.EVALUATION_FAILURE,
            action=lambda: (_ for _ in ()).throw(original_exception),
        )
    assert exc_info.value is original_exception
    assert len(collector.snapshot_operations()) == 1
    assert len(collector.snapshot_failures()) == 1


@pytest.mark.asyncio
async def test_async_logging_failure_preserves_success_result_and_record() -> None:
    collector = ObservabilityCollector()
    collector._logger = MagicMock()
    collector._logger.info.side_effect = RuntimeError("sink failed")
    result_object: dict[str, int] = {"value": 1}

    async def action() -> dict[str, int]:
        return result_object

    result = await collector.async_measured_call(
        OperationalStage.PUBLICATION,
        FailureCategory.PUBLICATION_FAILURE,
        action=action,
    )
    assert result is result_object
    assert len(collector.snapshot_operations()) == 1


@pytest.mark.asyncio
async def test_async_logging_failure_preserves_business_exception_and_records() -> None:
    collector = ObservabilityCollector()
    collector._logger = MagicMock()
    collector._logger.info.side_effect = RuntimeError("sink failed")
    collector._logger.error.side_effect = RuntimeError("sink failed")
    original_exception = RuntimeError("business failed")

    async def action() -> None:
        raise original_exception

    with pytest.raises(RuntimeError) as exc_info:
        await collector.async_measured_call(
            OperationalStage.PUBLICATION,
            FailureCategory.PUBLICATION_FAILURE,
            action=action,
        )
    assert exc_info.value is original_exception
    assert len(collector.snapshot_operations()) == 1
    assert len(collector.snapshot_failures()) == 1


@pytest.mark.parametrize(
    ("stage", "category", "context"),
    [
        ("evaluation", FailureCategory.EVALUATION_FAILURE, None),
        (OperationalStage.EVALUATION, "evaluation_failure", None),
        (OperationalStage.EVALUATION, FailureCategory.EVALUATION_FAILURE, {}),
    ],
)
def test_invalid_measurement_metadata_does_not_run_action(
    stage: Any, category: Any, context: Any
) -> None:
    collector = ObservabilityCollector()
    action = MagicMock()
    with pytest.raises(TypeError):
        collector.sync_measured_call(stage, category, context, action=action)
    action.assert_not_called()


def test_observability_semantic_fingerprint_is_optional_and_round_trips() -> None:
    assert ObservabilityContext().semantic_fingerprint is None
    fingerprint = "a" * 64
    context = ObservabilityContext(semantic_fingerprint=fingerprint)

    assert context.semantic_fingerprint == fingerprint
    assert (
        ObservabilityContext.model_validate_json(context.model_dump_json()) == context
    )


@pytest.mark.parametrize(
    "fingerprint",
    [
        "",
        "a" * 63,
        "a" * 65,
        "A" * 64,
        "g" * 64,
        True,
        42,
        ["a" * 64],
        {"value": "a" * 64},
    ],
)
def test_observability_rejects_invalid_semantic_fingerprint(
    fingerprint: object,
) -> None:
    with pytest.raises(ValidationError):
        ObservabilityContext(semantic_fingerprint=fingerprint)  # type: ignore[arg-type]


def test_observability_semantic_fingerprint_is_frozen_and_forbids_unknown_fields() -> (
    None
):
    context = ObservabilityContext(semantic_fingerprint="b" * 64)

    with pytest.raises(ValidationError):
        context.semantic_fingerprint = "c" * 64  # type: ignore[misc]
    with pytest.raises(ValidationError):
        ObservabilityContext.model_validate(
            {"semantic_fingerprint": "b" * 64, "unexpected": "value"}
        )
