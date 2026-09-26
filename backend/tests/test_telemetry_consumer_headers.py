import uuid

import pytest

from app.telemetry.transport.consumer import parse_kafka_headers
from app.telemetry.transport.errors import (
    DuplicateHeaderError,
    InvalidHeaderError,
    MissingRequiredHeaderError,
)
from app.telemetry.transport.serialization import MAX_UINT64


def _build_valid_headers(
    run_id: str | None = None,
    scenario_id: str = "test-scenario",
    scenario_version: str = "1.0",
    reproducibility_key: str = "rep-key-123",
    seed: str = "42",
    producer_version: str | None = "v1.0.0",
) -> list[tuple[str, bytes]]:
    if run_id is None:
        run_id = str(uuid.uuid4())
    headers = [
        ("run_id", run_id.encode("utf-8")),
        ("scenario_id", scenario_id.encode("utf-8")),
        ("scenario_version", scenario_version.encode("utf-8")),
        ("reproducibility_key", reproducibility_key.encode("utf-8")),
        ("seed", seed.encode("utf-8")),
    ]
    if producer_version is not None:
        headers.append(("producer_version", producer_version.encode("utf-8")))
    return headers


def test_valid_required_headers() -> None:
    expected_run_id = uuid.uuid4()
    headers = _build_valid_headers(
        run_id=str(expected_run_id),
        scenario_id="scenario-alpha",
        scenario_version="2.1",
        reproducibility_key="rk-abc",
        seed="999",
        producer_version="step2.4",
    )
    context, producer_version = parse_kafka_headers(headers)

    assert context.run_id == expected_run_id
    assert context.scenario_id == "scenario-alpha"
    assert context.scenario_version == "2.1"
    assert context.reproducibility_key == "rk-abc"
    assert context.seed == 999
    assert producer_version == "step2.4"


def test_full_seed_max_value() -> None:
    headers = _build_valid_headers(seed=str(MAX_UINT64))
    context, _ = parse_kafka_headers(headers)
    assert context.seed == MAX_UINT64
    assert context.seed == 18446744073709551615


def test_seed_zero() -> None:
    headers = _build_valid_headers(seed="0")
    context, _ = parse_kafka_headers(headers)
    assert context.seed == 0


def test_missing_headers_sequence_is_none() -> None:
    with pytest.raises(MissingRequiredHeaderError, match="Record headers are missing"):
        parse_kafka_headers(None)


def test_missing_run_id() -> None:
    headers = [
        ("scenario_id", b"test"),
        ("scenario_version", b"1.0"),
        ("reproducibility_key", b"rk"),
        ("seed", b"1"),
    ]
    with pytest.raises(MissingRequiredHeaderError, match="Missing required execution header: 'run_id'"):
        parse_kafka_headers(headers)


def test_missing_seed() -> None:
    headers = [
        ("run_id", str(uuid.uuid4()).encode("utf-8")),
        ("scenario_id", b"test"),
        ("scenario_version", b"1.0"),
        ("reproducibility_key", b"rk"),
    ]
    with pytest.raises(MissingRequiredHeaderError, match="Missing required execution header: 'seed'"):
        parse_kafka_headers(headers)


def test_missing_scenario_id() -> None:
    headers = [
        ("run_id", str(uuid.uuid4()).encode("utf-8")),
        ("scenario_version", b"1.0"),
        ("reproducibility_key", b"rk"),
        ("seed", b"1"),
    ]
    with pytest.raises(MissingRequiredHeaderError, match="Missing required execution header: 'scenario_id'"):
        parse_kafka_headers(headers)


def test_duplicate_run_id() -> None:
    valid_id = str(uuid.uuid4())
    headers = _build_valid_headers(run_id=valid_id)
    headers.append(("run_id", valid_id.encode("utf-8")))
    with pytest.raises(DuplicateHeaderError, match="Duplicate required execution header: 'run_id'"):
        parse_kafka_headers(headers)


def test_duplicate_seed() -> None:
    headers = _build_valid_headers(seed="10")
    headers.append(("seed", b"20"))
    with pytest.raises(DuplicateHeaderError, match="Duplicate required execution header: 'seed'"):
        parse_kafka_headers(headers)


def test_duplicate_producer_version() -> None:
    headers = _build_valid_headers()
    headers.append(("producer_version", b"v2"))
    with pytest.raises(DuplicateHeaderError, match="Duplicate execution header: 'producer_version'"):
        parse_kafka_headers(headers)


def test_invalid_uuid() -> None:
    headers = _build_valid_headers(run_id="not-a-valid-uuid")
    with pytest.raises(InvalidHeaderError, match="Header 'run_id' is not a valid UUID"):
        parse_kafka_headers(headers)


def test_invalid_utf8() -> None:
    headers: list[tuple[str, bytes]] = [
        ("run_id", str(uuid.uuid4()).encode("utf-8")),
        ("scenario_id", b"\xff\xfe\xfd"),
        ("scenario_version", b"1.0"),
        ("reproducibility_key", b"rk"),
        ("seed", b"1"),
    ]
    with pytest.raises(InvalidHeaderError, match="contains invalid UTF-8 bytes"):
        parse_kafka_headers(headers)


def test_empty_scenario_id() -> None:
    headers = _build_valid_headers(scenario_id="")
    with pytest.raises(InvalidHeaderError, match="Header 'scenario_id' cannot be empty"):
        parse_kafka_headers(headers)


def test_empty_scenario_version() -> None:
    headers = _build_valid_headers(scenario_version="")
    with pytest.raises(InvalidHeaderError, match="Header 'scenario_version' cannot be empty"):
        parse_kafka_headers(headers)


def test_empty_reproducibility_key() -> None:
    headers = _build_valid_headers(reproducibility_key="")
    with pytest.raises(InvalidHeaderError, match="Header 'reproducibility_key' cannot be empty"):
        parse_kafka_headers(headers)


def test_empty_run_id() -> None:
    headers = [
        ("run_id", b""),
        ("scenario_id", b"test"),
        ("scenario_version", b"1.0"),
        ("reproducibility_key", b"rk"),
        ("seed", b"1"),
    ]
    with pytest.raises(InvalidHeaderError, match="Header 'run_id' cannot be empty"):
        parse_kafka_headers(headers)


def test_empty_seed() -> None:
    headers = [
        ("run_id", str(uuid.uuid4()).encode("utf-8")),
        ("scenario_id", b"test"),
        ("scenario_version", b"1.0"),
        ("reproducibility_key", b"rk"),
        ("seed", b""),
    ]
    with pytest.raises(InvalidHeaderError, match="Header 'seed' cannot be empty"):
        parse_kafka_headers(headers)


def test_seed_negative_rejected() -> None:
    headers = _build_valid_headers(seed="-1")
    with pytest.raises(InvalidHeaderError, match="Header 'seed' must be in range"):
        parse_kafka_headers(headers)


def test_seed_overflow_rejected() -> None:
    headers = _build_valid_headers(seed=str(MAX_UINT64 + 1))
    with pytest.raises(InvalidHeaderError, match="Header 'seed' must be in range"):
        parse_kafka_headers(headers)


def test_seed_non_integer_rejected() -> None:
    headers = _build_valid_headers(seed="3.1415")
    with pytest.raises(InvalidHeaderError, match="Header 'seed' is not a valid integer"):
        parse_kafka_headers(headers)


def test_seed_alphanumeric_rejected() -> None:
    headers = _build_valid_headers(seed="seed123")
    with pytest.raises(InvalidHeaderError, match="Header 'seed' is not a valid integer"):
        parse_kafka_headers(headers)
