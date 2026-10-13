from __future__ import annotations

import json
import math
from typing import Any
import pytest
from pydantic import ValidationError

import app.correlation.config as config_module
from app.correlation.config import (
    MAX_GROUP_SIZE,
    MAX_HOP_LIMIT,
    MAX_LATENESS_SECONDS,
    MAX_QUIET_PERIOD_SECONDS,
    MAX_WINDOW_SECONDS,
    MIN_GROUP_SIZE,
    MIN_HOP_LIMIT,
    MIN_LATENESS_SECONDS,
    MIN_QUIET_PERIOD_SECONDS,
    MIN_WINDOW_SECONDS,
    SUPPORTED_CORRELATION_CONFIG_SCHEMA_VERSION,
    WEIGHT_SUM_TOLERANCE,
    ConflictingSignalDisposition,
    CorrelationConfig,
    CorrelationScoreWeights,
    DisconnectedSignalDisposition,
    DuplicateSignalDisposition,
    InsufficientContextDisposition,
    LateSignalDisposition,
    SeverityAggregationPolicy,
    compute_correlation_config_fingerprint,
    deserialize_correlation_config,
    serialize_correlation_config,
)
from app.correlation.errors import CorrelationConfigurationError
from app.correlation.models import MAX_POLICY_LEN


def _valid_weights(**overrides) -> dict[str, object]:
    base: dict[str, object] = {
        "temporal": 0.3,
        "service": 0.2,
        "topology": 0.2,
        "symptom": 0.2,
        "context": 0.1,
    }
    base.update(overrides)
    return base


def _valid_config_dict(**overrides) -> dict[str, object]:
    base: dict[str, object] = {
        "schema_version": SUPPORTED_CORRELATION_CONFIG_SCHEMA_VERSION,
        "policy_version": "v1.0",
        "window_seconds": 300,
        "allowed_lateness_seconds": 30,
        "quiet_period_seconds": 10,
        "hop_limit": 3,
        "score_weights": _valid_weights(),
        "acceptance_threshold": 0.75,
        "severity_aggregation": SeverityAggregationPolicy.MAXIMUM,
        "maximum_group_size": 100,
        "late_signal_disposition": LateSignalDisposition.DELAY,
        "duplicate_signal_disposition": DuplicateSignalDisposition.IGNORE,
        "conflicting_signal_disposition": ConflictingSignalDisposition.REJECT,
        "disconnected_signal_disposition": DisconnectedSignalDisposition.ISOLATE,
        "insufficient_context_disposition": InsufficientContextDisposition.IGNORE,
    }
    base.update(overrides)
    return base


def _valid_config(**overrides) -> CorrelationConfig:
    return CorrelationConfig.model_validate(_valid_config_dict(**overrides))


# 5. Restored pre-C1 contract tests
def test_valid_python_construction() -> None:
    weights = CorrelationScoreWeights.model_validate(_valid_weights())
    config = CorrelationConfig.model_validate(_valid_config_dict(score_weights=weights))
    assert config.schema_version == SUPPORTED_CORRELATION_CONFIG_SCHEMA_VERSION
    assert config.window_seconds == 300


def test_valid_json_construction() -> None:
    d = _valid_config_dict()
    raw_json = json.dumps(d)
    config = CorrelationConfig.model_validate_json(raw_json)
    assert config.window_seconds == 300


def test_model_dump_validate_equality() -> None:
    c1 = _valid_config()
    d = c1.model_dump(mode="json")
    c2 = CorrelationConfig.model_validate_json(json.dumps(d))
    assert c1 == c2


def test_json_round_trip_equality() -> None:
    c1 = _valid_config()
    bytes_data = serialize_correlation_config(c1)
    c2 = deserialize_correlation_config(bytes_data)
    assert c1 == c2


def test_canonical_byte_equality_after_round_trip() -> None:
    c1 = _valid_config()
    b1 = serialize_correlation_config(c1)
    c2 = deserialize_correlation_config(b1)
    assert b1 == serialize_correlation_config(c2)


def test_stable_fingerprint() -> None:
    c1 = _valid_config()
    f1 = compute_correlation_config_fingerprint(c1)
    c2 = deserialize_correlation_config(serialize_correlation_config(c1))
    f2 = compute_correlation_config_fingerprint(c2)
    assert f1 == f2
    assert len(f1) == 64
    assert all(c in "0123456789abcdef" for c in f1)


def test_model_immutability() -> None:
    c = _valid_config()
    with pytest.raises(ValidationError):
        c.window_seconds = 600  # type: ignore[misc]


def test_unknown_field_rejected() -> None:
    d = _valid_config_dict()
    d["unknown_field_xyz"] = 123
    with pytest.raises(ValidationError):
        CorrelationConfig.model_validate(d)


def test_strict_enum_behavior() -> None:
    with pytest.raises(ValidationError):
        CorrelationConfig.model_validate(
            _valid_config_dict(severity_aggregation="maximum")
        )


def test_unicode_determinism() -> None:
    c1 = _valid_config(policy_version="v1.0-é")
    b1 = serialize_correlation_config(c1)
    c2 = deserialize_correlation_config(b1)
    assert c1 == c2


def test_caller_input_unmutated() -> None:
    w_dict = _valid_weights()
    weights = CorrelationScoreWeights.model_validate(w_dict)
    assert weights.temporal == 0.3
    w_dict["temporal"] = 0.99
    assert weights.temporal == 0.3


# 5.1 Integer boundaries
@pytest.mark.parametrize(
    "field,min_v,max_v",
    [
        ("window_seconds", MIN_WINDOW_SECONDS, MAX_WINDOW_SECONDS),
        ("allowed_lateness_seconds", MIN_LATENESS_SECONDS, MAX_LATENESS_SECONDS),
        ("quiet_period_seconds", MIN_QUIET_PERIOD_SECONDS, MAX_QUIET_PERIOD_SECONDS),
        ("hop_limit", MIN_HOP_LIMIT, MAX_HOP_LIMIT),
        ("maximum_group_size", MIN_GROUP_SIZE, MAX_GROUP_SIZE),
    ],
)
def test_integer_boundaries(field: str, min_v: int, max_v: int) -> None:
    c_min = _valid_config(**{field: min_v})
    assert getattr(c_min, field) == min_v

    c_max = _valid_config(**{field: max_v})
    assert getattr(c_max, field) == max_v

    with pytest.raises(ValidationError):
        _valid_config(**{field: min_v - 1})

    with pytest.raises(ValidationError):
        _valid_config(**{field: max_v + 1})

    with pytest.raises(ValidationError):
        _valid_config(**{field: True})

    with pytest.raises(ValidationError):
        _valid_config(**{field: False})

    with pytest.raises(ValidationError):
        _valid_config(**{field: None})  # type: ignore[arg-type]

    with pytest.raises(ValidationError):
        _valid_config(**{field: str(min_v)})

    with pytest.raises(ValidationError):
        _valid_config(**{field: float(min_v)})


# 5.2 Acceptance threshold
def test_acceptance_threshold_boundaries() -> None:
    c_min = _valid_config(acceptance_threshold=0.0)
    assert c_min.acceptance_threshold == 0.0

    c_max = _valid_config(acceptance_threshold=1.0)
    assert c_max.acceptance_threshold == 1.0

    with pytest.raises(ValidationError):
        _valid_config(acceptance_threshold=-0.0001)

    with pytest.raises(ValidationError):
        _valid_config(acceptance_threshold=1.0001)

    for invalid_val in [True, False, None, 1, 0, "0.5", math.nan, math.inf, -math.inf]:
        with pytest.raises(ValidationError):
            _valid_config(acceptance_threshold=invalid_val)  # type: ignore[arg-type]


# 5.3 Weight field parameterization
@pytest.mark.parametrize(
    "field", ["temporal", "service", "topology", "symptom", "context"]
)
def test_weight_fields(field: str) -> None:
    valid_weights = _valid_weights()
    c = _valid_config(score_weights=valid_weights)
    assert getattr(c.score_weights, field) == valid_weights[field]

    w0_dict = {
        f: 0.0 if f == field else 0.25
        for f in ["temporal", "service", "topology", "symptom", "context"]
    }
    c0 = _valid_config(score_weights=w0_dict)
    assert getattr(c0.score_weights, field) == 0.0

    w1_dict = {
        f: 1.0 if f == field else 0.0
        for f in ["temporal", "service", "topology", "symptom", "context"]
    }
    c1 = _valid_config(score_weights=w1_dict)
    assert getattr(c1.score_weights, field) == 1.0

    with pytest.raises(ValidationError):
        _valid_config(score_weights=_valid_weights(**{field: -0.1}))

    with pytest.raises(ValidationError):
        _valid_config(score_weights=_valid_weights(**{field: 1.1}))

    for invalid_val in [True, False, None, 1, 0, "0.5", math.nan, math.inf, -math.inf]:
        with pytest.raises(ValidationError):
            _valid_config(score_weights=_valid_weights(**{field: invalid_val}))  # type: ignore[arg-type]


# 5.4 Genuine normalization-tolerance boundary
def test_weight_normalization_tolerance_genuine() -> None:
    # Accepted total using math.nextafter around 1.0 + WEIGHT_SUM_TOLERANCE
    acc_target = math.nextafter(1.0 + WEIGHT_SUM_TOLERANCE, 1.0)
    # Distribute acc_target across temporal and service, keeping rest standard
    rem = acc_target - 0.8
    weights_ok = {
        "temporal": rem,
        "service": 0.2,
        "topology": 0.2,
        "symptom": 0.2,
        "context": 0.2,
    }
    acc_diff = abs(math.fsum(weights_ok.values()) - 1.0)
    assert acc_diff <= WEIGHT_SUM_TOLERANCE
    CorrelationScoreWeights.model_validate(weights_ok)

    # Rejected total using math.nextafter above tolerance limit
    rej_target = math.nextafter(1.0 + WEIGHT_SUM_TOLERANCE, math.inf)
    rem_rej = rej_target - 0.8
    weights_bad = {
        "temporal": rem_rej,
        "service": 0.2,
        "topology": 0.2,
        "symptom": 0.2,
        "context": 0.2,
    }
    rej_diff = abs(math.fsum(weights_bad.values()) - 1.0)
    assert rej_diff > WEIGHT_SUM_TOLERANCE
    with pytest.raises(ValidationError):
        CorrelationScoreWeights.model_validate(weights_bad)


def test_weight_normalization_all_zero() -> None:
    with pytest.raises(ValidationError):
        CorrelationScoreWeights.model_validate(
            {
                "temporal": 0.0,
                "service": 0.0,
                "topology": 0.0,
                "symptom": 0.0,
                "context": 0.0,
            }
        )


# 6. Disposition matrix and byte sensitivity
@pytest.mark.parametrize(
    "field,enum_cls,valid_vals",
    [
        (
            "late_signal_disposition",
            LateSignalDisposition,
            [
                LateSignalDisposition.REJECT,
                LateSignalDisposition.ACCEPT,
                LateSignalDisposition.DELAY,
            ],
        ),
        (
            "duplicate_signal_disposition",
            DuplicateSignalDisposition,
            [DuplicateSignalDisposition.IGNORE, DuplicateSignalDisposition.REJECT],
        ),
        (
            "conflicting_signal_disposition",
            ConflictingSignalDisposition,
            [ConflictingSignalDisposition.REJECT, ConflictingSignalDisposition.ISOLATE],
        ),
        (
            "disconnected_signal_disposition",
            DisconnectedSignalDisposition,
            [
                DisconnectedSignalDisposition.ISOLATE,
                DisconnectedSignalDisposition.CORRELATE_TEMPORAL,
                DisconnectedSignalDisposition.REJECT,
            ],
        ),
        (
            "insufficient_context_disposition",
            InsufficientContextDisposition,
            [
                InsufficientContextDisposition.IGNORE,
                InsufficientContextDisposition.ISOLATE,
                InsufficientContextDisposition.ENRICH,
            ],
        ),
    ],
)
def test_dispositions_matrix_and_sensitivity(
    field: str, enum_cls: type, valid_vals: list[Any]
) -> None:
    baseline_config = _valid_config()
    baseline_bytes = serialize_correlation_config(baseline_config)

    for val in valid_vals:
        c = _valid_config(**{field: val})
        assert getattr(c, field) == val

        d_dict = _valid_config_dict(**{field: val.value})
        c_json = CorrelationConfig.model_validate_json(json.dumps(d_dict))
        assert getattr(c_json, field) == val

        with pytest.raises(ValidationError):
            CorrelationConfig.model_validate(_valid_config_dict(**{field: val.value}))

        with pytest.raises(ValidationError):
            CorrelationConfig.model_validate(
                _valid_config_dict(**{field: "invalid_xyz"})
            )

        # Assert disposition change alters canonical serialized bytes if value differs from baseline
        if val != getattr(baseline_config, field):
            changed_bytes = serialize_correlation_config(c)
            assert changed_bytes != baseline_bytes


def test_removed_dispositions_absent() -> None:
    assert not hasattr(DuplicateSignalDisposition, "AGGREGATE")
    assert not hasattr(ConflictingSignalDisposition, "OVERWRITE")

    with pytest.raises(ValidationError):
        CorrelationConfig.model_validate(
            _valid_config_dict(duplicate_signal_disposition="aggregate")
        )

    with pytest.raises(ValidationError):
        CorrelationConfig.model_validate(
            _valid_config_dict(conflicting_signal_disposition="overwrite")
        )


# 7. Secret/path rejection and non-disclosure
@pytest.mark.parametrize(
    "sentinel",
    [
        "password123",
        "passwd123",
        "my_secret_token",
        "Bearer abc123xyz",
        "api_key_secret",
        "api-key-123",
        "apikeyabc",
        "credential_xyz",
        "access_key_123",
        "private_key_pem",
        "token=secretvalue",
        "C:\\Users\\User\\private\\config.json",
        "C:/Users/User/private/config.json",
        "D:\\AegisOps\\private.txt",
        "D:/AegisOps/private.txt",
        "Z:\\private",
        "Z:/private",
        "\\\\server\\share\\foo",
        "//server/share/foo",
        "/home/user/secrets",
        "/etc/passwd",
        "file:///etc/passwd",
        "http://example.com",
        "https://example.com",
    ],
)
def test_secret_and_path_non_disclosure(sentinel: str) -> None:
    with pytest.raises(ValidationError) as exc_info:
        _valid_config(policy_version=sentinel)

    err_str = str(exc_info.value)
    assert sentinel not in err_str


def test_non_disclosure_other_validation_errors() -> None:
    oversized = "a" * (MAX_POLICY_LEN + 1)
    with pytest.raises(ValidationError) as exc_info:
        _valid_config(policy_version=oversized)
    assert oversized not in str(exc_info.value)

    bad_schema = "9.9.9"
    with pytest.raises(ValidationError) as exc_info2:
        _valid_config(schema_version=bad_schema)
    assert bad_schema not in str(exc_info2.value)

    ctrl = "v1.0\x00"
    with pytest.raises(ValidationError) as exc_info3:
        _valid_config(policy_version=ctrl)
    assert ctrl not in str(exc_info3.value)


# 8. Sentinel-based exact exception-cause identity tests
def test_exact_cause_identity_serialization(monkeypatch: pytest.MonkeyPatch) -> None:
    original_exception = RuntimeError("serialization sentinel")

    def mock_dumps(*args, **kwargs):
        raise original_exception

    monkeypatch.setattr(config_module.json, "dumps", mock_dumps)

    c = _valid_config()
    with pytest.raises(CorrelationConfigurationError) as exc_info:
        serialize_correlation_config(c)
    assert str(exc_info.value) == "Failed to serialize CorrelationConfig"
    assert exc_info.value.__cause__ is original_exception
    assert exc_info.value.__suppress_context__ is True


def test_exact_cause_identity_json_parsing(monkeypatch: pytest.MonkeyPatch) -> None:
    original_exception = RuntimeError("json parsing sentinel")

    def mock_loads(*args, **kwargs):
        raise original_exception

    monkeypatch.setattr(config_module.json, "loads", mock_loads)

    with pytest.raises(CorrelationConfigurationError) as exc_info:
        deserialize_correlation_config(b'{"schema_version":"1.0.0"}')
    assert str(exc_info.value) == "Invalid JSON payload"
    assert exc_info.value.__cause__ is original_exception
    assert exc_info.value.__suppress_context__ is True


def test_exact_cause_identity_model_validation(monkeypatch: pytest.MonkeyPatch) -> None:
    original_exception = ValidationError.from_exception_data("Model", [])

    def mock_validate_json(*args, **kwargs):
        raise original_exception

    monkeypatch.setattr(CorrelationConfig, "model_validate_json", mock_validate_json)

    with pytest.raises(CorrelationConfigurationError) as exc_info:
        deserialize_correlation_config(b'{"schema_version":"1.0.0"}')
    assert str(exc_info.value) == "CorrelationConfig validation failed"
    assert exc_info.value.__cause__ is original_exception
    assert exc_info.value.__suppress_context__ is True


def test_exact_cause_identity_fingerprint(monkeypatch: pytest.MonkeyPatch) -> None:
    original_exception = RuntimeError("fingerprint serialization sentinel")

    def mock_serialize(*args, **kwargs):
        raise original_exception

    monkeypatch.setattr(config_module, "serialize_correlation_config", mock_serialize)

    c = _valid_config()
    with pytest.raises(CorrelationConfigurationError) as exc_info:
        compute_correlation_config_fingerprint(c)
    assert str(exc_info.value) == "Failed to compute configuration fingerprint"
    assert exc_info.value.__cause__ is original_exception
    assert exc_info.value.__suppress_context__ is True


# 9. Input immutability during actual failures
def test_input_immutability_during_serialization_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    c = _valid_config()
    c_dump_before = c.model_dump(mode="json")

    def mock_dumps(*args, **kwargs):
        raise RuntimeError("fail dumps")

    monkeypatch.setattr(config_module.json, "dumps", mock_dumps)

    with pytest.raises(CorrelationConfigurationError):
        serialize_correlation_config(c)

    assert c.model_dump(mode="json") == c_dump_before


def test_input_immutability_during_fingerprint_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    c = _valid_config()
    c_dump_before = c.model_dump(mode="json")

    def mock_serialize(*args, **kwargs):
        raise RuntimeError("fail serialize")

    monkeypatch.setattr(config_module, "serialize_correlation_config", mock_serialize)

    with pytest.raises(CorrelationConfigurationError):
        compute_correlation_config_fingerprint(c)

    assert c.model_dump(mode="json") == c_dump_before


def test_mutable_buffer_immutability_on_deserialization_failure() -> None:
    buf = bytearray(b"invalid payload not starting with brace")
    buf_snapshot = bytes(buf)

    with pytest.raises(CorrelationConfigurationError):
        deserialize_correlation_config(buf)

    assert bytes(buf) == buf_snapshot


def test_deserialization_other_error_boundaries() -> None:
    with pytest.raises(CorrelationConfigurationError):
        deserialize_correlation_config("not bytes-like")  # type: ignore[arg-type]

    with pytest.raises(CorrelationConfigurationError):
        deserialize_correlation_config(b"\xff\xfe\xfd")

    with pytest.raises(CorrelationConfigurationError):
        deserialize_correlation_config(b"[]")


# 10. Every-field fingerprint sensitivity (R1B Blocker 1)


def _baseline_config() -> CorrelationConfig:
    return CorrelationConfig.model_validate(_valid_config_dict())


def _assert_validated_sensitivity(
    baseline: CorrelationConfig, mutated: dict[str, object]
) -> CorrelationConfig:
    baseline_bytes = serialize_correlation_config(baseline)
    baseline_fingerprint = compute_correlation_config_fingerprint(baseline)
    candidate = CorrelationConfig.model_validate(mutated)
    changed_bytes = serialize_correlation_config(candidate)
    changed_fingerprint = compute_correlation_config_fingerprint(candidate)
    assert changed_bytes != baseline_bytes
    assert changed_fingerprint != baseline_fingerprint
    assert serialize_correlation_config(baseline) == baseline_bytes
    assert compute_correlation_config_fingerprint(baseline) == baseline_fingerprint
    return candidate


@pytest.mark.parametrize(
    "field,changed",
    [
        ("policy_version", "v1.1"),
        ("window_seconds", 301),
        ("allowed_lateness_seconds", 31),
        ("quiet_period_seconds", 11),
        ("hop_limit", 4),
        ("acceptance_threshold", 0.76),
        ("maximum_group_size", 101),
    ],
)
def test_fingerprint_sensitivity_scalar_fields(field: str, changed: object) -> None:
    baseline = _baseline_config()
    mutated = _valid_config_dict(**{field: changed})
    candidate = _assert_validated_sensitivity(baseline, mutated)
    assert getattr(candidate, field) == changed


@pytest.mark.parametrize(
    "field,changed",
    [
        ("late_signal_disposition", LateSignalDisposition.REJECT),
        ("late_signal_disposition", LateSignalDisposition.ACCEPT),
        (
            "duplicate_signal_disposition",
            DuplicateSignalDisposition.REJECT,
        ),
        (
            "conflicting_signal_disposition",
            ConflictingSignalDisposition.ISOLATE,
        ),
        (
            "disconnected_signal_disposition",
            DisconnectedSignalDisposition.CORRELATE_TEMPORAL,
        ),
        (
            "disconnected_signal_disposition",
            DisconnectedSignalDisposition.REJECT,
        ),
        (
            "insufficient_context_disposition",
            InsufficientContextDisposition.ISOLATE,
        ),
        (
            "insufficient_context_disposition",
            InsufficientContextDisposition.ENRICH,
        ),
    ],
)
def test_fingerprint_sensitivity_dispositions(field: str, changed: object) -> None:
    baseline = _baseline_config()
    assert getattr(baseline, field) != changed
    mutated = _valid_config_dict(**{field: changed})
    candidate = _assert_validated_sensitivity(baseline, mutated)
    assert getattr(candidate, field) == changed


@pytest.mark.parametrize(
    "target,target_value,balancer,balancer_value",
    [
        ("temporal", 0.31, "service", 0.19),
        ("service", 0.21, "temporal", 0.29),
        ("topology", 0.21, "temporal", 0.29),
        ("symptom", 0.21, "temporal", 0.29),
        ("context", 0.11, "temporal", 0.29),
    ],
)
def test_fingerprint_sensitivity_score_weights(
    target: str, target_value: float, balancer: str, balancer_value: float
) -> None:
    baseline = _baseline_config()
    weights: dict[str, object] = {
        "temporal": 0.3,
        "service": 0.2,
        "topology": 0.2,
        "symptom": 0.2,
        "context": 0.1,
    }
    weights[target] = target_value
    weights[balancer] = balancer_value
    mutated = _valid_config_dict(score_weights=weights)
    candidate = _assert_validated_sensitivity(baseline, mutated)
    assert getattr(candidate.score_weights, target) == target_value
    assert getattr(candidate.score_weights, balancer) == balancer_value


def test_fingerprint_schema_version_fixed_discriminator() -> None:
    baseline = _baseline_config()
    baseline_bytes = serialize_correlation_config(baseline)
    assert '"schema_version":"1.0.0"' in baseline_bytes.decode("utf-8")
    assert baseline.schema_version == SUPPORTED_CORRELATION_CONFIG_SCHEMA_VERSION
    with pytest.raises(ValidationError):
        CorrelationConfig.model_validate(_valid_config_dict(schema_version="2.0.0"))
    with pytest.raises(ValidationError):
        CorrelationConfig.model_validate(_valid_config_dict(schema_version="9.9.9"))
    assert serialize_correlation_config(baseline) == baseline_bytes


def test_fingerprint_severity_aggregation_single_value_discriminator() -> None:
    baseline = _baseline_config()
    assert baseline.severity_aggregation == SeverityAggregationPolicy.MAXIMUM
    baseline_bytes = serialize_correlation_config(baseline)
    assert '"severity_aggregation":"maximum"' in baseline_bytes.decode("utf-8")
    with pytest.raises(ValidationError):
        CorrelationConfig.model_validate(
            _valid_config_dict(severity_aggregation="minimum")
        )
    assert serialize_correlation_config(baseline) == baseline_bytes


def test_fingerprint_baseline_immutability_across_mutations() -> None:
    baseline = _baseline_config()
    baseline_bytes = serialize_correlation_config(baseline)
    baseline_fingerprint = compute_correlation_config_fingerprint(baseline)
    mutations: list[dict[str, object]] = [
        _valid_config_dict(policy_version="v1.1"),
        _valid_config_dict(window_seconds=301),
        _valid_config_dict(allowed_lateness_seconds=31),
        _valid_config_dict(quiet_period_seconds=11),
        _valid_config_dict(hop_limit=4),
        _valid_config_dict(acceptance_threshold=0.76),
        _valid_config_dict(maximum_group_size=101),
        _valid_config_dict(late_signal_disposition=LateSignalDisposition.REJECT),
        _valid_config_dict(
            duplicate_signal_disposition=DuplicateSignalDisposition.REJECT
        ),
        _valid_config_dict(
            conflicting_signal_disposition=ConflictingSignalDisposition.ISOLATE
        ),
        _valid_config_dict(
            disconnected_signal_disposition=DisconnectedSignalDisposition.REJECT
        ),
        _valid_config_dict(
            insufficient_context_disposition=InsufficientContextDisposition.ENRICH
        ),
    ]
    for mutated in mutations:
        _assert_validated_sensitivity(baseline, mutated)
    assert serialize_correlation_config(baseline) == baseline_bytes
    assert compute_correlation_config_fingerprint(baseline) == baseline_fingerprint
    assert baseline == _baseline_config()
