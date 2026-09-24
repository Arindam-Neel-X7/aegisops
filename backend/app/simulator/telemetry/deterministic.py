import uuid

ROOT_NAMESPACE: uuid.UUID = uuid.NAMESPACE_DNS
AEGISOPS_TELEMETRY_NAMESPACE: uuid.UUID = uuid.uuid5(ROOT_NAMESPACE, "aegisops:simulator:telemetry")


def deterministic_telemetry_event_id(identity_key: str) -> uuid.UUID:
    """Derive a stable, deterministic UUIDv5 identifier for a telemetry event.

    Ensures 100% reproducible event IDs without relying on uuid.uuid4() or host state.
    """
    if not identity_key:
        raise ValueError("identity_key must not be empty")
    return uuid.uuid5(AEGISOPS_TELEMETRY_NAMESPACE, identity_key)
