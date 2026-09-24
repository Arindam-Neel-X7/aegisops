import hashlib


def derive_child_seed(master_seed: int, stream_name: str) -> int:
    """Derive a stable 64-bit integer seed for a named subsystem stream.

    Uses SHA-256 to ensure determinism across different OS platforms,
    Python versions, and process invocations without relying on Python's
    built-in hash() randomization.
    """
    if master_seed < 0:
        raise ValueError(f"master_seed must be non-negative, got {master_seed}")
    if not stream_name:
        raise ValueError("stream_name must not be empty")

    payload = f"{master_seed}:{stream_name}".encode("utf-8")
    digest = hashlib.sha256(payload).digest()
    return int.from_bytes(digest[:8], byteorder="big", signed=False)
