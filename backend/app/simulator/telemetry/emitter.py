from app.telemetry.schemas import TelemetryEvent


class _BufferedTelemetryEvent:
    __slots__ = ("sequence_number", "event")

    def __init__(self, sequence_number: int, event: TelemetryEvent) -> None:
        self.sequence_number = sequence_number
        self.event = event


class InMemoryTelemetryEmitter:
    """Concrete in-memory implementation of the frozen TelemetryEmitter protocol.

    Buffers synthesized TelemetryEvent objects deterministically in memory
    without external network, broker, or storage dependencies.
    """

    def __init__(self) -> None:
        self._events: list[_BufferedTelemetryEvent] = []
        self._sequence: int = 0

    async def emit(self, event: TelemetryEvent) -> None:
        """Asynchronously buffer a single TelemetryEvent with an internal sequence number."""
        self._sequence += 1
        self._events.append(_BufferedTelemetryEvent(self._sequence, event))

    def snapshot(self) -> tuple[TelemetryEvent, ...]:
        """Return an immutable tuple snapshot of all buffered TelemetryEvent objects."""
        return tuple(b.event for b in self._events)

    def clear(self) -> None:
        """Clear the internal buffer and reset the sequence counter."""
        self._events.clear()
        self._sequence = 0

    def __len__(self) -> int:
        return len(self._events)
