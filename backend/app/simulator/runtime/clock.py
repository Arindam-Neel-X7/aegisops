from datetime import datetime, timedelta


class SimulationClock:
    """Deterministic virtual simulation clock driven solely by explicit simulation steps."""

    def __init__(
        self,
        start_time: datetime,
        tick_seconds: float = 1.0,
    ) -> None:
        if start_time.tzinfo is None:
            raise ValueError("start_time must be timezone-aware")
        if tick_seconds <= 0.0:
            raise ValueError(f"tick_seconds must be positive, got {tick_seconds}")

        self.start_time: datetime = start_time
        self.tick_seconds: float = tick_seconds
        self.current_tick: int = 0

    @property
    def current_offset_seconds(self) -> float:
        """Return the current virtual simulation elapsed time in seconds."""
        return round(float(self.current_tick) * self.tick_seconds, 6)

    @property
    def current_time(self) -> datetime:
        """Return the current virtual simulation timezone-aware UTC datetime."""
        return self.start_time + timedelta(seconds=self.current_offset_seconds)

    def advance(self) -> float:
        """Advance virtual time by exactly one tick and return the new offset in seconds."""
        self.current_tick += 1
        return self.current_offset_seconds

    def reset(self) -> None:
        """Reset virtual clock back to tick index zero."""
        self.current_tick = 0
