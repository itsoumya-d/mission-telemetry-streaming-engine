"""Sensor point registry, linear calibrations, and threshold limit evaluations."""
from dataclasses import dataclass
from enum import Enum
from typing import Dict, Tuple, Optional


class LimitState(str, Enum):
    NOMINAL = "NOMINAL"
    WARNING = "WARNING"
    CRITICAL = "CRITICAL"


@dataclass(frozen=True)
class SensorPoint:
    point_id: str
    name: str
    subsystem: str
    unit: str
    scale: float = 1.0
    bias: float = 0.0
    warn_low: Optional[float] = None
    warn_high: Optional[float] = None
    crit_low: Optional[float] = None
    crit_high: Optional[float] = None

    def calibrate(self, raw_value: float) -> float:
        """Applies linear calibration: eng_value = scale * raw + bias."""
        return (raw_value * self.scale) + self.bias

    def evaluate_limits(self, engineering_value: float) -> LimitState:
        """Determines alarm state based on critical and warning thresholds."""
        if self.crit_high is not None and engineering_value >= self.crit_high:
            return LimitState.CRITICAL
        if self.crit_low is not None and engineering_value <= self.crit_low:
            return LimitState.CRITICAL
        if self.warn_high is not None and engineering_value >= self.warn_high:
            return LimitState.WARNING
        if self.warn_low is not None and engineering_value <= self.warn_low:
            return LimitState.WARNING
        return LimitState.NOMINAL


class TelemetryRegistry:
    """Manages sensor points and evaluates telemetry payload metrics."""

    def __init__(self) -> None:
        self._sensors: Dict[str, SensorPoint] = {}

    def register(self, sensor: SensorPoint) -> None:
        self._sensors[sensor.point_id] = sensor

    def get_sensor(self, point_id: str) -> Optional[SensorPoint]:
        return self._sensors.get(point_id)

    def process_point(self, point_id: str, raw_value: float) -> Tuple[float, LimitState, SensorPoint]:
        sensor = self._sensors.get(point_id)
        if not sensor:
            raise KeyError(f"Unknown sensor point: {point_id}")
        eng_val = sensor.calibrate(raw_value)
        state = sensor.evaluate_limits(eng_val)
        return eng_val, state, sensor
