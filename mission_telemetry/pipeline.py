"""Mission control telemetry ingestion and anomaly notification pipeline."""
import json
from typing import Dict, Any, List
from .packets import decode_frame, TelemetryFrame
from .telemetry_stream import TelemetryRegistry, LimitState
from .storage import TelemetryRingBuffer, S3ArchiveManager, TelemetryRecord


class MissionControlPipeline:
    """Unified telemetry ingest, calibration, anomaly alarm, and storage pipeline."""

    def __init__(self, registry: TelemetryRegistry, ring_capacity: int = 5000, archive_chunk_size: int = 25) -> None:
        self.registry = registry
        self.ring_buffer = TelemetryRingBuffer(capacity=ring_capacity)
        self.archive_manager = S3ArchiveManager(chunk_size=archive_chunk_size)
        self.alarms: List[Dict[str, Any]] = []
        self.metrics = {
            "frames_processed": 0,
            "frames_rejected": 0,
            "points_evaluated": 0,
            "nominal_count": 0,
            "warning_count": 0,
            "critical_count": 0,
        }

    def ingest_binary_frame(self, raw_bytes: bytes) -> List[TelemetryRecord]:
        """Parses binary telemetry frame, extracts metrics, evaluates limits, and streams."""
        try:
            frame = decode_frame(raw_bytes)
        except Exception:
            self.metrics["frames_rejected"] += 1
            raise

        self.metrics["frames_processed"] += 1
        return self._process_frame_payload(frame)

    def _process_frame_payload(self, frame: TelemetryFrame) -> List[TelemetryRecord]:
        data: Dict[str, float] = json.loads(frame.payload.decode("utf-8"))
        records: List[TelemetryRecord] = []

        for point_id, raw_val in data.items():
            sensor = self.registry.get_sensor(point_id)
            if not sensor:
                continue

            eng_val, state, _ = self.registry.process_point(point_id, raw_val)
            self.metrics["points_evaluated"] += 1

            if state == LimitState.NOMINAL:
                self.metrics["nominal_count"] += 1
            elif state == LimitState.WARNING:
                self.metrics["warning_count"] += 1
                self._record_alarm(point_id, eng_val, state, sensor.unit, frame.timestamp_epoch)
            elif state == LimitState.CRITICAL:
                self.metrics["critical_count"] += 1
                self._record_alarm(point_id, eng_val, state, sensor.unit, frame.timestamp_epoch)

            rec = self.ring_buffer.append(
                timestamp=frame.timestamp_epoch,
                point_id=point_id,
                subsystem=sensor.subsystem,
                raw_val=raw_val,
                eng_val=eng_val,
                unit=sensor.unit,
                limit_state=state.value,
            )
            self.archive_manager.stage_record(rec)
            records.append(rec)

        return records

    def _record_alarm(self, point_id: str, eng_val: float, state: LimitState, unit: str, timestamp: float) -> None:
        self.alarms.append({
            "timestamp": timestamp,
            "point_id": point_id,
            "engineering_value": eng_val,
            "unit": unit,
            "severity": state.value,
        })

    def flush_archives(self) -> None:
        self.archive_manager.flush()

    def get_active_alarms(self) -> List[Dict[str, Any]]:
        return list(self.alarms)
