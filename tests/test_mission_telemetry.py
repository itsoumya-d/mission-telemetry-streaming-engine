"""Unit tests for the Mission Telemetry Streaming Engine."""
import json
import pytest
from mission_telemetry.packets import encode_frame, decode_frame, FrameValidationError, calculate_crc16
from mission_telemetry.telemetry_stream import LimitState, SensorPoint, TelemetryRegistry
from mission_telemetry.storage import TelemetryRingBuffer, S3ArchiveManager
from mission_telemetry.pipeline import MissionControlPipeline


def test_crc_calculation_and_corrupt_frame_rejection():
    payload = b'{"PROP_PRESS": 12.5}'
    frame_bytes = encode_frame(subsystem_id=2, sequence_id=101, timestamp_epoch=1700000000.0, payload=payload)
    decoded = decode_frame(frame_bytes)

    assert decoded.subsystem_id == 2
    assert decoded.sequence_id == 101
    assert decoded.payload == payload

    # Corrupt one payload byte
    corrupted = bytearray(frame_bytes)
    corrupted[22] ^= 0xFF
    with pytest.raises(FrameValidationError, match="CRC mismatch"):
        decode_frame(bytes(corrupted))


def test_invalid_sync_marker_rejection():
    payload = b'{"BATT_V": 28.2}'
    frame_bytes = encode_frame(subsystem_id=4, sequence_id=1, timestamp_epoch=1700000000.0, payload=payload)
    corrupted = bytearray(frame_bytes)
    corrupted[0] = 0x00  # corrupt sync marker

    with pytest.raises(FrameValidationError, match="Invalid sync marker"):
        decode_frame(bytes(corrupted))


def test_telemetry_point_calibration():
    sensor = SensorPoint(
        point_id="PROP_CHAMBER_PRESS",
        name="Combustion Chamber Pressure",
        subsystem="propulsion",
        unit="bar",
        scale=0.1,
        bias=-1.0,
    )
    # raw 100 -> 100 * 0.1 - 1.0 = 9.0 bar
    eng = sensor.calibrate(100.0)
    assert pytest.approx(eng) == 9.0


def test_limit_states_evaluation():
    sensor = SensorPoint(
        point_id="AVIONICS_TEMP",
        name="Main Flight Computer Temperature",
        subsystem="avionics",
        unit="degC",
        warn_low=-10.0,
        warn_high=50.0,
        crit_low=-20.0,
        crit_high=65.0,
    )
    assert sensor.evaluate_limits(25.0) == LimitState.NOMINAL
    assert sensor.evaluate_limits(52.0) == LimitState.WARNING
    assert sensor.evaluate_limits(66.0) == LimitState.CRITICAL
    assert sensor.evaluate_limits(-15.0) == LimitState.WARNING
    assert sensor.evaluate_limits(-25.0) == LimitState.CRITICAL


def test_registry_processing():
    registry = TelemetryRegistry()
    registry.register(SensorPoint(
        point_id="BATT_VOLTAGE",
        name="Bus Voltage",
        subsystem="power",
        unit="V",
        scale=1.0,
        warn_low=24.0,
        crit_low=22.0,
    ))

    eng_val, state, _ = registry.process_point("BATT_VOLTAGE", 23.5)
    assert eng_val == 23.5
    assert state == LimitState.WARNING

    with pytest.raises(KeyError):
        registry.process_point("UNKNOWN_SENSOR", 10.0)


def test_ring_buffer_streaming_and_offsets():
    ring = TelemetryRingBuffer(capacity=5)
    for i in range(10):
        ring.append(
            timestamp=1700000000.0 + i,
            point_id=f"S_{i}",
            subsystem="propulsion" if i % 2 == 0 else "power",
            raw_val=float(i),
            eng_val=float(i * 2),
            unit="unit",
            limit_state="NOMINAL",
        )

    assert len(ring) == 5
    assert ring.latest_offset == 10

    # Fetch records after offset 7
    read_recs = ring.read_from(start_offset=7, limit=10)
    offsets = [r.offset for r in read_recs]
    assert offsets == [8, 9, 10]


def test_s3_chunked_archive_hashing():
    archive = S3ArchiveManager(chunk_size=3)
    ring = TelemetryRingBuffer(capacity=10)

    for i in range(5):
        rec = ring.append(
            timestamp=1700000000.0 + i,
            point_id="TEST_PT",
            subsystem="thermal",
            raw_val=float(i),
            eng_val=float(i),
            unit="K",
            limit_state="NOMINAL",
        )
        res = archive.stage_record(rec)
        if i == 2:
            assert res is not None
            assert res["record_count"] == 3
            assert len(res["sha256"]) == 64

    # Flush remaining 2 items
    final_chunk = archive.flush()
    assert final_chunk is not None
    assert final_chunk["record_count"] == 2
    assert len(archive.archived_chunks) == 2


def test_pipeline_end_to_end_telemetry_flow():
    registry = TelemetryRegistry()
    registry.register(SensorPoint(
        point_id="CHAMBER_P",
        name="Chamber Pressure",
        subsystem="propulsion",
        unit="bar",
        scale=1.0,
        warn_high=100.0,
        crit_high=120.0,
    ))
    registry.register(SensorPoint(
        point_id="OX_P",
        name="Oxidizer Pressure",
        subsystem="propulsion",
        unit="bar",
        scale=1.0,
        warn_low=10.0,
        crit_low=5.0,
    ))

    pipeline = MissionControlPipeline(registry=registry, archive_chunk_size=10)

    # Frame 1: Nominal values
    f1_payload = json.dumps({"CHAMBER_P": 80.0, "OX_P": 15.0}).encode("utf-8")
    f1_bytes = encode_frame(subsystem_id=2, sequence_id=1, timestamp_epoch=1700000001.0, payload=f1_payload)
    recs1 = pipeline.ingest_binary_frame(f1_bytes)
    assert len(recs1) == 2
    assert pipeline.metrics["nominal_count"] == 2
    assert len(pipeline.get_active_alarms()) == 0

    # Frame 2: Anomaly values (Chamber Critical, Ox Warning)
    f2_payload = json.dumps({"CHAMBER_P": 125.0, "OX_P": 8.0}).encode("utf-8")
    f2_bytes = encode_frame(subsystem_id=2, sequence_id=2, timestamp_epoch=1700000002.0, payload=f2_payload)
    recs2 = pipeline.ingest_binary_frame(f2_bytes)
    assert len(recs2) == 2
    assert pipeline.metrics["critical_count"] == 1
    assert pipeline.metrics["warning_count"] == 1

    alarms = pipeline.get_active_alarms()
    assert len(alarms) == 2
    severities = {a["point_id"]: a["severity"] for a in alarms}
    assert severities["CHAMBER_P"] == "CRITICAL"
    assert severities["OX_P"] == "WARNING"


def test_pipeline_metrics_and_rejected_frames():
    registry = TelemetryRegistry()
    pipeline = MissionControlPipeline(registry=registry)

    with pytest.raises(FrameValidationError):
        pipeline.ingest_binary_frame(b"invalid_short_frame")

    assert pipeline.metrics["frames_rejected"] == 1
    assert pipeline.metrics["frames_processed"] == 0
