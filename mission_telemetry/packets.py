"""Spacecraft and ground segment binary telemetry frame framing and CRC validation."""
import struct
from dataclasses import dataclass

SYNC_MARKER = 0x1ACFFC1D  # Standard CCSDS Frame Synchronization Marker


class FrameValidationError(Exception):
    """Raised when a telemetry frame fails framing or integrity checks."""
    pass


def calculate_crc16(data: bytes) -> int:
    """Computes CRC-16-CCITT (poly 0x1021, init 0xFFFF)."""
    crc = 0xFFFF
    for byte in data:
        crc ^= (byte << 8)
        for _ in range(8):
            if crc & 0x8000:
                crc = ((crc << 1) ^ 0x1021) & 0xFFFF
            else:
                crc = (crc << 1) & 0xFFFF
    return crc


@dataclass(frozen=True)
class TelemetryFrame:
    subsystem_id: int
    sequence_id: int
    timestamp_epoch: float
    payload: bytes
    checksum: int


def encode_frame(subsystem_id: int, sequence_id: int, timestamp_epoch: float, payload: bytes) -> bytes:
    """Encodes frame fields into binary wire format with sync marker and CRC16.
    Header format:
      - 4 bytes uint32: Sync Marker (0x1ACFFC1D)
      - 2 bytes uint16: Subsystem ID
      - 4 bytes uint32: Sequence ID
      - 8 bytes double: Timestamp (Unix epoch seconds)
      - 2 bytes uint16: Payload Length
    Body:
      - N bytes: Payload
    Footer:
      - 2 bytes uint16: CRC-16 Checksum
    """
    header = struct.pack(">IHIdH", SYNC_MARKER, subsystem_id, sequence_id, timestamp_epoch, len(payload))
    data_to_checksum = header + payload
    checksum = calculate_crc16(data_to_checksum)
    return data_to_checksum + struct.pack(">H", checksum)


def decode_frame(raw_bytes: bytes) -> TelemetryFrame:
    """Decodes raw binary packet and verifies sync marker and CRC."""
    if len(raw_bytes) < 22:
        raise FrameValidationError(f"Frame length {len(raw_bytes)} too short; minimum is 22 bytes")

    sync, subsystem_id, sequence_id, timestamp_epoch, payload_len = struct.unpack(">IHIdH", raw_bytes[:20])
    if sync != SYNC_MARKER:
        raise FrameValidationError(f"Invalid sync marker {hex(sync)}; expected {hex(SYNC_MARKER)}")

    expected_len = 20 + payload_len + 2
    if len(raw_bytes) != expected_len:
        raise FrameValidationError(f"Frame payload length mismatch: expected {expected_len}, got {len(raw_bytes)}")

    payload = raw_bytes[20: 20 + payload_len]
    (actual_crc,) = struct.unpack(">H", raw_bytes[20 + payload_len:])
    computed_crc = calculate_crc16(raw_bytes[:20 + payload_len])

    if actual_crc != computed_crc:
        raise FrameValidationError(f"CRC mismatch: expected {hex(computed_crc)}, got {hex(actual_crc)}")

    return TelemetryFrame(
        subsystem_id=subsystem_id,
        sequence_id=sequence_id,
        timestamp_epoch=timestamp_epoch,
        payload=payload,
        checksum=actual_crc,
    )
