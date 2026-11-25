"""Event stream ring buffer and chunked object archive manager."""
import collections
import hashlib
import json
import time
from dataclasses import dataclass, asdict
from typing import List, Dict, Any, Optional


@dataclass
class TelemetryRecord:
    offset: int
    timestamp: float
    point_id: str
    subsystem: str
    raw_val: float
    eng_val: float
    unit: str
    limit_state: str


class TelemetryRingBuffer:
    """High-performance in-memory ring buffer modeling Redis Streams / Kafka partitions."""

    def __init__(self, capacity: int = 10000) -> None:
        self.capacity = capacity
        self._buffer: collections.deque[TelemetryRecord] = collections.deque(maxlen=capacity)
        self._current_offset = 0

    def append(self, timestamp: float, point_id: str, subsystem: str, raw_val: float, eng_val: float, unit: str, limit_state: str) -> TelemetryRecord:
        self._current_offset += 1
        rec = TelemetryRecord(
            offset=self._current_offset,
            timestamp=timestamp,
            point_id=point_id,
            subsystem=subsystem,
            raw_val=raw_val,
            eng_val=eng_val,
            unit=unit,
            limit_state=limit_state,
        )
        self._buffer.append(rec)
        return rec

    def read_from(self, start_offset: int, limit: int = 100) -> List[TelemetryRecord]:
        results: List[TelemetryRecord] = []
        for rec in self._buffer:
            if rec.offset > start_offset:
                results.append(rec)
                if len(results) >= limit:
                    break
        return results

    def filter_by_subsystem(self, subsystem: str) -> List[TelemetryRecord]:
        return [r for r in self._buffer if r.subsystem == subsystem]

    @property
    def latest_offset(self) -> int:
        return self._current_offset

    def __len__(self) -> int:
        return len(self._buffer)


class S3ArchiveManager:
    """Batches telemetry into immutable object store chunks with SHA-256 integrity digests."""

    def __init__(self, chunk_size: int = 50) -> None:
        self.chunk_size = chunk_size
        self._pending: List[TelemetryRecord] = []
        self._archived_chunks: List[Dict[str, Any]] = []

    def stage_record(self, record: TelemetryRecord) -> Optional[Dict[str, Any]]:
        self._pending.append(record)
        if len(self._pending) >= self.chunk_size:
            return self.flush()
        return None

    def flush(self) -> Optional[Dict[str, Any]]:
        if not self._pending:
            return None

        chunk_id = f"chunk_{int(time.time()*1000)}_{len(self._archived_chunks)+1}"
        records_data = [asdict(r) for r in self._pending]
        serialized = json.dumps(records_data, sort_keys=True).encode("utf-8")
        sha256_hash = hashlib.sha256(serialized).hexdigest()

        chunk_meta = {
            "chunk_id": chunk_id,
            "record_count": len(records_data),
            "sha256": sha256_hash,
            "bytes_size": len(serialized),
            "start_offset": self._pending[0].offset,
            "end_offset": self._pending[-1].offset,
            "payload_json": serialized.decode("utf-8"),
        }
        self._archived_chunks.append(chunk_meta)
        self._pending.clear()
        return chunk_meta

    @property
    def archived_chunks(self) -> List[Dict[str, Any]]:
        return list(self._archived_chunks)
