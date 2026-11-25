# Mission Telemetry Streaming Engine

High-throughput, real-time mission operations telemetry streaming pipeline designed for ground segment flight controllers and aerospace testing operations.

## Capabilities
- **Frame Ingestion & CRC-16 Validation**: Synchronous and asynchronous decoding of telemetry frames with header parsing and checksum integrity verification.
- **Engineering Point Calibration & Limit Monitoring**: Polynomial sensor calibration with multi-tier limit checking (NOMINAL, WARNING, CRITICAL RED).
- **Streaming & Pub/Sub Distribution**: High-speed ring-buffer and message distribution modeling Redis Streams and Kafka event semantics.
- **Chunked Object Storage Archival**: Batch packaging and integrity-verified chunk serialization compatible with S3 and MinIO object storage.

## Running Tests
```bash
pytest -v
```
