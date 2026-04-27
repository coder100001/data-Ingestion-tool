# Data Ingestion Tool - Architecture Design Document

## Overview

This document describes the architecture and design decisions of the Data Ingestion Tool, an incremental data capture and processing system built in Go.

## System Architecture

### High-Level Design

```
┌─────────────────────────────────────────────────────────────────┐
│                        Data Ingestion Tool                       │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│  ┌──────────────┐    ┌──────────────┐    ┌──────────────┐      │
│  │   Connector  │───▶│   Pipeline   │───▶│   Storage    │      │
│  │    Layer     │    │    Layer     │    │    Layer     │      │
│  └──────────────┘    └──────────────┘    └──────────────┘      │
│         │                   │                   │               │
│         ▼                   ▼                   ▼               │
│  ┌──────────────┐    ┌──────────────┐    ┌──────────────┐      │
│  │  Checkpoint  │    │   Filter &   │    │   Partition  │      │
│  │   Manager    │    │  Transform   │    │   Manager    │      │
│  └──────────────┘    └──────────────┘    └──────────────┘      │
│                                                                  │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│                     External Systems                             │
│  ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌──────────┐        │
│  │  MySQL   │  │  Kafka   │  │PostgreSQL│  │  REST    │        │
│  │ (Binlog) │  │          │  │  (CDC)   │  │   API    │        │
│  └──────────┘  └──────────┘  └──────────┘  └──────────┘        │
└─────────────────────────────────────────────────────────────────┘
```

## Component Details

### 1. Connector Layer

**Purpose**: Abstract interface for connecting to various data sources

**Key Components**:
- `Connector` interface: Defines common operations
- `MySQLConnector`: Implements MySQL binlog CDC
- `KafkaConnector`: Placeholder for Kafka consumer
- `PostgresConnector`: Placeholder for PostgreSQL logical replication
- `RESTConnector`: Placeholder for REST API polling

**Design Patterns**:
- Factory Pattern: `ConnectorFactory` creates appropriate connector
- Strategy Pattern: Different connectors implement same interface

**MySQL CDC Implementation**:
- Uses `go-mysql` library for binlog parsing
- Supports ROW-based replication
- Captures INSERT, UPDATE, DELETE events
- Maintains table schema cache
- Handles DDL events (table structure changes)

### 2. Pipeline Layer

**Purpose**: Process and transform data changes

**Key Components**:
- `Pipeline`: Orchestrates data flow
- `Filter` interface: Data filtering rules
- `Transformer` interface: Data transformation rules
- Worker pool for parallel processing

**Processing Flow**:
1. Receive change from connector
2. Apply filters (drop unwanted changes)
3. Apply transformations (enrich/modify data)
4. Send to storage

**Design Patterns**:
- Pipeline Pattern: Data flows through stages
- Worker Pool Pattern: Concurrent processing

### 3. Storage Layer

**Purpose**: Persist processed data to data lake

**Key Components**:
- `LocalStorage`: File system storage implementation
- Partition management (date/hour based)
- File rotation based on size/count
- Multiple format support (JSON, CSV)

**Partition Strategies**:
- `date`: Partition by date (YYYY-MM-DD)
- `hour`: Partition by hour (YYYY-MM-DD/HH)
- `none`: No partitioning

**File Rotation**:
- Based on file size (configurable MB limit)
- Based on record count (configurable limit)
- Timestamp-based naming

### 4. Checkpoint Manager

**Purpose**: Track ingestion progress for fault tolerance

**Key Features**:
- Automatic position tracking
- Periodic saves (configurable interval)
- Atomic write operations
- Crash recovery support

**Storage Format**:
```json
{
  "source_type": "mysql",
  "position": {
    "binlog_file": "mysql-bin.000001",
    "binlog_pos": 1234
  },
  "updated_at": "2024-01-15T12:00:00Z"
}
```

### 5. Configuration Management

**Purpose**: Centralized configuration handling

**Features**:
- YAML-based configuration
- Environment-specific overrides
- Validation and defaults
- Support for multiple source types

## Data Flow

### Normal Operation Flow

```
1. MySQL Binlog Event
        │
        ▼
2. Connector captures event
   - Parse binlog
   - Extract row data
   - Map to DataChange model
        │
        ▼
3. Pipeline processes event
   - Apply filters
   - Apply transformations
        │
        ▼
4. Storage writes event
   - Determine partition
   - Write to file
   - Rotate if needed
        │
        ▼
5. Checkpoint updates
   - Save position
   - Periodic persistence
```

### Recovery Flow

```
1. Tool restarts
        │
        ▼
2. Load checkpoint
   - Read last position
        │
        ▼
3. Resume from position
   - Connect to source
   - Start from saved position
        │
        ▼
4. Continue processing
   - No data loss
   - No duplicates (at-least-once)
```

## Data Models

### DataChange

Core data model representing a single change event:

```go
type DataChange struct {
    ID         string                 // Unique identifier
    Timestamp  time.Time              // Event timestamp
    Source     string                 // Source identifier
    Type       ChangeType             // INSERT, UPDATE, DELETE
    Database   string                 // Database name
    Table      string                 // Table name
    Schema     map[string]interface{} // Column metadata
    Before     map[string]interface{} // Previous values
    After      map[string]interface{} // New values
    BinlogFile string                 // Binlog position info
    BinlogPos  uint32
}
```

### Checkpoint

Position tracking model:

```go
type Checkpoint struct {
    SourceType string    // mysql, kafka, etc.
    Position   Position  // Source-specific position
    UpdatedAt  time.Time
}

type Position struct {
    BinlogFile string // MySQL
    BinlogPos  uint32
    Topic      string // Kafka
    Partition  int32
    Offset     int64
    LSN        string // PostgreSQL
}
```

## Error Handling Strategy

### Connector Errors
- Connection failures: Automatic retry with exponential backoff
- Binlog read errors: Log and continue
- Schema changes: Invalidate cache and refresh

### Pipeline Errors
- Filter errors: Log and pass through (fail open)
- Transform errors: Log and skip record
- Storage errors: Retry with backoff, then fail

### Storage Errors
- Write errors: Retry 3 times, then fail
- Disk full: Log error and stop
- Permission errors: Fail fast

### Checkpoint Errors
- Save errors: Log warning, continue
- Load errors: Start from beginning

## Concurrency Model

### Goroutine Structure

```
Main Goroutine
    │
    ├── Connector Goroutine
    │       └── Binlog Event Loop
    │
    ├── Pipeline Workers (N goroutines)
    │       └── Process Changes
    │
    ├── Checkpoint Goroutine
    │       └── Periodic Save
    │
    └── Signal Handler
            └── Graceful Shutdown
```

### Channel Communication

```
Connector (Producer)
    │
    │ changeChan (buffered)
    ▼
Pipeline Workers (Consumers)
    │
    │ storage (direct call)
    ▼
Storage Layer
```

## Extensibility

### Adding New Sources

1. Implement `Connector` interface
2. Add configuration struct
3. Register in factory
4. Add tests

### Adding Custom Processing

1. Implement `Filter` or `Transformer` interface
2. Register in pipeline initialization
3. Configure via config file

### Adding New Storage Backends

1. Implement `Storage` interface
2. Add configuration options
3. Update factory

## Performance Considerations

### Throughput Optimization
- Worker pool for parallel processing
- Buffered channels for async communication
- Batch writes to storage
- Efficient JSON marshaling

### Memory Management
- Streaming processing (no full dataset in memory)
- Bounded channel buffers
- File rotation to control memory

### Disk I/O
- Async file writes
- Buffered writers
- Atomic file operations for checkpoint

## Security Considerations

### Data Protection
- No credentials in logs
- Configurable log levels
- Support for TLS connections (future)

### Access Control
- File permissions for data lake
- Checkpoint file protection
- Configurable via OS permissions

## Monitoring and Observability

### Logging
- Structured logging with logrus
- Configurable log levels
- Separate log files

### Metrics (Future)
- Records processed per second
- Lag time
- Error rates
- Storage utilization

### Health Checks
- Connector connectivity
- Storage availability
- Disk space monitoring

## Deployment Options

### Standalone
- Binary execution
- Systemd service
- Configuration file

### Docker
- Container image
- Volume mounts for data
- Environment variables

### Docker Compose
- Complete stack
- MySQL + Ingestion Tool
- Persistent volumes

### Kubernetes (Future)
- StatefulSet deployment
- ConfigMap for configuration
- PersistentVolumeClaims

## Testing Strategy

### Unit Tests
- Individual component testing
- Mock interfaces
- Table-driven tests

### Integration Tests
- MySQL container testing
- End-to-end data flow
- Checkpoint recovery

### Performance Tests
- Throughput benchmarking
- Memory profiling
- Load testing

## Future Enhancements

### Short Term
- [ ] Complete Kafka connector
- [ ] Complete PostgreSQL connector
- [ ] REST API connector
- [ ] Parquet format support

### Medium Term
- [ ] Schema registry integration
- [ ] Dead letter queue
- [ ] Exactly-once semantics
- [ ] HTTP API for monitoring

### Long Term
- [ ] Distributed mode
- [ ] Cloud storage backends (S3, GCS)
- [ ] SQL-based transformations
- [ ] Web UI for management
