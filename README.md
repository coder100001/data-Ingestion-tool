# Data Ingestion Tool

A high-performance incremental data ingestion tool that captures changes from MySQL binlog and stores them in a local data lake. Built with Go for reliability and scalability.

## Features

- **Incremental Data Capture**: Uses MySQL binlog CDC (Change Data Capture) to capture real-time data changes
- **Multiple Source Support**: Extensible architecture supporting MySQL, PostgreSQL, Kafka, and REST APIs
- **Flexible Storage**: Local filesystem storage with configurable partitioning strategies
- **Data Processing Pipeline**: Built-in filtering and transformation capabilities
- **Checkpoint Management**: Automatic progress tracking with crash recovery
- **High Performance**: Worker pool architecture for parallel processing
- **Production Ready**: Comprehensive logging, error handling, and monitoring

## Architecture

```
┌─────────────────┐     ┌──────────────────┐     ┌─────────────────┐
│   Data Source   │────▶│  Data Connector  │────▶│    Pipeline     │
│  (MySQL, etc.)  │     │   (Binlog CDC)   │     │ (Filter/Trans)  │
└─────────────────┘     └──────────────────┘     └────────┬────────┘
                                                          │
                                                          ▼
┌─────────────────┐     ┌──────────────────┐     ┌─────────────────┐
│    Checkpoint   │◀────│  Checkpoint Mgr  │◀────│     Storage     │
│    (Metadata)   │     │                  │     │  (Data Lake)    │
└─────────────────┘     └──────────────────┘     └─────────────────┘
```

## Quick Start

### Prerequisites

- Go 1.21 or higher
- MySQL 5.7+ with binlog enabled (ROW format)
- Docker and Docker Compose (optional)

### Installation

1. Clone the repository:
```bash
git clone <repository-url>
cd data-ingestion-tool
```

2. Install dependencies:
```bash
make deps
```

3. Build the application:
```bash
make build
```

### Configuration

Edit `config.yaml` to configure your data source:

```yaml
source:
  type: "mysql"
  mysql:
    host: "localhost"
    port: 3306
    user: "root"
    password: "password"
    server_id: 1001
    tables:
      - "testdb.users"
      - "testdb.orders"

storage:
  local:
    base_path: "./data-lake"
    partition_strategy: "date"  # date, hour, or none
    file_format: "json"         # json, csv
```

### Running with Docker Compose

1. Start the services:
```bash
docker-compose up -d
```

2. View logs:
```bash
docker-compose logs -f ingester
```

3. Stop the services:
```bash
docker-compose down
```

### Running Locally

1. Set up the test database:
```bash
# MySQL must have binlog enabled
mysql -u root -p < scripts/init.sql
```

2. Run the application:
```bash
make run
```

3. Reset checkpoint and start fresh:
```bash
make run-reset
```

## Usage

### Command Line Options

```bash
./data-ingestion-tool -h

Usage of ./data-ingestion-tool:
  -config string
        Path to configuration file (default "config.yaml")
  -reset
        Reset checkpoint and start from beginning
```

### Configuration Options

#### Source Configuration

| Option | Description | Default |
|--------|-------------|---------|
| `source.type` | Source type: mysql, kafka, postgresql, rest | mysql |
| `source.mysql.host` | MySQL host | localhost |
| `source.mysql.port` | MySQL port | 3306 |
| `source.mysql.user` | MySQL username | - |
| `source.mysql.password` | MySQL password | - |
| `source.mysql.server_id` | Unique server ID for replication | - |
| `source.mysql.tables` | Tables to monitor (empty = all) | [] |
| `source.mysql.exclude_tables` | Tables to exclude | [] |

#### Storage Configuration

| Option | Description | Default |
|--------|-------------|---------|
| `storage.local.base_path` | Base directory for data lake | ./data-lake |
| `storage.local.partition_strategy` | Partition by: date, hour, none | date |
| `storage.local.file_format` | Output format: json, csv | json |
| `storage.local.max_file_size_mb` | Max file size before rotation | 100 |
| `storage.local.max_records_per_file` | Max records per file | 10000 |

#### Processing Configuration

| Option | Description | Default |
|--------|-------------|---------|
| `processing.batch_size` | Batch size for processing | 100 |
| `processing.worker_count` | Number of worker goroutines | 4 |
| `processing.filters` | Filter rules | [] |
| `processing.transforms` | Transform rules | [] |

#### Checkpoint Configuration

| Option | Description | Default |
|--------|-------------|---------|
| `checkpoint.storage_path` | Path to checkpoint file | ./metadata/checkpoint.json |
| `checkpoint.save_interval_sec` | Auto-save interval | 10 |

## Data Lake Structure

The tool creates a partitioned directory structure in the data lake:

```
data-lake/
├── 2024-01-15/
│   ├── data_20240115_120000.json
│   ├── data_20240115_130000.json
│   └── ...
├── 2024-01-16/
│   ├── data_20240116_090000.json
│   └── ...
```

### JSON Format

```json
{
  "id": "20240115120000000000000",
  "timestamp": "2024-01-15T12:00:00Z",
  "source": "mysql://root@localhost:3306",
  "type": "INSERT",
  "database": "testdb",
  "table": "users",
  "schema": {
    "id": {"type": "int", "nullable": false},
    "username": {"type": "varchar", "nullable": false}
  },
  "after": {
    "id": 1,
    "username": "alice",
    "email": "alice@example.com"
  },
  "binlog_file": "mysql-bin.000001",
  "binlog_pos": 1234
}
```

## Development

### Project Structure

```
.
├── cmd/
│   └── ingester/         # Main application entry point
├── pkg/
│   ├── checkpoint/       # Checkpoint management
│   ├── config/           # Configuration handling
│   ├── connector/        # Data source connectors
│   ├── logger/           # Logging utilities
│   ├── models/           # Data models
│   └── pipeline/         # Data processing pipeline
├── scripts/              # Database initialization scripts
├── config.yaml           # Configuration file
├── docker-compose.yml    # Docker Compose setup
├── Dockerfile            # Docker image definition
├── Makefile              # Build automation
└── README.md             # This file
```

### Running Tests

```bash
# Run unit tests
make test

# Run tests with coverage
make test-coverage

# Run integration tests
make integration-test
```

### Building for Production

```bash
# Build for current platform
make build

# Build for multiple platforms
make build-all

# Build Docker image
make docker-build
```

## Extending the Tool

### Adding a New Data Source

1. Create a new connector in `pkg/connector/` implementing the `Connector` interface
2. Add configuration options in `pkg/config/config.go`
3. Register the connector in `pkg/connector/connector.go` factory

Example:
```go
type MyConnector struct {
    BaseConnector
    // Add specific fields
}

func (c *MyConnector) Connect(ctx context.Context) error {
    // Implementation
}
```

### Adding Custom Filters

```go
type MyFilter struct{}

func (f *MyFilter) Apply(change *models.DataChange) bool {
    // Return true to keep, false to filter out
    return change.Database == "important_db"
}
```

### Adding Custom Transformers

```go
type MyTransformer struct{}

func (t *MyTransformer) Transform(change *models.DataChange) error {
    // Modify the change in place
    change.After["processed_at"] = time.Now()
    return nil
}
```

## Monitoring and Troubleshooting

### Logs

Logs are written to both console and file (if configured):

```bash
# View logs
tail -f logs/ingestion.log
```

### Checkpoint

The checkpoint file stores the current ingestion position:

```bash
# View checkpoint
cat metadata/checkpoint.json
```

### Common Issues

1. **Binlog not enabled**: Ensure MySQL has binlog enabled with ROW format
2. **Permission denied**: Grant REPLICATION SLAVE and REPLICATION CLIENT privileges
3. **Server ID conflict**: Use a unique server_id not used by other replicas

## Performance Tuning

- **Increase worker_count** for higher throughput
- **Adjust batch_size** based on memory availability
- **Use appropriate partition_strategy** for your query patterns
- **Monitor file sizes** and adjust rotation settings

## License

MIT License - see LICENSE file for details

## Contributing

Contributions are welcome! Please read CONTRIBUTING.md for guidelines.
