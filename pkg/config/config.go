package config

import (
	"fmt"
	"os"
	"strings"

	"gopkg.in/yaml.v3"
)

// Config represents the complete application configuration
type Config struct {
	App        AppConfig        `yaml:"app"`
	Source     SourceConfig     `yaml:"source"`
	Storage    StorageConfig    `yaml:"storage"`
	Checkpoint CheckpointConfig `yaml:"checkpoint"`
	Processing ProcessingConfig `yaml:"processing"`
	Retry      RetryConfig      `yaml:"retry"`
}

// AppConfig contains application-level settings
type AppConfig struct {
	Name     string `yaml:"name"`
	LogLevel string `yaml:"log_level"`
	LogFile  string `yaml:"log_file"`
}

// SourceConfig contains data source configuration
type SourceConfig struct {
	Type       string         `yaml:"type"`
	MySQL      MySQLConfig    `yaml:"mysql"`
	Kafka      KafkaConfig    `yaml:"kafka"`
	PostgreSQL PostgresConfig `yaml:"postgresql"`
	REST       RESTConfig     `yaml:"rest"`
}

// MySQLConfig contains MySQL-specific settings
type MySQLConfig struct {
	Host          string   `yaml:"host"`
	Port          int      `yaml:"port"`
	User          string   `yaml:"user"`
	Password      string   `yaml:"password"`
	ServerID      uint32   `yaml:"server_id"`
	Tables        []string `yaml:"tables"`
	ExcludeTables []string `yaml:"exclude_tables"`
	BinlogFile    string   `yaml:"binlog_file"`
	BinlogPos     uint32   `yaml:"binlog_pos"`
}

// KafkaConfig contains Kafka-specific settings
type KafkaConfig struct {
	Brokers []string `yaml:"brokers"`
	Topics  []string `yaml:"topics"`
	GroupID string   `yaml:"group_id"`
}

// PostgresConfig contains PostgreSQL-specific settings
type PostgresConfig struct {
	Host     string `yaml:"host"`
	Port     int    `yaml:"port"`
	User     string `yaml:"user"`
	Password string `yaml:"password"`
	Database string `yaml:"database"`
	SlotName string `yaml:"slot_name"`
}

// RESTConfig contains REST API-specific settings
type RESTConfig struct {
	BaseURL      string            `yaml:"base_url"`
	Endpoints    []string          `yaml:"endpoints"`
	Headers      map[string]string `yaml:"headers"`
	PollInterval int               `yaml:"poll_interval_sec"`
}

// StorageConfig contains storage configuration
type StorageConfig struct {
	Type  string      `yaml:"type"`
	Local LocalConfig `yaml:"local"`
}

// LocalConfig contains local storage settings
type LocalConfig struct {
	BasePath          string        `yaml:"base_path"`
	PartitionStrategy string        `yaml:"partition_strategy"`
	FileFormat        string        `yaml:"file_format"`
	Compression       string        `yaml:"compression"`
	MaxFileSizeMB     int           `yaml:"max_file_size_mb"`
	MaxRecordsPerFile int           `yaml:"max_records_per_file"`
	Parquet           ParquetConfig `yaml:"parquet"`
	Schema            SchemaConfig  `yaml:"schema"`
}

// ParquetConfig contains Parquet-specific settings
type ParquetConfig struct {
	RowGroupSize     int  `yaml:"row_group_size"`
	PageSize         int  `yaml:"page_size"`
	EnableDictionary bool `yaml:"enable_dictionary"`
}

// SchemaConfig contains schema registry settings
type SchemaConfig struct {
	RegistryPath  string `yaml:"registry_path"`
	Compatibility string `yaml:"compatibility"`
	AutoRegister  bool   `yaml:"auto_register"`
}

// CheckpointConfig contains checkpoint settings
type CheckpointConfig struct {
	StoragePath     string `yaml:"storage_path"`
	SaveIntervalSec int    `yaml:"save_interval_sec"`
}

// ProcessingConfig contains data processing settings
type ProcessingConfig struct {
	BatchSize           int      `yaml:"batch_size"`
	WorkerCount         int      `yaml:"worker_count"`
	Filters             []string `yaml:"filters"`
	Transforms          []string `yaml:"transforms"`
	MaxRetries          int      `yaml:"max_retries"`
	RetryIntervalMs     int      `yaml:"retry_interval_ms"`
	DeadLetterQueuePath string   `yaml:"dead_letter_queue_path"`
	DeadLetterMaxSizeMB int      `yaml:"dead_letter_max_size_mb"`
}

// RetryConfig contains retry settings
type RetryConfig struct {
	MaxRetries        int `yaml:"max_retries"`
	InitialIntervalMs int `yaml:"initial_interval_ms"`
	MaxIntervalMs     int `yaml:"max_interval_ms"`
}

// Load reads configuration from a YAML file
func Load(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("failed to read config file: %w", err)
	}

	var cfg Config
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("failed to parse config file: %w", err)
	}

	// Set defaults
	cfg.setDefaults()

	// Resolve secrets from environment variables
	ResolveSecrets(&cfg)

	// Validate configuration
	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("invalid configuration: %w", err)
	}

	return &cfg, nil
}

// setDefaults sets default values for configuration fields
func (c *Config) setDefaults() {
	if c.App.Name == "" {
		c.App.Name = "data-ingestion-tool"
	}
	if c.App.LogLevel == "" {
		c.App.LogLevel = "info"
	}
	if c.Source.Type == "" {
		c.Source.Type = "mysql"
	}
	if c.Source.MySQL.Port == 0 {
		c.Source.MySQL.Port = 3306
	}
	if c.Storage.Type == "" {
		c.Storage.Type = "local"
	}
	if c.Storage.Local.BasePath == "" {
		c.Storage.Local.BasePath = "./data-lake"
	}
	if c.Storage.Local.PartitionStrategy == "" {
		c.Storage.Local.PartitionStrategy = "date"
	}
	if c.Storage.Local.FileFormat == "" {
		c.Storage.Local.FileFormat = "json"
	}
	if c.Storage.Local.Compression == "" {
		c.Storage.Local.Compression = "none"
	}
	if c.Storage.Local.MaxFileSizeMB == 0 {
		c.Storage.Local.MaxFileSizeMB = 100
	}
	if c.Storage.Local.MaxRecordsPerFile == 0 {
		c.Storage.Local.MaxRecordsPerFile = 10000
	}
	if c.Storage.Local.Parquet.RowGroupSize == 0 {
		c.Storage.Local.Parquet.RowGroupSize = 10000
	}
	if c.Storage.Local.Parquet.PageSize == 0 {
		c.Storage.Local.Parquet.PageSize = 8192
	}
	if c.Storage.Local.Schema.RegistryPath == "" {
		c.Storage.Local.Schema.RegistryPath = "./metadata/schemas"
	}
	if c.Storage.Local.Schema.Compatibility == "" {
		c.Storage.Local.Schema.Compatibility = "backward"
	}
	if c.Checkpoint.StoragePath == "" {
		c.Checkpoint.StoragePath = "./metadata/checkpoint.json"
	}
	if c.Checkpoint.SaveIntervalSec == 0 {
		c.Checkpoint.SaveIntervalSec = 10
	}
	if c.Processing.BatchSize == 0 {
		c.Processing.BatchSize = 100
	}
	if c.Processing.WorkerCount == 0 {
		c.Processing.WorkerCount = 4
	}
	if c.Processing.MaxRetries == 0 {
		c.Processing.MaxRetries = 3
	}
	if c.Processing.RetryIntervalMs == 0 {
		c.Processing.RetryIntervalMs = 1000
	}
	if c.Processing.DeadLetterQueuePath == "" {
		c.Processing.DeadLetterQueuePath = "./metadata/deadletter.jsonl"
	}
	if c.Processing.DeadLetterMaxSizeMB == 0 {
		c.Processing.DeadLetterMaxSizeMB = 100
	}
	if c.Retry.MaxRetries == 0 {
		c.Retry.MaxRetries = 3
	}
	if c.Retry.InitialIntervalMs == 0 {
		c.Retry.InitialIntervalMs = 100
	}
	if c.Retry.MaxIntervalMs == 0 {
		c.Retry.MaxIntervalMs = 10000
	}
}

// Validate checks if the configuration is valid
func (c *Config) Validate() error {
	// Validate source configuration
	switch c.Source.Type {
	case "mysql":
		if c.Source.MySQL.Host == "" {
			return fmt.Errorf("mysql host is required")
		}
		if c.Source.MySQL.User == "" {
			return fmt.Errorf("mysql user is required")
		}
		if c.Source.MySQL.ServerID == 0 {
			return fmt.Errorf("mysql server_id is required")
		}
	case "kafka":
		if len(c.Source.Kafka.Brokers) == 0 {
			return fmt.Errorf("kafka brokers are required")
		}
	case "postgresql":
		if c.Source.PostgreSQL.Host == "" {
			return fmt.Errorf("postgresql host is required")
		}
	case "rest":
		if c.Source.REST.BaseURL == "" {
			return fmt.Errorf("rest base_url is required")
		}
	default:
		return fmt.Errorf("unsupported source type: %s", c.Source.Type)
	}

	// Validate storage configuration
	if c.Storage.Type != "local" {
		return fmt.Errorf("unsupported storage type: %s", c.Storage.Type)
	}

	validFormats := map[string]bool{"json": true, "csv": true, "parquet": true}
	if !validFormats[c.Storage.Local.FileFormat] {
		return fmt.Errorf("unsupported file format: %s", c.Storage.Local.FileFormat)
	}

	validCompressions := map[string]bool{"none": true, "snappy": true, "gzip": true, "zstd": true}
	if !validCompressions[c.Storage.Local.Compression] {
		return fmt.Errorf("unsupported compression: %s", c.Storage.Local.Compression)
	}

	validStrategies := map[string]bool{"date": true, "hour": true, "none": true}
	if !validStrategies[c.Storage.Local.PartitionStrategy] {
		return fmt.Errorf("unsupported partition strategy: %s", c.Storage.Local.PartitionStrategy)
	}

	return nil
}

// IsPlaintextPassword checks if a password value is plaintext (not using env var)
func IsPlaintextPassword(password string) bool {
	if password == "" {
		return false
	}
	// If it starts with ${ and ends with }, it's using env var syntax
	if strings.HasPrefix(password, "${") && strings.HasSuffix(password, "}") {
		return false
	}
	return true
}

// GetPlaintextPasswordFields returns a list of fields that contain plaintext passwords
func (c *Config) GetPlaintextPasswordFields() []string {
	var fields []string

	// Check MySQL password
	if c.Source.Type == "mysql" && IsPlaintextPassword(c.Source.MySQL.Password) {
		fields = append(fields, "source.mysql.password")
	}

	// Check PostgreSQL password
	if c.Source.Type == "postgresql" && IsPlaintextPassword(c.Source.PostgreSQL.Password) {
		fields = append(fields, "source.postgresql.password")
	}

	// Check REST Authorization header
	if c.Source.Type == "rest" && c.Source.REST.Headers != nil {
		if auth, exists := c.Source.REST.Headers["Authorization"]; exists {
			if IsPlaintextPassword(auth) {
				fields = append(fields, "source.rest.headers.Authorization")
			}
		}
	}

	return fields
}
