package config

import (
	"fmt"
	"os"

	"gopkg.in/yaml.v3"
)

// Config represents the complete application configuration
type Config struct {
	App        AppConfig        `yaml:"app"`
	Source     SourceConfig     `yaml:"source"`
	Storage    StorageConfig    `yaml:"storage"`
	Checkpoint CheckpointConfig `yaml:"checkpoint"`
	Processing ProcessingConfig `yaml:"processing"`
}

// AppConfig contains application-level settings
type AppConfig struct {
	Name     string `yaml:"name"`
	LogLevel string `yaml:"log_level"`
	LogFile  string `yaml:"log_file"`
}

// SourceConfig contains data source configuration
type SourceConfig struct {
	Type   string       `yaml:"type"`
	MySQL  MySQLConfig  `yaml:"mysql"`
	Kafka  KafkaConfig  `yaml:"kafka"`
	PostgreSQL PostgresConfig `yaml:"postgresql"`
	REST   RESTConfig   `yaml:"rest"`
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
	BaseURL     string            `yaml:"base_url"`
	Endpoints   []string          `yaml:"endpoints"`
	Headers     map[string]string `yaml:"headers"`
	PollInterval int              `yaml:"poll_interval_sec"`
}

// StorageConfig contains storage configuration
type StorageConfig struct {
	Type  string      `yaml:"type"`
	Local LocalConfig `yaml:"local"`
}

// LocalConfig contains local storage settings
type LocalConfig struct {
	BasePath           string `yaml:"base_path"`
	PartitionStrategy  string `yaml:"partition_strategy"`
	FileFormat         string `yaml:"file_format"`
	MaxFileSizeMB      int    `yaml:"max_file_size_mb"`
	MaxRecordsPerFile  int    `yaml:"max_records_per_file"`
}

// CheckpointConfig contains checkpoint settings
type CheckpointConfig struct {
	StoragePath     string `yaml:"storage_path"`
	SaveIntervalSec int    `yaml:"save_interval_sec"`
}

// ProcessingConfig contains data processing settings
type ProcessingConfig struct {
	BatchSize   int      `yaml:"batch_size"`
	WorkerCount int      `yaml:"worker_count"`
	Filters     []string `yaml:"filters"`
	Transforms  []string `yaml:"transforms"`
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
	if c.Storage.Local.MaxFileSizeMB == 0 {
		c.Storage.Local.MaxFileSizeMB = 100
	}
	if c.Storage.Local.MaxRecordsPerFile == 0 {
		c.Storage.Local.MaxRecordsPerFile = 10000
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
}

// Validate checks if the configuration is valid
func (c *Config) Validate() error {
	// Validate source configuration
	switch c.Source.Type {
	case "mysql":
		if c.Source.MySQL.Host == "" {
			return fmt.Errorf("mysql host is required")
		}
		if c.Source.MySQL.Port == 0 {
			c.Source.MySQL.Port = 3306
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

	validStrategies := map[string]bool{"date": true, "hour": true, "none": true}
	if !validStrategies[c.Storage.Local.PartitionStrategy] {
		return fmt.Errorf("unsupported partition strategy: %s", c.Storage.Local.PartitionStrategy)
	}

	return nil
}
