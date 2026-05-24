package config

import (
	"os"
	"path/filepath"
	"testing"
)

func TestLoad(t *testing.T) {
	// Create a temporary config file
	tmpDir := t.TempDir()
	configPath := filepath.Join(tmpDir, "test_config.yaml")

	configContent := `
app:
  name: "test-app"
  log_level: "debug"
  log_file: "test.log"

source:
  type: "mysql"
  mysql:
    host: "localhost"
    port: 3306
    user: "root"
    password: "secret"
    server_id: 1001
    tables:
      - "db.table1"
      - "db.table2"

storage:
  type: "local"
  local:
    base_path: "./test-data"
    partition_strategy: "hour"
    file_format: "csv"
    max_file_size_mb: 50
    max_records_per_file: 5000

checkpoint:
  storage_path: "./test-checkpoint.json"
  save_interval_sec: 5

processing:
  batch_size: 50
  worker_count: 2
`

	if err := os.WriteFile(configPath, []byte(configContent), 0644); err != nil {
		t.Fatalf("Failed to create test config: %v", err)
	}

	// Test loading config
	cfg, err := Load(configPath)
	if err != nil {
		t.Fatalf("Failed to load config: %v", err)
	}

	// Verify values
	if cfg.App.Name != "test-app" {
		t.Errorf("Expected app name 'test-app', got '%s'", cfg.App.Name)
	}

	if cfg.App.LogLevel != "debug" {
		t.Errorf("Expected log level 'debug', got '%s'", cfg.App.LogLevel)
	}

	if cfg.Source.Type != "mysql" {
		t.Errorf("Expected source type 'mysql', got '%s'", cfg.Source.Type)
	}

	if cfg.Source.MySQL.Host != "localhost" {
		t.Errorf("Expected MySQL host 'localhost', got '%s'", cfg.Source.MySQL.Host)
	}

	if cfg.Source.MySQL.Port != 3306 {
		t.Errorf("Expected MySQL port 3306, got %d", cfg.Source.MySQL.Port)
	}

	if cfg.Source.MySQL.ServerID != 1001 {
		t.Errorf("Expected server_id 1001, got %d", cfg.Source.MySQL.ServerID)
	}

	if len(cfg.Source.MySQL.Tables) != 2 {
		t.Errorf("Expected 2 tables, got %d", len(cfg.Source.MySQL.Tables))
	}

	if cfg.Storage.Local.PartitionStrategy != "hour" {
		t.Errorf("Expected partition strategy 'hour', got '%s'", cfg.Storage.Local.PartitionStrategy)
	}

	if cfg.Storage.Local.FileFormat != "csv" {
		t.Errorf("Expected file format 'csv', got '%s'", cfg.Storage.Local.FileFormat)
	}

	if cfg.Processing.WorkerCount != 2 {
		t.Errorf("Expected worker count 2, got %d", cfg.Processing.WorkerCount)
	}
}

func TestLoadDefaults(t *testing.T) {
	// Create a minimal config file
	tmpDir := t.TempDir()
	configPath := filepath.Join(tmpDir, "minimal_config.yaml")

	configContent := `
source:
  mysql:
    host: "localhost"
    user: "root"
    password: "secret"
    server_id: 1001
`

	if err := os.WriteFile(configPath, []byte(configContent), 0644); err != nil {
		t.Fatalf("Failed to create test config: %v", err)
	}

	cfg, err := Load(configPath)
	if err != nil {
		t.Fatalf("Failed to load config: %v", err)
	}

	// Verify defaults
	if cfg.App.Name != "data-ingestion-tool" {
		t.Errorf("Expected default app name, got '%s'", cfg.App.Name)
	}

	if cfg.App.LogLevel != "info" {
		t.Errorf("Expected default log level 'info', got '%s'", cfg.App.LogLevel)
	}

	if cfg.Storage.Local.BasePath != "./data-lake" {
		t.Errorf("Expected default base path, got '%s'", cfg.Storage.Local.BasePath)
	}

	if cfg.Storage.Local.PartitionStrategy != "date" {
		t.Errorf("Expected default partition strategy 'date', got '%s'", cfg.Storage.Local.PartitionStrategy)
	}

	if cfg.Storage.Local.FileFormat != "json" {
		t.Errorf("Expected default file format 'json', got '%s'", cfg.Storage.Local.FileFormat)
	}
}

func TestValidate(t *testing.T) {
	tests := []struct {
		name    string
		config  Config
		wantErr bool
	}{
		{
			name: "valid mysql config",
			config: Config{
				Source: SourceConfig{
					Type: "mysql",
					MySQL: MySQLConfig{
						Host:     "localhost",
						Port:     3306,
						User:     "root",
						ServerID: 1001,
					},
				},
				Storage: StorageConfig{
					Type: "local",
					Local: LocalConfig{
						FileFormat:        "json",
						PartitionStrategy: "date",
						Compression:       "none",
					},
				},
			},
			wantErr: false,
		},
		{
			name: "missing mysql host",
			config: Config{
				Source: SourceConfig{
					Type: "mysql",
					MySQL: MySQLConfig{
						Port:     3306,
						User:     "root",
						ServerID: 1001,
					},
				},
			},
			wantErr: true,
		},
		{
			name: "missing mysql user",
			config: Config{
				Source: SourceConfig{
					Type: "mysql",
					MySQL: MySQLConfig{
						Host:     "localhost",
						Port:     3306,
						ServerID: 1001,
					},
				},
			},
			wantErr: true,
		},
		{
			name: "missing server_id",
			config: Config{
				Source: SourceConfig{
					Type: "mysql",
					MySQL: MySQLConfig{
						Host: "localhost",
						Port: 3306,
						User: "root",
					},
				},
			},
			wantErr: true,
		},
		{
			name: "invalid file format",
			config: Config{
				Source: SourceConfig{
					Type: "mysql",
					MySQL: MySQLConfig{
						Host:     "localhost",
						Port:     3306,
						User:     "root",
						ServerID: 1001,
					},
				},
				Storage: StorageConfig{
					Type: "local",
					Local: LocalConfig{
						FileFormat:        "xml",
						PartitionStrategy: "date",
					},
				},
			},
			wantErr: true,
		},
		{
			name: "invalid partition strategy",
			config: Config{
				Source: SourceConfig{
					Type: "mysql",
					MySQL: MySQLConfig{
						Host:     "localhost",
						Port:     3306,
						User:     "root",
						ServerID: 1001,
					},
				},
				Storage: StorageConfig{
					Type: "local",
					Local: LocalConfig{
						FileFormat:        "json",
						PartitionStrategy: "week",
					},
				},
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.config.Validate()
			if (err != nil) != tt.wantErr {
				t.Errorf("Validate() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestLoadNonExistentFile(t *testing.T) {
	_, err := Load("/nonexistent/path/config.yaml")
	if err == nil {
		t.Error("Expected error for non-existent file, got nil")
	}
}

func TestLoadInvalidYAML(t *testing.T) {
	tmpDir := t.TempDir()
	configPath := filepath.Join(tmpDir, "invalid_config.yaml")

	// Invalid YAML content
	configContent := `
app:
  name: "test
  log_level: debug
invalid yaml content: [
`

	if err := os.WriteFile(configPath, []byte(configContent), 0644); err != nil {
		t.Fatalf("Failed to create test config: %v", err)
	}

	_, err := Load(configPath)
	if err == nil {
		t.Error("Expected error for invalid YAML, got nil")
	}
}

func TestIsPlaintextPassword(t *testing.T) {
	tests := []struct {
		name     string
		password string
		expected bool
	}{
		{"empty", "", false},
		{"plaintext", "secret123", true},
		{"env var syntax", "${MYSQL_PASSWORD}", false},
		{"env var with default", "${MYSQL_PASSWORD:-default}", false},
		{"env var with braces", "${DB_PASS}", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := IsPlaintextPassword(tt.password)
			if result != tt.expected {
				t.Errorf("IsPlaintextPassword(%q) = %v, want %v", tt.password, result, tt.expected)
			}
		})
	}
}

func TestGetPlaintextPasswordFields(t *testing.T) {
	tests := []struct {
		name          string
		config        Config
		expectedCount int
	}{
		{
			name: "mysql plaintext password",
			config: Config{
				Source: SourceConfig{
					Type: "mysql",
					MySQL: MySQLConfig{
						Host:     "localhost",
						User:     "root",
						Password: "secret",
						ServerID: 1001,
					},
				},
			},
			expectedCount: 1,
		},
		{
			name: "mysql env var password",
			config: Config{
				Source: SourceConfig{
					Type: "mysql",
					MySQL: MySQLConfig{
						Host:     "localhost",
						User:     "root",
						Password: "${MYSQL_PASSWORD}",
						ServerID: 1001,
					},
				},
			},
			expectedCount: 0,
		},
		{
			name: "postgresql plaintext password",
			config: Config{
				Source: SourceConfig{
					Type: "postgresql",
					PostgreSQL: PostgresConfig{
						Host:     "localhost",
						User:     "postgres",
						Password: "secret",
					},
				},
			},
			expectedCount: 1,
		},
		{
			name: "rest plaintext auth",
			config: Config{
				Source: SourceConfig{
					Type: "rest",
					REST: RESTConfig{
						BaseURL: "http://api.example.com",
						Headers: map[string]string{
							"Authorization": "Bearer secret-token",
						},
					},
				},
			},
			expectedCount: 1,
		},
		{
			name: "rest env var auth",
			config: Config{
				Source: SourceConfig{
					Type: "rest",
					REST: RESTConfig{
						BaseURL: "http://api.example.com",
						Headers: map[string]string{
							"Authorization": "${API_TOKEN}",
						},
					},
				},
			},
			expectedCount: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fields := tt.config.GetPlaintextPasswordFields()
			if len(fields) != tt.expectedCount {
				t.Errorf("Expected %d plaintext fields, got %d: %v", tt.expectedCount, len(fields), fields)
			}
		})
	}
}

func TestValidateKafkaConfig(t *testing.T) {
	tests := []struct {
		name    string
		config  Config
		wantErr bool
	}{
		{
			name: "valid kafka config",
			config: Config{
				Source: SourceConfig{
					Type: "kafka",
					Kafka: KafkaConfig{
						Brokers: []string{"localhost:9092"},
						Topics:  []string{"test-topic"},
						GroupID: "test-group",
					},
				},
				Storage: StorageConfig{
					Type: "local",
					Local: LocalConfig{
						FileFormat:        "json",
						PartitionStrategy: "date",
						Compression:       "none",
					},
				},
			},
			wantErr: true, // kafka connector not yet implemented
		},
		{
			name: "missing kafka brokers",
			config: Config{
				Source: SourceConfig{
					Type: "kafka",
					Kafka: KafkaConfig{
						Topics:  []string{"test-topic"},
						GroupID: "test-group",
					},
				},
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.config.Validate()
			if (err != nil) != tt.wantErr {
				t.Errorf("Validate() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestValidatePostgreSQLConfig(t *testing.T) {
	tests := []struct {
		name    string
		config  Config
		wantErr bool
	}{
		{
			name: "valid postgresql config",
			config: Config{
				Source: SourceConfig{
					Type: "postgresql",
					PostgreSQL: PostgresConfig{
						Host:     "localhost",
						Port:     5432,
						User:     "postgres",
						Password: "secret",
						Database: "testdb",
						SlotName: "test_slot",
					},
				},
				Storage: StorageConfig{
					Type: "local",
					Local: LocalConfig{
						FileFormat:        "json",
						PartitionStrategy: "date",
						Compression:       "none",
					},
				},
			},
			wantErr: true, // postgresql connector not yet implemented
		},
		{
			name: "missing postgresql host",
			config: Config{
				Source: SourceConfig{
					Type: "postgresql",
					PostgreSQL: PostgresConfig{
						Port:     5432,
						User:     "postgres",
						Password: "secret",
					},
				},
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.config.Validate()
			if (err != nil) != tt.wantErr {
				t.Errorf("Validate() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestValidateRESTConfig(t *testing.T) {
	tests := []struct {
		name    string
		config  Config
		wantErr bool
	}{
		{
			name: "valid rest config",
			config: Config{
				Source: SourceConfig{
					Type: "rest",
					REST: RESTConfig{
						BaseURL:      "http://api.example.com",
						Endpoints:    []string{"/users", "/orders"},
						PollInterval: 60,
					},
				},
				Storage: StorageConfig{
					Type: "local",
					Local: LocalConfig{
						FileFormat:        "json",
						PartitionStrategy: "date",
						Compression:       "none",
					},
				},
			},
			wantErr: true, // rest connector not yet implemented
		},
		{
			name: "missing rest base_url",
			config: Config{
				Source: SourceConfig{
					Type: "rest",
					REST: RESTConfig{
						Endpoints: []string{"/users"},
					},
				},
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.config.Validate()
			if (err != nil) != tt.wantErr {
				t.Errorf("Validate() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestValidateCompression(t *testing.T) {
	tests := []struct {
		name    string
		config  Config
		wantErr bool
	}{
		{
			name: "valid compression snappy",
			config: Config{
				Source: SourceConfig{
					Type: "mysql",
					MySQL: MySQLConfig{
						Host:     "localhost",
						User:     "root",
						ServerID: 1001,
					},
				},
				Storage: StorageConfig{
					Type: "local",
					Local: LocalConfig{
						FileFormat:        "parquet",
						PartitionStrategy: "date",
						Compression:       "snappy",
					},
				},
			},
			wantErr: false,
		},
		{
			name: "valid compression gzip",
			config: Config{
				Source: SourceConfig{
					Type: "mysql",
					MySQL: MySQLConfig{
						Host:     "localhost",
						User:     "root",
						ServerID: 1001,
					},
				},
				Storage: StorageConfig{
					Type: "local",
					Local: LocalConfig{
						FileFormat:        "json",
						PartitionStrategy: "date",
						Compression:       "gzip",
					},
				},
			},
			wantErr: false,
		},
		{
			name: "valid compression zstd",
			config: Config{
				Source: SourceConfig{
					Type: "mysql",
					MySQL: MySQLConfig{
						Host:     "localhost",
						User:     "root",
						ServerID: 1001,
					},
				},
				Storage: StorageConfig{
					Type: "local",
					Local: LocalConfig{
						FileFormat:        "parquet",
						PartitionStrategy: "date",
						Compression:       "zstd",
					},
				},
			},
			wantErr: false,
		},
		{
			name: "invalid compression",
			config: Config{
				Source: SourceConfig{
					Type: "mysql",
					MySQL: MySQLConfig{
						Host:     "localhost",
						User:     "root",
						ServerID: 1001,
					},
				},
				Storage: StorageConfig{
					Type: "local",
					Local: LocalConfig{
						FileFormat:        "json",
						PartitionStrategy: "date",
						Compression:       "invalid",
					},
				},
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.config.Validate()
			if (err != nil) != tt.wantErr {
				t.Errorf("Validate() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestValidatePartitionStrategy(t *testing.T) {
	tests := []struct {
		name    string
		config  Config
		wantErr bool
	}{
		{
			name: "valid strategy date",
			config: Config{
				Source: SourceConfig{
					Type: "mysql",
					MySQL: MySQLConfig{
						Host:     "localhost",
						User:     "root",
						ServerID: 1001,
					},
				},
				Storage: StorageConfig{
					Type: "local",
					Local: LocalConfig{
						FileFormat:        "json",
						PartitionStrategy: "date",
						Compression:       "none",
					},
				},
			},
			wantErr: false,
		},
		{
			name: "valid strategy hour",
			config: Config{
				Source: SourceConfig{
					Type: "mysql",
					MySQL: MySQLConfig{
						Host:     "localhost",
						User:     "root",
						ServerID: 1001,
					},
				},
				Storage: StorageConfig{
					Type: "local",
					Local: LocalConfig{
						FileFormat:        "json",
						PartitionStrategy: "hour",
						Compression:       "none",
					},
				},
			},
			wantErr: false,
		},
		{
			name: "valid strategy none",
			config: Config{
				Source: SourceConfig{
					Type: "mysql",
					MySQL: MySQLConfig{
						Host:     "localhost",
						User:     "root",
						ServerID: 1001,
					},
				},
				Storage: StorageConfig{
					Type: "local",
					Local: LocalConfig{
						FileFormat:        "json",
						PartitionStrategy: "none",
						Compression:       "none",
					},
				},
			},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.config.Validate()
			if (err != nil) != tt.wantErr {
				t.Errorf("Validate() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestValidateUnsupportedSourceType(t *testing.T) {
	config := Config{
		Source: SourceConfig{
			Type: "unsupported",
		},
	}

	err := config.Validate()
	if err == nil {
		t.Error("Expected error for unsupported source type, got nil")
	}
}

func TestValidateUnsupportedStorageType(t *testing.T) {
	config := Config{
		Source: SourceConfig{
			Type: "mysql",
			MySQL: MySQLConfig{
				Host:     "localhost",
				User:     "root",
				ServerID: 1001,
			},
		},
		Storage: StorageConfig{
			Type: "s3",
		},
	}

	err := config.Validate()
	if err == nil {
		t.Error("Expected error for unsupported storage type, got nil")
	}
}
