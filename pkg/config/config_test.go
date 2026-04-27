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
