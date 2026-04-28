package connector

import (
	"testing"

	"data-ingestion-tool/pkg/config"
	"data-ingestion-tool/pkg/logger"
	"data-ingestion-tool/pkg/models"
)

func newTestLogger(t *testing.T) *logger.Logger {
	t.Helper()
	log, err := logger.New("debug", "")
	if err != nil {
		t.Fatalf("Failed to create test logger: %v", err)
	}
	return log
}

func TestFactory_CreateConnector(t *testing.T) {
	tests := []struct {
		name       string
		sourceType string
		wantErr    bool
		errMsg     string
	}{
		{
			name:       "create mysql connector",
			sourceType: "mysql",
			wantErr:    false,
		},
		{
			name:       "create kafka connector",
			sourceType: "kafka",
			wantErr:    false,
		},
		{
			name:       "create postgresql connector",
			sourceType: "postgresql",
			wantErr:    false,
		},
		{
			name:       "create rest connector",
			sourceType: "rest",
			wantErr:    false,
		},
		{
			name:       "unsupported source type",
			sourceType: "mongodb",
			wantErr:    true,
			errMsg:     "unsupported source type: mongodb",
		},
		{
			name:       "empty source type",
			sourceType: "",
			wantErr:    true,
			errMsg:     "unsupported source type: ",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := &config.Config{
				Source: config.SourceConfig{
					Type: tt.sourceType,
					MySQL: config.MySQLConfig{
						Host:     "localhost",
						Port:     3306,
						User:     "root",
						Password: "secret",
						ServerID: 1001,
					},
					Kafka: config.KafkaConfig{
						Brokers: []string{"localhost:9092"},
						Topics:  []string{"test-topic"},
						GroupID: "test-group",
					},
					PostgreSQL: config.PostgresConfig{
						Host:     "localhost",
						Port:     5432,
						User:     "postgres",
						Password: "secret",
						Database: "testdb",
						SlotName: "test_slot",
					},
					REST: config.RESTConfig{
						BaseURL:     "http://localhost:8080",
						Endpoints:   []string{"/api/data"},
						PollInterval: 10,
					},
				},
			}

			log := newTestLogger(t)
			factory := NewFactory(cfg, log)

			connector, err := factory.CreateConnector()

			if tt.wantErr {
				if err == nil {
					t.Errorf("CreateConnector() expected error, got nil")
					return
				}
				if err.Error() != tt.errMsg {
					t.Errorf("CreateConnector() error = %v, wantErr %v", err.Error(), tt.errMsg)
				}
				return
			}

			if err != nil {
				t.Errorf("CreateConnector() unexpected error: %v", err)
				return
			}

			if connector == nil {
				t.Error("CreateConnector() returned nil connector")
			}
		})
	}
}

func TestBaseConnector_IsConnected(t *testing.T) {
	cfg := &config.Config{
		Source: config.SourceConfig{
			Type: "mysql",
			MySQL: config.MySQLConfig{
				Host:     "localhost",
				Port:     3306,
				User:     "root",
				Password: "secret",
				ServerID: 1001,
			},
		},
	}

	log := newTestLogger(t)
	bc := NewBaseConnector(cfg, log)

	if bc.IsConnected() {
		t.Error("IsConnected() should return false for new connector")
	}

	bc.connected = true
	if !bc.IsConnected() {
		t.Error("IsConnected() should return true after setting connected")
	}

	bc.connected = false
	if bc.IsConnected() {
		t.Error("IsConnected() should return false after setting disconnected")
	}
}

func TestBaseConnector_GetPosition(t *testing.T) {
	cfg := &config.Config{
		Source: config.SourceConfig{
			Type: "mysql",
			MySQL: config.MySQLConfig{
				Host:     "localhost",
				Port:     3306,
				User:     "root",
				Password: "secret",
				ServerID: 1001,
			},
		},
	}

	log := newTestLogger(t)
	bc := NewBaseConnector(cfg, log)

	pos := bc.GetPosition()
	if pos.BinlogFile != "" || pos.BinlogPos != 0 {
		t.Error("GetPosition() should return empty position for new connector")
	}

	expectedPos := models.Position{
		BinlogFile: "mysql-bin.000001",
		BinlogPos:  12345,
	}
	bc.position = expectedPos

	gotPos := bc.GetPosition()
	if gotPos.BinlogFile != expectedPos.BinlogFile {
		t.Errorf("GetPosition().BinlogFile = %v, want %v", gotPos.BinlogFile, expectedPos.BinlogFile)
	}
	if gotPos.BinlogPos != expectedPos.BinlogPos {
		t.Errorf("GetPosition().BinlogPos = %v, want %v", gotPos.BinlogPos, expectedPos.BinlogPos)
	}
}

func TestBaseConnector_SetPosition(t *testing.T) {
	cfg := &config.Config{
		Source: config.SourceConfig{
			Type: "mysql",
			MySQL: config.MySQLConfig{
				Host:     "localhost",
				Port:     3306,
				User:     "root",
				Password: "secret",
				ServerID: 1001,
			},
		},
	}

	log := newTestLogger(t)
	bc := NewBaseConnector(cfg, log)

	testCases := []struct {
		name     string
		position models.Position
	}{
		{
			name: "mysql position",
			position: models.Position{
				BinlogFile: "mysql-bin.000002",
				BinlogPos:  54321,
			},
		},
		{
			name: "kafka position",
			position: models.Position{
				Topic:     "test-topic",
				Partition: 0,
				Offset:    100,
			},
		},
		{
			name: "postgresql position",
			position: models.Position{
				LSN: "0/16B6F48",
			},
		},
		{
			name: "empty position",
			position: models.Position{},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			err := bc.SetPosition(tc.position)
			if err != nil {
				t.Errorf("SetPosition() returned error: %v", err)
				return
			}

			gotPos := bc.GetPosition()
			if gotPos != tc.position {
				t.Errorf("SetPosition() failed, got %v, want %v", gotPos, tc.position)
			}
		})
	}
}

func TestBaseConnector_shouldProcessTable(t *testing.T) {
	tests := []struct {
		name          string
		tables        []string
		excludeTables []string
		database      string
		table         string
		want          bool
	}{
		{
			name:          "empty include and exclude - process all",
			tables:        nil,
			excludeTables: nil,
			database:      "testdb",
			table:         "users",
			want:          true,
		},
		{
			name:          "table in include list (full name)",
			tables:        []string{"testdb.users", "testdb.orders"},
			excludeTables: nil,
			database:      "testdb",
			table:         "users",
			want:          true,
		},
		{
			name:          "table in include list (short name)",
			tables:        []string{"users", "orders"},
			excludeTables: nil,
			database:      "testdb",
			table:         "users",
			want:          true,
		},
		{
			name:          "table not in include list",
			tables:        []string{"testdb.users", "testdb.orders"},
			excludeTables: nil,
			database:      "testdb",
			table:         "products",
			want:          false,
		},
		{
			name:          "table in exclude list (full name)",
			tables:        nil,
			excludeTables: []string{"testdb.logs", "testdb.temp"},
			database:      "testdb",
			table:         "logs",
			want:          false,
		},
		{
			name:          "table in exclude list (short name)",
			tables:        nil,
			excludeTables: []string{"logs", "temp"},
			database:      "testdb",
			table:         "logs",
			want:          false,
		},
		{
			name:          "exclude takes precedence over include",
			tables:        []string{"testdb.users"},
			excludeTables: []string{"testdb.users"},
			database:      "testdb",
			table:         "users",
			want:          false,
		},
		{
			name:          "table not in exclude list - process",
			tables:        nil,
			excludeTables: []string{"testdb.logs"},
			database:      "testdb",
			table:         "users",
			want:          true,
		},
		{
			name:          "include list with mixed names",
			tables:        []string{"users", "testdb.orders", "products"},
			excludeTables: nil,
			database:      "testdb",
			table:         "orders",
			want:          true,
		},
		{
			name:          "exclude list with mixed names",
			tables:        nil,
			excludeTables: []string{"logs", "testdb.temp"},
			database:      "testdb",
			table:         "temp",
			want:          false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := &config.Config{
				Source: config.SourceConfig{
					Type: "mysql",
					MySQL: config.MySQLConfig{
						Host:          "localhost",
						Port:          3306,
						User:          "root",
						Password:      "secret",
						ServerID:      1001,
						Tables:        tt.tables,
						ExcludeTables: tt.excludeTables,
					},
				},
			}

			log := newTestLogger(t)
			bc := NewBaseConnector(cfg, log)

			got := bc.shouldProcessTable(tt.database, tt.table)
			if got != tt.want {
				t.Errorf("shouldProcessTable(%s, %s) = %v, want %v", tt.database, tt.table, got, tt.want)
			}
		})
	}
}

func TestNewFactory(t *testing.T) {
	cfg := &config.Config{
		Source: config.SourceConfig{
			Type: "mysql",
		},
	}

	log := newTestLogger(t)
	factory := NewFactory(cfg, log)

	if factory == nil {
		t.Fatal("NewFactory() returned nil")
	}

	if factory.cfg != cfg {
		t.Error("Factory.cfg not set correctly")
	}

	if factory.logger != log {
		t.Error("Factory.logger not set correctly")
	}
}

func TestNewBaseConnector(t *testing.T) {
	cfg := &config.Config{
		Source: config.SourceConfig{
			Type: "mysql",
			MySQL: config.MySQLConfig{
				Host:     "localhost",
				Port:     3306,
				User:     "root",
				Password: "secret",
				ServerID: 1001,
			},
		},
	}

	log := newTestLogger(t)
	bc := NewBaseConnector(cfg, log)

	if bc.cfg != cfg {
		t.Error("BaseConnector.cfg not set correctly")
	}

	if bc.logger != log {
		t.Error("BaseConnector.logger not set correctly")
	}

	if bc.connected {
		t.Error("BaseConnector.connected should be false initially")
	}

	if bc.stopChan == nil {
		t.Error("BaseConnector.stopChan should not be nil")
	}
}
