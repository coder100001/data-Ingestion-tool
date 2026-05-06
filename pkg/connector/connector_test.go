package connector

import (
	"context"
	"reflect"
	"testing"
	"time"

	"data-ingestion-tool/pkg/config"
	"data-ingestion-tool/pkg/logger"
	"data-ingestion-tool/pkg/models"

	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
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
						BaseURL:      "http://localhost:8080",
						Endpoints:    []string{"/api/data"},
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

// TestMySQLConnector_convertValue tests the MySQL value type conversion
func TestMySQLConnector_convertValue(t *testing.T) {
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
	mc := NewMySQLConnector(cfg, log)

	tests := []struct {
		name string
		val  interface{}
		want interface{}
	}{
		{"nil value", nil, nil},
		{"byte slice", []byte("hello"), "hello"},
		{"string", "world", "world"},
		{"integer", 42, 42},
		{"float64", 3.14, 3.14},
		{"bool true", true, true},
		{"empty byte slice", []byte{}, ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := mc.convertValue(tt.val)
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("convertValue(%v) = %v (type: %T), want %v (type: %T)",
					tt.val, got, got, tt.want, tt.want)
			}
		})
	}
}

// TestMySQLConnector_rowToMap tests row-to-map conversion
func TestMySQLConnector_rowToMap(t *testing.T) {
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
	mc := NewMySQLConnector(cfg, log)

	t.Run("with tableInfo", func(t *testing.T) {
		tableInfo := &models.TableInfo{
			Database: "testdb",
			Table:    "users",
			Columns: []models.ColumnInfo{
				{Name: "id", Type: "int"},
				{Name: "name", Type: "varchar(255)"},
				{Name: "data", Type: "blob"},
			},
		}
		row := []interface{}{1, "Alice", []byte("binary-data")}
		result := mc.rowToMap(row, tableInfo)

		if result["id"] != 1 {
			t.Errorf("rowToMap id = %v, want 1", result["id"])
		}
		if result["name"] != "Alice" {
			t.Errorf("rowToMap name = %v, want Alice", result["name"])
		}
		if result["data"] != "binary-data" {
			t.Errorf("rowToMap data = %v, want binary-data", result["data"])
		}
	})

	t.Run("without tableInfo (fallback)", func(t *testing.T) {
		row := []interface{}{42, "test"}
		result := mc.rowToMap(row, nil)

		if result["col_0"] != 42 {
			t.Errorf("rowToMap col_0 = %v, want 42", result["col_0"])
		}
		if result["col_1"] != "test" {
			t.Errorf("rowToMap col_1 = %v, want test", result["col_1"])
		}
	})

	t.Run("row shorter than columns", func(t *testing.T) {
		tableInfo := &models.TableInfo{
			Database: "testdb",
			Table:    "users",
			Columns: []models.ColumnInfo{
				{Name: "id", Type: "int"},
				{Name: "name", Type: "varchar(255)"},
			},
		}
		row := []interface{}{1}
		result := mc.rowToMap(row, tableInfo)

		if result["id"] != 1 {
			t.Errorf("rowToMap id = %v, want 1", result["id"])
		}
		// col_1 should not exist
		if _, exists := result["name"]; exists {
			t.Error("rowToMap should not have name entry for missing column")
		}
	})
}

// TestMySQLConnector_invalidateTableCache tests table cache clearing
func TestMySQLConnector_invalidateTableCache(t *testing.T) {
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
	mc := NewMySQLConnector(cfg, log)

	// Add something to cache
	mc.tableCache["testdb.users"] = &models.TableInfo{
		Database: "testdb",
		Table:    "users",
	}

	if len(mc.tableCache) == 0 {
		t.Error("tableCache should have entries before invalidation")
	}

	mc.invalidateTableCache()

	if len(mc.tableCache) != 0 {
		t.Error("tableCache should be empty after invalidation")
	}

	if mc.tableCache == nil {
		t.Error("tableCache should not be nil after invalidation")
	}
}

// TestMySQLConnector_Connect_Validation tests MySQL connection validation errors
func TestMySQLConnector_Connect_Validation(t *testing.T) {
	tests := []struct {
		name    string
		config  config.MySQLConfig
		wantErr string
	}{
		{
			name: "missing host",
			config: config.MySQLConfig{
				Host:     "",
				Port:     3306,
				User:     "root",
				Password: "secret",
				ServerID: 1001,
			},
			wantErr: "MySQL host is required",
		},
		{
			name: "missing user",
			config: config.MySQLConfig{
				Host:     "localhost",
				Port:     3306,
				User:     "",
				Password: "secret",
				ServerID: 1001,
			},
			wantErr: "MySQL user is required",
		},
		{
			name: "missing server_id",
			config: config.MySQLConfig{
				Host:     "localhost",
				Port:     3306,
				User:     "root",
				Password: "secret",
				ServerID: 0,
			},
			wantErr: "MySQL server_id is required (must be unique for replication)",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := &config.Config{
				Source: config.SourceConfig{
					Type:  "mysql",
					MySQL: tt.config,
				},
			}
			log := newTestLogger(t)
			mc := NewMySQLConnector(cfg, log)

			err := mc.Connect(context.Background())
			if err == nil {
				t.Fatal("Connect() expected error, got nil")
			}
			if err.Error() != tt.wantErr {
				t.Errorf("Connect() error = %v, want %v", err.Error(), tt.wantErr)
			}
		})
	}
}

// TestMySQLConnector_Disconnect_NilResources tests disconnect with nil resources
func TestMySQLConnector_Disconnect_NilResources(t *testing.T) {
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
	mc := NewMySQLConnector(cfg, log)

	// Should not panic or error when syncer and canal are nil
	err := mc.Disconnect()
	if err != nil {
		t.Errorf("Disconnect() unexpected error: %v", err)
	}

	if mc.connected {
		t.Error("Disconnect() should set connected to false")
	}
}

// TestMySQLConnector_Stop tests the Stop method
func TestMySQLConnector_Stop(t *testing.T) {
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
	mc := NewMySQLConnector(cfg, log)

	err := mc.Stop()
	if err != nil {
		t.Errorf("Stop() unexpected error: %v", err)
	}

	// Stop again - should not panic on double-close of channel
	err = mc.Stop()
	if err != nil {
		t.Errorf("Stop() second call unexpected error: %v", err)
	}
}

// TestMySQLConnector_Start_NotConnected tests that Start fails when not connected
func TestMySQLConnector_Start_NotConnected(t *testing.T) {
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
	mc := NewMySQLConnector(cfg, log)

	changeChan := make(chan *models.DataChange, 10)
	err := mc.Start(context.Background(), changeChan)
	if err == nil {
		t.Fatal("Start() expected error when not connected, got nil")
	}
	if err.Error() != "not connected to MySQL" {
		t.Errorf("Start() error = %v, want 'not connected to MySQL'", err.Error())
	}
}

// TestBinlogHandler_Methods tests the binlogHandler event handler methods
func TestBinlogHandler_Methods(t *testing.T) {
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
	mc := NewMySQLConnector(cfg, log)

	changeChan := make(chan *models.DataChange, 10)
	handler := &binlogHandler{
		connector:  mc,
		changeChan: changeChan,
	}

	t.Run("String", func(t *testing.T) {
		if handler.String() != "binlogHandler" {
			t.Errorf("String() = %v, want binlogHandler", handler.String())
		}
	})

	t.Run("OnTableChanged", func(t *testing.T) {
		err := handler.OnTableChanged("testdb", "users")
		if err != nil {
			t.Errorf("OnTableChanged() unexpected error: %v", err)
		}
	})

	t.Run("OnRow", func(t *testing.T) {
		err := handler.OnRow(nil)
		if err != nil {
			t.Errorf("OnRow() unexpected error: %v", err)
		}
	})

	t.Run("OnXID", func(t *testing.T) {
		err := handler.OnXID(mysql.Position{})
		if err != nil {
			t.Errorf("OnXID() unexpected error: %v", err)
		}
	})
}

// TestPlaceholderConnectors verifies all placeholder connectors work correctly
func TestPlaceholderConnectors(t *testing.T) {
	cfg := &config.Config{
		Source: config.SourceConfig{
			Type: "postgresql",
			PostgreSQL: config.PostgresConfig{
				Host:     "localhost",
				Port:     5432,
				User:     "postgres",
				Password: "secret",
				Database: "testdb",
				SlotName: "test_slot",
			},
		},
	}
	log := newTestLogger(t)

	t.Run("PostgresConnector", func(t *testing.T) {
		pc := NewPostgresConnector(cfg, log)
		if pc == nil {
			t.Fatal("NewPostgresConnector returned nil")
		}

		// Disconnect should succeed
		err := pc.Disconnect()
		if err != nil {
			t.Errorf("Disconnect() unexpected error: %v", err)
		}

		// Stop should succeed
		err = pc.Stop()
		if err != nil {
			t.Errorf("Stop() unexpected error: %v", err)
		}

		// Connect should return error (not implemented)
		err = pc.Connect(context.Background())
		if err == nil || err.Error() != "PostgreSQL connector not yet implemented" {
			t.Errorf("Connect() error = %v, want 'PostgreSQL connector not yet implemented'", err)
		}

		// Start should return error (not implemented)
		err = pc.Start(context.Background(), make(chan *models.DataChange))
		if err == nil || err.Error() != "PostgreSQL connector not yet implemented" {
			t.Errorf("Start() error = %v, want 'PostgreSQL connector not yet implemented'", err)
		}

		// GetTableInfo should return error
		_, err = pc.GetTableInfo("testdb", "users")
		if err == nil || err.Error() != "GetTableInfo not yet implemented for PostgreSQL" {
			t.Errorf("GetTableInfo() error = %v, want 'GetTableInfo not yet implemented for PostgreSQL'", err)
		}
	})

	t.Run("KafkaConnector", func(t *testing.T) {
		kc := NewKafkaConnector(cfg, log)
		if kc == nil {
			t.Fatal("NewKafkaConnector returned nil")
		}

		err := kc.Disconnect()
		if err != nil {
			t.Errorf("Disconnect() unexpected error: %v", err)
		}

		err = kc.Stop()
		if err != nil {
			t.Errorf("Stop() unexpected error: %v", err)
		}

		err = kc.Connect(context.Background())
		if err == nil || err.Error() != "kafka connector not yet implemented" {
			t.Errorf("Connect() error = %v, want 'kafka connector not yet implemented'", err)
		}

		err = kc.Start(context.Background(), make(chan *models.DataChange))
		if err == nil || err.Error() != "kafka connector not yet implemented" {
			t.Errorf("Start() error = %v, want 'kafka connector not yet implemented'", err)
		}

		_, err = kc.GetTableInfo("testdb", "users")
		if err == nil || err.Error() != "GetTableInfo not applicable for Kafka source" {
			t.Errorf("GetTableInfo() error = %v, want 'GetTableInfo not applicable for Kafka source'", err)
		}
	})

	t.Run("RESTConnector", func(t *testing.T) {
		rc := NewRESTConnector(cfg, log)
		if rc == nil {
			t.Fatal("NewRESTConnector returned nil")
		}

		err := rc.Disconnect()
		if err != nil {
			t.Errorf("Disconnect() unexpected error: %v", err)
		}

		err = rc.Stop()
		if err != nil {
			t.Errorf("Stop() unexpected error: %v", err)
		}

		err = rc.Connect(context.Background())
		if err == nil || err.Error() != "REST connector not yet implemented" {
			t.Errorf("Connect() error = %v, want 'REST connector not yet implemented'", err)
		}

		err = rc.Start(context.Background(), make(chan *models.DataChange))
		if err == nil || err.Error() != "REST connector not yet implemented" {
			t.Errorf("Start() error = %v, want 'REST connector not yet implemented'", err)
		}

		_, err = rc.GetTableInfo("testdb", "users")
		if err == nil || err.Error() != "GetTableInfo not applicable for REST source" {
			t.Errorf("GetTableInfo() error = %v, want 'GetTableInfo not applicable for REST source'", err)
		}
	})
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
			name:     "empty position",
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

func TestMySQLConnector_shouldProcessTable(t *testing.T) {
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
			mc := NewMySQLConnector(cfg, log)

			got := mc.shouldProcessTable(tt.database, tt.table)
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

func TestMySQLConnector_convertValue_EdgeCases(t *testing.T) {
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
	mc := NewMySQLConnector(cfg, log)

	now := time.Now()
	tests := []struct {
		name string
		val  interface{}
		want interface{}
	}{
		{"int8", int8(8), int8(8)},
		{"int16", int16(16), int16(16)},
		{"int32", int32(32), int32(32)},
		{"int64", int64(64), int64(64)},
		{"uint", uint(42), uint(42)},
		{"uint8", uint8(8), uint8(8)},
		{"uint16", uint16(16), uint16(16)},
		{"uint32", uint32(32), uint32(32)},
		{"uint64", uint64(64), uint64(64)},
		{"float32", float32(3.14), float32(3.14)},
		{"time.Time", now, now},
		{"empty string", "", ""},
		{"byte slice with null bytes", []byte("hello\x00world"), "hello\x00world"},
		{"nil pointer", (*int)(nil), (*int)(nil)},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := mc.convertValue(tt.val)
			if got != tt.want {
				t.Errorf("convertValue(%v) = %v, want %v", tt.val, got, tt.want)
			}
		})
	}
}

func TestMySQLConnector_rowToMap_EdgeCases(t *testing.T) {
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
	mc := NewMySQLConnector(cfg, log)

	t.Run("nil row", func(t *testing.T) {
		result := mc.rowToMap(nil, nil)
		if result == nil {
			t.Error("rowToMap(nil, nil) should return empty map, not nil")
		}
		if len(result) != 0 {
			t.Errorf("rowToMap(nil, nil) should return empty map, got %d entries", len(result))
		}
	})

	t.Run("empty row", func(t *testing.T) {
		result := mc.rowToMap([]interface{}{}, nil)
		if result == nil {
			t.Error("rowToMap([], nil) should return empty map, not nil")
		}
		if len(result) != 0 {
			t.Errorf("rowToMap([], nil) should return empty map, got %d entries", len(result))
		}
	})

	t.Run("row longer than columns", func(t *testing.T) {
		tableInfo := &models.TableInfo{
			Database: "testdb",
			Table:    "users",
			Columns: []models.ColumnInfo{
				{Name: "id", Type: "int"},
			},
		}
		row := []interface{}{1, "unexpected"}
		result := mc.rowToMap(row, tableInfo)
		if result["id"] != 1 {
			t.Errorf("rowToMap id = %v, want 1", result["id"])
		}
		if len(result) != 1 {
			t.Errorf("rowToMap should have 1 entry for defined column, got %d", len(result))
		}
	})

	t.Run("nil values in row", func(t *testing.T) {
		tableInfo := &models.TableInfo{
			Database: "testdb",
			Table:    "users",
			Columns: []models.ColumnInfo{
				{Name: "id", Type: "int"},
				{Name: "name", Type: "varchar"},
			},
		}
		row := []interface{}{1, nil}
		result := mc.rowToMap(row, tableInfo)
		if result["id"] != 1 {
			t.Errorf("rowToMap id = %v, want 1", result["id"])
		}
		if result["name"] != nil {
			t.Errorf("rowToMap name = %v, want nil", result["name"])
		}
	})

	t.Run("byte slice values converted via convertValue", func(t *testing.T) {
		tableInfo := &models.TableInfo{
			Database: "testdb",
			Table:    "users",
			Columns: []models.ColumnInfo{
				{Name: "data", Type: "blob"},
			},
		}
		row := []interface{}{[]byte("binary-data")}
		result := mc.rowToMap(row, tableInfo)
		if result["data"] != "binary-data" {
			t.Errorf("rowToMap data = %v, want binary-data", result["data"])
		}
	})
}

func TestMySQLConnector_processEvent_Routing(t *testing.T) {
	cfg := &config.Config{
		Source: config.SourceConfig{
			Type: "mysql",
			MySQL: config.MySQLConfig{
				Host:          "localhost",
				Port:          3306,
				User:          "root",
				Password:      "secret",
				ServerID:      1001,
				Tables:        []string{"testdb.users"},
				ExcludeTables: nil,
			},
		},
	}
	log := newTestLogger(t)
	mc := NewMySQLConnector(cfg, log)

	changeChan := make(chan *models.DataChange, 10)
	mc.handler = &binlogHandler{
		connector:  mc,
		changeChan: changeChan,
	}

	t.Run("unknown event type returns nil", func(t *testing.T) {
		event := &replication.BinlogEvent{
			Header: &replication.EventHeader{
				EventType: replication.ROTATE_EVENT,
				LogPos:    100,
			},
		}
		err := mc.processEvent(event)
		if err != nil {
			t.Errorf("processEvent() unexpected error: %v", err)
		}
	})

	t.Run("nil event handler does not panic", func(t *testing.T) {
		mcNoHandler := NewMySQLConnector(cfg, log)
		event := &replication.BinlogEvent{
			Header: &replication.EventHeader{
				EventType: replication.QUERY_EVENT,
				LogPos:    100,
			},
		}
		err := mcNoHandler.processEvent(event)
		if err != nil {
			t.Errorf("processEvent() unexpected error: %v", err)
		}
	})
}

func TestMySQLConnector_processTableMapEvent_Cached(t *testing.T) {
	cfg := &config.Config{
		Source: config.SourceConfig{
			Type: "mysql",
			MySQL: config.MySQLConfig{
				Host:          "localhost",
				Port:          3306,
				User:          "root",
				Password:      "secret",
				ServerID:      1001,
				Tables:        []string{"testdb.users"},
				ExcludeTables: nil,
			},
		},
	}
	log := newTestLogger(t)
	mc := NewMySQLConnector(cfg, log)

	key := "testdb.users"
	mc.tableCache[key] = &models.TableInfo{
		Database: "testdb",
		Table:    "users",
		Columns: []models.ColumnInfo{
			{Name: "id", Type: "int"},
		},
	}

	tme := &replication.TableMapEvent{
		Schema: []byte("testdb"),
		Table:  []byte("users"),
	}

	err := mc.processTableMapEvent(tme)
	if err != nil {
		t.Errorf("processTableMapEvent() unexpected error for cached table: %v", err)
	}
}

func TestMySQLConnector_processRowsEvent_Insert(t *testing.T) {
	cfg := &config.Config{
		Source: config.SourceConfig{
			Type: "mysql",
			MySQL: config.MySQLConfig{
				Host:          "localhost",
				Port:          3306,
				User:          "root",
				Password:      "secret",
				ServerID:      1001,
				Tables:        []string{"testdb.users"},
				ExcludeTables: nil,
			},
		},
	}
	log := newTestLogger(t)
	mc := NewMySQLConnector(cfg, log)

	changeChan := make(chan *models.DataChange, 10)
	mc.handler = &binlogHandler{
		connector:  mc,
		changeChan: changeChan,
	}
	mc.position = models.Position{
		BinlogFile: "mysql-bin.000001",
		BinlogPos:  12345,
	}

	key := "testdb.users"
	mc.tableCache[key] = &models.TableInfo{
		Database: "testdb",
		Table:    "users",
		Columns: []models.ColumnInfo{
			{Name: "id", Type: "int"},
			{Name: "name", Type: "varchar(255)"},
		},
	}

	header := &replication.EventHeader{
		EventType: replication.WRITE_ROWS_EVENTv1,
		LogPos:    200,
	}

	rowsEvent := &replication.RowsEvent{
		Table: &replication.TableMapEvent{
			Schema: []byte("testdb"),
			Table:  []byte("users"),
		},
		Rows: [][]interface{}{
			{1, "Alice"},
		},
	}

	err := mc.processRowsEvent(header, rowsEvent)
	if err != nil {
		t.Errorf("processRowsEvent() unexpected error: %v", err)
	}

	select {
	case change := <-changeChan:
		if change.Type != models.Insert {
			t.Errorf("Type = %v, want Insert", change.Type)
		}
		if change.Database != "testdb" {
			t.Errorf("Database = %v, want testdb", change.Database)
		}
		if change.Table != "users" {
			t.Errorf("Table = %v, want users", change.Table)
		}
		if change.BinlogFile != "mysql-bin.000001" {
			t.Errorf("BinlogFile = %v, want mysql-bin.000001", change.BinlogFile)
		}
		if change.After["id"] != 1 || change.After["name"] != "Alice" {
			t.Errorf("After data mismatch: got %v", change.After)
		}
	default:
		t.Error("expected change event on channel, got none")
	}
}

func TestMySQLConnector_processRowsEvent_Update(t *testing.T) {
	cfg := &config.Config{
		Source: config.SourceConfig{
			Type: "mysql",
			MySQL: config.MySQLConfig{
				Host:          "localhost",
				Port:          3306,
				User:          "root",
				Password:      "secret",
				ServerID:      1001,
				Tables:        []string{"testdb.users"},
				ExcludeTables: nil,
			},
		},
	}
	log := newTestLogger(t)
	mc := NewMySQLConnector(cfg, log)

	changeChan := make(chan *models.DataChange, 10)
	mc.handler = &binlogHandler{
		connector:  mc,
		changeChan: changeChan,
	}
	mc.position = models.Position{
		BinlogFile: "mysql-bin.000001",
		BinlogPos:  12345,
	}

	key := "testdb.users"
	mc.tableCache[key] = &models.TableInfo{
		Database: "testdb",
		Table:    "users",
		Columns: []models.ColumnInfo{
			{Name: "id", Type: "int"},
			{Name: "name", Type: "varchar(255)"},
		},
	}

	header := &replication.EventHeader{
		EventType: replication.UPDATE_ROWS_EVENTv1,
		LogPos:    200,
	}

	rowsEvent := &replication.RowsEvent{
		Table: &replication.TableMapEvent{
			Schema: []byte("testdb"),
			Table:  []byte("users"),
		},
		Rows: [][]interface{}{
			{1, "Alice"},
			{1, "AliceUpdated"},
		},
	}

	err := mc.processRowsEvent(header, rowsEvent)
	if err != nil {
		t.Errorf("processRowsEvent() unexpected error: %v", err)
	}

	select {
	case change := <-changeChan:
		if change.Type != models.Update {
			t.Errorf("Type = %v, want Update", change.Type)
		}
		if change.Before["id"] != 1 || change.Before["name"] != "Alice" {
			t.Errorf("Before data mismatch: got %v", change.Before)
		}
		if change.After["name"] != "AliceUpdated" {
			t.Errorf("After data mismatch: got %v", change.After)
		}
	default:
		t.Error("expected change event on channel, got none")
	}
}

func TestMySQLConnector_processRowsEvent_Delete(t *testing.T) {
	cfg := &config.Config{
		Source: config.SourceConfig{
			Type: "mysql",
			MySQL: config.MySQLConfig{
				Host:          "localhost",
				Port:          3306,
				User:          "root",
				Password:      "secret",
				ServerID:      1001,
				Tables:        []string{"testdb.users"},
				ExcludeTables: nil,
			},
		},
	}
	log := newTestLogger(t)
	mc := NewMySQLConnector(cfg, log)

	changeChan := make(chan *models.DataChange, 10)
	mc.handler = &binlogHandler{
		connector:  mc,
		changeChan: changeChan,
	}
	mc.position = models.Position{
		BinlogFile: "mysql-bin.000001",
		BinlogPos:  12345,
	}

	key := "testdb.users"
	mc.tableCache[key] = &models.TableInfo{
		Database: "testdb",
		Table:    "users",
		Columns: []models.ColumnInfo{
			{Name: "id", Type: "int"},
			{Name: "name", Type: "varchar(255)"},
		},
	}

	header := &replication.EventHeader{
		EventType: replication.DELETE_ROWS_EVENTv1,
		LogPos:    200,
	}

	rowsEvent := &replication.RowsEvent{
		Table: &replication.TableMapEvent{
			Schema: []byte("testdb"),
			Table:  []byte("users"),
		},
		Rows: [][]interface{}{
			{1, "Alice"},
		},
	}

	err := mc.processRowsEvent(header, rowsEvent)
	if err != nil {
		t.Errorf("processRowsEvent() unexpected error: %v", err)
	}

	select {
	case change := <-changeChan:
		if change.Type != models.Delete {
			t.Errorf("Type = %v, want Delete", change.Type)
		}
		if change.Before["id"] != 1 || change.Before["name"] != "Alice" {
			t.Errorf("Before data mismatch: got %v", change.Before)
		}
		if len(change.After) != 0 {
			t.Error("After should be empty for Delete events")
		}
	default:
		t.Error("expected change event on channel, got none")
	}
}

func TestMySQLConnector_processRowsEvent_TableNotInIncludeList(t *testing.T) {
	cfg := &config.Config{
		Source: config.SourceConfig{
			Type: "mysql",
			MySQL: config.MySQLConfig{
				Host:          "localhost",
				Port:          3306,
				User:          "root",
				Password:      "secret",
				ServerID:      1001,
				Tables:        []string{"testdb.users"},
				ExcludeTables: nil,
			},
		},
	}
	log := newTestLogger(t)
	mc := NewMySQLConnector(cfg, log)
	changeChan := make(chan *models.DataChange, 1)
	mc.handler = &binlogHandler{
		connector:  mc,
		changeChan: changeChan,
	}

	header := &replication.EventHeader{
		EventType: replication.WRITE_ROWS_EVENTv1,
	}
	rowsEvent := &replication.RowsEvent{
		Table: &replication.TableMapEvent{
			Schema: []byte("otherdb"),
			Table:  []byte("other_table"),
		},
		Rows: [][]interface{}{{1}},
	}

	err := mc.processRowsEvent(header, rowsEvent)
	if err != nil {
		t.Errorf("processRowsEvent() should not error for excluded table: %v", err)
	}

	select {
	case <-changeChan:
		t.Error("should not send event for excluded table")
	default:
	}
}

func TestBinlogHandler_OnRotate(t *testing.T) {
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
	mc := NewMySQLConnector(cfg, log)
	handler := &binlogHandler{
		connector: mc,
	}

	err := handler.OnRotate(&replication.RotateEvent{
		NextLogName: []byte("mysql-bin.000002"),
		Position:    4,
	})
	if err != nil {
		t.Errorf("OnRotate() unexpected error: %v", err)
	}
}

func TestBinlogHandler_OnDDL(t *testing.T) {
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
	mc := NewMySQLConnector(cfg, log)

	key := "testdb.users"
	mc.tableCache[key] = &models.TableInfo{
		Database: "testdb",
		Table:    "users",
	}

	handler := &binlogHandler{
		connector: mc,
	}

	t.Run("CREATE TABLE triggers cache invalidation", func(t *testing.T) {
		err := handler.OnDDL(mysql.Position{}, &replication.QueryEvent{
			Query: []byte("CREATE TABLE testdb.orders (id INT)"),
		})
		if err != nil {
			t.Errorf("OnDDL() unexpected error: %v", err)
		}
		if len(mc.tableCache) != 0 {
			t.Error("tableCache should be empty after CREATE TABLE DDL")
		}
	})

	t.Run("non-table DDL does not panic", func(t *testing.T) {
		mc.tableCache[key] = &models.TableInfo{
			Database: "testdb",
			Table:    "users",
		}
		err := handler.OnDDL(mysql.Position{}, &replication.QueryEvent{
			Query: []byte("DROP INDEX idx ON users"),
		})
		if err != nil {
			t.Errorf("OnDDL() unexpected error: %v", err)
		}
		if len(mc.tableCache) == 0 {
			t.Error("tableCache should not be empty for non-table DDL")
		}
	})
}

func TestBinlogHandler_OnGTID(t *testing.T) {
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
	mc := NewMySQLConnector(cfg, log)
	handler := &binlogHandler{
		connector: mc,
	}

	gtidSet, _ := mysql.ParseGTIDSet("mysql", "3E11FA47-61CA-11ED-9C7A-507B9D9E1A2A:1-10")
	err := handler.OnGTID(gtidSet)
	if err != nil {
		t.Errorf("OnGTID() unexpected error: %v", err)
	}
}

func TestBinlogHandler_OnPosSynced(t *testing.T) {
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
	mc := NewMySQLConnector(cfg, log)
	handler := &binlogHandler{
		connector: mc,
	}

	pos := mysql.Position{
		Name: "mysql-bin.000001",
		Pos:  12345,
	}
	err := handler.OnPosSynced(pos, nil, false)
	if err != nil {
		t.Errorf("OnPosSynced() unexpected error: %v", err)
	}

	if mc.position.BinlogFile != "mysql-bin.000001" {
		t.Errorf("BinlogFile = %v, want mysql-bin.000001", mc.position.BinlogFile)
	}
	if mc.position.BinlogPos != 12345 {
		t.Errorf("BinlogPos = %v, want 12345", mc.position.BinlogPos)
	}
}

func TestMySQLConnector_processTableMapEvent_ExcludedTable(t *testing.T) {
	cfg := &config.Config{
		Source: config.SourceConfig{
			Type: "mysql",
			MySQL: config.MySQLConfig{
				Host:          "localhost",
				Port:          3306,
				User:          "root",
				Password:      "secret",
				ServerID:      1001,
				Tables:        nil,
				ExcludeTables: []string{"testdb.logs"},
			},
		},
	}
	log := newTestLogger(t)
	mc := NewMySQLConnector(cfg, log)

	tme := &replication.TableMapEvent{
		Schema: []byte("testdb"),
		Table:  []byte("logs"),
	}

	err := mc.processTableMapEvent(tme)
	if err != nil {
		t.Errorf("processTableMapEvent() should not error for excluded table: %v", err)
	}
}

func TestMySQLConnector_processRowsEvent_UnknownEventType(t *testing.T) {
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
	mc := NewMySQLConnector(cfg, log)

	header := &replication.EventHeader{
		EventType: replication.EventType(255),
	}
	rowsEvent := &replication.RowsEvent{
		Table: &replication.TableMapEvent{
			Schema: []byte("testdb"),
			Table:  []byte("users"),
		},
		Rows: [][]interface{}{{1}},
	}

	err := mc.processRowsEvent(header, rowsEvent)
	if err == nil {
		t.Error("processRowsEvent() should error for unknown event type")
	}
}
