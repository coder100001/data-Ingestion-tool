package models

import (
	"encoding/json"
	"testing"
	"time"
)

func TestNewDataChange(t *testing.T) {
	change := NewDataChange(Insert, "testdb", "users")
	
	if change.Type != Insert {
		t.Errorf("Expected type INSERT, got %s", change.Type)
	}
	
	if change.Database != "testdb" {
		t.Errorf("Expected database 'testdb', got '%s'", change.Database)
	}
	
	if change.Table != "users" {
		t.Errorf("Expected table 'users', got '%s'", change.Table)
	}
	
	if change.ID == "" {
		t.Error("Expected non-empty ID")
	}
	
	if change.Timestamp.IsZero() {
		t.Error("Expected non-zero timestamp")
	}
	
	if change.Before == nil {
		t.Error("Expected non-nil Before map")
	}
	
	if change.After == nil {
		t.Error("Expected non-nil After map")
	}
	
	if change.Schema == nil {
		t.Error("Expected non-nil Schema map")
	}
}

func TestDataChangeToJSON(t *testing.T) {
	change := NewDataChange(Update, "testdb", "orders")
	change.After = map[string]interface{}{
		"id":     1,
		"status": "completed",
		"amount": 99.99,
	}
	change.Before = map[string]interface{}{
		"id":     1,
		"status": "pending",
		"amount": 99.99,
	}
	change.BinlogFile = "mysql-bin.000001"
	change.BinlogPos = 1234
	
	jsonBytes, err := change.ToJSON()
	if err != nil {
		t.Fatalf("Failed to convert to JSON: %v", err)
	}
	
	// Verify it's valid JSON
	var result map[string]interface{}
	if err := json.Unmarshal(jsonBytes, &result); err != nil {
		t.Fatalf("Failed to unmarshal JSON: %v", err)
	}
	
	// Verify fields
	if result["type"] != "UPDATE" {
		t.Errorf("Expected type 'UPDATE', got '%v'", result["type"])
	}
	
	if result["database"] != "testdb" {
		t.Errorf("Expected database 'testdb', got '%v'", result["database"])
	}
	
	if result["table"] != "orders" {
		t.Errorf("Expected table 'orders', got '%v'", result["table"])
	}
}

func TestNewCheckpoint(t *testing.T) {
	cp := NewCheckpoint("mysql")
	
	if cp.SourceType != "mysql" {
		t.Errorf("Expected source type 'mysql', got '%s'", cp.SourceType)
	}
	
	if cp.UpdatedAt.IsZero() {
		t.Error("Expected non-zero UpdatedAt")
	}
}

func TestCheckpointUpdatePosition(t *testing.T) {
	cp := NewCheckpoint("mysql")
	
	oldTime := cp.UpdatedAt
	
	// Wait a bit to ensure time difference
	time.Sleep(10 * time.Millisecond)
	
	newPos := Position{
		BinlogFile: "mysql-bin.000002",
		BinlogPos:  5678,
	}
	
	cp.UpdatePosition(newPos)
	
	if cp.Position.BinlogFile != "mysql-bin.000002" {
		t.Errorf("Expected binlog file 'mysql-bin.000002', got '%s'", cp.Position.BinlogFile)
	}
	
	if cp.Position.BinlogPos != 5678 {
		t.Errorf("Expected binlog pos 5678, got %d", cp.Position.BinlogPos)
	}
	
	if !cp.UpdatedAt.After(oldTime) {
		t.Error("Expected UpdatedAt to be updated")
	}
}

func TestChangeTypeConstants(t *testing.T) {
	if Insert != "INSERT" {
		t.Errorf("Expected Insert to be 'INSERT', got '%s'", Insert)
	}
	
	if Update != "UPDATE" {
		t.Errorf("Expected Update to be 'UPDATE', got '%s'", Update)
	}
	
	if Delete != "DELETE" {
		t.Errorf("Expected Delete to be 'DELETE', got '%s'", Delete)
	}
}

func TestPositionStruct(t *testing.T) {
	// Test MySQL position
	mysqlPos := Position{
		BinlogFile: "mysql-bin.000001",
		BinlogPos:  1234,
	}
	
	if mysqlPos.BinlogFile != "mysql-bin.000001" {
		t.Errorf("Expected binlog file 'mysql-bin.000001', got '%s'", mysqlPos.BinlogFile)
	}
	
	// Test Kafka position
	kafkaPos := Position{
		Topic:     "test-topic",
		Partition: 0,
		Offset:    100,
	}
	
	if kafkaPos.Topic != "test-topic" {
		t.Errorf("Expected topic 'test-topic', got '%s'", kafkaPos.Topic)
	}
	
	if kafkaPos.Offset != 100 {
		t.Errorf("Expected offset 100, got %d", kafkaPos.Offset)
	}
	
	// Test PostgreSQL position
	pgPos := Position{
		LSN: "0/12345678",
	}
	
	if pgPos.LSN != "0/12345678" {
		t.Errorf("Expected LSN '0/12345678', got '%s'", pgPos.LSN)
	}
}

func TestTableInfo(t *testing.T) {
	tableInfo := TableInfo{
		Database: "testdb",
		Table:    "users",
		Columns: []ColumnInfo{
			{Name: "id", Type: "int", Nullable: false},
			{Name: "name", Type: "varchar", Nullable: false},
			{Name: "email", Type: "varchar", Nullable: true},
		},
		PrimaryKey: []string{"id"},
	}
	
	if tableInfo.Database != "testdb" {
		t.Errorf("Expected database 'testdb', got '%s'", tableInfo.Database)
	}
	
	if len(tableInfo.Columns) != 3 {
		t.Errorf("Expected 3 columns, got %d", len(tableInfo.Columns))
	}
	
	if len(tableInfo.PrimaryKey) != 1 || tableInfo.PrimaryKey[0] != "id" {
		t.Errorf("Expected primary key ['id'], got %v", tableInfo.PrimaryKey)
	}
}

func TestColumnInfo(t *testing.T) {
	col := ColumnInfo{
		Name:     "created_at",
		Type:     "timestamp",
		Nullable: false,
		Default:  "CURRENT_TIMESTAMP",
	}
	
	if col.Name != "created_at" {
		t.Errorf("Expected column name 'created_at', got '%s'", col.Name)
	}
	
	if col.Type != "timestamp" {
		t.Errorf("Expected type 'timestamp', got '%s'", col.Type)
	}
	
	if col.Nullable {
		t.Error("Expected column to be non-nullable")
	}
	
	if col.Default != "CURRENT_TIMESTAMP" {
		t.Errorf("Expected default 'CURRENT_TIMESTAMP', got '%s'", col.Default)
	}
}

func TestStorageFile(t *testing.T) {
	now := time.Now()
	storageFile := StorageFile{
		Path:        "data-lake/2024-01-15/data_120000.json",
		Size:        1024,
		RecordCount: 100,
		CreatedAt:   now,
		Partition:   "2024-01-15",
		Format:      "json",
	}
	
	if storageFile.Path != "data-lake/2024-01-15/data_120000.json" {
		t.Errorf("Expected path 'data-lake/2024-01-15/data_120000.json', got '%s'", storageFile.Path)
	}
	
	if storageFile.Size != 1024 {
		t.Errorf("Expected size 1024, got %d", storageFile.Size)
	}
	
	if storageFile.RecordCount != 100 {
		t.Errorf("Expected record count 100, got %d", storageFile.RecordCount)
	}
	
	if storageFile.Partition != "2024-01-15" {
		t.Errorf("Expected partition '2024-01-15', got '%s'", storageFile.Partition)
	}
}

func TestFilterRule(t *testing.T) {
	rule := FilterRule{
		Column:   "age",
		Operator: ">",
		Value:    18,
	}
	
	if rule.Column != "age" {
		t.Errorf("Expected column 'age', got '%s'", rule.Column)
	}
	
	if rule.Operator != ">" {
		t.Errorf("Expected operator '>', got '%s'", rule.Operator)
	}
	
	if rule.Value != 18 {
		t.Errorf("Expected value 18, got %v", rule.Value)
	}
}

func TestTransformRule(t *testing.T) {
	rule := TransformRule{
		Type:       "mask",
		Column:     "email",
		Expression: "***",
		Config: map[string]interface{}{
			"length": 3,
		},
	}
	
	if rule.Type != "mask" {
		t.Errorf("Expected type 'mask', got '%s'", rule.Type)
	}
	
	if rule.Column != "email" {
		t.Errorf("Expected column 'email', got '%s'", rule.Column)
	}
	
	if rule.Config["length"] != 3 {
		t.Errorf("Expected config length 3, got %v", rule.Config["length"])
	}
}
