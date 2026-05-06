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

func TestDataChangeToJSONString(t *testing.T) {
	change := NewDataChange(Insert, "testdb", "users")
	change.After["id"] = 1
	change.After["name"] = "test"

	jsonStr, err := change.ToJSONString()
	if err != nil {
		t.Fatalf("Failed to convert to JSON string: %v", err)
	}

	if jsonStr == "" {
		t.Error("Expected non-empty JSON string")
	}

	var result map[string]interface{}
	if err := json.Unmarshal([]byte(jsonStr), &result); err != nil {
		t.Fatalf("Failed to unmarshal JSON string: %v", err)
	}

	if result["type"] != "INSERT" {
		t.Errorf("Expected type 'INSERT', got '%v'", result["type"])
	}
}

func TestDataChangeJSONRoundTrip(t *testing.T) {
	change := NewDataChange(Update, "testdb", "products")
	change.Before = map[string]interface{}{
		"id":    1,
		"price": 10.0,
	}
	change.After = map[string]interface{}{
		"id":    1,
		"price": 15.0,
	}
	change.BinlogFile = "mysql-bin.000001"
	change.BinlogPos = 1234

	jsonBytes, err := change.ToJSON()
	if err != nil {
		t.Fatalf("Failed to marshal: %v", err)
	}

	var unmarshaled DataChange
	if err := json.Unmarshal(jsonBytes, &unmarshaled); err != nil {
		t.Fatalf("Failed to unmarshal: %v", err)
	}

	if unmarshaled.Type != Update {
		t.Errorf("Expected type UPDATE, got %s", unmarshaled.Type)
	}

	if unmarshaled.Database != "testdb" {
		t.Errorf("Expected database 'testdb', got '%s'", unmarshaled.Database)
	}

	if unmarshaled.BinlogFile != "mysql-bin.000001" {
		t.Errorf("Expected binlog file 'mysql-bin.000001', got '%s'", unmarshaled.BinlogFile)
	}

	if unmarshaled.BinlogPos != 1234 {
		t.Errorf("Expected binlog pos 1234, got %d", unmarshaled.BinlogPos)
	}
}

func TestDataChangeEmptyMaps(t *testing.T) {
	change := &DataChange{
		ID:        "test-id",
		Timestamp: time.Now(),
		Source:    "test",
		Type:      Insert,
		Database:  "testdb",
		Table:     "testtable",
	}

	jsonBytes, err := change.ToJSON()
	if err != nil {
		t.Fatalf("Failed to marshal with empty maps: %v", err)
	}

	var result map[string]interface{}
	if err := json.Unmarshal(jsonBytes, &result); err != nil {
		t.Fatalf("Failed to unmarshal: %v", err)
	}

	if result["type"] != "INSERT" {
		t.Errorf("Expected type 'INSERT', got '%v'", result["type"])
	}
}

func TestCheckpointJSONRoundTrip(t *testing.T) {
	cp := NewCheckpoint("mysql")
	pos := Position{
		BinlogFile: "mysql-bin.000003",
		BinlogPos:  9999,
	}
	cp.UpdatePosition(pos)

	jsonBytes, err := json.Marshal(cp)
	if err != nil {
		t.Fatalf("Failed to marshal checkpoint: %v", err)
	}

	var unmarshaled Checkpoint
	if err := json.Unmarshal(jsonBytes, &unmarshaled); err != nil {
		t.Fatalf("Failed to unmarshal checkpoint: %v", err)
	}

	if unmarshaled.SourceType != "mysql" {
		t.Errorf("Expected source type 'mysql', got '%s'", unmarshaled.SourceType)
	}

	if unmarshaled.Position.BinlogFile != "mysql-bin.000003" {
		t.Errorf("Expected binlog file 'mysql-bin.000003', got '%s'", unmarshaled.Position.BinlogFile)
	}
}

func TestPositionTimestamp(t *testing.T) {
	now := time.Now().UTC()
	pos := Position{
		Timestamp: now,
	}

	if !pos.Timestamp.Equal(now) {
		t.Errorf("Expected timestamp %v, got %v", now, pos.Timestamp)
	}
}

func TestTableInfoJSONRoundTrip(t *testing.T) {
	tableInfo := TableInfo{
		Database: "testdb",
		Table:    "users",
		Columns: []ColumnInfo{
			{Name: "id", Type: "int", Nullable: false},
			{Name: "name", Type: "varchar(100)", Nullable: true},
		},
		PrimaryKey: []string{"id"},
	}

	jsonBytes, err := json.Marshal(tableInfo)
	if err != nil {
		t.Fatalf("Failed to marshal table info: %v", err)
	}

	var unmarshaled TableInfo
	if err := json.Unmarshal(jsonBytes, &unmarshaled); err != nil {
		t.Fatalf("Failed to unmarshal table info: %v", err)
	}

	if unmarshaled.Database != "testdb" {
		t.Errorf("Expected database 'testdb', got '%s'", unmarshaled.Database)
	}

	if len(unmarshaled.Columns) != 2 {
		t.Errorf("Expected 2 columns, got %d", len(unmarshaled.Columns))
	}

	if unmarshaled.Columns[0].Name != "id" {
		t.Errorf("Expected first column name 'id', got '%s'", unmarshaled.Columns[0].Name)
	}
}

func TestStorageFileWithClosedAt(t *testing.T) {
	now := time.Now()
	closedAt := now.Add(1 * time.Hour)
	storageFile := StorageFile{
		Path:        "data-lake/2024-01-15/data_120000.parquet",
		Size:        2048,
		RecordCount: 200,
		CreatedAt:   now,
		ClosedAt:    closedAt,
		Partition:   "2024-01-15",
		Format:      "parquet",
	}

	if storageFile.ClosedAt.IsZero() {
		t.Error("Expected non-zero ClosedAt")
	}

	if !storageFile.ClosedAt.After(storageFile.CreatedAt) {
		t.Error("Expected ClosedAt to be after CreatedAt")
	}
}

func TestFilterRuleJSONRoundTrip(t *testing.T) {
	rule := FilterRule{
		Column:   "status",
		Operator: "=",
		Value:    "active",
	}

	jsonBytes, err := json.Marshal(rule)
	if err != nil {
		t.Fatalf("Failed to marshal filter rule: %v", err)
	}

	var unmarshaled FilterRule
	if err := json.Unmarshal(jsonBytes, &unmarshaled); err != nil {
		t.Fatalf("Failed to unmarshal filter rule: %v", err)
	}

	if unmarshaled.Column != "status" {
		t.Errorf("Expected column 'status', got '%s'", unmarshaled.Column)
	}

	if unmarshaled.Operator != "=" {
		t.Errorf("Expected operator '=', got '%s'", unmarshaled.Operator)
	}
}

func TestTransformRuleJSONRoundTrip(t *testing.T) {
	rule := TransformRule{
		Type:       "hash",
		Column:     "password",
		Expression: "sha256",
		Config: map[string]interface{}{
			"salt": "random_salt",
		},
	}

	jsonBytes, err := json.Marshal(rule)
	if err != nil {
		t.Fatalf("Failed to marshal transform rule: %v", err)
	}

	var unmarshaled TransformRule
	if err := json.Unmarshal(jsonBytes, &unmarshaled); err != nil {
		t.Fatalf("Failed to unmarshal transform rule: %v", err)
	}

	if unmarshaled.Type != "hash" {
		t.Errorf("Expected type 'hash', got '%s'", unmarshaled.Type)
	}

	if unmarshaled.Config["salt"] != "random_salt" {
		t.Errorf("Expected salt 'random_salt', got '%v'", unmarshaled.Config["salt"])
	}
}

func TestChangeTypeString(t *testing.T) {
	tests := []struct {
		name     string
		ctype    ChangeType
		expected string
	}{
		{"Insert", Insert, "INSERT"},
		{"Update", Update, "UPDATE"},
		{"Delete", Delete, "DELETE"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if string(tt.ctype) != tt.expected {
				t.Errorf("Expected '%s', got '%s'", tt.expected, string(tt.ctype))
			}
		})
	}
}
