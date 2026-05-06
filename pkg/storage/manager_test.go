package storage

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"data-ingestion-tool/pkg/config"
	"data-ingestion-tool/pkg/logger"
	"data-ingestion-tool/pkg/models"
)

func newTestConfig(t *testing.T, tmpDir, fileFormat string) *config.LocalConfig {
	t.Helper()
	return &config.LocalConfig{
		BasePath:          tmpDir,
		PartitionStrategy: "date",
		FileFormat:        fileFormat,
		Compression:       "none",
		MaxFileSizeMB:     100,
		MaxRecordsPerFile: 10000,
		Parquet: config.ParquetConfig{
			RowGroupSize:     10000,
			PageSize:         8192,
			EnableDictionary: true,
		},
		Schema: config.SchemaConfig{
			RegistryPath:  filepath.Join(tmpDir, "schemas"),
			Compatibility: "backward",
			AutoRegister:  true,
		},
	}
}

func newTestLogger(t *testing.T) *logger.Logger {
	t.Helper()
	log, err := logger.New("debug", "")
	if err != nil {
		t.Fatalf("Failed to create logger: %v", err)
	}
	return log
}

func newTestDataChange(changeType models.ChangeType, database, table string, data map[string]interface{}) *models.DataChange {
	return &models.DataChange{
		ID:        time.Now().UTC().Format("20060102150405.000000000"),
		Timestamp: time.Now().UTC(),
		Source:    "test-source",
		Type:      changeType,
		Database:  database,
		Table:     table,
		After:     data,
	}
}

func TestLocalStorage_Write_JSON(t *testing.T) {
	tmpDir := t.TempDir()
	cfg := newTestConfig(t, tmpDir, "json")
	log := newTestLogger(t)

	storage, err := NewLocalStorage(cfg, log)
	if err != nil {
		t.Fatalf("Failed to create local storage: %v", err)
	}
	defer storage.Close()

	change := newTestDataChange(models.Insert, "testdb", "testtable", map[string]interface{}{
		"id":    1,
		"name":  "test",
		"email": "test@example.com",
	})

	if err := storage.Write(change); err != nil {
		t.Errorf("Failed to write data: %v", err)
	}

	if err := storage.Flush(); err != nil {
		t.Errorf("Failed to flush: %v", err)
	}

	files, err := os.ReadDir(tmpDir)
	if err != nil {
		t.Fatalf("Failed to read directory: %v", err)
	}

	found := false
	for _, f := range files {
		if f.IsDir() {
			subFiles, _ := os.ReadDir(filepath.Join(tmpDir, f.Name()))
			for _, sf := range subFiles {
				if filepath.Ext(sf.Name()) == ".json" {
					found = true
					break
				}
			}
		}
	}

	if !found {
		t.Error("Expected JSON file to be created")
	}
}

func TestLocalStorage_Write_CSV(t *testing.T) {
	tmpDir := t.TempDir()
	cfg := newTestConfig(t, tmpDir, "csv")
	log := newTestLogger(t)

	storage, err := NewLocalStorage(cfg, log)
	if err != nil {
		t.Fatalf("Failed to create local storage: %v", err)
	}
	defer storage.Close()

	change := newTestDataChange(models.Insert, "testdb", "testtable", map[string]interface{}{
		"id":    1,
		"name":  "test",
		"email": "test@example.com",
	})

	if err := storage.Write(change); err != nil {
		t.Errorf("Failed to write data: %v", err)
	}

	if err := storage.Flush(); err != nil {
		t.Errorf("Failed to flush: %v", err)
	}
}

func TestLocalStorage_Close(t *testing.T) {
	tmpDir := t.TempDir()
	cfg := newTestConfig(t, tmpDir, "json")
	log := newTestLogger(t)

	storage, err := NewLocalStorage(cfg, log)
	if err != nil {
		t.Fatalf("Failed to create local storage: %v", err)
	}

	change := newTestDataChange(models.Insert, "testdb", "testtable", map[string]interface{}{
		"id": 1,
	})

	if err := storage.Write(change); err != nil {
		t.Errorf("Failed to write data: %v", err)
	}

	if err := storage.Close(); err != nil {
		t.Errorf("Failed to close storage: %v", err)
	}

	if storage.currentFile != nil {
		t.Error("Expected current file to be nil after close")
	}
}

func TestLocalStorage_FileRotation(t *testing.T) {
	tmpDir := t.TempDir()
	cfg := &config.LocalConfig{
		BasePath:          tmpDir,
		PartitionStrategy: "none",
		FileFormat:        "json",
		Compression:       "none",
		MaxFileSizeMB:     100,
		MaxRecordsPerFile: 2,
	}
	log := newTestLogger(t)

	storage, err := NewLocalStorage(cfg, log)
	if err != nil {
		t.Fatalf("Failed to create local storage: %v", err)
	}
	defer storage.Close()

	for i := 0; i < 5; i++ {
		change := newTestDataChange(models.Insert, "testdb", "testtable", map[string]interface{}{
			"id":    i,
			"value": "test",
		})
		if err := storage.Write(change); err != nil {
			t.Errorf("Failed to write data %d: %v", i, err)
		}
	}

	if storage.recordCount > 2 {
		t.Errorf("Expected record count <= 2 after rotation, got %d", storage.recordCount)
	}
}

func TestLocalStorage_PartitionStrategy(t *testing.T) {
	tests := []struct {
		name     string
		strategy string
		wantDir  bool
	}{
		{
			name:     "date partition",
			strategy: "date",
			wantDir:  true,
		},
		{
			name:     "hour partition",
			strategy: "hour",
			wantDir:  true,
		},
		{
			name:     "no partition",
			strategy: "none",
			wantDir:  false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tmpDir := t.TempDir()
			cfg := &config.LocalConfig{
				BasePath:          tmpDir,
				PartitionStrategy: tt.strategy,
				FileFormat:        "json",
				Compression:       "none",
				MaxFileSizeMB:     100,
				MaxRecordsPerFile: 10000,
			}
			log := newTestLogger(t)

			storage, err := NewLocalStorage(cfg, log)
			if err != nil {
				t.Fatalf("Failed to create local storage: %v", err)
			}
			defer storage.Close()

			change := newTestDataChange(models.Insert, "testdb", "testtable", map[string]interface{}{
				"id": 1,
			})

			if err := storage.Write(change); err != nil {
				t.Errorf("Failed to write data: %v", err)
			}

			if tt.wantDir {
				expectedPrefix := time.Now().UTC().Format("2006-01-02")
				if tt.strategy == "hour" {
					expectedPrefix = time.Now().UTC().Format("2006-01-02")
				}
				found := false
				files, _ := os.ReadDir(tmpDir)
				for _, f := range files {
					if f.IsDir() && len(f.Name()) >= 10 && f.Name()[:10] == expectedPrefix[:10] {
						found = true
						break
					}
				}
				if !found {
					t.Errorf("Expected partition directory for strategy %s", tt.strategy)
				}
			}
		})
	}
}

func TestLocalStorage_ConcurrentWrite(t *testing.T) {
	tmpDir := t.TempDir()
	cfg := newTestConfig(t, tmpDir, "json")
	log := newTestLogger(t)

	storage, err := NewLocalStorage(cfg, log)
	if err != nil {
		t.Fatalf("Failed to create local storage: %v", err)
	}
	defer storage.Close()

	var wg sync.WaitGroup
	numGoroutines := 10
	numWrites := 10
	errChan := make(chan error, numGoroutines*numWrites)

	for g := 0; g < numGoroutines; g++ {
		wg.Add(1)
		go func(goroutineID int) {
			defer wg.Done()
			for i := 0; i < numWrites; i++ {
				change := newTestDataChange(models.Insert, "testdb", "testtable", map[string]interface{}{
					"id":        goroutineID*numWrites + i,
					"goroutine": goroutineID,
					"iteration": i,
				})
				if err := storage.Write(change); err != nil {
					errChan <- err
				}
			}
		}(g)
	}

	wg.Wait()
	close(errChan)

	for err := range errChan {
		t.Errorf("Concurrent write error: %v", err)
	}
}

func TestManager_NewManager(t *testing.T) {
	tmpDir := t.TempDir()
	cfg := newTestConfig(t, tmpDir, "json")
	log := newTestLogger(t)

	manager, err := NewManager(cfg, log)
	if err != nil {
		t.Fatalf("Failed to create manager: %v", err)
	}

	if manager == nil {
		t.Fatal("Expected manager to be non-nil")
	}
	defer manager.Close()

	if manager.catalog == nil {
		t.Error("Expected catalog to be initialized")
	}

	if manager.registry == nil {
		t.Error("Expected registry to be initialized")
	}
}

func TestManager_Write_JSON(t *testing.T) {
	tmpDir := t.TempDir()
	cfg := newTestConfig(t, tmpDir, "json")
	log := newTestLogger(t)

	manager, err := NewManager(cfg, log)
	if err != nil {
		t.Fatalf("Failed to create manager: %v", err)
	}
	defer manager.Close()

	change := newTestDataChange(models.Insert, "testdb", "users", map[string]interface{}{
		"id":    1,
		"name":  "John Doe",
		"email": "john@example.com",
	})

	if err := manager.Write(change); err != nil {
		t.Errorf("Failed to write data: %v", err)
	}
}

// --- QualityChecker Tests ---

func TestQualityChecker_NewChecker(t *testing.T) {
	qc := NewQualityChecker()
	if qc == nil {
		t.Fatal("NewQualityChecker() returned nil")
	}
	if len(qc.checks) != 3 {
		t.Errorf("expected 3 default checks, got %d", len(qc.checks))
	}
}

func TestQualityChecker_Check_Insert(t *testing.T) {
	qc := NewQualityChecker()
	change := &models.DataChange{
		Type:  models.Insert,
		After: map[string]interface{}{"id": 1, "name": "test", "created_at": time.Now().UTC()},
	}

	dq := qc.Check(change)
	if dq == nil {
		t.Fatal("Check() returned nil")
	}
	if dq.Completeness <= 0 {
		t.Errorf("Completeness should be > 0 for non-empty data, got %f", dq.Completeness)
	}
	if dq.Validity <= 0 {
		t.Errorf("Validity should be > 0, got %f", dq.Validity)
	}
	if len(dq.Checks) != 3 {
		t.Errorf("expected 3 checks, got %d", len(dq.Checks))
	}
}

func TestQualityChecker_Check_Delete(t *testing.T) {
	qc := NewQualityChecker()
	change := &models.DataChange{
		Type:   models.Delete,
		Before: map[string]interface{}{"id": 1, "name": "deleted"},
	}

	dq := qc.Check(change)
	if dq == nil {
		t.Fatal("Check() returned nil")
	}
	if dq.Completeness <= 0 {
		t.Errorf("Completeness should be > 0 for non-empty Before data, got %f", dq.Completeness)
	}
}

func TestQualityChecker_Check_NilData(t *testing.T) {
	qc := NewQualityChecker()

	t.Run("nil After on Insert", func(t *testing.T) {
		change := &models.DataChange{Type: models.Insert, After: nil}
		dq := qc.Check(change)
		if dq == nil {
			t.Fatal("Check() returned nil")
		}
		if dq.Completeness != 0 || dq.Validity != 0 || dq.Timeliness != 0 {
			t.Error("all scores should be 0 for nil data")
		}
	})

	t.Run("nil Before on Delete", func(t *testing.T) {
		change := &models.DataChange{Type: models.Delete, Before: nil}
		dq := qc.Check(change)
		if dq.Completeness != 0 {
			t.Errorf("Completeness should be 0 for nil Before data, got %f", dq.Completeness)
		}
	})
}

func TestQualityChecker_AddCustomCheck(t *testing.T) {
	qc := NewQualityChecker()
	qc.AddCustomCheck(QualityCheckRule{
		Name:        "custom_check",
		Description: "Custom check for testing",
		CheckFunc: func(data map[string]interface{}) (bool, float64, string) {
			return true, 1.0, "custom check passed"
		},
	})

	if len(qc.checks) != 4 {
		t.Errorf("expected 4 checks after adding custom check, got %d", len(qc.checks))
	}

	change := &models.DataChange{
		Type:  models.Insert,
		After: map[string]interface{}{"id": 1},
	}

	dq := qc.Check(change)
	if len(dq.Checks) != 4 {
		t.Errorf("expected 4 check results, got %d", len(dq.Checks))
	}
	if dq.Checks[3].Name != "custom_check" {
		t.Errorf("last check should be custom_check, got %s", dq.Checks[3].Name)
	}
}

func TestCheckCompleteness(t *testing.T) {
	t.Run("empty data", func(t *testing.T) {
		passed, score, _ := checkCompleteness(map[string]interface{}{})
		if passed {
			t.Error("should not pass for empty data")
		}
		if score != 0 {
			t.Errorf("score should be 0 for empty data, got %f", score)
		}
	})

	t.Run("full data", func(t *testing.T) {
		passed, score, _ := checkCompleteness(map[string]interface{}{
			"id":   1,
			"name": "test",
		})
		if !passed {
			t.Error("should pass for complete data")
		}
		if score != 1.0 {
			t.Errorf("score should be 1.0, got %f", score)
		}
	})

	t.Run("partial data with nil fields", func(t *testing.T) {
		passed, score, _ := checkCompleteness(map[string]interface{}{
			"id":   1,
			"name": nil,
		})
		if score != 0.5 {
			t.Errorf("score should be 0.5 for 1/2 non-nil, got %f", score)
		}
		if passed {
			t.Error("should not pass for 50% completeness")
		}
	})

	t.Run("80% completeness passes threshold", func(t *testing.T) {
		passed, score, _ := checkCompleteness(map[string]interface{}{
			"a": 1, "b": 2, "c": 3, "d": 4, "e": nil,
		})
		if !passed {
			t.Error("should pass for 80% completeness (4/5)")
		}
		if score != 0.8 {
			t.Errorf("score should be 0.8, got %f", score)
		}
	})
}

func TestCheckValidity(t *testing.T) {
	t.Run("empty data", func(t *testing.T) {
		passed, score, _ := checkValidity(map[string]interface{}{})
		if passed {
			t.Error("should not pass for empty data")
		}
		if score != 0 {
			t.Errorf("score should be 0, got %f", score)
		}
	})

	t.Run("valid types", func(t *testing.T) {
		passed, score, _ := checkValidity(map[string]interface{}{
			"string": "hello",
			"int":    42,
			"float":  3.14,
			"bool":   true,
			"time":   time.Now(),
		})
		if !passed {
			t.Error("should pass for all valid types")
		}
		if score != 1.0 {
			t.Errorf("score should be 1.0, got %f", score)
		}
	})

	t.Run("nil value is valid", func(t *testing.T) {
		passed, score, _ := checkValidity(map[string]interface{}{
			"nullable": nil,
		})
		if !passed {
			t.Error("nil should be valid")
		}
		if score != 1.0 {
			t.Errorf("score should be 1.0, got %f", score)
		}
	})

	t.Run("very long string fails", func(t *testing.T) {
		longString := string(make([]byte, 10001))
		passed, score, _ := checkValidity(map[string]interface{}{
			"long": longString,
		})
		if passed {
			t.Error("should not pass for very long string")
		}
		if score != 0 {
			t.Errorf("score should be 0, got %f", score)
		}
	})
}

func TestCheckTimeliness(t *testing.T) {
	t.Run("no time fields", func(t *testing.T) {
		passed, score, _ := checkTimeliness(map[string]interface{}{
			"id":   1,
			"name": "test",
		})
		if !passed {
			t.Error("should pass when no time fields exist")
		}
		if score != 1.0 {
			t.Errorf("score should be 1.0, got %f", score)
		}
	})

	t.Run("valid time.Time field", func(t *testing.T) {
		passed, score, _ := checkTimeliness(map[string]interface{}{
			"created_at": time.Now(),
		})
		if !passed {
			t.Error("should pass for valid time")
		}
		if score != 1.0 {
			t.Errorf("score should be 1.0, got %f", score)
		}
	})

	t.Run("valid string timestamp", func(t *testing.T) {
		passed, _, _ := checkTimeliness(map[string]interface{}{
			"timestamp": time.Now().UTC().Format(time.RFC3339),
		})
		if !passed {
			t.Error("should pass for valid RFC3339 string")
		}
	})

	t.Run("invalid string timestamp", func(t *testing.T) {
		_, score, _ := checkTimeliness(map[string]interface{}{
			"timestamp": "not-a-timestamp",
		})
		if score != 1.0 && score != 0 {
			t.Logf("timeliness score for invalid string: %f", score)
		}
	})

	t.Run("old timestamp fails", func(t *testing.T) {
		passed, score, _ := checkTimeliness(map[string]interface{}{
			"created_at": time.Now().AddDate(-2, 0, 0),
		})
		if passed {
			t.Error("should not pass for very old timestamp")
		}
		if score != 0 {
			t.Errorf("score should be 0, got %f", score)
		}
	})
}

// --- SchemaValidator Tests ---

func TestSchemaValidator_Validate_NilSchema(t *testing.T) {
	sv := NewSchemaValidator(nil)
	result := sv.Validate(map[string]interface{}{"id": 1})
	if !result.Valid {
		t.Error("should be valid when no schema defined")
	}
	if len(result.Warnings) == 0 {
		t.Error("should have warning about no schema")
	}
}

func TestSchemaValidator_Validate_RequiredFields(t *testing.T) {
	schema := &SchemaVersion{
		Columns: []ColumnInfo{
			{Name: "id", Type: "int", Nullable: false},
			{Name: "name", Type: "string", Nullable: false},
			{Name: "email", Type: "string", Nullable: true},
		},
	}
	sv := NewSchemaValidator(schema)

	t.Run("all required fields present", func(t *testing.T) {
		result := sv.Validate(map[string]interface{}{
			"id":    1,
			"name":  "test",
			"email": "test@example.com",
		})
		if !result.Valid {
			t.Errorf("should be valid, got errors: %v", result.Errors)
		}
	})

	t.Run("missing required field", func(t *testing.T) {
		result := sv.Validate(map[string]interface{}{
			"id": 1,
		})
		if result.Valid {
			t.Error("should be invalid when required field missing")
		}
		if len(result.Errors) == 0 {
			t.Error("should have error about missing required field")
		}
	})
}

func TestSchemaValidator_Validate_UnknownFields(t *testing.T) {
	schema := &SchemaVersion{
		Columns: []ColumnInfo{
			{Name: "id", Type: "int"},
		},
	}
	sv := NewSchemaValidator(schema)

	result := sv.Validate(map[string]interface{}{
		"id":       1,
		"unknown":  "value",
		"also_new": 42,
	})
	if !result.Valid {
		t.Error("unknown fields should not make validation fail")
	}
	if len(result.Warnings) != 2 {
		t.Errorf("expected 2 warnings for unknown fields, got %d: %v", len(result.Warnings), result.Warnings)
	}
}

func TestIsValidType(t *testing.T) {
	t.Run("string types", func(t *testing.T) {
		if !isValidType("hello", "string") {
			t.Error("string should be valid for string type")
		}
		if !isValidType("hello", "varchar") {
			t.Error("string should be valid for varchar type")
		}
		if isValidType(42, "string") {
			t.Error("int should not be valid for string type")
		}
	})

	t.Run("int types", func(t *testing.T) {
		if !isValidType(42, "int") {
			t.Error("int should be valid for int type")
		}
		if !isValidType(int32(42), "integer") {
			t.Error("int32 should be valid for integer type")
		}
		if !isValidType(int64(42), "bigint") {
			t.Error("int64 should be valid for bigint type")
		}
		if isValidType("42", "int") {
			t.Error("string should not be valid for int type")
		}
	})

	t.Run("float types", func(t *testing.T) {
		if !isValidType(3.14, "float") {
			t.Error("float64 should be valid for float type")
		}
		if !isValidType(float32(3.14), "double") {
			t.Error("float32 should be valid for double type")
		}
		if isValidType("3.14", "decimal") {
			t.Error("string should not be valid for decimal type")
		}
	})

	t.Run("bool types", func(t *testing.T) {
		if !isValidType(true, "bool") {
			t.Error("bool should be valid for bool type")
		}
		if !isValidType(false, "boolean") {
			t.Error("bool should be valid for boolean type")
		}
		if isValidType(1, "bool") {
			t.Error("int should not be valid for bool type")
		}
	})

	t.Run("timestamp types", func(t *testing.T) {
		if !isValidType(time.Now(), "timestamp") {
			t.Error("time.Time should be valid for timestamp type")
		}
		if !isValidType(time.Now().Format(time.RFC3339), "datetime") {
			t.Error("RFC3339 string should be valid for datetime type")
		}
		if isValidType("invalid-date", "date") {
			t.Error("invalid date string should not be valid for date type")
		}
	})

	t.Run("unknown type passes", func(t *testing.T) {
		if !isValidType("anything", "unknown_type") {
			t.Error("any value should pass for unknown types")
		}
	})
}

func TestLocalStorage_WriteAfterClose(t *testing.T) {
	tmpDir := t.TempDir()
	cfg := newTestConfig(t, tmpDir, "json")
	log := newTestLogger(t)

	storage, err := NewLocalStorage(cfg, log)
	if err != nil {
		t.Fatalf("Failed to create storage: %v", err)
	}

	if err := storage.Close(); err != nil {
		t.Fatalf("Failed to close storage: %v", err)
	}

	change := newTestDataChange(models.Insert, "testdb", "testtable", map[string]interface{}{
		"id": 1,
	})
	err = storage.Write(change)
	if err != nil {
		t.Errorf("Write() after close should reopen file, got error: %v", err)
	}
}

func TestLocalStorage_FullRotationCycle(t *testing.T) {
	tmpDir := t.TempDir()
	cfg := &config.LocalConfig{
		BasePath:          tmpDir,
		PartitionStrategy: "none",
		FileFormat:        "json",
		Compression:       "none",
		MaxFileSizeMB:     100,
		MaxRecordsPerFile: 3,
	}
	log := newTestLogger(t)

	storage, err := NewLocalStorage(cfg, log)
	if err != nil {
		t.Fatalf("Failed to create storage: %v", err)
	}
	defer storage.Close()

	for i := 0; i < 9; i++ {
		change := newTestDataChange(models.Insert, "testdb", "testtable", map[string]interface{}{
			"id":    i,
			"value": "data",
		})
		if err := storage.Write(change); err != nil {
			t.Errorf("Failed to write data %d: %v", i, err)
		}
	}

	storage.mu.Lock()
	recordCount := storage.recordCount
	currentPath := storage.currentPath
	storage.mu.Unlock()

	if recordCount != 3 {
		t.Errorf("After 9 writes with max 3 per file, expected recordCount=3, got %d", recordCount)
	}

	if currentPath == "" {
		t.Error("currentPath should not be empty after writes")
	}
}

func TestManager_Write_MultipleRecords(t *testing.T) {
	tmpDir := t.TempDir()
	cfg := newTestConfig(t, tmpDir, "json")
	log := newTestLogger(t)

	manager, err := NewManager(cfg, log)
	if err != nil {
		t.Fatalf("Failed to create manager: %v", err)
	}
	defer manager.Close()

	for i := 0; i < 10; i++ {
		change := newTestDataChange(models.Insert, "testdb", "users", map[string]interface{}{
			"id":    i,
			"name":  "User",
			"email": "user@example.com",
		})
		if err := manager.Write(change); err != nil {
			t.Errorf("Failed to write data %d: %v", i, err)
		}
	}
}

func TestManager_Flush(t *testing.T) {
	tmpDir := t.TempDir()
	cfg := newTestConfig(t, tmpDir, "json")
	log := newTestLogger(t)

	manager, err := NewManager(cfg, log)
	if err != nil {
		t.Fatalf("Failed to create manager: %v", err)
	}
	defer manager.Close()

	change := newTestDataChange(models.Insert, "testdb", "users", map[string]interface{}{
		"id": 1,
	})

	if err := manager.Write(change); err != nil {
		t.Fatalf("Failed to write data: %v", err)
	}

	if err := manager.Flush(); err != nil {
		t.Errorf("Failed to flush: %v", err)
	}
}

func TestManager_Close(t *testing.T) {
	tmpDir := t.TempDir()
	cfg := newTestConfig(t, tmpDir, "json")
	log := newTestLogger(t)

	manager, err := NewManager(cfg, log)
	if err != nil {
		t.Fatalf("Failed to create manager: %v", err)
	}

	change := newTestDataChange(models.Insert, "testdb", "users", map[string]interface{}{
		"id": 1,
	})

	if err := manager.Write(change); err != nil {
		t.Fatalf("Failed to write data: %v", err)
	}

	if err := manager.Close(); err != nil {
		t.Errorf("Failed to close manager: %v", err)
	}

	if len(manager.writers) != 0 {
		t.Error("Expected writers to be cleared after close")
	}
}

func TestManager_RotateWriters(t *testing.T) {
	tmpDir := t.TempDir()
	cfg := newTestConfig(t, tmpDir, "json")
	log := newTestLogger(t)

	manager, err := NewManager(cfg, log)
	if err != nil {
		t.Fatalf("Failed to create manager: %v", err)
	}
	defer manager.Close()

	change := newTestDataChange(models.Insert, "testdb", "users", map[string]interface{}{
		"id": 1,
	})

	if err := manager.Write(change); err != nil {
		t.Fatalf("Failed to write data: %v", err)
	}

	if err := manager.RotateWriters(); err != nil {
		t.Errorf("Failed to rotate writers: %v", err)
	}

	if len(manager.writers) != 0 {
		t.Error("Expected writers to be cleared after rotation")
	}
}

func TestManager_GetStats(t *testing.T) {
	tmpDir := t.TempDir()
	cfg := newTestConfig(t, tmpDir, "json")
	log := newTestLogger(t)

	manager, err := NewManager(cfg, log)
	if err != nil {
		t.Fatalf("Failed to create manager: %v", err)
	}
	defer manager.Close()

	stats := manager.GetStats()
	if stats == nil {
		t.Fatal("Expected stats to be non-nil")
	}

	if stats.Tables == nil {
		t.Error("Expected tables map to be initialized")
	}
}

func TestManager_GetCatalog(t *testing.T) {
	tmpDir := t.TempDir()
	cfg := newTestConfig(t, tmpDir, "json")
	log := newTestLogger(t)

	manager, err := NewManager(cfg, log)
	if err != nil {
		t.Fatalf("Failed to create manager: %v", err)
	}
	defer manager.Close()

	catalog := manager.GetCatalog()
	if catalog == nil {
		t.Error("Expected catalog to be non-nil")
	}
}

func TestManager_GetRegistry(t *testing.T) {
	tmpDir := t.TempDir()
	cfg := newTestConfig(t, tmpDir, "json")
	log := newTestLogger(t)

	manager, err := NewManager(cfg, log)
	if err != nil {
		t.Fatalf("Failed to create manager: %v", err)
	}
	defer manager.Close()

	registry := manager.GetRegistry()
	if registry == nil {
		t.Error("Expected registry to be non-nil")
	}
}

func TestManager_ConcurrentWrite(t *testing.T) {
	tmpDir := t.TempDir()
	cfg := newTestConfig(t, tmpDir, "json")
	log := newTestLogger(t)

	manager, err := NewManager(cfg, log)
	if err != nil {
		t.Fatalf("Failed to create manager: %v", err)
	}
	defer manager.Close()

	var wg sync.WaitGroup
	numGoroutines := 10
	numWrites := 10
	errChan := make(chan error, numGoroutines*numWrites)

	for g := 0; g < numGoroutines; g++ {
		wg.Add(1)
		go func(goroutineID int) {
			defer wg.Done()
			for i := 0; i < numWrites; i++ {
				change := newTestDataChange(models.Insert, "testdb", "users", map[string]interface{}{
					"id":        goroutineID*numWrites + i,
					"goroutine": goroutineID,
				})
				if err := manager.Write(change); err != nil {
					errChan <- err
				}
			}
		}(g)
	}

	wg.Wait()
	close(errChan)

	for err := range errChan {
		t.Errorf("Concurrent write error: %v", err)
	}
}

func TestManager_Write_DifferentTables(t *testing.T) {
	tmpDir := t.TempDir()
	cfg := newTestConfig(t, tmpDir, "json")
	log := newTestLogger(t)

	manager, err := NewManager(cfg, log)
	if err != nil {
		t.Fatalf("Failed to create manager: %v", err)
	}
	defer manager.Close()

	tables := []struct {
		database string
		table    string
	}{
		{"db1", "table1"},
		{"db1", "table2"},
		{"db2", "table1"},
	}

	for _, tt := range tables {
		change := newTestDataChange(models.Insert, tt.database, tt.table, map[string]interface{}{
			"id": 1,
		})
		if err := manager.Write(change); err != nil {
			t.Errorf("Failed to write to %s.%s: %v", tt.database, tt.table, err)
		}
	}
}

func TestManager_Write_Delete(t *testing.T) {
	tmpDir := t.TempDir()
	cfg := newTestConfig(t, tmpDir, "json")
	log := newTestLogger(t)

	manager, err := NewManager(cfg, log)
	if err != nil {
		t.Fatalf("Failed to create manager: %v", err)
	}
	defer manager.Close()

	change := &models.DataChange{
		ID:        time.Now().UTC().Format("20060102150405.000000000"),
		Timestamp: time.Now().UTC(),
		Source:    "test-source",
		Type:      models.Delete,
		Database:  "testdb",
		Table:     "users",
		Before: map[string]interface{}{
			"id":   1,
			"name": "deleted",
		},
	}

	if err := manager.Write(change); err != nil {
		t.Errorf("Failed to write delete: %v", err)
	}
}

func TestManager_Write_Update(t *testing.T) {
	tmpDir := t.TempDir()
	cfg := newTestConfig(t, tmpDir, "json")
	log := newTestLogger(t)

	manager, err := NewManager(cfg, log)
	if err != nil {
		t.Fatalf("Failed to create manager: %v", err)
	}
	defer manager.Close()

	change := &models.DataChange{
		ID:        time.Now().UTC().Format("20060102150405.000000000"),
		Timestamp: time.Now().UTC(),
		Source:    "test-source",
		Type:      models.Update,
		Database:  "testdb",
		Table:     "users",
		Before: map[string]interface{}{
			"id":   1,
			"name": "old",
		},
		After: map[string]interface{}{
			"id":   1,
			"name": "new",
		},
	}

	if err := manager.Write(change); err != nil {
		t.Errorf("Failed to write update: %v", err)
	}
}

func TestManager_UnsupportedFormat(t *testing.T) {
	tmpDir := t.TempDir()
	cfg := newTestConfig(t, tmpDir, "xml")
	log := newTestLogger(t)

	storage, err := NewLocalStorage(cfg, log)
	if err != nil {
		t.Fatalf("Failed to create local storage: %v", err)
	}
	defer storage.Close()

	change := newTestDataChange(models.Insert, "testdb", "users", map[string]interface{}{
		"id": 1,
	})

	err = storage.Write(change)
	if err == nil {
		t.Error("Expected error for unsupported format")
	}
}

// --- Layered Common Helper Tests ---

func TestTrimString(t *testing.T) {
	tests := []struct {
		input string
		want  string
	}{
		{"  hello  ", "hello"},
		{"\t\nhello\n\t", "hello"},
		{"hello", "hello"},
		{"   ", ""},
		{"", ""},
		{"  hello world  ", "hello world"},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			if got := trimString(tt.input); got != tt.want {
				t.Errorf("trimString(%q) = %q, want %q", tt.input, got, tt.want)
			}
		})
	}
}

func TestToLowerCase(t *testing.T) {
	tests := []struct {
		input string
		want  string
	}{
		{"HELLO", "hello"},
		{"Hello World", "hello world"},
		{"hello", "hello"},
		{"", ""},
		{"ABC123", "abc123"},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			if got := toLowerCase(tt.input); got != tt.want {
				t.Errorf("toLowerCase(%q) = %q, want %q", tt.input, got, tt.want)
			}
		})
	}
}

func TestToUpperCase(t *testing.T) {
	tests := []struct {
		input string
		want  string
	}{
		{"hello", "HELLO"},
		{"Hello World", "HELLO WORLD"},
		{"HELLO", "HELLO"},
		{"", ""},
		{"abc123", "ABC123"},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			if got := toUpperCase(tt.input); got != tt.want {
				t.Errorf("toUpperCase(%q) = %q, want %q", tt.input, got, tt.want)
			}
		})
	}
}

func TestReplaceString(t *testing.T) {
	tests := []struct {
		input string
		old   string
		new   string
		want  string
	}{
		{"hello world", "world", "there", "hello there"},
		{"aaa", "a", "b", "bbb"},
		{"hello", "", "x", "hello"},
		{"", "a", "b", ""},
		{"hello", "x", "y", "hello"},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			if got := replaceString(tt.input, tt.old, tt.new); got != tt.want {
				t.Errorf("replaceString(%q, %q, %q) = %q, want %q",
					tt.input, tt.old, tt.new, got, tt.want)
			}
		})
	}
}

func TestCheckType(t *testing.T) {
	tests := []struct {
		value    interface{}
		typeName string
		want     bool
	}{
		{"hello", "string", true},
		{42, "string", false},
		{42, "int", true},
		{int32(42), "int", true},
		{int64(42), "int", true},
		{3.14, "float", true},
		{float32(3.14), "float", true},
		{true, "bool", true},
		{false, "bool", true},
		{time.Now(), "datetime", true},
		{nil, "unknown", true},
	}

	for _, tt := range tests {
		name := fmt.Sprintf("%T/%s", tt.value, tt.typeName)
		t.Run(name, func(t *testing.T) {
			if got := checkType(tt.value, tt.typeName); got != tt.want {
				t.Errorf("checkType(%v, %q) = %v, want %v", tt.value, tt.typeName, got, tt.want)
			}
		})
	}
}

func TestLocalStorage_ShouldRotate(t *testing.T) {
	tests := []struct {
		name          string
		maxRecords    int
		maxFileSizeMB int
		recordCount   int
		fileSize      int64
		shouldRotate  bool
	}{
		{
			name:          "no rotation needed",
			maxRecords:    100,
			maxFileSizeMB: 100,
			recordCount:   50,
			fileSize:      1024,
			shouldRotate:  false,
		},
		{
			name:          "rotation by record count",
			maxRecords:    100,
			maxFileSizeMB: 100,
			recordCount:   100,
			fileSize:      1024,
			shouldRotate:  true,
		},
		{
			name:          "rotation by file size",
			maxRecords:    10000,
			maxFileSizeMB: 1,
			recordCount:   50,
			fileSize:      2 * 1024 * 1024,
			shouldRotate:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tmpDir := t.TempDir()
			cfg := &config.LocalConfig{
				BasePath:          tmpDir,
				PartitionStrategy: "none",
				FileFormat:        "json",
				Compression:       "none",
				MaxFileSizeMB:     tt.maxFileSizeMB,
				MaxRecordsPerFile: tt.maxRecords,
			}
			log := newTestLogger(t)

			storage, err := NewLocalStorage(cfg, log)
			if err != nil {
				t.Fatalf("Failed to create storage: %v", err)
			}
			defer storage.Close()

			change := newTestDataChange(models.Insert, "testdb", "testtable", map[string]interface{}{
				"id": 1,
			})
			storage.Write(change)

			storage.mu.Lock()
			storage.recordCount = tt.recordCount
			storage.fileSize = tt.fileSize
			result := storage.shouldRotate()
			storage.mu.Unlock()

			if result != tt.shouldRotate {
				t.Errorf("shouldRotate() = %v, want %v", result, tt.shouldRotate)
			}
		})
	}
}

func TestManager_Compression_None(t *testing.T) {
	tmpDir := t.TempDir()
	cfg := newTestConfig(t, tmpDir, "json")
	cfg.Compression = "none"
	log := newTestLogger(t)

	manager, err := NewManager(cfg, log)
	if err != nil {
		t.Fatalf("Failed to create manager: %v", err)
	}
	defer manager.Close()

	change := newTestDataChange(models.Insert, "testdb", "users", map[string]interface{}{
		"id": 1,
	})

	if err := manager.Write(change); err != nil {
		t.Errorf("Failed to write data: %v", err)
	}
}

func TestManager_Compression_Snappy(t *testing.T) {
	tmpDir := t.TempDir()
	cfg := newTestConfig(t, tmpDir, "json")
	cfg.Compression = "snappy"
	log := newTestLogger(t)

	manager, err := NewManager(cfg, log)
	if err != nil {
		t.Fatalf("Failed to create manager: %v", err)
	}
	defer manager.Close()

	change := newTestDataChange(models.Insert, "testdb", "users", map[string]interface{}{
		"id": 1,
	})

	if err := manager.Write(change); err != nil {
		t.Errorf("Failed to write data: %v", err)
	}
}

func TestManager_Compression_Gzip(t *testing.T) {
	tmpDir := t.TempDir()
	cfg := newTestConfig(t, tmpDir, "json")
	cfg.Compression = "gzip"
	log := newTestLogger(t)

	manager, err := NewManager(cfg, log)
	if err != nil {
		t.Fatalf("Failed to create manager: %v", err)
	}
	defer manager.Close()

	change := newTestDataChange(models.Insert, "testdb", "users", map[string]interface{}{
		"id": 1,
	})

	if err := manager.Write(change); err != nil {
		t.Errorf("Failed to write data: %v", err)
	}
}

func TestManager_Compression_Zstd(t *testing.T) {
	tmpDir := t.TempDir()
	cfg := newTestConfig(t, tmpDir, "json")
	cfg.Compression = "zstd"
	log := newTestLogger(t)

	manager, err := NewManager(cfg, log)
	if err != nil {
		t.Fatalf("Failed to create manager: %v", err)
	}
	defer manager.Close()

	change := newTestDataChange(models.Insert, "testdb", "users", map[string]interface{}{
		"id": 1,
	})

	if err := manager.Write(change); err != nil {
		t.Errorf("Failed to write data: %v", err)
	}
}
