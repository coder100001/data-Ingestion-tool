package storage

import (
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
		BasePath:           tmpDir,
		PartitionStrategy:  "date",
		FileFormat:         fileFormat,
		Compression:        "none",
		MaxFileSizeMB:      100,
		MaxRecordsPerFile:  10000,
		Parquet:            config.ParquetConfig{
			RowGroupSize:     10000,
			PageSize:         8192,
			EnableDictionary: true,
		},
		Schema:             config.SchemaConfig{
			RegistryPath:   filepath.Join(tmpDir, "schemas"),
			Compatibility:  "backward",
			AutoRegister:   true,
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
					"id":          goroutineID*numWrites + i,
					"goroutine":   goroutineID,
					"iteration":   i,
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
	defer manager.Close()

	if manager == nil {
		t.Error("Expected manager to be non-nil")
	}

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
