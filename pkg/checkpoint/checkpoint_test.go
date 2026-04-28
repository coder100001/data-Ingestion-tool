package checkpoint

import (
	"encoding/json"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"data-ingestion-tool/pkg/config"
	"data-ingestion-tool/pkg/logger"
	"data-ingestion-tool/pkg/models"
)

func newTestLogger(t *testing.T) *logger.Logger {
	t.Helper()
	log, err := logger.New("error", "")
	if err != nil {
		t.Fatalf("Failed to create test logger: %v", err)
	}
	return log
}

func newTestConfig(t *testing.T, storagePath string) *config.CheckpointConfig {
	t.Helper()
	return &config.CheckpointConfig{
		StoragePath:     storagePath,
		SaveIntervalSec: 3600,
	}
}

func stopManager(mgr *Manager) {
	if mgr != nil {
		mgr.Stop()
		time.Sleep(50 * time.Millisecond)
	}
}

func TestNewManager(t *testing.T) {
	tmpDir := t.TempDir()
	storagePath := filepath.Join(tmpDir, "checkpoint.json")

	cfg := newTestConfig(t, storagePath)
	log := newTestLogger(t)

	mgr, err := NewManager(cfg, log)
	if err != nil {
		t.Fatalf("Failed to create manager: %v", err)
	}
	defer stopManager(mgr)

	if mgr == nil {
		t.Fatal("Expected manager to be non-nil")
	}

	if mgr.IsInitialized() {
		t.Error("Expected manager to not be initialized initially")
	}
}

func TestLoadSave(t *testing.T) {
	tmpDir := t.TempDir()
	storagePath := filepath.Join(tmpDir, "checkpoint.json")

	cfg := newTestConfig(t, storagePath)
	log := newTestLogger(t)

	mgr, err := NewManager(cfg, log)
	if err != nil {
		t.Fatalf("Failed to create manager: %v", err)
	}
	defer stopManager(mgr)

	position := models.Position{
		BinlogFile: "mysql-bin.000001",
		BinlogPos:  12345,
	}

	mgr.Initialize("mysql", position)

	if err := mgr.Save(); err != nil {
		t.Fatalf("Failed to save checkpoint: %v", err)
	}

	if _, err := os.Stat(storagePath); os.IsNotExist(err) {
		t.Fatal("Expected checkpoint file to exist after save")
	}

	mgr2, err := NewManager(cfg, log)
	if err != nil {
		t.Fatalf("Failed to create second manager: %v", err)
	}
	defer stopManager(mgr2)

	if !mgr2.IsInitialized() {
		t.Fatal("Expected second manager to be initialized after loading")
	}

	loadedPosition := mgr2.GetPosition()
	if loadedPosition.BinlogFile != position.BinlogFile {
		t.Errorf("Expected binlog file %s, got %s", position.BinlogFile, loadedPosition.BinlogFile)
	}

	if loadedPosition.BinlogPos != position.BinlogPos {
		t.Errorf("Expected binlog pos %d, got %d", position.BinlogPos, loadedPosition.BinlogPos)
	}
}

func TestLoadNonExistent(t *testing.T) {
	tmpDir := t.TempDir()
	storagePath := filepath.Join(tmpDir, "checkpoint.json")

	cfg := newTestConfig(t, storagePath)
	log := newTestLogger(t)

	mgr, err := NewManager(cfg, log)
	if err != nil {
		t.Fatalf("Failed to create manager: %v", err)
	}
	defer stopManager(mgr)

	if mgr.IsInitialized() {
		t.Error("Expected manager to not be initialized when no checkpoint file exists")
	}
}

func TestLoadInvalidJSON(t *testing.T) {
	tmpDir := t.TempDir()
	storagePath := filepath.Join(tmpDir, "checkpoint.json")

	invalidContent := `{"source_type": "mysql", "position": {invalid json}`
	if err := os.WriteFile(storagePath, []byte(invalidContent), 0644); err != nil {
		t.Fatalf("Failed to write invalid checkpoint: %v", err)
	}

	cfg := newTestConfig(t, storagePath)
	log := newTestLogger(t)

	mgr, err := NewManager(cfg, log)
	if err != nil {
		t.Fatalf("Failed to create manager: %v", err)
	}
	defer stopManager(mgr)

	if mgr.IsInitialized() {
		t.Error("Expected manager to not be initialized with invalid JSON")
	}
}

func TestUpdatePosition(t *testing.T) {
	tmpDir := t.TempDir()
	storagePath := filepath.Join(tmpDir, "checkpoint.json")

	cfg := newTestConfig(t, storagePath)
	log := newTestLogger(t)

	mgr, err := NewManager(cfg, log)
	if err != nil {
		t.Fatalf("Failed to create manager: %v", err)
	}
	defer stopManager(mgr)

	position1 := models.Position{
		BinlogFile: "mysql-bin.000001",
		BinlogPos:  100,
	}

	mgr.UpdatePosition(position1)

	if !mgr.IsInitialized() {
		t.Fatal("Expected manager to be initialized after UpdatePosition")
	}

	currentPos := mgr.GetPosition()
	if currentPos.BinlogFile != position1.BinlogFile {
		t.Errorf("Expected binlog file %s, got %s", position1.BinlogFile, currentPos.BinlogFile)
	}

	position2 := models.Position{
		BinlogFile: "mysql-bin.000002",
		BinlogPos:  200,
	}

	mgr.UpdatePosition(position2)

	updatedPos := mgr.GetPosition()
	if updatedPos.BinlogFile != position2.BinlogFile {
		t.Errorf("Expected binlog file %s, got %s", position2.BinlogFile, updatedPos.BinlogFile)
	}

	if updatedPos.BinlogPos != position2.BinlogPos {
		t.Errorf("Expected binlog pos %d, got %d", position2.BinlogPos, updatedPos.BinlogPos)
	}
}

func TestUpdatePositionKafka(t *testing.T) {
	tmpDir := t.TempDir()
	storagePath := filepath.Join(tmpDir, "checkpoint.json")

	cfg := newTestConfig(t, storagePath)
	log := newTestLogger(t)

	mgr, err := NewManager(cfg, log)
	if err != nil {
		t.Fatalf("Failed to create manager: %v", err)
	}
	defer stopManager(mgr)

	position := models.Position{
		Topic:     "test-topic",
		Partition: 1,
		Offset:    1000,
	}

	mgr.UpdatePosition(position)

	currentPos := mgr.GetPosition()
	if currentPos.Topic != position.Topic {
		t.Errorf("Expected topic %s, got %s", position.Topic, currentPos.Topic)
	}

	if currentPos.Partition != position.Partition {
		t.Errorf("Expected partition %d, got %d", position.Partition, currentPos.Partition)
	}

	if currentPos.Offset != position.Offset {
		t.Errorf("Expected offset %d, got %d", position.Offset, currentPos.Offset)
	}
}

func TestReset(t *testing.T) {
	tmpDir := t.TempDir()
	storagePath := filepath.Join(tmpDir, "checkpoint.json")

	cfg := newTestConfig(t, storagePath)
	log := newTestLogger(t)

	mgr, err := NewManager(cfg, log)
	if err != nil {
		t.Fatalf("Failed to create manager: %v", err)
	}
	defer stopManager(mgr)

	position := models.Position{
		BinlogFile: "mysql-bin.000001",
		BinlogPos:  12345,
	}

	mgr.Initialize("mysql", position)

	if err := mgr.Save(); err != nil {
		t.Fatalf("Failed to save checkpoint: %v", err)
	}

	if _, err := os.Stat(storagePath); os.IsNotExist(err) {
		t.Fatal("Expected checkpoint file to exist before reset")
	}

	if err := mgr.Reset(); err != nil {
		t.Fatalf("Failed to reset checkpoint: %v", err)
	}

	if mgr.IsInitialized() {
		t.Error("Expected manager to not be initialized after reset")
	}

	if _, err := os.Stat(storagePath); !os.IsNotExist(err) {
		t.Error("Expected checkpoint file to be removed after reset")
	}
}

func TestResetNonExistentFile(t *testing.T) {
	tmpDir := t.TempDir()
	storagePath := filepath.Join(tmpDir, "checkpoint.json")

	cfg := newTestConfig(t, storagePath)
	log := newTestLogger(t)

	mgr, err := NewManager(cfg, log)
	if err != nil {
		t.Fatalf("Failed to create manager: %v", err)
	}
	defer stopManager(mgr)

	if err := mgr.Reset(); err != nil {
		t.Fatalf("Reset should not fail when file doesn't exist: %v", err)
	}
}

func TestIsInitialized(t *testing.T) {
	tmpDir := t.TempDir()
	storagePath := filepath.Join(tmpDir, "checkpoint.json")

	cfg := newTestConfig(t, storagePath)
	log := newTestLogger(t)

	mgr, err := NewManager(cfg, log)
	if err != nil {
		t.Fatalf("Failed to create manager: %v", err)
	}
	defer stopManager(mgr)

	if mgr.IsInitialized() {
		t.Error("Expected manager to not be initialized initially")
	}

	position := models.Position{
		BinlogFile: "mysql-bin.000001",
		BinlogPos:  100,
	}

	mgr.Initialize("mysql", position)

	if !mgr.IsInitialized() {
		t.Error("Expected manager to be initialized after Initialize")
	}

	mgr.UpdatePosition(models.Position{
		BinlogFile: "mysql-bin.000002",
		BinlogPos:  200,
	})

	if !mgr.IsInitialized() {
		t.Error("Expected manager to remain initialized after UpdatePosition")
	}

	if err := mgr.Reset(); err != nil {
		t.Fatalf("Failed to reset: %v", err)
	}

	if mgr.IsInitialized() {
		t.Error("Expected manager to not be initialized after Reset")
	}
}

func TestConcurrentAccess(t *testing.T) {
	tmpDir := t.TempDir()
	storagePath := filepath.Join(tmpDir, "checkpoint.json")

	cfg := newTestConfig(t, storagePath)
	log := newTestLogger(t)

	mgr, err := NewManager(cfg, log)
	if err != nil {
		t.Fatalf("Failed to create manager: %v", err)
	}
	defer stopManager(mgr)

	var wg sync.WaitGroup
	numGoroutines := 10
	numOperations := 100

	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < numOperations; j++ {
				position := models.Position{
					BinlogFile: "mysql-bin.000001",
					BinlogPos:  uint32(id*1000 + j),
				}
				mgr.UpdatePosition(position)

				_ = mgr.GetPosition()
				_ = mgr.IsInitialized()
			}
		}(i)
	}

	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < numOperations; j++ {
				_ = mgr.GetCheckpoint()
			}
		}()
	}

	wg.Wait()

	if err := mgr.Save(); err != nil {
		t.Fatalf("Failed to save after concurrent access: %v", err)
	}
}

func TestConcurrentSaveLoad(t *testing.T) {
	tmpDir := t.TempDir()
	storagePath := filepath.Join(tmpDir, "checkpoint.json")

	cfg := newTestConfig(t, storagePath)
	log := newTestLogger(t)

	mgr, err := NewManager(cfg, log)
	if err != nil {
		t.Fatalf("Failed to create manager: %v", err)
	}
	defer stopManager(mgr)

	position := models.Position{
		BinlogFile: "mysql-bin.000001",
		BinlogPos:  100,
	}
	mgr.Initialize("mysql", position)

	var wg sync.WaitGroup
	numSaves := 20

	for i := 0; i < numSaves; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			pos := models.Position{
				BinlogFile: "mysql-bin.000001",
				BinlogPos:  uint32(idx * 100),
			}
			mgr.UpdatePosition(pos)
			if err := mgr.Save(); err != nil {
				t.Errorf("Concurrent save failed: %v", err)
			}
		}(i)
	}

	wg.Wait()

	data, err := os.ReadFile(storagePath)
	if err != nil {
		t.Fatalf("Failed to read checkpoint file: %v", err)
	}

	var checkpoint models.Checkpoint
	if err := json.Unmarshal(data, &checkpoint); err != nil {
		t.Fatalf("Failed to unmarshal checkpoint: %v", err)
	}

	if checkpoint.SourceType != "mysql" {
		t.Errorf("Expected source type 'mysql', got '%s'", checkpoint.SourceType)
	}
}

func TestGetCheckpoint(t *testing.T) {
	tmpDir := t.TempDir()
	storagePath := filepath.Join(tmpDir, "checkpoint.json")

	cfg := newTestConfig(t, storagePath)
	log := newTestLogger(t)

	mgr, err := NewManager(cfg, log)
	if err != nil {
		t.Fatalf("Failed to create manager: %v", err)
	}
	defer stopManager(mgr)

	if cp := mgr.GetCheckpoint(); cp != nil {
		t.Error("Expected nil checkpoint when not initialized")
	}

	position := models.Position{
		BinlogFile: "mysql-bin.000001",
		BinlogPos:  12345,
	}
	mgr.Initialize("mysql", position)

	cp := mgr.GetCheckpoint()
	if cp == nil {
		t.Fatal("Expected non-nil checkpoint after initialization")
	}

	if cp.SourceType != "mysql" {
		t.Errorf("Expected source type 'mysql', got '%s'", cp.SourceType)
	}

	cp.Position.BinlogPos = 99999

	originalPos := mgr.GetPosition()
	if originalPos.BinlogPos == 99999 {
		t.Error("GetCheckpoint should return a copy, not a reference")
	}
}

func TestGetPosition(t *testing.T) {
	tmpDir := t.TempDir()
	storagePath := filepath.Join(tmpDir, "checkpoint.json")

	cfg := newTestConfig(t, storagePath)
	log := newTestLogger(t)

	mgr, err := NewManager(cfg, log)
	if err != nil {
		t.Fatalf("Failed to create manager: %v", err)
	}
	defer stopManager(mgr)

	pos := mgr.GetPosition()
	if pos.BinlogFile != "" || pos.BinlogPos != 0 {
		t.Error("Expected empty position when not initialized")
	}

	position := models.Position{
		BinlogFile: "mysql-bin.000001",
		BinlogPos:  12345,
	}
	mgr.Initialize("mysql", position)

	currentPos := mgr.GetPosition()
	if currentPos.BinlogFile != position.BinlogFile {
		t.Errorf("Expected binlog file %s, got %s", position.BinlogFile, currentPos.BinlogFile)
	}

	if currentPos.BinlogPos != position.BinlogPos {
		t.Errorf("Expected binlog pos %d, got %d", position.BinlogPos, currentPos.BinlogPos)
	}
}

func TestInitialize(t *testing.T) {
	tmpDir := t.TempDir()
	storagePath := filepath.Join(tmpDir, "checkpoint.json")

	cfg := newTestConfig(t, storagePath)
	log := newTestLogger(t)

	mgr, err := NewManager(cfg, log)
	if err != nil {
		t.Fatalf("Failed to create manager: %v", err)
	}
	defer stopManager(mgr)

	position := models.Position{
		BinlogFile: "mysql-bin.000001",
		BinlogPos:  12345,
	}

	mgr.Initialize("mysql", position)

	if !mgr.IsInitialized() {
		t.Fatal("Expected manager to be initialized")
	}

	cp := mgr.GetCheckpoint()
	if cp == nil {
		t.Fatal("Expected checkpoint to be non-nil")
	}

	if cp.SourceType != "mysql" {
		t.Errorf("Expected source type 'mysql', got '%s'", cp.SourceType)
	}

	if cp.Position.BinlogFile != position.BinlogFile {
		t.Errorf("Expected binlog file %s, got %s", position.BinlogFile, cp.Position.BinlogFile)
	}
}

func TestSaveCreatesDirectory(t *testing.T) {
	tmpDir := t.TempDir()
	storagePath := filepath.Join(tmpDir, "subdir", "another", "checkpoint.json")

	cfg := newTestConfig(t, storagePath)
	log := newTestLogger(t)

	mgr, err := NewManager(cfg, log)
	if err != nil {
		t.Fatalf("Failed to create manager: %v", err)
	}
	defer stopManager(mgr)

	position := models.Position{
		BinlogFile: "mysql-bin.000001",
		BinlogPos:  12345,
	}
	mgr.Initialize("mysql", position)

	if err := mgr.Save(); err != nil {
		t.Fatalf("Failed to save checkpoint: %v", err)
	}

	if _, err := os.Stat(storagePath); os.IsNotExist(err) {
		t.Fatal("Expected checkpoint file to be created in nested directory")
	}
}

func TestSaveNilCheckpoint(t *testing.T) {
	tmpDir := t.TempDir()
	storagePath := filepath.Join(tmpDir, "checkpoint.json")

	cfg := newTestConfig(t, storagePath)
	log := newTestLogger(t)

	mgr, err := NewManager(cfg, log)
	if err != nil {
		t.Fatalf("Failed to create manager: %v", err)
	}
	defer stopManager(mgr)

	if err := mgr.Save(); err != nil {
		t.Errorf("Save should not fail with nil checkpoint: %v", err)
	}
}

func TestStop(t *testing.T) {
	tmpDir := t.TempDir()
	storagePath := filepath.Join(tmpDir, "checkpoint.json")

	cfg := newTestConfig(t, storagePath)
	log := newTestLogger(t)

	mgr, err := NewManager(cfg, log)
	if err != nil {
		t.Fatalf("Failed to create manager: %v", err)
	}

	position := models.Position{
		BinlogFile: "mysql-bin.000001",
		BinlogPos:  12345,
	}
	mgr.Initialize("mysql", position)

	if err := mgr.Stop(); err != nil {
		t.Fatalf("Failed to stop manager: %v", err)
	}

	time.Sleep(50 * time.Millisecond)

	if _, err := os.Stat(storagePath); os.IsNotExist(err) {
		t.Fatal("Expected checkpoint to be saved on stop")
	}
}

func TestAtomicWrite(t *testing.T) {
	tmpDir := t.TempDir()
	storagePath := filepath.Join(tmpDir, "checkpoint.json")

	cfg := newTestConfig(t, storagePath)
	log := newTestLogger(t)

	mgr, err := NewManager(cfg, log)
	if err != nil {
		t.Fatalf("Failed to create manager: %v", err)
	}
	defer stopManager(mgr)

	position := models.Position{
		BinlogFile: "mysql-bin.000001",
		BinlogPos:  12345,
	}
	mgr.Initialize("mysql", position)

	if err := mgr.Save(); err != nil {
		t.Fatalf("Failed to save checkpoint: %v", err)
	}

	tmpPath := storagePath + ".tmp"
	if _, err := os.Stat(tmpPath); !os.IsNotExist(err) {
		t.Error("Temporary file should be removed after successful save")
	}

	data, err := os.ReadFile(storagePath)
	if err != nil {
		t.Fatalf("Failed to read checkpoint: %v", err)
	}

	var checkpoint models.Checkpoint
	if err := json.Unmarshal(data, &checkpoint); err != nil {
		t.Fatalf("Failed to unmarshal checkpoint: %v", err)
	}

	if checkpoint.SourceType != "mysql" {
		t.Errorf("Expected source type 'mysql', got '%s'", checkpoint.SourceType)
	}
}

func TestUpdatedAt(t *testing.T) {
	tmpDir := t.TempDir()
	storagePath := filepath.Join(tmpDir, "checkpoint.json")

	cfg := newTestConfig(t, storagePath)
	log := newTestLogger(t)

	mgr, err := NewManager(cfg, log)
	if err != nil {
		t.Fatalf("Failed to create manager: %v", err)
	}
	defer stopManager(mgr)

	position := models.Position{
		BinlogFile: "mysql-bin.000001",
		BinlogPos:  100,
	}
	mgr.Initialize("mysql", position)

	time.Sleep(100 * time.Millisecond)

	newPosition := models.Position{
		BinlogFile: "mysql-bin.000002",
		BinlogPos:  200,
	}
	mgr.UpdatePosition(newPosition)

	cp := mgr.GetCheckpoint()
	if cp == nil {
		t.Fatal("Expected checkpoint to be non-nil")
	}

	if !cp.UpdatedAt.After(time.Now().Add(-1 * time.Second)) {
		t.Error("Expected UpdatedAt to be recent")
	}
}
