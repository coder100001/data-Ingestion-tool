package test

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"data-ingestion-tool/pkg/config"
	"data-ingestion-tool/pkg/logger"
	"data-ingestion-tool/pkg/models"
	"data-ingestion-tool/pkg/pipeline"
)

// TestLocalStorage 测试本地存储功能
func TestLocalStorage(t *testing.T) {
	// 创建临时目录
	tmpDir := t.TempDir()
	
	log, err := logger.New("debug", "")
	if err != nil {
		t.Fatalf("Failed to create logger: %v", err)
	}
	
	cfg := &config.LocalConfig{
		BasePath:          tmpDir,
		PartitionStrategy: "date",
		FileFormat:        "json",
		MaxFileSizeMB:     10,
		MaxRecordsPerFile: 100,
	}
	
	storage, err := pipeline.NewLocalStorage(cfg, log)
	if err != nil {
		t.Fatalf("Failed to create storage: %v", err)
	}
	defer storage.Close()
	
	// 测试写入数据变更
	change := models.NewDataChange(models.Insert, "testdb", "users")
	change.After = map[string]interface{}{
		"id":       1,
		"username": "test_user",
		"email":    "test@example.com",
	}
	
	err = storage.Write(change)
	if err != nil {
		t.Fatalf("Failed to write change: %v", err)
	}
	
	// 验证文件是否创建
	err = storage.Flush()
	if err != nil {
		t.Fatalf("Failed to flush storage: %v", err)
	}
	
	// 检查目录结构
	entries, err := os.ReadDir(tmpDir)
	if err != nil {
		t.Fatalf("Failed to read tmp dir: %v", err)
	}
	
	if len(entries) == 0 {
		t.Error("Expected at least one partition directory")
	}
	
	t.Logf("Created %d partition directories", len(entries))
}

// TestDataChange 测试数据变更模型
func TestDataChange(t *testing.T) {
	change := models.NewDataChange(models.Update, "testdb", "orders")
	change.Before = map[string]interface{}{
		"id":     1,
		"status": "pending",
	}
	change.After = map[string]interface{}{
		"id":     1,
		"status": "completed",
	}
	
	// 测试JSON序列化
	jsonData, err := change.ToJSON()
	if err != nil {
		t.Fatalf("Failed to marshal to JSON: %v", err)
	}
	
	if len(jsonData) == 0 {
		t.Error("Expected non-empty JSON data")
	}
	
	t.Logf("JSON output: %s", string(jsonData))
}

// TestConfigLoading 测试配置加载
func TestConfigLoading(t *testing.T) {
	// 创建临时配置文件
	tmpDir := t.TempDir()
	configPath := filepath.Join(tmpDir, "test_config.yaml")
	
	configContent := `
app:
  name: "test-app"
  log_level: "debug"

source:
  type: "mysql"
  mysql:
    host: "localhost"
    port: 3306
    user: "root"
    password: "password"
    server_id: 1001

storage:
  local:
    base_path: "./test-data"
    partition_strategy: "date"
    file_format: "json"
`
	
	err := os.WriteFile(configPath, []byte(configContent), 0644)
	if err != nil {
		t.Fatalf("Failed to write config file: %v", err)
	}
	
	cfg, err := config.Load(configPath)
	if err != nil {
		t.Fatalf("Failed to load config: %v", err)
	}
	
	if cfg.App.Name != "test-app" {
		t.Errorf("Expected app name 'test-app', got '%s'", cfg.App.Name)
	}
	
	if cfg.Source.MySQL.Host != "localhost" {
		t.Errorf("Expected MySQL host 'localhost', got '%s'", cfg.Source.MySQL.Host)
	}
}

// TestPipeline 测试数据处理管道
func TestPipeline(t *testing.T) {
	tmpDir := t.TempDir()
	
	cfg := &config.Config{
		App: config.AppConfig{
			LogLevel: "debug",
		},
		Storage: config.StorageConfig{
			Local: config.LocalConfig{
				BasePath:          tmpDir,
				PartitionStrategy: "date",
				FileFormat:        "json",
				MaxFileSizeMB:     10,
				MaxRecordsPerFile: 100,
			},
		},
		Processing: config.ProcessingConfig{
			BatchSize:   10,
			WorkerCount: 2,
		},
	}
	
	log, err := logger.New("debug", "")
	if err != nil {
		t.Fatalf("Failed to create logger: %v", err)
	}
	
	storage, err := pipeline.NewLocalStorage(&cfg.Storage.Local, log)
	if err != nil {
		t.Fatalf("Failed to create storage: %v", err)
	}
	
	pipe := pipeline.NewPipeline(cfg, log, storage)
	
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	
	err = pipe.Start()
	if err != nil {
		t.Fatalf("Failed to start pipeline: %v", err)
	}
	defer pipe.Stop()
	
	// 发送测试数据
	changeChan := pipe.GetChangeChannel()
	
	for i := 0; i < 5; i++ {
		change := models.NewDataChange(models.Insert, "testdb", "users")
		change.After = map[string]interface{}{
			"id":       i,
			"username": "user_" + string(rune('a'+i)),
		}
		
		select {
		case changeChan <- change:
			t.Logf("Sent change %d", i)
		case <-ctx.Done():
			t.Fatal("Timeout sending change")
		}
	}
	
	// 等待处理完成
	time.Sleep(1 * time.Second)
	
	t.Log("Pipeline test completed")
}

// BenchmarkDataChange 基准测试
func BenchmarkDataChange(b *testing.B) {
	for i := 0; i < b.N; i++ {
		change := models.NewDataChange(models.Insert, "testdb", "users")
		change.After = map[string]interface{}{
			"id":       i,
			"username": "test_user",
			"email":    "test@example.com",
		}
		_, err := change.ToJSON()
		if err != nil {
			b.Fatalf("Failed to marshal: %v", err)
		}
	}
}
