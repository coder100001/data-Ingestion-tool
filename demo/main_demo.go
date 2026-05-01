package main

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"data-ingestion-tool/pkg/config"
	"data-ingestion-tool/pkg/logger"
	"data-ingestion-tool/pkg/models"
	"data-ingestion-tool/pkg/pipeline"
)

func main() {
	fmt.Println("========================================")
	fmt.Println("  数据摄取工具 - 演示程序")
	fmt.Println("========================================")
	fmt.Println()

	// 创建演示目录
	demoDir := "./demo-output"
	os.MkdirAll(demoDir, 0755)

	// 初始化配置
	cfg := &config.Config{
		App: config.AppConfig{
			LogLevel: "info",
			LogFile:  "",
		},
		Storage: config.StorageConfig{
			Local: config.LocalConfig{
				BasePath:          demoDir,
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

	// 初始化日志
	log, err := logger.New("info", "")
	if err != nil {
		fmt.Printf("初始化日志失败: %v\n", err)
		os.Exit(1)
	}

	fmt.Println("[1/5] 初始化存储管理器...")
	storage, err := pipeline.NewLocalStorage(&cfg.Storage.Local, log)
	if err != nil {
		fmt.Printf("初始化存储失败: %v\n", err)
		os.Exit(1)
	}
	fmt.Println("      ✓ 存储管理器已初始化")
	fmt.Printf("      ✓ 数据湖路径: %s\n", demoDir)
	fmt.Println()

	fmt.Println("[2/5] 初始化数据处理管道...")
	pipe := pipeline.NewPipeline(cfg, log, storage)
	fmt.Println("      ✓ 管道已创建")
	fmt.Printf("      ✓ 工作线程数: %d\n", cfg.Processing.WorkerCount)
	fmt.Println()

	fmt.Println("[3/5] 启动管道...")
	if err := pipe.Start(); err != nil {
		fmt.Printf("启动管道失败: %v\n", err)
		os.Exit(1)
	}
	fmt.Println("      ✓ 管道已启动")
	fmt.Println()

	fmt.Println("[4/5] 模拟数据变更事件...")
	changeChan := pipe.GetChangeChannel()

	// 模拟不同类型的数据变更
	events := []struct {
		typ    models.ChangeType
		db     string
		table  string
		before map[string]interface{}
		after  map[string]interface{}
	}{
		{
			typ:   models.Insert,
			db:    "ecommerce",
			table: "users",
			after: map[string]interface{}{
				"id":         1,
				"username":   "alice",
				"email":      "alice@example.com",
				"created_at": "2026-04-27T10:00:00Z",
			},
		},
		{
			typ:   models.Insert,
			db:    "ecommerce",
			table: "users",
			after: map[string]interface{}{
				"id":         2,
				"username":   "bob",
				"email":      "bob@example.com",
				"created_at": "2026-04-27T10:01:00Z",
			},
		},
		{
			typ:   models.Update,
			db:    "ecommerce",
			table: "users",
			before: map[string]interface{}{
				"id":       1,
				"username": "alice",
				"email":    "alice@example.com",
			},
			after: map[string]interface{}{
				"id":       1,
				"username": "alice",
				"email":    "alice.new@example.com",
			},
		},
		{
			typ:   models.Insert,
			db:    "ecommerce",
			table: "orders",
			after: map[string]interface{}{
				"id":         1001,
				"user_id":    1,
				"total":      199.99,
				"status":     "pending",
				"created_at": "2026-04-27T10:05:00Z",
			},
		},
		{
			typ:   models.Insert,
			db:    "ecommerce",
			table: "orders",
			after: map[string]interface{}{
				"id":         1002,
				"user_id":    2,
				"total":      299.99,
				"status":     "completed",
				"created_at": "2026-04-27T10:06:00Z",
			},
		},
		{
			typ:   models.Delete,
			db:    "ecommerce",
			table: "users",
			before: map[string]interface{}{
				"id":       2,
				"username": "bob",
				"email":    "bob@example.com",
			},
		},
	}

	for i, event := range events {
		change := models.NewDataChange(event.typ, event.db, event.table)
		change.Before = event.before
		change.After = event.after
		change.BinlogFile = "mysql-bin.000001"
		change.BinlogPos = uint32(1000 + i*100)

		changeChan <- change
		fmt.Printf("      ✓ 发送事件 %d: [%s] %s.%s\n", i+1, event.typ, event.db, event.table)
		time.Sleep(100 * time.Millisecond)
	}
	fmt.Println()

	// 等待处理完成
	time.Sleep(1 * time.Second)

	fmt.Println("[5/5] 停止管道并查看结果...")
	pipe.Stop()
	fmt.Println("      ✓ 管道已停止")
	fmt.Println()

	// 显示生成的文件
	fmt.Println("========================================")
	fmt.Println("  数据湖输出结果")
	fmt.Println("========================================")
	fmt.Println()

	showDataLakeContents(demoDir)

	fmt.Println()
	fmt.Println("========================================")
	fmt.Println("  演示完成!")
	fmt.Println("========================================")
	fmt.Printf("数据已保存到: %s\n", demoDir)
}

func showDataLakeContents(dir string) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		fmt.Printf("读取目录失败: %v\n", err)
		return
	}

	for _, entry := range entries {
		if entry.IsDir() {
			subDir := filepath.Join(dir, entry.Name())
			fmt.Printf("📁 分区目录: %s/\n", entry.Name())

			subEntries, _ := os.ReadDir(subDir)
			for _, subEntry := range subEntries {
				if !subEntry.IsDir() {
					filePath := filepath.Join(subDir, subEntry.Name())
					info, _ := os.Stat(filePath)
					fmt.Printf("   📄 %s (%d bytes)\n", subEntry.Name(), info.Size())

					// 显示文件内容预览
					content, err := os.ReadFile(filePath)
					if err == nil {
						var records []map[string]interface{}
						if err := json.Unmarshal(content, &records); err == nil {
							fmt.Printf("      包含 %d 条记录:\n", len(records))
							for i, record := range records {
								if i >= 3 {
									fmt.Printf("      ... 还有 %d 条记录\n", len(records)-3)
									break
								}
								data, _ := json.MarshalIndent(record, "      ", "  ")
								fmt.Printf("      记录 %d: %s\n", i+1, string(data))
							}
						}
					}
				}
			}
		}
	}
}
