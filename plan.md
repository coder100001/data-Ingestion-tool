# 代码审查问题修复计划

## 1. 需求概述
- **功能名称**: Data Ingestion Tool 代码审查问题修复
- **目标**: 修复代码审查中发现的 21 个问题，按优先级分三批次处理
- **优先级**: P0 (批次1) / P1 (批次2) / P2 (批次3)
- **复杂度级别**: L2 (中等任务)

## 2. 变更范围

### 2.1 批次1 — P0 紧急修复 (L1 简单流程)

| 文件路径 | 变更类型 | 问题描述 |
|---------|---------|---------|
| `pkg/connector/mysql.go` | 修改 | #1 Channel 满时事件丢失，改为阻塞发送+超时 |
| `pkg/models/models.go` | 修改 | #2 generateID 使用 UUID 替代时间戳 |
| `Dockerfile` | 修改 | #8 COPY config.yaml 不存在，修复为 config.example.yaml |
| `pkg/storage/parquet/reader.go` | 修改 | #9 Reader.Seek 签名不符合 io.Seeker |
| `cmd/ingester/main.go` | 修改 | #16 删除未使用的 handleError 和 printStats 函数 |

### 2.2 批次2 — P1 重要修复 (L2 标准流程)

| 文件路径 | 变更类型 | 问题描述 |
|---------|---------|---------|
| `pkg/connector/connector.go` | 修改 | #3 shouldProcessTable 从 BaseConnector 移到 MySQLConnector |
| `pkg/connector/mysql.go` | 修改 | #3 接收 shouldProcessTable 方法 |
| `pkg/checkpoint/checkpoint.go` | 修改 | #4 Stop() 竞态条件修复 |
| `pkg/storage/manager.go` | 修改 | #6 writeCompressedJSON 标记未实现 |
| `pkg/pipeline/pipeline.go` | 修改 | #7 parseFilterRule/parseTransformRule 返回错误 |
| `pkg/connector/mysql.go` | 修改 | #15 移除冗余 canal 实例 |
| `pkg/storage/local_storage.go` | 修改 | #20 CSV 写入更新 fileSize |

### 2.3 批次3 — P2 改进 (L2 标准流程)

| 文件路径 | 变更类型 | 问题描述 |
|---------|---------|---------|
| `config.example.yaml` | 修改 | #10 统一配置文件与代码结构 |
| `pkg/config/secrets.go` | 修改 | #11 unresolvedEnvVars 并发安全 |
| `pkg/logger/logger.go` | 修改 | #12 fileHook 文件句柄释放 |
| `pkg/storage/catalog.go` | 修改 | #13 ColumnInfo 重命名避免混淆 |
| `cmd/ingester/main.go` | 修改 | #19 Graceful shutdown 超时控制 |

## 3. 逻辑设计

### 3.1 批次1 核心变更

#### #1 Channel 满时事件丢失
```
当前: select { case ch <- change: ...; default: drop }
改为: select {
        case ch <- change: ...
        case <-time.After(5 * time.Second):
            log.Error("Timeout sending to channel")
            // 写入死信队列
      }
```

#### #2 generateID 改用 UUID
```
当前: time.Now().UTC().Format("20060102150405.000000000")
改为: uuid.New().String()
```

#### #8 Dockerfile 修复
```
当前: COPY --from=builder /app/config.yaml .
改为: COPY --from=builder /app/config.example.yaml ./config.example.yaml
```

#### #9 Reader.Seek 签名修复
```
当前: Seek(rowNum int64) error
改为: Seek(offset int64, whence int) (int64, error)
```

#### #16 删除未使用函数
```
删除: handleError(), printStats()
```

### 3.2 批次2 核心变更

#### #3 shouldProcessTable 移到 MySQLConnector
```
从 BaseConnector 删除 shouldProcessTable
在 MySQLConnector 中新增 shouldProcessTable 方法
MySQLConnector.processRowsEvent 调用自己的 shouldProcessTable
```

#### #4 Checkpoint Stop 竞态修复
```
当前: close(stopChan) → Save()
改为: 使用 sync.Once 保证只保存一次
      autoSave 退出时保存，Stop() 等待 autoSave 退出
```

#### #6 压缩功能标记
```
writeCompressedJSON 添加日志警告和 TODO
配置验证时若 compression != "none" 且 format != "parquet" 则警告
```

#### #7 Filter/Transform 解析器
```
当前: 返回 NoOp 实现（静默失败）
改为: 返回错误 "filter/transform rules not yet implemented, remove from config"
```

#### #15 移除冗余 canal
```
Connect() 中不再创建 canal.Canal
GetTableInfo() 改为直接查询 MySQL information_schema
```

#### #20 CSV fileSize 更新
```
writeCSV 中估算写入大小并更新 fileSize
```

### 3.3 批次3 核心变更

#### #10 配置文件统一
```
移除 config.example.yaml 中的 advanced 节
确保所有配置项与 Config 结构体对应
```

#### #11 unresolvedEnvVars 并发安全
```
改为使用 sync.Mutex 保护
或改为在 ResolveSecrets 函数内局部变量
```

#### #12 Logger fileHook 释放
```
Logger 新增 Close() 方法
fileHook 在 Close() 时关闭文件
```

#### #13 ColumnInfo 重命名
```
catalog.go 中的 ColumnInfo → CatalogColumnInfo
避免与 models.ColumnInfo 混淆
```

#### #19 Graceful shutdown 超时
```
shutdown() 添加 30 秒超时
超时后强制退出
```

## 4. 边界条件

| 场景 | 输入 | 预期输出 | 处理方式 |
|-----|------|---------|---------|
| Channel 发送超时 | 5秒内无法发送 | 记录错误日志 | 超时后丢弃并记录 |
| UUID 生成失败 | 极端情况 | 降级为时间戳 | 添加 fallback |
| Checkpoint 并发保存 | Stop + autoSave 同时触发 | 只保存一次 | sync.Once |
| CSV 文件大小估算 | 不精确 | 近似值 | 可接受 |
| Shutdown 超时 | 30秒内未完成 | 强制退出 | os.Exit(1) |

## 5. 风险评估

| 风险类型 | 概率 | 影响 | 缓解措施 |
|---------|------|------|---------|
| Channel 行为变更 | 低 | 中 | 超时时间可配置 |
| UUID 依赖 | 低 | 低 | 已在 go.mod 中 |
| MySQL 查询替代 canal | 中 | 中 | 保留 canal 作为备选 |
| 向后兼容 | 低 | 高 | Seek 签名变更可能影响调用方 |

## 6. 测试策略

### 6.1 单元测试
- 覆盖率目标: >= 80%
- 批次1: 修改的每个函数都需要测试
- 批次2: 重点测试 checkpoint 竞态、filter/transform 错误返回
- 批次3: 重点测试 logger Close、shutdown 超时

### 6.2 回归测试
- 每批次完成后运行 `go test ./...`
- 确保现有测试不受影响

## 7. 验收标准
- [ ] 所有 P0 问题已修复
- [ ] 所有 P1 问题已修复
- [ ] 所有 P2 问题已修复
- [ ] `go vet ./...` 无错误
- [ ] `go test ./...` 全部通过
- [ ] `go build ./...` 编译成功
- [ ] 无新增竞态条件 (`go test -race`)

## 8. 回滚策略
- 每批次独立提交，可单独回滚
- 不涉及数据库迁移
- 不涉及 API 变更（Seek 除外，但当前无外部调用方）

---
**计划状态**: 已完成 (commit 3a9e81b)
**创建时间**: 2026-04-28
**最后更新**: 2026-04-30
