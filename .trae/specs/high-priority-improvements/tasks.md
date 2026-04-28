# Tasks

## Phase 1: 单元测试

- [x] Task 1: 为 pipeline 模块添加单元测试
  - [x] Task 1.1: 创建 pipeline_test.go 文件
  - [x] Task 1.2: 测试 NewPipeline 创建逻辑
  - [x] Task 1.3: 测试 Start/Stop 生命周期
  - [x] Task 1.4: 测试 processChange 处理逻辑
  - [x] Task 1.5: 测试 Filter 和 Transformer 功能
  - [x] Task 1.6: 测试并发安全性

- [x] Task 2: 为 storage 模块添加单元测试
  - [x] Task 2.1: 创建 manager_test.go 文件
  - [x] Task 2.2: 测试 Write 方法（JSON/Parquet 格式）
  - [x] Task 2.3: 测试 Flush 和 Close 方法
  - [x] Task 2.4: 测试 RotateWriters 功能
  - [x] Task 2.5: 测试并发写入安全性

- [x] Task 3: 为 connector 模块添加单元测试
  - [x] Task 3.1: 创建 connector_test.go 文件
  - [x] Task 3.2: 测试 Factory 创建连接器逻辑
  - [x] Task 3.3: 测试 BaseConnector 通用方法
  - [x] Task 3.4: 测试 shouldProcessTable 过滤逻辑

- [x] Task 4: 为 checkpoint 模块添加单元测试
  - [x] Task 4.1: 创建 checkpoint_test.go 文件
  - [x] Task 4.2: 测试 Load/Save 功能
  - [x] Task 4.3: 测试 UpdatePosition 功能
  - [x] Task 4.4: 测试 Reset 功能
  - [x] Task 4.5: 测试并发安全性

## Phase 2: 错误处理机制

- [x] Task 5: 实现错误重试机制
  - [x] Task 5.1: 创建 pkg/retry/retry.go 重试模块
  - [x] Task 5.2: 实现指数退避重试策略
  - [x] Task 5.3: 实现可配置的最大重试次数
  - [x] Task 5.4: 添加重试相关配置项到 config.go
  - [x] Task 5.5: 集成重试机制到 pipeline.processChange

- [x] Task 6: 实现死信队列
  - [x] Task 6.1: 创建 pkg/deadletter/queue.go 死信队列模块
  - [x] Task 6.2: 实现失败记录存储（JSON Lines 格式）
  - [x] Task 6.3: 实现死信队列读取和重放功能
  - [x] Task 6.4: 添加死信队列配置项
  - [x] Task 6.5: 集成死信队列到 pipeline

## Phase 3: 敏感信息安全

- [x] Task 7: 支持环境变量读取敏感信息
  - [x] Task 7.1: 创建 pkg/config/secrets.go 敏感信息处理模块
  - [x] Task 7.2: 实现 `${ENV_VAR}` 格式解析
  - [x] Task 7.3: 实现环境变量优先级逻辑
  - [x] Task 7.4: 修改 config.go 集成环境变量读取

- [x] Task 8: 实现日志脱敏
  - [x] Task 8.1: 创建 pkg/logger/sanitize.go 脱敏模块
  - [x] Task 8.2: 实现敏感字段识别和替换
  - [x] Task 8.3: 修改 logger.go 集成脱敏功能
  - [x] Task 8.4: 添加启动时安全配置警告

## Phase 4: 验证

- [x] Task 9: 运行完整测试套件并验证覆盖率
  - [x] Task 9.1: 运行 `go test ./...` 确保所有测试通过
  - [x] Task 9.2: 生成测试覆盖率报告
  - [x] Task 9.3: 验证覆盖率达标（>= 70%）

# Task Dependencies

- Task 5 依赖 Task 1（需要测试验证重试机制）
- Task 6 依赖 Task 5（死信队列需要重试机制）
- Task 7 和 Task 8 可并行执行
- Task 9 依赖所有前置任务完成

# Parallelizable Work

以下任务可以并行执行：
- Task 1, Task 2, Task 3, Task 4（测试任务）
- Task 7, Task 8（安全相关任务）
