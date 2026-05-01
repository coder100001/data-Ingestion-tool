# Tasks

> 基于 EXECUTION-PLAN.md 和 TASKS.md 的任务分解，按执行顺序建议排列。

## 批次 A — P0 紧急（测试覆盖 + 工具链）

### A-5: 安装 golangci-lint（优先建立代码质量基准）

- [x] Task A-5.1: 安装 golangci-lint 工具
  - 使用 Homebrew 或官方安装脚本安装 golangci-lint
  - 验证安装：`golangci-lint --version` ✓ (v2.11.4)

- [ ] Task A-5.2: 创建 `.golangci.yml` 配置文件
  - 配置常用 linter（errcheck, gosimple, govet, ineffassign, staticcheck, unused 等）
  - 排除项目特定的 false positive 规则

- [ ] Task A-5.3: 修复所有 lint 问题
  - 运行 `golangci-lint run ./...` 识别问题
  - 分类修复：格式问题、未使用变量、错误处理等

- [ ] Task A-5.4: 添加 lint 到 Makefile
  - 新增 `make lint` 目标
  - 新增 `make lint-fix` 目标（自动修复可修复的问题）

### A-3: schema/registry 包测试覆盖率（当前 0% → 目标 60%+）

- [x] Task A-3.1: 创建 `pkg/schema/registry_test.go` ✓
  - 测试 Registry 初始化和加载逻辑
  - 测试 RegisterSchema 和 GetSchema 方法

- [x] Task A-3.2: 测试四种兼容性策略 ✓
  - BACKWARD、FORWARD、FULL、NONE 策略验证
  - 测试兼容和不兼容的 schema 变更场景

- [x] Task A-3.3: 测试拒绝不兼容 schema ✓
  - 验证不兼容变更时返回错误
  - 验证兼容变更时正常通过

- [x] Task A-3.4: 测试 GetSchemaDiff() ✓
  - 验证字段增删改的差异检测
  - 验证类型变更检测

- [x] Task A-3.5: 测试版本管理方法 ✓
  - 测试版本递增逻辑
  - 测试历史版本回溯

**覆盖率**: 91.9% (目标 60%+ ✓)

### A-4: logger 包测试覆盖率（当前 0% → 目标 50%+）

- [x] Task A-4.1: 创建 `pkg/logger/logger_test.go` ✓
  - 测试 Logger.New() 创建逻辑（不同日志级别）
  - 测试日志输出格式

- [x] Task A-4.2: 测试 Logger.Close() ✓
  - 验证文件句柄正确释放
  - 验证无 panic 关闭

- [x] Task A-4.3: 测试 SanitizeFields / SanitizeStringValue ✓
  - 验证密码等敏感字段被替换为 `******`
  - 验证非敏感字段不受影响

**覆盖率**: 91.7% (目标 50%+ ✓)

### A-1: connector 包测试覆盖率（当前 9.7% → 目标 60%+）

- [ ] Task A-1.1: 测试 MySQLConnector.Connect() 错误路径
  - 模拟连接失败场景
  - 验证错误返回和日志记录

- [ ] Task A-1.2: 测试 convertValue() 方法
  - 测试各种 MySQL 类型到 Go 类型的转换
  - 测试边界值（NULL、空字符串、大数值）

- [ ] Task A-1.3: 测试 rowToMap()
  - 验证行数据正确转换为 map
  - 测试列名映射

- [ ] Task A-1.4: 测试 processEvent() 事件分发
  - 模拟不同 binlog 事件类型（INSERT/UPDATE/DELETE）
  - 验证事件正确分发到 change channel

- [ ] Task A-1.5: 测试 BinlogStreamer goroutine panic recovery
  - 模拟 goroutine 内部 panic
  - 验证 recovery 机制和错误传播

- [ ] Task A-1.6: 测试占位符连接器错误返回
  - 验证未实现方法的错误返回

### A-2: storage 包测试覆盖率（当前 16.3% → 目标 50%+）

- [ ] Task A-2.1: 测试 LocalStorage.Write() JSON + 文件轮转
  - 测试 JSON 格式写入
  - 测试文件大小达到阈值时自动轮转

- [ ] Task A-2.2: 测试 LocalStorage.Write() CSV + fileSize
  - 测试 CSV 格式写入
  - 验证文件大小计算准确

- [ ] Task A-2.3: 测试 LayeredStorage 各层写入
  - 测试 Bronze/Silver/Gold 三层写入逻辑
  - 验证数据在各层的转换

- [ ] Task A-2.4: 测试 Manager JSON 写入路径
  - 测试 Manager 对 JSON 格式的处理
  - 验证配置解析和路由

- [ ] Task A-2.5: 测试 QualityChecker.Check()
  - 测试数据质量检查规则
  - 测试异常数据检测和报告

## 批次 B — P1 重要（重构 + 代码清理）

### B-1: 拆分 layered_storage.go（1012行）

- [ ] Task B-1.1: 创建 `pkg/storage/bronze_storage.go`
  - 提取 BronzeRecord 结构体和相关方法
  - 提取 BronzeLayer 写入逻辑

- [ ] Task B-1.2: 创建 `pkg/storage/silver_storage.go`
  - 提取 SilverRecord 结构体和相关方法
  - 提取 SilverLayer 写入和转换逻辑

- [ ] Task B-1.3: 创建 `pkg/storage/gold_storage.go`
  - 提取 GoldRecord 结构体和相关方法
  - 提取 GoldLayer 写入和聚合逻辑

- [ ] Task B-1.4: 创建 `pkg/storage/layered_common.go`
  - 提取共享接口（Layer、Storage 等）
  - 提取共享类型定义和常量

- [ ] Task B-1.5: 缩减 `layered_storage.go` 为 orchestrator
  - 仅保留 LayeredStorage 结构体和编排逻辑
  - 目标：约 200 行

- [ ] Task B-1.6: 回归测试
  - 运行 `go test ./pkg/storage/...` 确保全部通过
  - 验证功能无损

### B-2: 消除 compareValues 代码重复 ✓

- [x] Task B-2.1: 搜索并确认 3 处重复定义 ✓
  - `pkg/pipeline/pipeline.go`
  - `pkg/storage/layered_storage.go`
  - `pkg/storage/parquet/util.go`

- [x] Task B-2.2: 创建 `pkg/util/util.go` ✓
  - 实现统一的 compareValues 函数
  - 编写单元测试

- [x] Task B-2.3: 修改所有引用点 ✓
  - 删除 3 处的本地定义
  - 改为引用 `pkg/util.CompareValues`

- [x] Task B-2.4: 回归测试 ✓
  - 运行 `go test ./...` 确保全部通过
  - 验证功能无损

## 批次 C — P2 改进（工具链 + 可观测性）

### C-1: 安装 shellcheck

- [x] Task C-1.1: 安装 shellcheck 工具 ✓
  - 使用 Homebrew 或官方方式安装
  - 验证：`shellcheck --version` (v0.11.0)

- [ ] Task C-1.2: 修复 `scripts/test_cdc.sh` 问题
  - 运行 `shellcheck scripts/*.sh` 识别问题
  - 修复所有警告和错误

### C-2: 添加 Prometheus Metrics

- [ ] Task C-2.1: 创建 `pkg/metrics/metrics.go`
  - 定义指标：data_ingestion_records_total、data_ingestion_latency_seconds、data_ingestion_binlog_lag_seconds、data_ingestion_errors_total
  - 实现指标记录方法

- [ ] Task C-2.2: `main.go` 添加 `/metrics` + `/health` 端点
  - 使用 `github.com/prometheus/client_golang/prometheus/promhttp`
  - `/health` 返回 JSON 健康状态

- [ ] Task C-2.3: Pipeline 记录处理计数/延迟
  - 在 processChange 中记录 records_total 和 latency_seconds

- [ ] Task C-2.4: Connector 记录 binlog lag
  - 在 binlog 事件处理中计算并记录 lag

- [ ] Task C-2.5: 更新 go.mod + 回归测试
  - `go get` 添加 prometheus 依赖
  - `go mod tidy`
  - 运行 `go test ./...` 和 `go build ./...`

## 整体验证

- [ ] Task V-1: 运行完整测试套件
  - `go test -count=1 -cover ./...` 整体覆盖率 >= 50%
  - `go test -race ./...` 全部通过
  - `go vet ./...` 无错误

- [ ] Task V-2: 验证工具链
  - `golangci-lint run ./...` 无错误
  - `shellcheck scripts/*.sh` 无错误

- [ ] Task V-3: 验证可观测性
  - `go build ./...` 编译成功
  - `/metrics` 和 `/health` HTTP 端点可用

# Task Dependencies

- A-5.3（修复 lint 问题）依赖 A-5.1/5.2（安装和配置）
- A-3/A-4 可并行执行（都是 0% 覆盖率的模块）
- A-1/A-2 可并行执行（但建议在 A-3/A-4 完成后参考其模式）
- B-1 和 B-2 可并行执行
- 所有 B 批次任务依赖对应 A 批次测试完成（避免在即将重构的代码上写测试）
- C-2 依赖 A-5（需要代码质量基准）
- V-1/V-2/V-3 依赖所有前置任务完成

# Parallelizable Work

以下任务可以并行执行：
- A-3 和 A-4（独立的测试模块）
- A-1 和 A-2（独立的测试模块）
- B-1 和 B-2（独立的重构任务）
- C-1 和 C-2 的部分工作（工具安装和指标设计）
