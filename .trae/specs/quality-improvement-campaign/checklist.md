# Checklist

## 批次 A — P0 紧急（测试覆盖 + 工具链）

### A-5: golangci-lint 工具链

- [ ] golangci-lint 已安装并可运行（`golangci-lint --version` 有输出）
- [ ] `.golangci.yml` 配置文件存在且配置合理
- [ ] `golangci-lint run ./...` 无错误
- [ ] Makefile 包含 `lint` 和 `lint-fix` 目标

### A-3: schema/registry 包测试（0% → 60%+）

- [ ] `pkg/schema/registry_test.go` 文件存在
- [ ] Registry 初始化和加载测试通过
- [ ] 四种兼容性策略（BACKWARD/FORWARD/FULL/NONE）测试通过
- [ ] 拒绝不兼容 schema 测试通过
- [ ] GetSchemaDiff() 测试通过
- [ ] 版本管理方法测试通过
- [ ] `go test ./pkg/schema/registry/...` 覆盖率 >= 60%

### A-4: logger 包测试（0% → 50%+）

- [ ] `pkg/logger/logger_test.go` 文件存在
- [ ] Logger.New() 创建测试通过（不同日志级别）
- [ ] Logger.Close() 句柄释放测试通过
- [ ] SanitizeFields / SanitizeStringValue 脱敏测试通过
- [ ] `go test ./pkg/logger/...` 覆盖率 >= 50%

### A-1: connector 包测试（9.7% → 60%+）

- [ ] MySQLConnector.Connect() 错误路径测试通过
- [ ] convertValue() 方法测试通过（含边界值）
- [ ] rowToMap() 测试通过
- [ ] processEvent() 事件分发测试通过
- [ ] BinlogStreamer goroutine panic recovery 测试通过
- [ ] 占位符连接器错误返回测试通过
- [ ] `go test ./pkg/connector/...` 覆盖率 >= 60%

### A-2: storage 包测试（16.3% → 50%+）

- [ ] LocalStorage.Write() JSON + 文件轮转测试通过
- [ ] LocalStorage.Write() CSV + fileSize 测试通过
- [ ] LayeredStorage 各层写入测试通过
- [ ] Manager JSON 写入路径测试通过
- [ ] QualityChecker.Check() 测试通过
- [ ] `go test ./pkg/storage/...` 覆盖率 >= 50%

## 批次 B — P1 重要（重构 + 代码清理）

### B-1: 拆分 layered_storage.go

- [ ] `pkg/storage/bronze_storage.go` 存在且包含 Bronze 层逻辑
- [ ] `pkg/storage/silver_storage.go` 存在且包含 Silver 层逻辑
- [ ] `pkg/storage/gold_storage.go` 存在且包含 Gold 层逻辑
- [ ] `pkg/storage/layered_common.go` 存在且包含共享接口/类型
- [ ] `pkg/storage/layered_storage.go` 缩减为仅保留编排逻辑（约 200 行）
- [ ] `go test ./pkg/storage/...` 全部通过

### B-2: 消除 compareValues 代码重复

- [ ] `pkg/util/util.go` 存在且包含统一 compareValues 实现
- [ ] `pkg/util/util_test.go` 存在且测试通过
- [ ] `pkg/pipeline/pipeline.go` 中本地 compareValues 已删除，引用 pkg/util
- [ ] `pkg/storage/layered_storage.go` 中本地 compareValues 已删除，引用 pkg/util
- [ ] `pkg/storage/parquet/util.go` 中本地 compareValues 已删除，引用 pkg/util
- [ ] 全局仅剩 1 处 compareValues 定义
- [ ] `go test ./...` 全部通过

## 批次 C — P2 改进（工具链 + 可观测性）

### C-1: shellcheck

- [ ] shellcheck 已安装并可运行（`shellcheck --version` 有输出）
- [ ] `shellcheck scripts/*.sh` 无错误

### C-2: Prometheus Metrics

- [ ] `pkg/metrics/metrics.go` 存在且定义了 4 个核心指标
- [ ] `go build ./...` 编译成功（含 prometheus 依赖）
- [ ] `/metrics` HTTP 端点返回 Prometheus 格式数据
- [ ] `/health` HTTP 端点返回 JSON 健康状态
- [ ] Pipeline 中记录了 records_total 和 latency_seconds
- [ ] Connector 中记录了 binlog_lag_seconds

## 整体验收

### 功能验收

- [ ] `go test -count=1 -cover ./...` 整体覆盖率 >= 50%
- [ ] `golangci-lint run ./...` 无错误
- [ ] `shellcheck scripts/*.sh` 无错误
- [ ] `go test -race ./...` 全部通过
- [ ] `go vet ./...` 无错误
- [ ] `go build ./...` 编译成功
- [ ] `/metrics` 和 `/health` HTTP 端点可用

### 质量验收

- [ ] 无新增竞态条件（`-race` 检测通过）
- [ ] 无新增内存泄漏
- [ ] 代码复杂度合理（圈复杂度 < 15）
- [ ] 注释覆盖率 > 30%
