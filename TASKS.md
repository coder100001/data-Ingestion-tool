# 任务列表

> 基于 plan.md （21个 code review issues）已在 commit `3a9e81b` 中全部完成。
> 以下为当前质量分析中发现的新任务分解。

## 文档索引

| 文档 | 描述 | 状态 |
|------|------|------|
| [EXECUTION-PLAN.md](./EXECUTION-PLAN.md) | 详细执行计划，包含时间估算和风险分析 | 已创建 |
| [ARCHITECTURE.md](./ARCHITECTURE.md) | 系统架构设计文档 | 已完成 |
| [plan.md](./plan.md) | 代码审查问题修复计划 | 已完成 |

## 执行顺序建议

1. **A-5** → 安装golangci-lint，建立代码质量基准
2. **A-3, A-4** → 新增schema和logger测试（当前0%覆盖率）
3. **A-1, A-2** → 提升现有包测试覆盖率
4. **B-1, B-2** → 代码重构（避免测试写在即将重构的代码上）
5. **C-1, C-2** → 工具链和可观测性

## 时间估算

| 批次 | 任务数 | 预计总时间 | 优先级 |
|------|--------|------------|--------|
| A | 22 | 35小时 | P0 |
| B | 10 | 17小时 | P1 |
| C | 7 | 10小时 | P2 |
| **总计** | **39** | **62小时** | - |

---

## 批次 A — P0 紧急（测试覆盖 + 工具链）

### A-1: connector 包测试覆盖率 (当前 9.7% → 目标 60%+)

| 任务ID | 任务描述 | 优先级 | 预计时间 | 状态 |
|--------|----------|--------|----------|------|
| A-1.1 | MySQLConnector.Connect() 错误路径测试 | 高 | 2小时 | 待开始 |
| A-1.2 | convertValue() 方法测试 | 中 | 1小时 | 待开始 |
| A-1.3 | rowToMap() 测试 | 中 | 1.5小时 | 待开始 |
| A-1.4 | processEvent() 事件分发测试 | 高 | 3小时 | 待开始 |
| A-1.5 | BinlogStreamer goroutine panic recovery 测试 | 高 | 2小时 | 待开始 |
| A-1.6 | 占位符连接器错误返回测试 | 低 | 1小时 | 待开始 |

**技术要点**: 使用mock模拟MySQL连接，测试binlog事件处理逻辑，验证goroutine错误恢复机制

### A-2: storage 包测试覆盖率 (当前 16.3% → 目标 50%+)

| 任务ID | 任务描述 | 优先级 | 预计时间 | 状态 |
|--------|----------|--------|----------|------|
| A-2.1 | LocalStorage.Write() JSON + 文件轮转测试 | 高 | 2小时 | 待开始 |
| A-2.2 | LocalStorage.Write() CSV + fileSize 测试 | 中 | 1.5小时 | 待开始 |
| A-2.3 | LayeredStorage 各层写入测试 | 高 | 3小时 | 待开始 |
| A-2.4 | Manager JSON 写入路径测试 | 中 | 2小时 | 待开始 |
| A-2.5 | QualityChecker.Check() 测试 | 中 | 2小时 | 待开始 |

**技术要点**: 使用临时目录进行文件系统测试，测试文件轮转逻辑，验证分层存储（Bronze/Silver/Gold）

### A-3: schema/registry 包测试覆盖率 (当前 0% → 目标 60%+)

| 任务ID | 任务描述 | 优先级 | 预计时间 | 状态 |
|--------|----------|--------|----------|------|
| A-3.1 | Registry 初始化 + 加载测试 | 高 | 2小时 | 待开始 |
| A-3.2 | 四种兼容性策略测试 | 高 | 3小时 | 待开始 |
| A-3.3 | 拒绝不兼容 schema 测试 | 高 | 2小时 | 待开始 |
| A-3.4 | GetSchemaDiff() 测试 | 中 | 2小时 | 待开始 |
| A-3.5 | 版本管理方法测试 | 中 | 2小时 | 待开始 |

**技术要点**: 创建新的测试文件 `pkg/schema/registry_test.go`，测试schema兼容性检查逻辑，验证版本管理功能

### A-4: logger 包测试覆盖率 (当前 0% → 目标 50%+)

| 任务ID | 任务描述 | 优先级 | 预计时间 | 状态 |
|--------|----------|--------|----------|------|
| A-4.1 | Logger.New() 创建测试 | 高 | 1小时 | 待开始 |
| A-4.2 | Logger.Close() 句柄释放测试 | 中 | 1小时 | 待开始 |
| A-4.3 | SanitizeFields/SanitizeStringValue 测试 | 中 | 1.5小时 | 待开始 |

**技术要点**: 创建新的测试文件 `pkg/logger/logger_test.go`，测试日志级别配置，验证敏感信息脱敏功能

### A-5: 安装 golangci-lint

| 任务ID | 任务描述 | 优先级 | 预计时间 | 状态 |
|--------|----------|--------|----------|------|
| A-5.1 | 安装 golangci-lint 工具 | 高 | 0.5小时 | 待开始 |
| A-5.2 | 修复所有 lint 问题 | 高 | 4小时 | 待开始 |
| A-5.3 | 添加 lint 到 Makefile | 中 | 0.5小时 | 待开始 |

**技术要点**: 使用Homebrew或官方安装脚本，配置.golangci.yml，集成到CI/CD流程

---

## 批次 B — P1 重要（重构 + 代码清理）

### B-1: 拆分 layered_storage.go (1012行)

**当前状态**: 1012行 → **目标**: 拆分为5个文件

| 任务ID | 任务描述 | 优先级 | 预计时间 | 状态 |
|--------|----------|--------|----------|------|
| B-1.1 | 创建 bronze_storage.go | 高 | 2小时 | 待开始 |
| B-1.2 | 创建 silver_storage.go | 高 | 2小时 | 待开始 |
| B-1.3 | 创建 gold_storage.go | 高 | 2小时 | 待开始 |
| B-1.4 | 创建 layered_common.go | 中 | 1.5小时 | 待开始 |
| B-1.5 | 缩减 layered_storage.go 为 orchestrator | 高 | 2小时 | 待开始 |
| B-1.6 | 回归测试 | 高 | 2小时 | 待开始 |

**拆分策略**:
```
layered_storage.go (1012行)
├── bronze_storage.go      # BronzeRecord, BronzeLayer逻辑
├── silver_storage.go      # SilverRecord, SilverLayer逻辑  
├── gold_storage.go        # GoldRecord, GoldLayer逻辑
├── layered_common.go      # 共享接口、类型定义
└── layered_storage.go     # 仅保留编排逻辑 (约200行)
```

### B-2: 消除 compareValues 代码重复

**当前状态**: 3处重复定义 → **目标**: 1处统一实现

| 任务ID | 任务描述 | 优先级 | 预计时间 | 状态 |
|--------|----------|--------|----------|------|
| B-2.1 | 搜索重复定义 | 高 | 0.5小时 | 待开始 |
| B-2.2 | 创建 pkg/util/util.go | 高 | 1小时 | 待开始 |
| B-2.3 | 修改所有引用点 | 高 | 1小时 | 待开始 |
| B-2.4 | 回归测试 | 高 | 1小时 | 待开始 |

**重复位置**:
1. `pkg/pipeline/pipeline.go`
2. `pkg/storage/layered_storage.go`
3. `pkg/storage/parquet/util.go`

---

## 批次 C — P2 改进（工具链 + 可观测性）

### C-1: 安装 shellcheck

| 任务ID | 任务描述 | 优先级 | 预计时间 | 状态 |
|--------|----------|--------|----------|------|
| C-1.1 | 安装 shellcheck 工具 | 中 | 0.5小时 | 待开始 |
| C-1.2 | 修复 test_cdc.sh 问题 | 中 | 1小时 | 待开始 |

### C-2: 添加 Prometheus Metrics

| 任务ID | 任务描述 | 优先级 | 预计时间 | 状态 |
|--------|----------|--------|----------|------|
| C-2.1 | 创建 pkg/metrics/metrics.go | 高 | 2小时 | 待开始 |
| C-2.2 | main.go 添加 /metrics + /health 端点 | 高 | 1.5小时 | 待开始 |
| C-2.3 | pipeline 记录处理计数/延迟 | 中 | 2小时 | 待开始 |
| C-2.4 | connector 记录 binlog lag | 中 | 1.5小时 | 待开始 |
| C-2.5 | 更新 go.mod + 回归测试 | 中 | 1小时 | 待开始 |

**监控指标**:
- `data_ingestion_records_total` - 处理记录总数
- `data_ingestion_latency_seconds` - 处理延迟
- `data_ingestion_binlog_lag_seconds` - binlog延迟
- `data_ingestion_errors_total` - 错误计数

---

## 验收标准

### 功能验收
- [ ] `go test -count=1 -cover ./...` 整体覆盖率 >= 50%
- [ ] `golangci-lint run ./...` 无错误
- [ ] `shellcheck scripts/*.sh` 无错误
- [ ] `go test -race ./...` 全部通过
- [ ] `go vet ./...` 无错误
- [ ] `go build ./...` 编译成功
- [ ] `/metrics` 和 `/health` HTTP 端点可用

### 质量验收
- [ ] 无新增竞态条件
- [ ] 无新增内存泄漏
- [ ] 代码复杂度合理（圈复杂度<15）
- [ ] 注释覆盖率>30%

---

## 风险与缓解

| 风险 | 概率 | 影响 | 缓解措施 |
|------|------|------|----------|
| 测试编写耗时超出预期 | 中 | 高 | 优先核心路径，分批进行 |
| 重构引入新bug | 中 | 高 | 充分的回归测试 |
| 依赖兼容性问题 | 低 | 中 | 使用go mod tidy验证 |
| 工具链安装失败 | 低 | 低 | 提供手动安装备选方案 |

---

## 状态追踪

**最后更新**: 2026-05-01
**当前阶段**: 待开始
**总体进度**: 0/39 任务完成

| 批次 | 完成任务 | 总任务 | 进度 | 状态 |
|------|----------|--------|------|------|
| A | 0 | 22 | 0% | 待开始 |
| B | 0 | 10 | 0% | 待开始 |
| C | 0 | 7 | 0% | 待开始 |
| **总计** | **0** | **39** | **0%** | **待开始** |
