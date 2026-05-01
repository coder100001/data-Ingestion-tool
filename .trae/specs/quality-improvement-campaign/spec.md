# 代码质量提升战役 Spec

## Why
当前项目整体测试覆盖率仅约25%，核心模块（connector 9.7%、schema/registry 0%、logger 0%）严重缺乏测试保护；代码存在重复定义（compareValues 3处重复）、超大文件（layered_storage.go 1012行）等维护性问题；缺少静态分析工具（golangci-lint、shellcheck）和可观测性指标（Prometheus），导致代码质量和运行时状态难以保障。

## What Changes
- **批次A (P0)**：大幅提升核心模块测试覆盖率（connector→60%+、storage→50%+、schema/registry→60%+、logger→50%+），安装并配置 golangci-lint
- **批次B (P1)**：拆分 layered_storage.go（1012行）为5个文件，消除 compareValues 3处重复定义
- **批次C (P2)**：安装 shellcheck 修复脚本问题，添加 Prometheus 监控指标和 /metrics、/health HTTP 端点

## Impact
- Affected specs: 测试覆盖、代码重构、工具链、可观测性
- Affected code:
  - `pkg/connector/` — 新增/完善测试
  - `pkg/storage/` — 新增测试 + 拆分 layered_storage.go
  - `pkg/schema/registry/` — 新增测试
  - `pkg/logger/` — 新增测试
  - `pkg/pipeline/` — 消除 compareValues 重复
  - `pkg/metrics/` — 新增监控模块
  - `cmd/ingester/main.go` — 添加 HTTP 端点
  - `Makefile` — 添加 lint 目标
  - 新增 `.golangci.yml` 配置

## ADDED Requirements

### Requirement: 测试覆盖率提升
系统 SHALL 为核心模块提供充分的单元测试覆盖，确保关键业务逻辑的正确性。

#### Scenario: Connector 模块测试
- **WHEN** 执行 `go test ./pkg/connector/...`
- **THEN** 所有测试通过，覆盖率 >= 60%

#### Scenario: Storage 模块测试
- **WHEN** 执行 `go test ./pkg/storage/...`
- **THEN** 所有测试通过，覆盖率 >= 50%

#### Scenario: Schema Registry 模块测试
- **WHEN** 执行 `go test ./pkg/schema/registry/...`
- **THEN** 所有测试通过，覆盖率 >= 60%

#### Scenario: Logger 模块测试
- **WHEN** 执行 `go test ./pkg/logger/...`
- **THEN** 所有测试通过，覆盖率 >= 50%

### Requirement: 代码重构 — 拆分超大文件
系统 SHALL 将 layered_storage.go（1012行）拆分为职责单一的多个文件。

#### Scenario: 文件拆分验证
- **WHEN** 检查 `pkg/storage/` 目录
- **THEN** 存在 bronze_storage.go、silver_storage.go、gold_storage.go、layered_common.go
- **AND** layered_storage.go 仅保留编排逻辑（约200行）
- **AND** `go test ./pkg/storage/...` 全部通过

### Requirement: 消除代码重复
系统 SHALL 将 compareValues 函数的统一实现提取到公共包。

#### Scenario: 重复消除验证
- **WHEN** 搜索 compareValues 定义
- **THEN** 仅存在1处定义（pkg/util/util.go）
- **AND** 所有引用点使用统一实现
- **AND** `go test ./...` 全部通过

### Requirement: 静态分析工具链
系统 SHALL 集成 golangci-lint 和 shellcheck 到开发工作流。

#### Scenario: Lint 执行
- **WHEN** 执行 `make lint` 或 `golangci-lint run ./...`
- **THEN** 无错误输出

#### Scenario: Shell 脚本检查
- **WHEN** 执行 `shellcheck scripts/*.sh`
- **THEN** 无错误输出

### Requirement: 可观测性指标
系统 SHALL 暴露 Prometheus 格式的监控指标和 health 检查端点。

#### Scenario: Metrics 端点
- **WHEN** 访问 `GET /metrics`
- **THEN** 返回 Prometheus 格式的指标数据
- **AND** 包含 data_ingestion_records_total、data_ingestion_latency_seconds 等关键指标

#### Scenario: Health 端点
- **WHEN** 访问 `GET /health`
- **THEN** 返回 JSON 格式的健康状态

## MODIFIED Requirements

### Requirement: Makefile 构建流程
系统 SHALL 在 Makefile 中添加 lint 和 coverage 目标。

#### Scenario: 构建目标可用
- **WHEN** 执行 `make help`
- **THEN** 显示 lint、test、coverage 等目标

## REMOVED Requirements
无移除需求。
