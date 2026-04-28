# 高优先级代码质量改进 Spec

## Why
当前项目存在三个关键质量问题：核心模块缺少单元测试导致回归风险高；错误处理机制不完善导致数据丢失风险；敏感信息明文存储存在安全隐患。

## What Changes
- 为 `pipeline`、`storage`、`connector`、`checkpoint` 模块添加单元测试
- 实现错误重试机制和死信队列处理
- 支持敏感信息（密码、密钥）的环境变量读取和加密存储

## Impact
- Affected specs: 错误处理、安全性、测试覆盖
- Affected code: 
  - `pkg/pipeline/pipeline.go`
  - `pkg/storage/manager.go`
  - `pkg/config/config.go`
  - 新增测试文件

## ADDED Requirements

### Requirement: 单元测试覆盖
系统 SHALL 为核心模块提供单元测试覆盖，确保关键业务逻辑的正确性。

#### Scenario: Pipeline 模块测试
- **WHEN** 执行 `go test ./pkg/pipeline/...`
- **THEN** 所有测试通过，覆盖率 >= 70%

#### Scenario: Storage 模块测试
- **WHEN** 执行 `go test ./pkg/storage/...`
- **THEN** 所有测试通过，覆盖率 >= 70%

#### Scenario: Connector 模块测试
- **WHEN** 执行 `go test ./pkg/connector/...`
- **THEN** 所有测试通过，覆盖率 >= 70%

#### Scenario: Checkpoint 模块测试
- **WHEN** 执行 `go test ./pkg/checkpoint/...`
- **THEN** 所有测试通过，覆盖率 >= 70%

### Requirement: 错误重试机制
系统 SHALL 提供可配置的错误重试机制，支持自动重试可恢复错误。

#### Scenario: 可恢复错误重试
- **GIVEN** 配置了最大重试次数为 3
- **WHEN** 处理数据变更时发生可恢复错误
- **THEN** 系统自动重试最多 3 次
- **AND** 重试间隔采用指数退避策略

#### Scenario: 重试失败处理
- **GIVEN** 重试次数已用尽
- **WHEN** 错误仍然存在
- **THEN** 将失败记录写入死信队列
- **AND** 记录详细错误日志

### Requirement: 死信队列
系统 SHALL 提供死信队列机制，存储处理失败的数据变更记录。

#### Scenario: 失败记录存储
- **WHEN** 数据变更处理失败且重试用尽
- **THEN** 将原始数据变更记录保存到死信队列文件
- **AND** 包含失败原因和时间戳

#### Scenario: 死信队列恢复
- **WHEN** 用户请求重放死信队列记录
- **THEN** 系统重新处理失败的记录

### Requirement: 敏感信息环境变量支持
系统 SHALL 支持从环境变量读取敏感配置信息。

#### Scenario: 密码环境变量读取
- **GIVEN** 配置文件中密码字段为空或设置为 `${ENV_VAR}` 格式
- **WHEN** 系统加载配置
- **THEN** 从环境变量 `MYSQL_PASSWORD` 读取密码值

#### Scenario: 环境变量优先级
- **GIVEN** 配置文件和环境变量都设置了密码
- **WHEN** 系统加载配置
- **THEN** 环境变量值优先于配置文件值

### Requirement: 敏感信息日志脱敏
系统 SHALL 在日志输出中对敏感信息进行脱敏处理。

#### Scenario: 密码脱敏
- **WHEN** 记录包含密码字段的配置信息
- **THEN** 密码显示为 `******`

## MODIFIED Requirements

### Requirement: 配置验证
系统 SHALL 验证敏感信息是否通过安全方式配置。

#### Scenario: 密码未配置警告
- **GIVEN** 配置文件中密码为明文
- **WHEN** 系统启动
- **THEN** 输出警告日志建议使用环境变量

## Implementation Notes

1. **测试框架**: 使用 Go 标准测试框架 `testing` 和 `testify` 断言库
2. **Mock 策略**: 使用接口 mock 进行依赖隔离
3. **重试策略**: 采用指数退避，初始间隔 100ms，最大间隔 10s
4. **死信队列格式**: JSON Lines 格式，便于分析和重放
5. **环境变量命名**: `{SOURCE_TYPE}_{FIELD_NAME}` 格式，如 `MYSQL_PASSWORD`
