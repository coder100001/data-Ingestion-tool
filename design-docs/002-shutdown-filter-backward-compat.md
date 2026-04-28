# Design Doc 002: Shutdown Recovery & Filter Backward Compatibility

## 元数据
- **编号**: 002
- **标题**: Shutdown Recovery & Filter Backward Compatibility
- **状态**: approved
- **创建日期**: 2026-04-28
- **最后更新**: 2026-04-28
- **关联任务**: 修复 Issue1 (shutdown 超时恢复) + Issue2 (filter/transform 向后兼容)
- **复杂度级别**: L2
- **前置 Design Doc**: 001-code-review-fixes.md

## 1. 背景与动机

在 Design Doc 001 的代码审查修复中，引入了以下两个问题：

1. **Issue 1 — shutdown 超时处理缺失错误恢复**: shutdown 函数添加了 30 秒超时，但超时后直接 `os.Exit(1)` 强制退出，没有尝试保存当前状态或记录关键信息。这可能导致数据丢失。

2. **Issue 2 — 过滤器和转换器解析破坏向后兼容性**: `parseFilterRule` 和 `parseTransformRule` 从返回 NoOp 实现改为直接返回错误。这种变更会破坏现有配置的兼容性，用户配置中如果有 filter/transform 规则，启动会直接失败。

## 2. 调研与现状分析

### 2.1 现有实现

**Issue 1 现状**:
```go
select {
case <-done:
    log.Info("Shutdown complete")
case <-time.After(30 * time.Second):
    log.Error("Shutdown timed out after 30 seconds, forcing exit")
    os.Exit(1)
}
```
超时后直接退出，没有保存 checkpoint。

**Issue 2 现状**:
```go
func parseFilterRule(rule string) (Filter, error) {
    return nil, fmt.Errorf("filter rule parsing not yet implemented: remove '%s' from config filters", rule)
}
```
直接返回错误，导致 `initFilters` 返回错误，Pipeline 启动失败。

### 2.2 业界实践

- **Graceful Shutdown**: Kubernetes、Docker 等容器平台的 graceful shutdown 实践是在超时前尽最大努力保存状态。例如：
  - 先尝试优雅关闭（30秒）
  - 超时后尝试紧急保存（5秒）
  - 最后强制退出

- **向后兼容性**: 开源项目在移除功能时通常采用 deprecation 模式：
  - 先标记为 deprecated，输出警告日志
  - 保留功能但记录弃用时间线
  - 在下一个 major 版本中移除

### 2.3 技术约束

- checkpoint.Manager 已有 `Save()` 方法，但受 `dirty` 标志限制
- NoOpFilter/NoOpTransformer 实现仍然存在，只是未被使用
- 用户可能在配置文件中配置了 filter/transform 规则

## 3. 可选方案

### Issue 1 — shutdown 超时恢复

#### 方案 A: 超时前尝试 ForceSave
- **描述**: 在超时后调用 `manager.ForceSave()` 强制保存 checkpoint，然后退出
- **优点**: 简单直接，能保存当前进度
- **缺点**: ForceSave 可能也需要时间，如果 ForceSave 也阻塞，问题依旧
- **工作量**: 小

#### 方案 B: 分层超时 — 优雅关闭 + 紧急保存 + 强制退出 (选定)
- **描述**: 
  1. 第一阶段 (30秒): 优雅关闭 (connector → pipeline → checkpoint)
  2. 第二阶段 (5秒): 如果第一阶段超时，尝试 ForceSave
  3. 第三阶段: 如果 ForceSave 也超时，记录日志后强制退出
- **优点**: 最大化数据保存机会，层次分明
- **缺点**: 逻辑稍复杂
- **工作量**: 中

#### 方案 C: 信号驱动 + 状态机
- **描述**: 使用更复杂的信号处理和状态机管理关闭流程
- **优点**: 最健壮
- **缺点**: 过度设计，当前项目不需要
- **工作量**: 大

### Issue 2 — filter/transform 向后兼容

#### 方案 A: 恢复 NoOp 行为 + 警告日志 (选定)
- **描述**: `initFilters`/`initTransformers` 中捕获错误，使用 NoOp 实现替代，同时输出警告日志
- **优点**: 完全向后兼容，用户无需修改配置
- **缺点**: 静默忽略用户配置，可能让用户误以为 filter 生效
- **工作量**: 小

#### 方案 B: 配置验证阶段报错
- **描述**: 在配置加载阶段就检查 filter/transform 配置，给出明确的迁移指南
- **优点**: 用户明确知道问题
- **缺点**: 仍然破坏兼容性，启动失败
- **工作量**: 小

#### 方案 C: 功能开关
- **描述**: 添加配置项 `strict_mode`，strict 模式下报错，非 strict 模式下警告
- **优点**: 灵活
- **缺点**: 增加配置复杂度
- **工作量**: 中

## 4. 决策

### Issue 1
- **选定方案**: B (分层超时)
- **决策理由**: 平衡了简单性和健壮性。30秒优雅关闭 + 紧急 ForceSave 是业界标准做法
- **权衡取舍**: 不采用更复杂的信号驱动方案，当前项目不需要

### Issue 2
- **选定方案**: A (恢复 NoOp + 警告日志)
- **决策理由**: 向后兼容优先。用户配置不应导致启动失败。警告日志明确告知用户 filter 未生效
- **权衡取舍**: 不采用功能开关方案，避免配置过度复杂化

## 5. 影响范围

- `cmd/ingester/main.go` — shutdown 超时逻辑
- `pkg/checkpoint/checkpoint.go` — 新增 ForceSave 方法
- `pkg/pipeline/pipeline.go` — initFilters/initTransformers 错误处理
- `pkg/pipeline/pipeline_test.go` — 更新测试用例

## 6. 开放问题
- [ ] 是否需要将 ForceSave 的超时也纳入考虑？→ 当前 ForceSave 是同步操作，通常很快完成
- [ ] 警告日志是否足够引起用户注意？→ 考虑在文档中明确说明

---
**文档状态**: approved
