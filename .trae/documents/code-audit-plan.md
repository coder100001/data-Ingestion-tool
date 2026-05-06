# 代码审计计划 (Code Audit Plan)

## 1. 需求概述

- **任务名称**: Data-Ingestion-Tool 代码审计
- **目标**: 识别关键漏洞、架构缺陷与并发风险，输出审计报告
- **优先级**: P0
- **复杂度级别**: L3 (复杂任务)
- **Design Doc**: 基于现有代码库与 EXECUTION-PLAN.md 进行审计

## 2. 变更范围

### 2.1 审计范围
| 包/模块 | 文件 | 关注点 |
|---------|------|--------|
| `pkg/connector` | `mysql.go`, `connector.go` | Panic恢复、并发竞态、Binlog处理 |
| `pkg/pipeline` | `pipeline.go` | 并发安全、死锁风险 |
| `pkg/storage` | `layered_storage.go`, `manager.go`, `catalog.go` | 大文件耦合、竞态条件 |
| `pkg/storage/schema` | `registry.go` | Schema漂移、兼容性检查 |
| `pkg/util` | `util.go` | 重复代码、精度丢失 |
| `pkg/logger` | `logger.go`, `sanitize.go` | 敏感信息泄露 |
| `cmd/ingester` | `main.go` | 优雅关闭、资源泄漏 |

### 2.2 不修改文件（只读审计）
- 本次审计以分析为主，不修改源代码
- 产出审计报告与风险清单

## 3. 逻辑设计

### 3.1 审计流程
```
1. 架构概览分析
2. 逐模块深度审计
3. 并发安全扫描
4. 数据流追踪
5. 风险定级与报告
```

### 3.2 审计检查清单

#### 3.2.1 逻辑健壮性与并发安全 (Critical)
- [ ] `BinlogStreamer` panic恢复机制是否完善
- [ ] `pipeline` 多worker并发访问 `SchemaRegistry` / `StorageManager`
- [ ] `tableCache` 读写锁使用是否正确
- [ ] `changeChan` 关闭与发送的竞态条件
- [ ] `checkpoint` autoSave goroutine生命周期

#### 3.2.2 架构风险：大文件与耦合 (High)
- [ ] `layered_storage.go` (922行) 职责划分
- [ ] Bronze/Silver/Gold三层耦合度
- [ ] 文件句柄释放（Parquet Writer等）

#### 3.2.3 数据一致性与转换风险 (Medium)
- [ ] `compareValues` 重复定义检查
- [ ] MySQL Decimal/Unsigned BigInt 转换精度
- [ ] Schema Registry 兼容性策略实现完整性

#### 3.2.4 安全性风险 (Medium)
- [ ] 日志脱敏功能完整性
- [ ] 配置文件中密码硬编码检查
- [ ] DeadLetterQueue 中敏感数据存储

#### 3.2.5 可观测性 (High)
- [ ] Prometheus Metrics 缺失
- [ ] Health Check 端点缺失
- [ ] 关键路径日志不足

## 4. 接口设计

### 4.1 审计输出
- 审计报告 Markdown 文件
- 风险等级分布表
- 具体代码位置引用

## 5. 依赖分析

### 5.1 外部依赖风险
| 依赖 | 版本 | 风险 |
|------|------|------|
| `github.com/go-mysql-org/go-mysql` | v1.7.0 | 中 |
| `github.com/sirupsen/logrus` | v1.9.3 | 低 |
| `github.com/pingcap/tidb/parser` | v0.0.0-20221126021158-6b02a5d8ba7d | 中（间接依赖） |

## 6. 风险评估

| 风险类型 | 概率 | 影响 | 缓解措施 |
|---------|------|------|---------|
| 并发竞态导致数据损坏 | 高 | 极高 | 增加 `-race` 检测 |
| Binlog异常导致进程崩溃 | 中 | 极高 | 完善panic恢复 |
| Schema漂移未检测 | 中 | 高 | 补充Registry测试 |
| 敏感信息泄露 | 中 | 中 | 完善脱敏规则 |

## 7. 验收标准
- [ ] 覆盖所有核心模块
- [ ] 识别至少5个高危问题
- [ ] 提供可执行的修复建议
- [ ] 输出风险等级分布图

## 8. 回滚策略
- 本次为只读审计，无回滚需求

---
**计划状态**: 待用户确认
**创建时间**: 2026-05-06
