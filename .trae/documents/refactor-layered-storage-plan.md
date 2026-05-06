# 拆分 layered_storage.go - 实现计划

## 1. 需求概述
- **功能名称**: 拆分 layered_storage.go 以提高可维护性
- **目标**: 将 922 行的大文件拆分为 5 个职责清晰的文件
- **优先级**: P1
- **复杂度级别**: L2 (中等任务)
- **Design Doc**: [003-refactor-layered-storage](../design-docs/003-refactor-layered-storage.md)

## 2. 变更范围

### 2.1 新增文件
| 文件路径 | 用途 | 行数预估 |
|---------|------|---------|
| `pkg/storage/layered_common.go` | 共享类型、接口、规则定义 | ~150 |
| `pkg/storage/bronze_storage.go` | Bronze 层实现 | ~150 |
| `pkg/storage/silver_storage.go` | Silver 层实现 | ~200 |
| `pkg/storage/gold_storage.go` | Gold 层实现 | ~200 |
| `pkg/storage/bronze_storage_test.go` | Bronze 层测试 | ~100 |
| `pkg/storage/silver_storage_test.go` | Silver 层测试 | ~100 |
| `pkg/storage/gold_storage_test.go` | Gold 层测试 | ~100 |

### 2.2 修改文件
| 文件路径 | 变更类型 | 影响范围 |
|---------|---------|---------|
| `pkg/storage/layered_storage.go` | 重构 | 从 922 行减少到 ~220 行 |
| `pkg/pipeline/pipeline.go` | 导入调整 | 无功能变更 |
| `design-docs/README.md` | 文档更新 | 添加 003 索引 |

### 2.3 删除文件
| 文件路径 | 原因 |
|---------|------|
| 无 | - |

## 3. 逻辑设计

### 3.1 拆分策略

```
原文件: layered_storage.go (922 行)
├── 类型定义 (L16-93) → layered_common.go
├── 接口定义 (L95-123) → layered_common.go
├── 规则定义 (L110-123) → layered_common.go
├── 构造函数 (L125-161) → layered_storage.go (保留)
├── Bronze层方法 (L192-262) → bronze_storage.go
├── Silver层方法 (L264-421) → silver_storage.go
├── Gold层方法 (L423-560) → gold_storage.go
└── 辅助方法 (L561-922) → 各自对应的文件
```

### 3.2 文件职责

#### layered_common.go
```go
// 共享类型
type LayerType string
type LayerConfig struct { ... }
type BinlogInfo struct { ... }
type ValidationResult struct { ... }

// 共享接口
type DataCleaner interface { ... }
type DataValidator interface { ... }
type DataAggregator interface { ... }

// 共享规则
type CleaningRule struct { ... }
type ValidationRule struct { ... }
```

#### bronze_storage.go
```go
// Bronze 层数据结构
type BronzeRecord struct { ... }

// Bronze 层方法
func (s *LayeredStorage) WriteToBronze(change *models.DataChange) error { ... }
func (s *LayeredStorage) writeBronzeJSON(record BronzeRecord, path string) error { ... }
func (s *LayeredStorage) writeBronzeParquet(record BronzeRecord, path string) error { ... }
```

#### silver_storage.go
```go
// Silver 层数据结构
type SilverRecord struct { ... }

// Silver 层方法
func (s *LayeredStorage) ProcessBronzeToSilver(database, table string, date string) error { ... }
func (s *LayeredStorage) writeSilverRecord(...) error { ... }
func (s *LayeredStorage) writeSilverJSON(...) error { ... }
func (s *LayeredStorage) writeSilverParquet(...) error { ... }
```

#### gold_storage.go
```go
// Gold 层数据结构
type GoldRecord struct { ... }

// Gold 层方法
func (s *LayeredStorage) ProcessSilverToGold(database, table, grain string, date string) error { ... }
func (s *LayeredStorage) writeGoldRecord(...) error { ... }
func (s *LayeredStorage) writeGoldJSON(...) error { ... }
func (s *LayeredStorage) writeGoldParquet(...) error { ... }
```

#### layered_storage.go (重构后)
```go
// 主结构体
type LayeredStorage struct { ... }

// 构造函数
func NewLayeredStorage(...) *LayeredStorage { ... }

// 配置方法
func (s *LayeredStorage) SetCleaner(cleaner DataCleaner) { ... }
func (s *LayeredStorage) SetValidator(validator DataValidator) { ... }
func (s *LayeredStorage) SetAggregator(aggregator DataAggregator) { ... }

// 初始化
func (s *LayeredStorage) Initialize() error { ... }
```

### 3.3 边界条件
| 场景 | 输入 | 预期输出 | 处理方式 |
|-----|------|---------|---------|
| 编译错误 | 拆分后导入错误 | 编译失败 | 逐步拆分，每步验证 |
| 测试失败 | 拆分后测试失败 | 测试不通过 | 保持向后兼容，测试先行 |
| 性能下降 | 拆分后性能下降 | 性能基准不满足 | 避免不必要的函数调用 |

## 4. 接口设计

### 4.1 公共 API
无变化，保持向后兼容：
```go
// 公共方法保持不变
func NewLayeredStorage(basePath string, catalog *DataCatalog, logger *logger.Logger) *LayeredStorage
func (s *LayeredStorage) WriteToBronze(change *models.DataChange) error
func (s *LayeredStorage) ProcessBronzeToSilver(database, table string, date string) error
func (s *LayeredStorage) ProcessSilverToGold(database, table, grain string, date string) error
```

### 4.2 内部重组
- 所有方法仍属于 `LayeredStorage` 类型
- 只是文件位置变化，不影响调用方式

## 5. 依赖分析

### 5.1 内部依赖
- `pkg/models` - 数据模型
- `pkg/logger` - 日志
- `pkg/storage/parquet` - Parquet 读写
- `pkg/util` - 工具函数

### 5.2 外部依赖
- 无新增外部依赖

### 5.3 循环依赖风险
- [x] 无风险
  - 所有文件在同一个包内
  - 依赖方向清晰（common → 各层 → storage）

## 6. 风险评估

| 风险类型 | 概率 | 影响 | 缓解措施 |
|---------|------|------|---------|
| 编译错误 | 低 | 中 | 逐步拆分，每步验证编译 |
| 测试失败 | 低 | 中 | 保持向后兼容，运行现有测试 |
| 性能下降 | 极低 | 低 | 避免不必要的函数调用 |
| 导入错误 | 低 | 低 | 使用 IDE 重构工具 |

## 7. 测试策略

### 7.1 单元测试
- 覆盖率目标: >= 60% (各层独立测试)
- 关键路径: Bronze/Silver/Gold 写入逻辑

### 7.2 集成测试
- 测试场景: 完整的数据流转（Bronze → Silver → Gold）
- 使用现有测试验证向后兼容

### 7.3 性能测试
- 基准: 拆分前后性能对比
- 目标: 无性能下降

## 8. 验收标准
- [ ] 所有文件拆分完成
- [ ] 编译通过，无错误
- [ ] 所有现有测试通过
- [ ] 新增各层单元测试
- [ ] 代码覆盖率 >= 60%
- [ ] 无性能下降
- [ ] 文档更新完成

## 9. 回滚策略
- [ ] Git 分支开发，随时可回滚
- [ ] 保留原文件备份
- [ ] 分阶段提交，每阶段可独立回滚

## 10. 实施步骤

### 步骤 1: 创建 layered_common.go
- 移动共享类型、接口、规则定义
- 验证编译通过

### 步骤 2: 创建 bronze_storage.go
- 移动 BronzeRecord 和相关方法
- 验证编译通过
- 添加 Bronze 层测试

### 步骤 3: 创建 silver_storage.go
- 移动 SilverRecord 和相关方法
- 验证编译通过
- 添加 Silver 层测试

### 步骤 4: 创建 gold_storage.go
- 移动 GoldRecord 和相关方法
- 验证编译通过
- 添加 Gold 层测试

### 步骤 5: 清理 layered_storage.go
- 删除已移动的代码
- 仅保留编排逻辑
- 验证编译通过

### 步骤 6: 运行测试
- 运行所有现有测试
- 运行新增测试
- 验证覆盖率

### 步骤 7: 文档更新
- 更新代码注释
- 更新 ARCHITECTURE.md
- 更新 README.md

---
**计划状态**: 待用户确认
**创建时间**: 2026-05-06
**最后更新**: 2026-05-06
