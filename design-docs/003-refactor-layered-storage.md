# Design Doc 003: 拆分 layered_storage.go

## 元数据
- **编号**: 003
- **标题**: 拆分 layered_storage.go 以提高可维护性
- **状态**: draft
- **创建日期**: 2026-05-06
- **最后更新**: 2026-05-06
- **关联任务**: EXECUTION-PLAN.md B-1
- **复杂度级别**: L2
- **前置 Design Doc**: 无

## 1. 背景与动机

### 为什么需要这个改动？
`layered_storage.go` 当前有 **922 行**代码，违反了单一职责原则（SRP）。该文件混合了：
- Bronze 层的读写逻辑
- Silver 层的读写逻辑
- Gold 层的读写逻辑
- 共享的类型定义和接口
- 编排逻辑

### 当前系统的痛点是什么？
1. **维护困难**: 修改一层逻辑可能意外影响其他层
2. **测试困难**: 难以隔离测试各层功能
3. **代码审查困难**: 大文件导致审查效率降低
4. **认知负担**: 开发者需要理解整个文件才能修改一小部分

### 业务/技术驱动因素
- 提高代码可维护性
- 降低修改风险
- 提高测试覆盖率
- 符合 SOLID 原则

## 2. 调研与现状分析

### 2.1 现有实现

**文件结构**:
```
pkg/storage/layered_storage.go (922 行)
├── 类型定义 (L16-93): LayerType, LayeredStorage, LayerConfig, BronzeRecord, SilverRecord, GoldRecord
├── 接口定义 (L95-123): DataCleaner, DataValidator, DataAggregator
├── 规则定义 (L110-123): CleaningRule, ValidationRule
├── 构造函数 (L125-161): NewLayeredStorage
├── Bronze层方法 (L192-262): WriteToBronze
├── Silver层方法 (L264-421): ProcessBronzeToSilver, writeSilverRecord, writeSilverParquet, writeSilverJSON
├── Gold层方法 (L423-560): ProcessSilverToGold, writeGoldRecord, writeGoldParquet, writeGoldJSON
└── 辅助方法 (L561-922): getCleaningRules, getValidationRules, cleanData, validateData 等
```

**已有的限制和瓶颈**:
- 单文件过大，难以导航
- 各层逻辑耦合
- 缺少清晰的模块边界

### 2.2 业界实践

**Go 项目组织最佳实践**:
1. **按职责拆分**: 每个文件负责一个清晰的职责
2. **内聚性**: 相关的代码放在一起
3. **接口隔离**: 接口定义与实现分离

**参考项目**:
- Kubernetes: 按功能模块拆分大文件
- Prometheus: 存储层按职责分离
- TiDB: 分层架构，每层独立文件

### 2.3 技术约束
- 保持向后兼容，不改变公共 API
- 不引入新的外部依赖
- 保持测试通过
- 不影响性能

## 3. 可选方案

### 方案 A: 按层拆分（推荐）
**描述**: 按数据湖三层架构拆分文件

**拆分策略**:
```
layered_storage.go (922行)
├── bronze_storage.go      # BronzeRecord + WriteToBronze + Bronze辅助方法
├── silver_storage.go      # SilverRecord + ProcessBronzeToSilver + Silver辅助方法
├── gold_storage.go        # GoldRecord + ProcessSilverToGold + Gold辅助方法
├── layered_common.go      # 共享类型、接口、规则定义
└── layered_storage.go     # 仅保留 LayeredStorage 结构体和编排逻辑
```

**优点**:
- 清晰的职责划分
- 符合数据湖三层架构
- 易于理解和维护
- 测试隔离性好

**缺点**:
- 需要调整导入
- 文件数量增加

**工作量**: 中（约 4-6 小时）

---

### 方案 B: 按功能拆分
**描述**: 按功能类型拆分（类型、接口、实现）

**拆分策略**:
```
layered_storage.go
├── types.go          # 所有类型定义
├── interfaces.go     # 所有接口定义
├── bronze.go         # Bronze 层实现
├── silver.go         # Silver 层实现
├── gold.go           # Gold 层实现
└── storage.go        # 编排逻辑
```

**优点**:
- 类型定义集中
- 接口定义集中

**缺点**:
- 不符合 Go 惯例（类型应与使用它的代码在一起）
- 跨文件导航频繁

**工作量**: 中（约 4-6 小时）

---

### 方案 C: 保持现状，仅提取接口
**描述**: 仅将接口提取到单独文件，保持其他不变

**拆分策略**:
```
layered_storage.go
├── interfaces.go     # 接口定义
└── layered_storage.go # 其他所有代码
```

**优点**:
- 改动最小
- 风险最低

**缺点**:
- 不解决主要问题（文件过大）
- 治标不治本

**工作量**: 小（约 1-2 小时）

## 4. 决策

**选定方案**: 方案 A - 按层拆分

**决策理由**:
1. 符合数据湖三层架构的设计理念
2. 职责划分清晰，符合单一职责原则
3. 便于后续扩展和维护
4. 测试隔离性好，可以提高测试覆盖率
5. 符合 Go 项目组织最佳实践

**权衡取舍**:
- 增加了文件数量，但提高了可维护性
- 需要调整导入，但一次性工作
- 需要更新测试，但可以逐步进行

## 5. 影响范围

### 影响的模块/包
- `pkg/storage/` - 主要影响
- `pkg/pipeline/` - 可能需要调整导入
- `test/` - 可能需要调整测试导入

### 影响的接口
- 公共 API 不变
- 内部实现重组

### 影响的配置
- 无配置变更

### 影响的部署
- 无部署变更

## 6. 实施计划

### 阶段 1: 创建新文件
1. 创建 `layered_common.go` - 移动共享类型和接口
2. 创建 `bronze_storage.go` - 移动 Bronze 层代码
3. 创建 `silver_storage.go` - 移动 Silver 层代码
4. 创建 `gold_storage.go` - 移动 Gold 层代码

### 阶段 2: 重构原文件
1. 清理 `layered_storage.go`，仅保留编排逻辑
2. 更新导入语句
3. 确保编译通过

### 阶段 3: 测试验证
1. 运行现有测试，确保通过
2. 补充各层的单元测试
3. 运行集成测试

### 阶段 4: 文档更新
1. 更新代码注释
2. 更新 ARCHITECTURE.md
3. 更新 README.md

## 7. 开放问题
- [ ] 是否需要为每个层创建独立的测试文件？
- [ ] 是否需要调整包的公开 API？
- [ ] 是否需要更新性能基准测试？

---
**文档状态**: draft
