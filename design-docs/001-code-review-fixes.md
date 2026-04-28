# Design Doc 001: Code Review Fixes

## 元数据
- **编号**: 001
- **标题**: Code Review Fixes
- **状态**: approved
- **创建日期**: 2026-04-28
- **最后更新**: 2026-04-28
- **关联任务**: 首次全面代码审查问题修复
- **复杂度级别**: L2
- **前置 Design Doc**: 无

## 1. 背景与动机
对 data-ingestion-tool 进行首次全面代码审查，发现 21 个问题，涵盖数据丢失风险、并发安全、接口设计违反 SOLID 原则、功能未实现但静默通过等。需要按优先级分批次修复。

## 2. 调研与现状分析

### 2.1 现有实现
- MySQL CDC 连接器通过 BinlogSyncer 捕获变更，channel 满时直接丢弃事件
- generateID 使用时间戳，高并发下可能重复
- shouldProcessTable 放在 BaseConnector 中但只适用于 MySQL
- Checkpoint Stop() 和 autoSave 存在竞态条件
- Parquet 实现为自定义格式，非标准 Apache Parquet
- Filter/Transform 解析器返回 NoOp 实现，静默失败
- Dockerfile COPY 不存在的 config.yaml

### 2.2 业界实践
- CDC 系统应保证 at-least-once 语义，channel 满时应阻塞而非丢弃
- ID 生成应使用 UUID 或雪花算法
- 接口设计应遵循 ISP/LSP，MySQL 专属逻辑不应放在基类
- 并发控制应使用 channel 同步或 sync.Once

### 2.3 技术约束
- 项目已有 github.com/google/uuid 依赖
- Parquet 格式替换为标准库影响面大，暂不处理
- canal 依赖用于获取表结构，完全移除需引入 database/sql

## 3. 可选方案

### 方案 A: 最小修复 — 只修 P0 问题
- **描述**: 仅修复数据丢失、UUID、Dockerfile 等 5 个 P0 问题
- **优点**: 改动最小，风险最低
- **缺点**: P1/P2 问题持续存在，技术债累积
- **工作量**: 小

### 方案 B: 分批修复 — P0→P1→P2 (选定)
- **描述**: 按优先级分三批次修复，每批次独立验证
- **优点**: 风险可控，每批次可独立回滚；P0 先行保证数据安全
- **缺点**: 需要更多时间
- **工作量**: 中

### 方案 C: 一次性全部修复
- **描述**: 一次性修复所有 21 个问题
- **优点**: 一步到位
- **缺点**: 变更面大，回归风险高，难以定位问题
- **工作量**: 大

## 4. 决策
- **选定方案**: B (分批修复)
- **决策理由**: 平衡了风险控制和问题解决效率。P0 问题涉及数据安全必须立即修复，P1/P2 可逐步推进
- **权衡取舍**: Parquet 标准化 (#5) 和 Kafka/PostgreSQL/REST 连接器 (#18) 暂不处理，作为后续 Design Doc

## 5. 影响范围
- `pkg/connector/mysql.go` — channel 发送逻辑、canal 延迟初始化、shouldProcessTable
- `pkg/connector/connector.go` — 移除 shouldProcessTable
- `pkg/models/models.go` — generateID 改用 UUID
- `pkg/checkpoint/checkpoint.go` — Stop 竞态修复
- `pkg/pipeline/pipeline.go` — Filter/Transform 返回错误
- `pkg/storage/manager.go` — 压缩功能标记
- `pkg/storage/local_storage.go` — CSV fileSize 更新
- `pkg/storage/parquet/reader.go` — Seek 签名修复
- `pkg/config/secrets.go` — 并发安全
- `pkg/logger/logger.go` — fileHook 释放
- `cmd/ingester/main.go` — 删除未使用函数、shutdown 超时
- `Dockerfile` — COPY 路径修复
- `config.example.yaml` — 移除 advanced 节

## 6. 开放问题
- [x] Parquet 格式是否替换为标准库？→ 暂不处理，影响面太大
- [x] canal 是否完全移除？→ 改为延迟初始化，减少资源占用
- [ ] Filter/Transform 规则语法如何设计？→ 需要单独的 Design Doc

---
**文档状态**: approved
