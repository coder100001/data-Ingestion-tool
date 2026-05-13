# Design Doc 004: 项目演进方向 - 渐进式 Web 管理平台

## 元数据
- **编号**: 004
- **标题**: 项目演进方向 - 渐进式 Web 管理平台
- **状态**: approved
- **创建日期**: 2026-05-13
- **最后更新**: 2026-05-13
- **复杂度级别**: L3
- **前置 Design Doc**: 无

## 1. 背景与动机

### 为什么需要这个演进？

当前数据摄取工具是一个纯 CLI 工具，具备以下能力：
- MySQL Binlog CDC
- 分层数据湖架构（Bronze/Silver/Gold）
- Parquet 格式支持
- 数据压缩（Zstd/Snappy/Gzip）
- Schema 演化

但在使用过程中存在以下痛点：
1. **配置管理困难**: 需要频繁修改 YAML 文件，容易出错
2. **监控和可观测性不足**: 不知道当前运行状态、处理进度、是否有错误
3. **数据质量不可见**: 数据质量问题难以发现，缺少数据验证和监控
4. **运维操作繁琐**: 部署、升级、回滚流程不够顺畅

### 目标定位

- **使用场景**: 后台服务/数据管道
- **操作人员**: 个人/小团队内部使用
- **专业程度**: 开源项目级别，可作为开源项目发布
- **UI 需求**: 完整管理界面（状态监控 + 配置管理）

## 2. 业内最佳实践调研

### 2.1 参考项目对比

| 工具 | UI 策略 | 架构特点 | 适用场景 |
|------|---------|----------|----------|
| **Airbyte** | 完整 Web UI | 微服务架构（API + Worker + Scheduler） | 企业级数据集成平台 |
| **Debezium** | 无独立 UI | Kafka Connect 集成 + JMX 指标 | CDC 专用，依赖外部监控 |
| **Vector** | 轻量 Web Console | 单一二进制 + REST API + Prometheus | 高性能数据管道 |

### 2.2 关键借鉴点

从 Airbyte 学习：
- 模块化架构设计（平台 + 连接器分离）
- 完整的 REST API 作为统一管理入口
- 配置管理和版本控制

从 Debezium 学习：
- Prometheus 指标导出
- Grafana Dashboard 集成
- JMX 健康检查机制

从 Vector 学习：
- 单一二进制部署
- 轻量级 Web Console
- 健康检查 API

## 3. 演进方案

### 3.1 方案选择

经过对比分析，选择 **方案 A：渐进式演进**。

```
Phase 1 (1-2周)          Phase 2 (1-2周)          Phase 3 (2-3周)
┌─────────────────┐     ┌─────────────────┐     ┌─────────────────┐
│  REST API 层    │────▶│ Prometheus 指标 │────▶│  Web Dashboard  │
│  - 健康检查     │     │  - 性能指标     │     │  - 状态监控     │
│  - 配置管理     │     │  - 数据质量指标 │     │  - 配置编辑     │
│  - 运维操作     │     │  - Grafana 集成 │     │  - 数据预览     │
└─────────────────┘     └─────────────────┘     └─────────────────┘
```

**选择理由**：
1. 风险可控，每个阶段可独立验证
2. 符合开源项目演进路径
3. 可以先解决最紧迫的监控痛点
4. 技术栈简单（Go + 嵌入式前端）

## 4. 详细设计

### 4.1 整体架构

```
┌─────────────────────────────────────────────────────────────────────────┐
│                        Data Ingestion Tool v2.0                          │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                          │
│   ┌─────────────┐    ┌─────────────┐    ┌─────────────┐                │
│   │   CLI 入口   │    │  REST API   │    │ Web Dashboard│                │
│   │  (现有增强)  │    │   (新增)    │    │    (新增)    │                │
│   └──────┬──────┘    └──────┬──────┘    └──────┬──────┘                │
│          │                  │                  │                        │
│          └──────────────────┼──────────────────┘                        │
│                             │                                           │
│                             ▼                                           │
│   ┌─────────────────────────────────────────────────────────────────┐  │
│   │                      Core Engine (现有)                          │  │
│   │  ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌──────────┐        │  │
│   │  │Connector │  │ Pipeline │  │ Storage  │  │Checkpoint│        │  │
│   │  │  Layer   │  │  Layer   │  │  Layer   │  │  Manager │        │  │
│   │  └──────────┘  └──────────┘  └──────────┘  └──────────┘        │  │
│   └─────────────────────────────────────────────────────────────────┘  │
│                                                                          │
│   ┌─────────────────────────────────────────────────────────────────┐  │
│   │                    Observability Layer (新增)                    │  │
│   │  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐          │  │
│   │  │ Metrics      │  │ Health       │  │ Data Quality │          │  │
│   │  │ Collector    │  │ Checker      │  │ Monitor      │          │  │
│   │  └──────────────┘  └──────────────┘  └──────────────┘          │  │
│   └─────────────────────────────────────────────────────────────────┘  │
│                                                                          │
└─────────────────────────────────────────────────────────────────────────┘
```

### 4.2 数据源连接器架构

```
┌─────────────────────────────────────────────────────────────────────────┐
│                        Connector Architecture                            │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                          │
│   ┌──────────────────────────────────────────────────────────────────┐  │
│   │                    Connector Manager (新增)                       │  │
│   │  - 连接器生命周期管理                                              │  │
│   │  - 连接状态监控                                                    │  │
│   │  - 连接器配置验证                                                  │  │
│   │  - 重连策略管理                                                    │  │
│   └───────────────────────────┬──────────────────────────────────────┘  │
│                               │                                          │
│                               ▼                                          │
│   ┌──────────────────────────────────────────────────────────────────┐  │
│   │                    Connector Interface (现有增强)                 │  │
│   │                                                                   │  │
│   │  type Connector interface {                                      │  │
│   │      Connect(ctx) error           // 建立连接                     │  │
│   │      Disconnect(ctx) error        // 断开连接                     │  │
│   │      Status() ConnectorStatus     // 获取状态 (新增)              │  │
│   │      Metrics() ConnectorMetrics   // 获取指标 (新增)              │  │
│   │      Validate() []ValidationError // 配置验证 (新增)              │  │
│   │      Stream(ctx, chan DataChange) // 数据流 (现有)                │  │
│   │  }                                                               │  │
│   └───────────────────────────┬──────────────────────────────────────┘  │
│                               │                                          │
│          ┌────────────────────┼────────────────────┐                    │
│          │                    │                    │                    │
│          ▼                    ▼                    ▼                    │
│   ┌─────────────┐     ┌─────────────┐     ┌─────────────┐              │
│   │   MySQL     │     │   Kafka     │     │ PostgreSQL  │              │
│   │  Connector  │     │  Connector  │     │  Connector  │              │
│   │  (现有增强) │     │  (待实现)   │     │  (待实现)   │              │
│   └─────────────┘     └─────────────┘     └─────────────┘              │
│                                                                          │
└─────────────────────────────────────────────────────────────────────────┘
```

### 4.3 数据同步与变更监控

```
┌─────────────────────────────────────────────────────────────────────────┐
│                    Data Sync & Change Monitoring                         │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                          │
│   ┌──────────────────────────────────────────────────────────────────┐  │
│   │                    Sync Manager (新增)                            │  │
│   │                                                                   │  │
│   │  - 同步任务调度                                                   │  │
│   │  - 同步进度追踪                                                   │  │
│   │  - 增量/全量同步管理                                              │  │
│   │  - 同步冲突检测                                                   │  │
│   └───────────────────────────┬──────────────────────────────────────┘  │
│                               │                                          │
│          ┌────────────────────┼────────────────────┐                    │
│          │                    │                    │                    │
│          ▼                    ▼                    ▼                    │
│   ┌─────────────┐     ┌─────────────┐     ┌─────────────┐              │
│   │  Sync Job   │     │ Change Log  │     │  Data       │              │
│   │  Tracker    │     │  Viewer     │     │  Preview    │              │
│   └─────────────┘     └─────────────┘     └─────────────┘              │
│                                                                          │
└─────────────────────────────────────────────────────────────────────────┘
```

## 5. Phase 1: REST API 层

### 5.1 API 端点设计

```
┌─────────────────────────────────────────────────────────────────────────┐
│                          API Endpoints                                   │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                          │
│  Health & Status                                                         │
│  ─────────────────────────────────────────────────────────────────────  │
│  GET  /health                    # 健康检查（用于负载均衡器）            │
│  GET  /ready                    # 就绪检查（用于 Kubernetes）            │
│  GET  /api/v1/status             # 详细状态                              │
│                                                                          │
│  Configuration Management                                                │
│  ─────────────────────────────────────────────────────────────────────  │
│  GET  /api/v1/config             # 获取当前配置                          │
│  PUT  /api/v1/config             # 更新配置（热更新）                     │
│  POST /api/v1/config/reload      # 从文件重新加载配置                     │
│  POST /api/v1/config/validate    # 验证配置有效性                         │
│                                                                          │
│  Connectors                                                              │
│  ─────────────────────────────────────────────────────────────────────  │
│  GET  /api/v1/connectors         # 列出所有连接器                        │
│  GET  /api/v1/connectors/{id}    # 获取连接器详情                        │
│  GET  /api/v1/connectors/{id}/status   # 连接状态                        │
│  POST /api/v1/connectors/{id}/restart  # 重启连接器                      │
│  GET  /api/v1/connectors/{id}/metrics  # 性能指标                        │
│                                                                          │
│  Sync Management                                                         │
│  ─────────────────────────────────────────────────────────────────────  │
│  GET  /api/v1/sync/status        # 同步状态                              │
│  POST /api/v1/sync/pause         # 暂停同步                              │
│  POST /api/v1/sync/resume        # 恢复同步                              │
│  POST /api/v1/sync/reset         # 重置同步位置                          │
│  GET  /api/v1/sync/progress      # 同步进度                              │
│                                                                          │
│  Change Data                                                             │
│  ─────────────────────────────────────────────────────────────────────  │
│  GET  /api/v1/changes            # 查询变更记录                          │
│  GET  /api/v1/changes/stream     # 实时变更流 (SSE)                      │
│  GET  /api/v1/changes/stats      # 变更统计                              │
│                                                                          │
│  Data Lake                                                               │
│  ─────────────────────────────────────────────────────────────────────  │
│  GET  /api/v1/data/preview       # 数据预览                              │
│  GET  /api/v1/data/schema        # 数据 Schema                           │
│  GET  /api/v1/data/quality       # 数据质量报告                          │
│  GET  /api/v1/data/catalog       # 数据目录                              │
│                                                                          │
│  Operations                                                              │
│  ─────────────────────────────────────────────────────────────────────  │
│  POST /api/v1/operations/shutdown # 优雅关闭                             │
│  GET  /api/v1/operations/logs     # 获取日志                             │
│  GET  /api/v1/operations/metrics  # Prometheus 格式指标                  │
│                                                                          │
└─────────────────────────────────────────────────────────────────────────┘
```

### 5.2 核心数据类型

```go
type ConnectorStatus struct {
    ID           string          `json:"id"`
    Type         string          `json:"type"`          // mysql, kafka, postgres
    State        ConnectorState  `json:"state"`         // connected, disconnected, error
    LastActive   time.Time       `json:"last_active"`
    ErrorMessage string          `json:"error_message"`
    Uptime       time.Duration   `json:"uptime"`
}

type SyncProgress struct {
    TotalRecords      int64   `json:"total_records"`
    ProcessedRecords  int64   `json:"processed_records"`
    Percentage        float64 `json:"percentage"`
    CurrentPosition   string  `json:"current_position"`
    EstimatedRemaining time.Duration `json:"estimated_remaining"`
}

type DataChange struct {
    ID         string                 `json:"id"`
    Timestamp  time.Time              `json:"timestamp"`
    Type       string                 `json:"type"`       // INSERT, UPDATE, DELETE
    Database   string                 `json:"database"`
    Table      string                 `json:"table"`
    Before     map[string]interface{} `json:"before,omitempty"`
    After      map[string]interface{} `json:"after,omitempty"`
    Metadata   ChangeMetadata         `json:"metadata"`
}
```

### 5.3 配置更新

```yaml
# config.yaml 新增 API 配置
api:
  enabled: true
  host: "0.0.0.0"
  port: 8080
  
  auth:
    enabled: false
    type: "api_key"
    api_key: "${API_KEY}"
  
  cors:
    enabled: true
    allowed_origins: ["*"]
  
  rate_limit:
    enabled: true
    requests_per_second: 100
    burst: 50
```

## 6. Phase 2: Prometheus 指标

### 6.1 指标分类

| 类别 | 指标名称 | 描述 |
|------|----------|------|
| 系统 | `ingestion_uptime_seconds` | 运行时间 |
| 系统 | `ingestion_memory_usage_bytes` | 内存使用 |
| 连接器 | `ingestion_connector_status` | 连接状态 |
| 连接器 | `ingestion_connector_events_total` | 事件总数 |
| 连接器 | `ingestion_connector_lag_milliseconds` | 复制延迟 |
| 管道 | `ingestion_pipeline_queue_size` | 队列大小 |
| 管道 | `ingestion_pipeline_processing_duration_seconds` | 处理耗时 |
| 存储 | `ingestion_storage_writes_total` | 写入总数 |
| 存储 | `ingestion_storage_bytes_written_total` | 写入字节数 |
| 数据质量 | `ingestion_data_quality_score` | 数据质量分数 |

### 6.2 Grafana Dashboard

提供预置的 Grafana Dashboard JSON，包含：
- Events Per Second 图表
- Replication Lag 仪表盘
- Processing Duration 直方图
- Data Quality Score 统计

## 7. Phase 3: Web Dashboard

### 7.1 技术栈

- **前端框架**: React + TypeScript
- **样式**: Tailwind CSS
- **状态管理**: TanStack Query (React Query)
- **图表**: Recharts 或 ECharts
- **部署**: 嵌入 Go 二进制 或 独立部署

### 7.2 页面结构

```
/
├── /dashboard          # 首页概览
├── /connectors         # 连接器管理
├── /sync               # 同步管理
├── /changes            # 变更查看
├── /data               # 数据浏览
├── /config             # 配置管理
└── /settings           # 系统设置
```

### 7.3 前后端对接

**前端异步/并行模式**：
1. 实时数据流 - Server-Sent Events (SSE)
2. 并行数据获取 - Promise.all() / React Query
3. 定时轮询 - setInterval + AbortController
4. 长时间操作 - 异步任务 + 轮询状态

## 8. 文件变更清单

### 8.1 新增文件

```
pkg/api/
├── server.go           # HTTP 服务器
├── handlers.go         # API 处理器
├── middleware.go       # 中间件（认证、限流等）
└── routes.go           # 路由定义

pkg/metrics/
├── collector.go        # 指标收集器
├── metrics.go          # Prometheus 指标定义
└── exporter.go         # 指标导出器

pkg/sync/
├── manager.go          # 同步管理器
├── progress.go         # 进度追踪
└── status.go           # 状态管理

web/
├── src/
│   ├── api/            # API 客户端
│   ├── components/     # UI 组件
│   ├── hooks/          # React Hooks
│   ├── pages/          # 页面组件
│   └── types/          # TypeScript 类型
├── package.json
└── vite.config.ts
```

### 8.2 修改文件

```
cmd/ingester/main.go    # 添加 API 服务器启动
pkg/config/config.go    # 添加 API 配置
pkg/connector/connector.go  # 增强 Connector 接口
go.mod                  # 添加 gin, prometheus 依赖
```

## 9. 实施计划

### 9.1 Phase 1: REST API 层 (1-2 周)

**Week 1**:
- [ ] 设计 API 接口规范
- [ ] 实现 HTTP 服务器框架
- [ ] 实现健康检查端点
- [ ] 实现配置管理 API

**Week 2**:
- [ ] 实现连接器管理 API
- [ ] 实现同步管理 API
- [ ] 实现变更数据 API
- [ ] 实现数据预览 API
- [ ] 编写 API 文档

### 9.2 Phase 2: Prometheus 指标 (1-2 周)

**Week 3**:
- [ ] 定义 Prometheus 指标
- [ ] 实现指标收集器
- [ ] 集成到各组件
- [ ] 实现 /metrics 端点

**Week 4**:
- [ ] 创建 Grafana Dashboard
- [ ] 编写监控文档
- [ ] 性能测试

### 9.3 Phase 3: Web Dashboard (2-3 周)

**Week 5-6**:
- [ ] 搭建前端项目
- [ ] 实现核心页面
- [ ] 实现数据可视化
- [ ] 实现实时更新

**Week 7**:
- [ ] 集成测试
- [ ] 性能优化
- [ ] 文档完善

## 10. 风险评估

| 风险 | 概率 | 影响 | 缓解措施 |
|------|------|------|----------|
| API 性能影响主流程 | 中 | 高 | 使用异步处理，限制并发 |
| 前端技术栈学习曲线 | 低 | 中 | 使用熟悉的 React 生态 |
| 嵌入式前端打包复杂 | 中 | 中 | 优先独立部署，后续优化嵌入 |
| Prometheus 指标过多 | 低 | 低 | 按需暴露，支持配置 |

## 11. 验收标准

### Phase 1
- [ ] 所有 API 端点可用
- [ ] API 文档完整
- [ ] 单元测试覆盖率 > 80%
- [ ] 可通过 curl/Postman 测试

### Phase 2
- [ ] Prometheus 可抓取指标
- [ ] Grafana Dashboard 可用
- [ ] 关键指标都有告警规则

### Phase 3
- [ ] Web UI 可访问
- [ ] 实时数据流正常
- [ ] 配置管理功能可用
- [ ] 数据预览功能正常

---
**文档状态**: approved
