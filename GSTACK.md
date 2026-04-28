# Data Ingestion Tool - 工程演进 GStack 规划

> GStack: Goal-Stack 目标栈方法论
> 本文档定义项目的演进路线图，从当前状态到企业级数据摄取平台

---

## 项目现状概览

| 维度 | 当前状态 |
|------|----------|
| **核心功能** | MySQL CDC → 本地数据湖 (JSON/CSV/Parquet) |
| **架构** | 分层架构 (Connector → Pipeline → Storage) |
| **代码质量** | 单元测试覆盖核心模块，错误重试/死信队列已实现 |
| **安全** | 环境变量支持、日志脱敏已完成 |
| **存储** | Parquet格式、压缩算法(Zstd/Snappy/Gzip)、Schema注册中心 |

---

## 演进路线图

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                         工程演进路线图                                       │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│   当前状态        Phase 1         Phase 2         Phase 3         目标状态   │
│      │              │               │               │               │       │
│      ▼              ▼               ▼               ▼               ▼       │
│   ┌──────┐      ┌──────┐       ┌──────┐       ┌──────┐       ┌──────────┐  │
│   │基础  │ ───▶ │高可用│ ───▶  │多源  │ ───▶  │云原生│ ───▶  │企业级平台│  │
│   │CDC   │      │架构  │       │生态  │       │部署  │       │          │  │
│   └──────┘      └──────┘       └──────┘       └──────┘       └──────────┘  │
│                                                                             │
│   已完成:        目标:          目标:          目标:          目标:          │
│   - 基础CDC      - 分布式      - Kafka       - K8s          - SaaS化       │
│   - 单元测试     - 容错        - PG逻辑复制   - 自动扩缩容    - 多租户       │
│   - 错误处理     - 监控告警    - REST API    - 云存储        - 数据治理     │
│   - Parquet      - 水平扩展    - 流处理      - Serverless    - 数据目录     │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## Phase 1: 高可用架构 (当前 → 3个月)

### 目标
将单机部署升级为高可用分布式架构，支持故障自动恢复和水平扩展。

### 技术栈
- **协调服务**: etcd / Consul
- **消息队列**: Apache Kafka (内置或外置)
- **分布式锁**: Redis Redlock / etcd
- **监控**: Prometheus + Grafana

### 任务分解

#### P1-1: 分布式协调层
```go
// pkg/coordinator/coordinator.go

type Coordinator interface {
    // 节点注册与发现
    RegisterNode(node NodeInfo) error
    DeregisterNode(nodeID string) error
    GetActiveNodes() ([]NodeInfo, error)
    
    // 任务分配
    AssignPartition(partition Partition, nodeID string) error
    GetPartitionAssignment() (map[string]string, error)
    
    // 领导选举
    ElectLeader() (string, error)
    IsLeader(nodeID string) bool
    
    // 分布式锁
    AcquireLock(lockName string, ttl time.Duration) (Lock, error)
}
```

**任务清单**:
- [ ] 实现 etcd 协调器
- [ ] 实现节点心跳机制
- [ ] 实现分区分配算法 (一致性哈希)
- [ ] 实现领导选举

#### P1-2: 集群模式支持
```yaml
# config.cluster.yaml
cluster:
  enabled: true
  node_id: "node-1"
  bind_addr: "0.0.0.0:7946"
  advertise_addr: "10.0.0.1:7946"
  peers:
    - "10.0.0.2:7946"
    - "10.0.0.3:7946"
  
  coordination:
    type: "etcd"
    endpoints:
      - "etcd-1:2379"
      - "etcd-2:2379"
      - "etcd-3:2379"
```

**任务清单**:
- [ ] 集群配置扩展
- [ ] 节点间通信协议 (gRPC)
- [ ] 状态同步机制
- [ ] 优雅启停

#### P1-3: 分区并行消费
```go
// pkg/connector/mysql_cluster.go

type ClusterMySQLConnector struct {
    coordinator Coordinator
    partitioner Partitioner
    
    // 当前节点负责的分区
    assignedPartitions []Partition
}

func (c *ClusterMySQLConnector) Start() error {
    // 1. 注册节点
    // 2. 等待分区分配
    // 3. 只消费分配给当前节点的表
    // 4. 监听分区变化事件
}
```

**任务清单**:
- [ ] 表级分区策略
- [ ] 动态分区重平衡
- [ ] 分区迁移 (零停机)

#### P1-4: 监控与告警
```go
// pkg/metrics/metrics.go

var (
    ChangesProcessed = prometheus.NewCounterVec(...)
    ProcessingLatency = prometheus.NewHistogramVec(...)
    ConnectorLag = prometheus.NewGaugeVec(...)
    StorageWriteErrors = prometheus.NewCounter(...)
)
```

**任务清单**:
- [ ] Prometheus metrics 暴露
- [ ] 健康检查端点
- [ ] 关键指标告警规则
- [ ] Grafana Dashboard

---

## Phase 2: 多源生态 (3个月 → 6个月)

### 目标
扩展数据源支持，构建完整的数据摄取生态，支持流处理和实时分析。

### 技术栈
- **PostgreSQL**: 逻辑复制 (pglogical)
- **Kafka**: Kafka Connect 兼容
- **MongoDB**: Change Streams
- **Flink**: 流处理集成

### 任务分解

#### P2-1: PostgreSQL CDC 完整实现
```go
// pkg/connector/postgres.go (完整实现)

type PostgresConnector struct {
    conn *pgx.Conn
    slotName string
    
    // 逻辑复制连接
    replicationConn *pgconn.PgConn
}

func (p *PostgresConnector) Start() error {
    // 1. 创建复制槽 (如果不存在)
    // 2. 启动逻辑复制流
    // 3. 解析 WAL 消息
    // 4. 转换为 DataChange
}
```

**任务清单**:
- [ ] pgoutput 协议解析
- [ ] 复制槽管理
- [ ] DDL 事件捕获
- [ ] 大对象支持

#### P2-2: Kafka Connect 兼容
```go
// pkg/connect/kafka_connect.go

// 实现 Kafka Connect Source Connector 接口
type SourceConnector interface {
    Start(config map[string]string) error
    Stop() error
    
    // 轮询获取变更记录
    Poll() ([]SourceRecord, error)
    
    // 提交偏移量
    Commit(record SourceRecord) error
}
```

**任务清单**:
- [ ] Kafka Connect REST API
- [ ] 配置验证接口
- [ ] 任务分配支持
- [ ] 偏移量管理

#### P2-3: MongoDB Change Streams
```go
// pkg/connector/mongodb.go

type MongoDBConnector struct {
    client *mongo.Client
    watcher *mongo.ChangeStream
}

func (m *MongoDBConnector) Start() error {
    // 监听 Change Stream
    // 支持 resume token
    // 处理分片集群
}
```

**任务清单**:
- [ ] Change Stream 监听
- [ ] Resume Token 持久化
- [ ] 分片集群支持
- [ ] 全量同步模式

#### P2-4: REST API 源完整实现
```go
// pkg/connector/rest.go (完整实现)

type RESTConnector struct {
    client *http.Client
    config RESTConfig
    
    // 轮询或 Webhook
    mode ConnectionMode
}

func (r *RESTConnector) Start() error {
    // 支持多种模式:
    // 1. 轮询模式 (定时拉取)
    // 2. Webhook 模式 (接收推送)
    // 3. SSE 模式 (Server-Sent Events)
}
```

**任务清单**:
- [ ] 轮询策略 (增量/全量)
- [ ] Webhook 接收端
- [ ] 认证机制 (OAuth2/API Key)
- [ ] 速率限制处理

#### P2-5: 流处理集成 (Flink/Spark)
```go
// pkg/stream/flink_integration.go

// 提供 Flink Source Function
type CDCSourceFunction struct {
    connector Connector
    deserializer DeserializationSchema
}

func (f *CDCSourceFunction) Run(ctx SourceContext) {
    // 将 CDC 事件转换为 Flink DataStream
}
```

**任务清单**:
- [ ] Flink CDC Source
- [ ] Spark Streaming Source
- [ ] 事件时间处理
- [ ] 精确一次语义

---

## Phase 3: 云原生部署 (6个月 → 9个月)

### 目标
实现云原生部署，支持 Kubernetes 自动扩缩容、云存储和多租户。

### 技术栈
- **容器编排**: Kubernetes
- **自动扩缩容**: HPA / KEDA
- **云存储**: S3 / GCS / Azure Blob
- **服务网格**: Istio (可选)

### 任务分解

#### P3-1: Kubernetes Operator
```yaml
# crd/cdc-connector.yaml
apiVersion: apiextensions.k8s.io/v1
kind: CustomResourceDefinition
metadata:
  name: cdcconnectors.dataingestion.io
spec:
  group: dataingestion.io
  versions:
    - name: v1
      served: true
      storage: true
      schema:
        openAPIV3Schema:
          type: object
          properties:
            spec:
              type: object
              properties:
                source:
                  type: object
                  properties:
                    type:
                      type: string
                      enum: ["mysql", "postgres", "kafka"]
                    connection:
                      type: object
                storage:
                  type: object
                  properties:
                    type:
                      type: string
                      enum: ["local", "s3", "gcs"]
                replicas:
                  type: integer
                  minimum: 1
```

**任务清单**:
- [ ] CRD 定义
- [ ] Operator 控制器
- [ ] 状态管理
- [ ] 升级策略

#### P3-2: 云存储支持
```go
// pkg/storage/cloud/s3_storage.go

type S3Storage struct {
    client *s3.Client
    bucket string
    prefix string
}

func (s *S3Storage) Write(change DataChange) error {
    // 写入 S3
    // 支持分区 (s3://bucket/prefix/db/table/date/)
    // 支持多部分上传
}
```

**任务清单**:
- [ ] S3 存储实现
- [ ] GCS 存储实现
- [ ] Azure Blob 实现
- [ ] 统一云存储接口

#### P3-3: 自动扩缩容
```yaml
# keda-scaledobject.yaml
apiVersion: keda.sh/v1alpha1
kind: ScaledObject
metadata:
  name: cdc-connector-scaler
spec:
  scaleTargetRef:
    name: cdc-connector
  minReplicaCount: 2
  maxReplicaCount: 10
  triggers:
    - type: prometheus
      metadata:
        serverAddress: http://prometheus:9090
        metricName: connector_lag_seconds
        threshold: '30'
        query: |
          avg(connector_lag_seconds{connector="mysql-orders"})
```

**任务清单**:
- [ ] KEDA 集成
- [ ] 自定义指标暴露
- [ ] 扩缩容策略
- [ ] 成本优化

#### P3-4: 配置中心
```go
// pkg/config/dynamic.go

type DynamicConfig struct {
    provider ConfigProvider // etcd / Consul / K8s ConfigMap
    
    // 热更新支持
    onChange func(key string, value interface{})
}

func (d *DynamicConfig) Watch(key string, callback ConfigCallback) {
    // 监听配置变化
    // 触发回调更新运行时配置
}
```

**任务清单**:
- [ ] 配置热更新
- [ ] 配置版本管理
- [ ] 灰度发布
- [ ] 配置加密

---

## Phase 4: 企业级平台 (9个月 → 12个月)

### 目标
构建完整的数据摄取平台，支持多租户、数据治理和数据目录。

### 技术栈
- **多租户**: 命名空间隔离 + RBAC
- **数据目录**: Apache Atlas / DataHub
- **血缘追踪**: OpenLineage
- **数据质量**: Great Expectations

### 任务分解

#### P4-1: 多租户架构
```go
// pkg/tenant/tenant.go

type Tenant struct {
    ID string
    Name string
    ResourceQuota ResourceQuota
    IsolationLevel IsolationLevel // 共享/独立
}

type MultiTenantManager struct {
    // 租户隔离策略
    // - 共享集群: 逻辑隔离
    // - 独立集群: 物理隔离
}
```

**任务清单**:
- [ ] 租户管理 API
- [ ] 资源配额控制
- [ ] 隔离策略实现
- [ ] 计费计量

#### P4-2: 数据目录集成
```go
// pkg/catalog/integration.go

type DataCatalog interface {
    // 注册数据集
    RegisterDataset(dataset DatasetMetadata) error
    
    // 更新 Schema
    UpdateSchema(datasetID string, schema Schema) error
    
    // 血缘关系
    RecordLineage(source, target string, operation string) error
}
```

**任务清单**:
- [ ] Apache Atlas 集成
- [ ] DataHub 集成
- [ ] 自动 Schema 注册
- [ ] 数据发现

#### P4-3: 数据血缘
```go
// pkg/lineage/tracker.go

type LineageTracker struct {
    // OpenLineage 兼容
    client OpenLineageClient
}

func (t *LineageTracker) Track(event DataChange) {
    // 记录输入数据集
    // 记录输出数据集
    // 记录转换逻辑
}
```

**任务清单**:
- [ ] OpenLineage 集成
- [ ] 血缘图生成
- [ ] 影响分析
- [ ] 合规报告

#### P4-4: 数据质量管理
```go
// pkg/quality/validator.go

type QualityRule struct {
    Field string
    Rule  string  // "not_null", "unique", "range", "regex"
    Params map[string]interface{}
}

type QualityValidator struct {
    rules []QualityRule
}

func (v *QualityValidator) Validate(change DataChange) QualityResult {
    // 执行质量检查
    // 记录质量指标
}
```

**任务清单**:
- [ ] 质量规则引擎
- [ ] 异常数据隔离
- [ ] 质量报告
- [ ] Great Expectations 集成

#### P4-5: Web 管理平台
```
┌─────────────────────────────────────────────────────────────┐
│                    Data Ingestion Platform                   │
├─────────────────────────────────────────────────────────────┤
│  ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌──────────┐    │
│  │ Dashboard│  │ Connectors│  │ Data Map │  │ Settings │    │
│  └──────────┘  └──────────┘  └──────────┘  └──────────┘    │
│                                                             │
│  ┌───────────────────────────────────────────────────────┐ │
│  │                                                       │ │
│  │              实时数据流可视化                          │ │
│  │                                                       │ │
│  │    MySQL ──▶ Connector ──▶ Pipeline ──▶ Storage      │ │
│  │      ↓          ↓            ↓           ↓           │ │
│  │   [健康]      [健康]       [健康]      [健康]         │ │
│  │   1.2k/s     1.2k/s       1.2k/s      1.2k/s         │ │
│  │                                                       │ │
│  └───────────────────────────────────────────────────────┘ │
│                                                             │
│  ┌──────────────────┐  ┌──────────────────┐                │
│  │   最近告警        │  │   数据血缘图      │                │
│  │   - 延迟 > 30s   │  │   [可视化图]      │                │
│  │   - 连接断开      │  │                  │                │
│  └──────────────────┘  └──────────────────┘                │
│                                                             │
└─────────────────────────────────────────────────────────────┘
```

**任务清单**:
- [ ] React/Vue 前端
- [ ] REST API 后端
- [ ] 实时数据推送 (WebSocket/SSE)
- [ ] 权限管理

---

## 技术债务清理

### 高优先级
| 问题 | 影响 | 解决方案 | 计划 |
|------|------|----------|------|
| 代码重复 (compareValues) | 维护困难 | 提取到公共包 | Phase 1 |
| 函数过长 (writeParquet) | 可读性差 | 拆分小函数 | Phase 1 |
| 资源泄漏风险 | 稳定性 | defer + 上下文 | Phase 1 |

### 中优先级
| 问题 | 影响 | 解决方案 | 计划 |
|------|------|----------|------|
| 配置验证分散 | 易出错 | 统一验证框架 | Phase 2 |
| 错误信息不完整 | 排查困难 | 错误包装 + 上下文 | Phase 2 |
| 缺少集成测试 | 回归风险 | 容器化测试 | Phase 2 |

### 低优先级
| 问题 | 影响 | 解决方案 | 计划 |
|------|------|----------|------|
| 文档不完整 | 上手困难 | 完善文档 | 持续 |
| 性能基准缺失 | 优化困难 | 基准测试套件 | Phase 3 |

---

## 里程碑规划

```
2024 Q1 (Phase 1)
├── Week 1-4: 分布式协调层
├── Week 5-8: 集群模式 + 分区并行
└── Week 9-12: 监控告警 + 稳定性优化

2024 Q2 (Phase 2)
├── Week 1-4: PostgreSQL CDC 完整实现
├── Week 5-6: MongoDB CDC
├── Week 7-8: Kafka Connect 兼容
├── Week 9-10: REST API 完整实现
└── Week 11-12: 流处理集成

2024 Q3 (Phase 3)
├── Week 1-4: K8s Operator + CRD
├── Week 5-8: 云存储支持
├── Week 9-10: 自动扩缩容
└── Week 11-12: 配置中心

2024 Q4 (Phase 4)
├── Week 1-4: 多租户架构
├── Week 5-6: 数据目录
├── Week 7-8: 数据血缘
├── Week 9-10: 数据质量
└── Week 11-12: Web 平台
```

---

## 风险与缓解策略

| 风险 | 可能性 | 影响 | 缓解策略 |
|------|--------|------|----------|
| 分布式复杂性 | 高 | 高 | 渐进式演进，先实现基础协调层 |
| 性能瓶颈 | 中 | 高 | 持续基准测试，早期发现 |
| 云厂商锁定 | 中 | 中 | 抽象接口，多云支持 |
| 人才缺口 | 中 | 中 | 文档完善，降低上手门槛 |

---

## 决策记录 (ADRs)

### ADR-001: 协调服务选型
- **决策**: 使用 etcd 作为默认协调服务
- **理由**: Kubernetes 原生支持，Raft 共识算法成熟
- **备选**: Consul (服务发现更强，但较重)

### ADR-002: 消息队列策略
- **决策**: 内置 Kafka 或外置 Kafka 可选
- **理由**: 小型部署不需要外部依赖，大型部署需要专业运维
- **备选**: Pulsar (功能更强，但生态较小)

### ADR-003: 云存储抽象
- **决策**: 使用 S3 API 作为通用接口
- **理由**: 事实标准，GCS/Azure 兼容 S3 API
- **备选**: 各云厂商 SDK (性能更好，但锁定)

---

## 附录

### A. 依赖升级计划

| 依赖 | 当前 | 目标 | 原因 |
|------|------|------|------|
| go-mysql | v1.7.0 | v1.8.0+ | 新特性支持 |
| logrus | v1.9.3 | 保持 | 稳定 |
| yaml.v3 | v3.0.1 | 保持 | 稳定 |

### B. 性能目标

| 指标 | 当前 | Phase 1 | Phase 2 | Phase 3 |
|------|------|---------|---------|---------|
| 吞吐量 | 5k/s | 10k/s | 20k/s | 50k/s |
| 延迟 (P99) | 100ms | 50ms | 30ms | 20ms |
| 可用性 | 99% | 99.9% | 99.95% | 99.99% |

### C. 团队配置建议

| 阶段 | 后端 | 前端 | SRE | 总计 |
|------|------|------|-----|------|
| Phase 1 | 3 | 0 | 1 | 4 |
| Phase 2 | 4 | 0 | 1 | 5 |
| Phase 3 | 3 | 0 | 2 | 5 |
| Phase 4 | 3 | 2 | 2 | 7 |

---

> 本文档为活文档，随项目演进持续更新
> 最后更新: 2024-01
