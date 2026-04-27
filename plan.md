# Data Ingestion Tool - 架构设计文档

> 本文档与代码保持强一致性，描述 MySQL CDC 到本地数据湖的增量数据同步工具。

## 1. 项目概览

**项目名称**: data-ingestion-tool  
**目标**: 从 MySQL 监听 binlog 事件，解析并写入本地数据湖（支持 JSON/CSV/Parquet 格式）  
**核心能力**: 增量同步、断点续传、多种数据源支持

---

## 2. 现有代码结构

```
data-Ingestion-tool/
├── config.yaml              # YAML 配置文件
├── go.mod                   # Go 模块定义
└── pkg/
    ├── config/
    │   └── config.go        # 配置加载与验证
    ├── connector/
    │   ├── connector.go     # 连接器接口与工厂
    │   └── mysql.go         # MySQL CDC 连接器 (待实现)
    ├── logger/
    │   └── logger.go        # 日志封装 (logrus)
    └── models/
        └── models.go        # 数据模型定义
```

---

## 3. 模块架构

### 3.1 模块职责

| 模块 | 路径 | 职责 |
|------|------|------|
| **config** | `pkg/config/` | 加载 YAML 配置，支持 MySQL/Kafka/PostgreSQL/REST |
| **connector** | `pkg/connector/` | 数据源连接器，监听 CDC 事件 |
| **logger** | `pkg/logger/` | 统一日志输出，支持文件+控制台 |
| **models** | `pkg/models/` | 数据模型：DataChange、Checkpoint、Position |
| **cdc** | `pkg/cdc/` | *(待实现)* binlog 事件处理 |
| **parser** | `pkg/parser/` | *(待实现)* 事件解析与转换 |
| **checkpoint** | `pkg/checkpoint/` | *(待实现)* 同步位置持久化 |
| **storage** | `pkg/storage/` | *(待实现)* 数据湖写入 |
| **cli** | `cmd/` | *(待实现)* Cobra CLI 命令 |

### 3.2 数据流

```
MySQL → Connector(CDC) → Parser → Storage(Parquet/JSON/CSV)
              ↓
         Checkpoint
```

---

## 4. 现有模型定义 (`pkg/models/models.go`)

### 4.1 DataChange - 数据变更事件

```go
type DataChange struct {
    ID        string                 // 唯一ID (时间戳格式)
    Timestamp time.Time              // 事件时间
    Source    string                 // 数据源
    Type      ChangeType             // INSERT/UPDATE/DELETE
    Database  string                 // 数据库名
    Table     string                 // 表名
    Schema    map[string]interface{} // 表结构
    Before    map[string]interface{} // 变更前数据
    After     map[string]interface{} // 变更后数据
    BinlogFile string                // binlog 文件名
    BinlogPos  uint32                // binlog 位置
}
```

### 4.2 Position - 同步位置

```go
type Position struct {
    BinlogFile string    // MySQL binlog 文件
    BinlogPos  uint32     // binlog 偏移量
    Topic      string    // Kafka topic
    Partition  int32      // Kafka 分区
    Offset     int64      // Kafka 偏移
    LSN        string    // PostgreSQL LSN
    Timestamp  time.Time // 通用时间戳
}
```

### 4.3 Checkpoint - 断点信息

```go
type Checkpoint struct {
    SourceType string    // 源类型
    Position   Position  // 当前位置
    UpdatedAt  time.Time // 更新时间
}
```

---

## 5. 配置结构 (`pkg/config/config.go`)

### 5.1 顶层配置

```go
type Config struct {
    App        AppConfig        // 应用设置
    Source     SourceConfig     // 数据源配置
    Storage    StorageConfig    // 存储配置
    Checkpoint CheckpointConfig // 断点配置
    Processing ProcessingConfig // 处理配置
}
```

### 5.2 数据源配置

```go
type SourceConfig struct {
    Type  string  // "mysql" | "kafka" | "postgresql" | "rest"
    MySQL MySQLConfig
    Kafka KafkaConfig
    PostgreSQL PostgresConfig
    REST   RESTConfig
}

type MySQLConfig struct {
    Host          string   // 主机地址
    Port          int      // 端口 (默认 3306)
    User          string   // 用户名
    Password      string   // 密码
    ServerID      uint32   // 复制服务器ID
    Tables        []string // 监听表列表
    ExcludeTables []string // 排除表列表
    BinlogFile    string   // 起始 binlog 文件
    BinlogPos     uint32   // 起始 binlog 位置
}
```

### 5.3 存储配置

```go
type StorageConfig struct {
    Type  string     // "local"
    Local LocalConfig
}

type LocalConfig struct {
    BasePath           string // 数据湖根目录 (默认 "./data-lake")
    PartitionStrategy  string // "date" | "hour" | "none"
    FileFormat         string // "json" | "csv" | "parquet"
    MaxFileSizeMB      int    // 最大文件大小 MB
    MaxRecordsPerFile  int    // 最大记录数
}
```

---

## 6. 待实现模块设计

### 6.1 CDC 模块 (`pkg/cdc/`)

**依赖**: `github.com/go-mysql-org/go-mysql v1.7.0`

```go
// cdc/canal.go
type CanalCDC struct {
    canal   *canal.Canal
    cfg     *config.MySQLConfig
    handler *CDCEventHandler
}

// CDCEventHandler 实现了 canal.EventHandler 接口
type CDCEventHandler struct {
    changeChan chan<- *models.DataChange
    logger     *logger.Logger
}
```

**事件处理流程**:
1. RowEvent → 根据 DML 类型构建 DataChange
2. DDL Event → 记录 Schema 变更
3. 将事件发送到 Parser 模块

### 6.2 Parser 模块 (`pkg/parser/`)

```go
// parser/parser.go
type EventParser struct {
    schemaCache map[string]*models.TableInfo
}

// ParseRowEvent 将 binlog 行事件解析为 DataChange
func (p *EventParser) ParseRowEvent(...) (*models.DataChange, error)

// InferSchema 从 INSERT 事件推断表结构
func (p *EventParser) InferSchema(...) error
```

### 6.3 Checkpoint 模块 (`pkg/checkpoint/`)

```go
// checkpoint/manager.go
type Manager struct {
    storagePath string
    checkpoint  *models.Checkpoint
    ticker      *time.Ticker
    mu          sync.RWMutex
}

// Load 从文件加载断点
func (m *Manager) Load() error

// Save 持久化断点到文件
func (m *Manager) Save() error

// UpdatePosition 更新同步位置
func (m *Manager) UpdatePosition(pos models.Position)
```

**存储格式**: JSON 文件
```json
{
  "source_type": "mysql",
  "position": {
    "binlog_file": "mysql-bin.000001",
    "binlog_pos": 1234
  },
  "updated_at": "2026-04-27T14:00:00Z"
}
```

### 6.4 Storage 模块 (`pkg/storage/`)

```go
// storage/writer.go
type LakeWriter struct {
    basePath   string
    format     string        // json | csv | parquet
    partition  string        // date | hour | none
    writers    map[string]FileWriter
    mu         sync.Mutex
}

type FileWriter interface {
    Write(records []*models.DataChange) error
    Close() error
    ShouldRotate() bool
}
```

**分区目录结构**:
```
data-lake/
└── orders/
    └── 2026-04-27/
        └── orders_0001.json
```

### 6.5 CLI 模块 (`cmd/`)

```go
// cmd/root.go
var rootCmd = &cobra.Command{
    Use:   "dit",
    Short: "Data Ingestion Tool",
}

// cmd/start.go
var startCmd = &cobra.Command{
    Use:   "start",
    Run:   runStart,
}

// cmd/status.go
var statusCmd = &cobra.Command{
    Use:   "status",
    Run:   runStatus,
}

// cmd/checkpoint.go
var checkpointCmd = &cobra.Command{
    Use:   "checkpoint",
    Subcommands: []*cobra.Command{
        {Name: "show", Run: showCheckpoint},
        {Name: "reset", Run: resetCheckpoint},
    },
}
```

---

## 7. 依赖关系

```
go.mod
├── github.com/go-mysql-org/go-mysql v1.7.0  # MySQL CDC
├── github.com/sirupsen/logrus v1.9.3        # 日志
├── gopkg.in/yaml.v3 v3.0.1                  # 配置解析
├── github.com/spf13/cobra v1.8.0            # CLI (待添加)
├── github.com/parquet-go/parquet-go         # Parquet 写入 (待添加)
└── github.com/google/uuid v1.3.0            # UUID (间接依赖)
```

---

## 8. 错误处理策略

| 场景 | 处理策略 |
|------|----------|
| MySQL 连接失败 | 重试 3 次，指数退避 |
| Binlog 位置无效 | 回退到最早的 binlog |
| 写入失败 | 重试 + 落盘缓冲 |
| 解析异常 | 跳过并记录日志 |

---

## 9. 实现优先级

| 优先级 | 模块 | 说明 |
|--------|------|------|
| P0 | CLI + Main | 命令行入口 |
| P1 | CDC | MySQL binlog 监听 |
| P1 | Storage | JSON 文件写入 |
| P2 | Checkpoint | 断点续传 |
| P2 | Parser | 事件解析 |
| P3 | Storage | CSV/Parquet 支持 |

---

## 10. 配置示例

```yaml
# config.yaml
app:
  name: "data-ingestion-tool"
  log_level: "info"
  log_file: "logs/ingestion.log"

source:
  type: "mysql"
  mysql:
    host: "localhost"
    port: 3306
    user: "root"
    password: "password"
    server_id: 1001
    tables:
      - "testdb.orders"

storage:
  type: "local"
  local:
    base_path: "./data-lake"
    partition_strategy: "date"
    file_format: "json"
    max_records_per_file: 10000

checkpoint:
  storage_path: "./metadata/checkpoint.json"
  save_interval_sec: 10

processing:
  batch_size: 100
  worker_count: 4
```

---

## 11. 验证计划

1. **单元测试**: 各模块独立测试
2. **集成测试**: 启动 MySQL，执行 DML，验证数据湖输出
3. **断点验证**: 停止后重启，确认从断点继续
4. **性能测试**: 10K/100K/1M 记录写入性能

---

*文档版本: v1.0*  
*生成时间: 2026-04-27*  
*与代码一致性: pkg/config/config.go, pkg/models/models.go, pkg/connector/connector.go*
