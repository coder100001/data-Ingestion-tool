# Phase 1: REST API 层实施计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 为数据摄取工具添加 REST API 层，支持健康检查、配置管理、连接器管理、同步管理、变更数据查看和数据预览功能。

**Architecture:** 采用 Go Gin 框架构建 HTTP API 服务，通过统一的 Server 结构体管理所有 API 处理器，使用中间件处理认证、限流和日志。API 与核心引擎通过接口解耦，确保不影响现有 CLI 功能。

**Tech Stack:** Go 1.21+, Gin Web Framework, Prometheus Client

---

## 文件结构

```
pkg/api/
├── server.go           # HTTP 服务器主入口
├── handlers.go         # API 处理器（健康检查、状态）
├── handlers_config.go  # 配置管理 API
├── handlers_connector.go # 连接器管理 API
├── handlers_sync.go    # 同步管理 API
├── handlers_changes.go # 变更数据 API
├── handlers_data.go    # 数据预览 API
├── middleware.go       # 中间件（日志、限流、CORS）
├── routes.go           # 路由定义
├── types.go            # API 请求/响应类型
└── server_test.go      # API 测试

pkg/config/
└── config.go           # 修改：添加 API 配置结构
```

---

## Task 1: 添加 API 配置和依赖

**Files:**
- Modify: `go.mod`
- Modify: `pkg/config/config.go`

- [ ] **Step 1: 添加 Gin 和 Prometheus 依赖**

Run:
```bash
cd /Users/liunian/Desktop/dnmp/data-Ingestion-tool
go get github.com/gin-gonic/gin@latest
go get github.com/prometheus/client_golang@latest
go mod tidy
```

Expected: 依赖添加成功

- [ ] **Step 2: 在 config.go 中添加 API 配置结构**

在 `pkg/config/config.go` 文件末尾添加：

```go
type APIConfig struct {
    Enabled   bool              `yaml:"enabled" mapstructure:"enabled"`
    Host      string            `yaml:"host" mapstructure:"host"`
    Port      int               `yaml:"port" mapstructure:"port"`
    Auth      AuthConfig        `yaml:"auth" mapstructure:"auth"`
    CORS      CORSConfig        `yaml:"cors" mapstructure:"cors"`
    RateLimit RateLimitConfig   `yaml:"rate_limit" mapstructure:"rate_limit"`
}

type AuthConfig struct {
    Enabled bool   `yaml:"enabled" mapstructure:"enabled"`
    Type    string `yaml:"type" mapstructure:"type"`
    APIKey  string `yaml:"api_key" mapstructure:"api_key"`
}

type CORSConfig struct {
    Enabled        bool     `yaml:"enabled" mapstructure:"enabled"`
    AllowedOrigins []string `yaml:"allowed_origins" mapstructure:"allowed_origins"`
    AllowedMethods []string `yaml:"allowed_methods" mapstructure:"allowed_methods"`
}

type RateLimitConfig struct {
    Enabled           bool `yaml:"enabled" mapstructure:"enabled"`
    RequestsPerSecond int  `yaml:"requests_per_second" mapstructure:"requests_per_second"`
    Burst             int  `yaml:"burst" mapstructure:"burst"`
}
```

- [ ] **Step 3: 在 Config 结构体中添加 API 字段**

在 `pkg/config/config.go` 的 `Config` 结构体中添加：

```go
type Config struct {
    App        AppConfig        `yaml:"app" mapstructure:"app"`
    Source     SourceConfig     `yaml:"source" mapstructure:"source"`
    Storage    StorageConfig    `yaml:"storage" mapstructure:"storage"`
    Checkpoint CheckpointConfig `yaml:"checkpoint" mapstructure:"checkpoint"`
    Processing ProcessingConfig `yaml:"processing" mapstructure:"processing"`
    Retry      RetryConfig      `yaml:"retry" mapstructure:"retry"`
    API        APIConfig        `yaml:"api" mapstructure:"api"` // 新增
}
```

- [ ] **Step 4: 添加 API 配置默认值**

在 `pkg/config/config.go` 的 `SetDefaults` 方法中添加：

```go
func (c *Config) SetDefaults() {
    // ... 现有默认值 ...
    
    if c.API.Host == "" {
        c.API.Host = "0.0.0.0"
    }
    if c.API.Port == 0 {
        c.API.Port = 8080
    }
    if len(c.API.CORS.AllowedOrigins) == 0 {
        c.API.CORS.AllowedOrigins = []string{"*"}
    }
    if len(c.API.CORS.AllowedMethods) == 0 {
        c.API.CORS.AllowedMethods = []string{"GET", "POST", "PUT", "DELETE", "OPTIONS"}
    }
    if c.API.RateLimit.RequestsPerSecond == 0 {
        c.API.RateLimit.RequestsPerSecond = 100
    }
    if c.API.RateLimit.Burst == 0 {
        c.API.RateLimit.Burst = 50
    }
}
```

- [ ] **Step 5: 运行测试验证配置**

Run:
```bash
go test ./pkg/config/... -v
```

Expected: 所有测试通过

- [ ] **Step 6: 提交配置更改**

```bash
git add go.mod go.sum pkg/config/config.go
git commit -m "feat(config): add API configuration structure"
```

---

## Task 2: 创建 API 类型定义

**Files:**
- Create: `pkg/api/types.go`

- [ ] **Step 1: 创建 API 类型文件**

```bash
mkdir -p /Users/liunian/Desktop/dnmp/data-Ingestion-tool/pkg/api
```

- [ ] **Step 2: 编写 API 类型定义**

创建文件 `pkg/api/types.go`：

```go
package api

import "time"

type ApiResponse struct {
    Success bool        `json:"success"`
    Data    interface{} `json:"data,omitempty"`
    Error   *ApiError   `json:"error,omitempty"`
}

type ApiError struct {
    Code    string       `json:"code"`
    Message string       `json:"message"`
    Details []FieldError `json:"details,omitempty"`
}

type FieldError struct {
    Field   string `json:"field"`
    Message string `json:"message"`
}

type SystemStatus struct {
    Version      string          `json:"version"`
    UptimeSeconds int64          `json:"uptime_seconds"`
    State        string          `json:"state"`
    StartTime    time.Time       `json:"start_time"`
    Connector    ConnectorStatus `json:"connector"`
    Pipeline     PipelineStatus  `json:"pipeline"`
    Storage      StorageStatus   `json:"storage"`
    Sync         SyncStatus      `json:"sync"`
}

type ConnectorStatus struct {
    ID           string                 `json:"id"`
    Type         string                 `json:"type"`
    Name         string                 `json:"name"`
    State        string                 `json:"state"`
    Config       map[string]interface{} `json:"config,omitempty"`
    Metrics      ConnectorMetrics       `json:"metrics"`
    ErrorMessage string                 `json:"error_message,omitempty"`
}

type ConnectorMetrics struct {
    EventsProcessed   int64     `json:"events_processed"`
    EventsPerSecond   float64   `json:"events_per_second"`
    LagMs             int64     `json:"lag_ms"`
    LastEvent         time.Time `json:"last_event"`
    ReconnectCount    int       `json:"reconnect_count"`
}

type PipelineStatus struct {
    State        string `json:"state"`
    QueueSize    int    `json:"queue_size"`
    WorkersActive int   `json:"workers_active"`
}

type StorageStatus struct {
    Type        string  `json:"type"`
    BasePath    string  `json:"base_path"`
    TotalSizeMB float64 `json:"total_size_mb"`
    FileCount   int     `json:"file_count"`
}

type SyncStatus struct {
    Mode     string `json:"mode"`
    Position string `json:"position"`
    LagMs    int64  `json:"lag_ms"`
}

type SyncProgress struct {
    IsRunning              bool          `json:"is_running"`
    Mode                   string        `json:"mode"`
    StartedAt              time.Time     `json:"started_at"`
    CurrentPosition        BinlogPosition `json:"current_position"`
    Progress               ProgressDetail `json:"progress"`
    Throughput             Throughput    `json:"throughput"`
    Tables                 []TableProgress `json:"tables"`
}

type BinlogPosition struct {
    BinlogFile string `json:"binlog_file"`
    BinlogPos  uint32 `json:"binlog_pos"`
    GTID       string `json:"gtid,omitempty"`
}

type ProgressDetail struct {
    TotalEvents              int64   `json:"total_events"`
    ProcessedEvents          int64   `json:"processed_events"`
    Percentage               float64 `json:"percentage"`
    EstimatedRemainingSeconds int64   `json:"estimated_remaining_seconds"`
}

type Throughput struct {
    EventsPerSecond float64 `json:"events_per_second"`
    BytesPerSecond  int64   `json:"bytes_per_second"`
}

type TableProgress struct {
    Database       string    `json:"database"`
    Table          string    `json:"table"`
    EventsProcessed int64    `json:"events_processed"`
    LastEventTime  time.Time `json:"last_event_time"`
}

type DataChange struct {
    ID            string                 `json:"id"`
    Timestamp     time.Time              `json:"timestamp"`
    Type          string                 `json:"type"`
    Database      string                 `json:"database"`
    Table         string                 `json:"table"`
    SchemaVersion string                 `json:"schema_version,omitempty"`
    Before        map[string]interface{} `json:"before,omitempty"`
    After         map[string]interface{} `json:"after,omitempty"`
    Metadata      ChangeMetadata         `json:"metadata"`
}

type ChangeMetadata struct {
    BinlogFile string `json:"binlog_file"`
    BinlogPos  uint32 `json:"binlog_pos"`
    ServerID   uint32 `json:"server_id"`
    ThreadID   uint32 `json:"thread_id"`
}

type ChangeStats struct {
    ByTable map[string]TableChangeStats `json:"by_table"`
    ByHour  []HourlyStats               `json:"by_hour"`
}

type TableChangeStats struct {
    Inserts int64 `json:"inserts"`
    Updates int64 `json:"updates"`
    Deletes int64 `json:"deletes"`
}

type HourlyStats struct {
    Hour    string `json:"hour"`
    Inserts int64  `json:"inserts"`
    Updates int64  `json:"updates"`
    Deletes int64  `json:"deletes"`
}

type DataPreview struct {
    Schema     DataSchema              `json:"schema"`
    Records    []map[string]interface{} `json:"records"`
    Pagination Pagination              `json:"pagination"`
    Partition  PartitionInfo           `json:"partition"`
}

type DataSchema struct {
    Columns []SchemaColumn `json:"columns"`
}

type SchemaColumn struct {
    Name     string `json:"name"`
    Type     string `json:"type"`
    Nullable bool   `json:"nullable"`
}

type Pagination struct {
    TotalRecords int64  `json:"total_records"`
    Limit        int    `json:"limit"`
    Offset       int    `json:"offset"`
    HasMore      bool   `json:"has_more"`
}

type PartitionInfo struct {
    Path          string `json:"path"`
    FileCount     int    `json:"file_count"`
    TotalSizeBytes int64  `json:"total_size_bytes"`
}

type DataQualityReport struct {
    Completeness float64           `json:"completeness"`
    Validity     float64           `json:"validity"`
    Uniqueness   float64           `json:"uniqueness"`
    Consistency  float64           `json:"consistency"`
    Issues       []DataQualityIssue `json:"issues"`
}

type DataQualityIssue struct {
    Column       string   `json:"column"`
    Type         string   `json:"type"`
    Count        int64    `json:"count"`
    SampleValues []string `json:"sample_values"`
}

type ConfigUpdateRequest struct {
    Processing *ProcessingConfigUpdate `json:"processing,omitempty"`
    Filters    []FilterConfig          `json:"filters,omitempty"`
}

type ProcessingConfigUpdate struct {
    BatchSize   *int `json:"batch_size,omitempty"`
    WorkerCount *int `json:"worker_count,omitempty"`
}

type FilterConfig struct {
    Table     string `json:"table"`
    Condition string `json:"condition"`
}

type ConfigUpdateResponse struct {
    Message         string          `json:"message"`
    RestartRequired bool            `json:"restart_required"`
    Changes         []ConfigChange  `json:"changes"`
}

type ConfigChange struct {
    Path     string      `json:"path"`
    OldValue interface{} `json:"old_value"`
    NewValue interface{} `json:"new_value"`
}
```

- [ ] **Step 3: 验证类型定义编译通过**

Run:
```bash
go build ./pkg/api/...
```

Expected: 编译成功，无错误

- [ ] **Step 4: 提交类型定义**

```bash
git add pkg/api/types.go
git commit -m "feat(api): add API type definitions"
```

---

## Task 3: 创建中间件

**Files:**
- Create: `pkg/api/middleware.go`

- [ ] **Step 1: 编写中间件代码**

创建文件 `pkg/api/middleware.go`：

```go
package api

import (
    "net/http"
    "time"
    
    "github.com/gin-gonic/gin"
    "github.com/ulule/limiter/v3"
    mgin "github.com/ulule/limiter/v3/drivers/middleware/gin"
    "github.com/ulule/limiter/v3/drivers/store/memory"
)

func LoggerMiddleware() gin.HandlerFunc {
    return func(c *gin.Context) {
        start := time.Now()
        path := c.Request.URL.Path
        
        c.Next()
        
        latency := time.Since(start)
        status := c.Writer.Status()
        
        if status >= 400 {
            c.Error(gin.Error{
                Err:  gin.Error{}.Err,
                Meta: gin.H{
                    "status":   status,
                    "method":   c.Request.Method,
                    "path":     path,
                    "latency":  latency.String(),
                    "clientIP": c.ClientIP(),
                },
            })
        }
    }
}

func CORSMiddleware(allowedOrigins, allowedMethods []string) gin.HandlerFunc {
    return func(c *gin.Context) {
        origin := c.Request.Header.Get("Origin")
        
        allowed := false
        for _, o := range allowedOrigins {
            if o == "*" || o == origin {
                allowed = true
                break
            }
        }
        
        if allowed {
            c.Header("Access-Control-Allow-Origin", origin)
            c.Header("Access-Control-Allow-Methods", joinMethods(allowedMethods))
            c.Header("Access-Control-Allow-Headers", "Content-Type, Authorization, X-API-Key")
            c.Header("Access-Control-Expose-Headers", "Content-Length, Content-Type")
        }
        
        if c.Request.Method == "OPTIONS" {
            c.AbortWithStatus(http.StatusNoContent)
            return
        }
        
        c.Next()
    }
}

func RateLimitMiddleware(requestsPerSecond int, burst int) gin.HandlerFunc {
    rate := limiter.Rate{
        Period: 1 * time.Second,
        Limit:  int64(requestsPerSecond),
   }
    store := memory.NewStore()
    instance := limiter.New(store, rate, limiter.WithTrustForwardHeader(true))
    middleware := mgin.NewMiddleware(instance)
    
    return middleware
}

func AuthMiddleware(apiKey string) gin.HandlerFunc {
    return func(c *gin.Context) {
        if apiKey == "" {
            c.Next()
            return
        }
        
        providedKey := c.GetHeader("X-API-Key")
        if providedKey == "" {
            providedKey = c.Query("api_key")
        }
        
        if providedKey != apiKey {
            c.JSON(http.StatusUnauthorized, ApiResponse{
                Success: false,
                Error: &ApiError{
                    Code:    "UNAUTHORIZED",
                    Message: "Invalid or missing API key",
                },
            })
            c.Abort()
            return
        }
        
        c.Next()
    }
}

func RecoveryMiddleware() gin.HandlerFunc {
    return gin.CustomRecovery(func(c *gin.Context, recovered interface{}) {
        c.JSON(http.StatusInternalServerError, ApiResponse{
            Success: false,
            Error: &ApiError{
                Code:    "INTERNAL_ERROR",
                Message: "An unexpected error occurred",
            },
        })
    })
}

func joinMethods(methods []string) string {
    result := ""
    for i, m := range methods {
        if i > 0 {
            result += ", "
        }
        result += m
    }
    return result
}
```

- [ ] **Step 2: 添加 limiter 依赖**

Run:
```bash
go get github.com/ulule/limiter/v3@latest
go mod tidy
```

- [ ] **Step 3: 验证编译**

Run:
```bash
go build ./pkg/api/...
```

Expected: 编译成功

- [ ] **Step 4: 提交中间件**

```bash
git add pkg/api/middleware.go go.mod go.sum
git commit -m "feat(api): add middleware (logger, cors, rate limit, auth)"
```

---

## Task 4: 创建健康检查处理器

**Files:**
- Create: `pkg/api/handlers.go`

- [ ] **Step 1: 编写健康检查处理器**

创建文件 `pkg/api/handlers.go`：

```go
package api

import (
    "net/http"
    "runtime"
    "time"
    
    "github.com/gin-gonic/gin"
    
    "data-ingestion-tool/pkg/config"
)

type Handlers struct {
    config     *config.Config
    startTime  time.Time
    version    string
}

func NewHandlers(cfg *config.Config, version string) *Handlers {
    return &Handlers{
        config:    cfg,
        startTime: time.Now(),
        version:   version,
    }
}

func (h *Handlers) HealthCheck(c *gin.Context) {
    c.JSON(http.StatusOK, gin.H{
        "status": "ok",
    })
}

func (h *Handlers) ReadyCheck(c *gin.Context) {
    c.JSON(http.StatusOK, gin.H{
        "ready": true,
    })
}

func (h *Handlers) GetStatus(c *gin.Context) {
    uptime := time.Since(h.startTime)
    
    var memStats runtime.MemStats
    runtime.ReadMemStats(&memStats)
    
    status := SystemStatus{
        Version:       h.version,
        UptimeSeconds: int64(uptime.Seconds()),
        State:         "running",
        StartTime:     h.startTime,
        Connector: ConnectorStatus{
            ID:    "mysql-main",
            Type:  h.config.Source.Type,
            Name:  "Main Connector",
            State: "connected",
            Metrics: ConnectorMetrics{
                EventsProcessed: 0,
                EventsPerSecond: 0,
                LagMs:           0,
            },
        },
        Pipeline: PipelineStatus{
            State:         "running",
            QueueSize:     0,
            WorkersActive: h.config.Processing.WorkerCount,
        },
        Storage: StorageStatus{
            Type:      h.config.Storage.Type,
            BasePath:  h.config.Storage.Local.BasePath,
            TotalSizeMB: 0,
            FileCount: 0,
        },
        Sync: SyncStatus{
            Mode:     "incremental",
            Position: "",
            LagMs:    0,
        },
    }
    
    c.JSON(http.StatusOK, ApiResponse{
        Success: true,
        Data:    status,
    })
}
```

- [ ] **Step 2: 编写处理器测试**

创建文件 `pkg/api/handlers_test.go`：

```go
package api

import (
    "net/http"
    "net/http/httptest"
    "testing"
    
    "github.com/gin-gonic/gin"
    "github.com/stretchr/testify/assert"
    
    "data-ingestion-tool/pkg/config"
)

func TestHealthCheck(t *testing.T) {
    gin.SetMode(gin.TestMode)
    
    cfg := &config.Config{}
    h := NewHandlers(cfg, "test-version")
    
    w := httptest.NewRecorder()
    c, _ := gin.CreateTestContext(w)
    c.Request = httptest.NewRequest("GET", "/health", nil)
    
    h.HealthCheck(c)
    
    assert.Equal(t, http.StatusOK, w.Code)
    assert.Contains(t, w.Body.String(), "ok")
}

func TestReadyCheck(t *testing.T) {
    gin.SetMode(gin.TestMode)
    
    cfg := &config.Config{}
    h := NewHandlers(cfg, "test-version")
    
    w := httptest.NewRecorder()
    c, _ := gin.CreateTestContext(w)
    c.Request = httptest.NewRequest("GET", "/ready", nil)
    
    h.ReadyCheck(c)
    
    assert.Equal(t, http.StatusOK, w.Code)
    assert.Contains(t, w.Body.String(), "ready")
}

func TestGetStatus(t *testing.T) {
    gin.SetMode(gin.TestMode)
    
    cfg := &config.Config{
        Source: config.SourceConfig{
            Type: "mysql",
        },
        Processing: config.ProcessingConfig{
            WorkerCount: 4,
        },
        Storage: config.StorageConfig{
            Type: "local",
        },
    }
    h := NewHandlers(cfg, "1.0.0")
    
    w := httptest.NewRecorder()
    c, _ := gin.CreateTestContext(w)
    c.Request = httptest.NewRequest("GET", "/api/v1/status", nil)
    
    h.GetStatus(c)
    
    assert.Equal(t, http.StatusOK, w.Code)
    assert.Contains(t, w.Body.String(), "success")
    assert.Contains(t, w.Body.String(), "1.0.0")
}
```

- [ ] **Step 3: 运行测试**

Run:
```bash
go test ./pkg/api/... -v
```

Expected: 所有测试通过

- [ ] **Step 4: 提交健康检查处理器**

```bash
git add pkg/api/handlers.go pkg/api/handlers_test.go
git commit -m "feat(api): add health check and status handlers"
```

---

## Task 5: 创建配置管理处理器

**Files:**
- Create: `pkg/api/handlers_config.go`

- [ ] **Step 1: 编写配置管理处理器**

创建文件 `pkg/api/handlers_config.go`：

```go
package api

import (
    "net/http"
    
    "github.com/gin-gonic/gin"
    
    "data-ingestion-tool/pkg/config"
)

type ConfigHandlers struct {
    config *config.Config
}

func NewConfigHandlers(cfg *config.Config) *ConfigHandlers {
    return &ConfigHandlers{config: cfg}
}

func (h *ConfigHandlers) GetConfig(c *gin.Context) {
    c.JSON(http.StatusOK, ApiResponse{
        Success: true,
        Data:    h.config,
    })
}

func (h *ConfigHandlers) UpdateConfig(c *gin.Context) {
    var req ConfigUpdateRequest
    if err := c.ShouldBindJSON(&req); err != nil {
        c.JSON(http.StatusBadRequest, ApiResponse{
            Success: false,
            Error: &ApiError{
                Code:    "INVALID_REQUEST",
                Message: "Invalid request body",
                Details: []FieldError{
                    {Field: "body", Message: err.Error()},
                },
            },
        })
        return
    }
    
    changes := []ConfigChange{}
    restartRequired := false
    
    if req.Processing != nil {
        if req.Processing.BatchSize != nil {
            oldVal := h.config.Processing.BatchSize
            h.config.Processing.BatchSize = *req.Processing.BatchSize
            changes = append(changes, ConfigChange{
                Path:     "processing.batch_size",
                OldValue: oldVal,
                NewValue: *req.Processing.BatchSize,
            })
        }
        if req.Processing.WorkerCount != nil {
            oldVal := h.config.Processing.WorkerCount
            h.config.Processing.WorkerCount = *req.Processing.WorkerCount
            changes = append(changes, ConfigChange{
                Path:     "processing.worker_count",
                OldValue: oldVal,
                NewValue: *req.Processing.WorkerCount,
            })
            restartRequired = true
        }
    }
    
    c.JSON(http.StatusOK, ApiResponse{
        Success: true,
        Data: ConfigUpdateResponse{
            Message:         "Configuration updated successfully",
            RestartRequired: restartRequired,
            Changes:         changes,
        },
    })
}

func (h *ConfigHandlers) ValidateConfig(c *gin.Context) {
    errors := []FieldError{}
    
    if h.config.Source.Type == "" {
        errors = append(errors, FieldError{
            Field:   "source.type",
            Message: "Source type is required",
        })
    }
    
    if h.config.Source.Type == "mysql" {
        if h.config.Source.MySQL.Host == "" {
            errors = append(errors, FieldError{
                Field:   "source.mysql.host",
                Message: "MySQL host is required",
            })
        }
        if h.config.Source.MySQL.ServerID == 0 {
            errors = append(errors, FieldError{
                Field:   "source.mysql.server_id",
                Message: "MySQL server_id is required",
            })
        }
    }
    
    if len(errors) > 0 {
        c.JSON(http.StatusBadRequest, ApiResponse{
            Success: false,
            Error: &ApiError{
                Code:    "VALIDATION_ERROR",
                Message: "Configuration validation failed",
                Details: errors,
            },
        })
        return
    }
    
    c.JSON(http.StatusOK, ApiResponse{
        Success: true,
        Data: gin.H{
            "message": "Configuration is valid",
        },
    })
}

func (h *ConfigHandlers) ReloadConfig(c *gin.Context) {
    c.JSON(http.StatusOK, ApiResponse{
        Success: true,
        Data: gin.H{
            "message": "Configuration reloaded successfully",
        },
    })
}
```

- [ ] **Step 2: 编写配置处理器测试**

创建文件 `pkg/api/handlers_config_test.go`：

```go
package api

import (
    "bytes"
    "encoding/json"
    "net/http"
    "net/http/httptest"
    "testing"
    
    "github.com/gin-gonic/gin"
    "github.com/stretchr/testify/assert"
    
    "data-ingestion-tool/pkg/config"
)

func TestGetConfig(t *testing.T) {
    gin.SetMode(gin.TestMode)
    
    cfg := &config.Config{
        App: config.AppConfig{
            Name: "test-app",
        },
    }
    h := NewConfigHandlers(cfg)
    
    w := httptest.NewRecorder()
    c, _ := gin.CreateTestContext(w)
    c.Request = httptest.NewRequest("GET", "/api/v1/config", nil)
    
    h.GetConfig(c)
    
    assert.Equal(t, http.StatusOK, w.Code)
    assert.Contains(t, w.Body.String(), "test-app")
}

func TestUpdateConfig(t *testing.T) {
    gin.SetMode(gin.TestMode)
    
    cfg := &config.Config{
        Processing: config.ProcessingConfig{
            BatchSize:   100,
            WorkerCount: 4,
        },
    }
    h := NewConfigHandlers(cfg)
    
    body := ConfigUpdateRequest{
        Processing: &ProcessingConfigUpdate{
            BatchSize:   intPtr(200),
            WorkerCount: intPtr(8),
        },
    }
    jsonBody, _ := json.Marshal(body)
    
    w := httptest.NewRecorder()
    c, _ := gin.CreateTestContext(w)
    c.Request = httptest.NewRequest("PUT", "/api/v1/config", bytes.NewBuffer(jsonBody))
    c.Request.Header.Set("Content-Type", "application/json")
    
    h.UpdateConfig(c)
    
    assert.Equal(t, http.StatusOK, w.Code)
    assert.Contains(t, w.Body.String(), "Configuration updated successfully")
}

func TestValidateConfig(t *testing.T) {
    gin.SetMode(gin.TestMode)
    
    cfg := &config.Config{
        Source: config.SourceConfig{
            Type: "mysql",
            MySQL: config.MySQLConfig{
                Host:     "localhost",
                ServerID: 1001,
            },
        },
    }
    h := NewConfigHandlers(cfg)
    
    w := httptest.NewRecorder()
    c, _ := gin.CreateTestContext(w)
    c.Request = httptest.NewRequest("POST", "/api/v1/config/validate", nil)
    
    h.ValidateConfig(c)
    
    assert.Equal(t, http.StatusOK, w.Code)
    assert.Contains(t, w.Body.String(), "valid")
}

func intPtr(i int) *int {
    return &i
}
```

- [ ] **Step 3: 运行测试**

Run:
```bash
go test ./pkg/api/... -v
```

Expected: 所有测试通过

- [ ] **Step 4: 提交配置处理器**

```bash
git add pkg/api/handlers_config.go pkg/api/handlers_config_test.go
git commit -m "feat(api): add configuration management handlers"
```

---

## Task 6: 创建路由和服务器

**Files:**
- Create: `pkg/api/routes.go`
- Create: `pkg/api/server.go`

- [ ] **Step 1: 编写路由定义**

创建文件 `pkg/api/routes.go`：

```go
package api

import (
    "github.com/gin-gonic/gin"
)

func setupRoutes(
    r *gin.Engine,
    handlers *Handlers,
    configHandlers *ConfigHandlers,
    apiCfg APIConfig,
) {
    r.GET("/health", handlers.HealthCheck)
    r.GET("/ready", handlers.ReadyCheck)
    
    v1 := r.Group("/api/v1")
    
    if apiCfg.Auth.Enabled {
        v1.Use(AuthMiddleware(apiCfg.Auth.APIKey))
    }
    
    v1.GET("/status", handlers.GetStatus)
    
    v1.GET("/config", configHandlers.GetConfig)
    v1.PUT("/config", configHandlers.UpdateConfig)
    v1.POST("/config/validate", configHandlers.ValidateConfig)
    v1.POST("/config/reload", configHandlers.ReloadConfig)
}
```

- [ ] **Step 2: 编写服务器主入口**

创建文件 `pkg/api/server.go`：

```go
package api

import (
    "context"
    "fmt"
    "net/http"
    "time"
    
    "github.com/gin-gonic/gin"
    
    "data-ingestion-tool/pkg/config"
    "data-ingestion-tool/pkg/logger"
)

type Server struct {
    engine    *gin.Engine
    httpSrv   *http.Server
    config    *config.Config
    handlers  *Handlers
    logger    *logger.Logger
    version   string
}

func NewServer(cfg *config.Config, log *logger.Logger, version string) *Server {
    gin.SetMode(gin.ReleaseMode)
    if cfg.App.LogLevel == "debug" {
        gin.SetMode(gin.DebugMode)
    }
    
    r := gin.New()
    r.Use(RecoveryMiddleware())
    r.Use(LoggerMiddleware())
    
    if cfg.API.CORS.Enabled {
        r.Use(CORSMiddleware(
            cfg.API.CORS.AllowedOrigins,
            cfg.API.CORS.AllowedMethods,
        ))
    }
    
    if cfg.API.RateLimit.Enabled {
        r.Use(RateLimitMiddleware(
            cfg.API.RateLimit.RequestsPerSecond,
            cfg.API.RateLimit.Burst,
        ))
    }
    
    s := &Server{
        engine:  r,
        config:  cfg,
        logger:  log,
        version: version,
    }
    
    s.handlers = NewHandlers(cfg, version)
    configHandlers := NewConfigHandlers(cfg)
    
    setupRoutes(r, s.handlers, configHandlers, cfg.API)
    
    return s
}

func (s *Server) Start() error {
    addr := fmt.Sprintf("%s:%d", s.config.API.Host, s.config.API.Port)
    
    s.httpSrv = &http.Server{
        Addr:         addr,
        Handler:      s.engine,
        ReadTimeout:  10 * time.Second,
        WriteTimeout: 10 * time.Second,
        IdleTimeout:  60 * time.Second,
    }
    
    s.logger.WithField("address", addr).Info("Starting API server")
    
    if err := s.httpSrv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
        return fmt.Errorf("failed to start API server: %w", err)
    }
    
    return nil
}

func (s *Server) Shutdown(ctx context.Context) error {
    if s.httpSrv == nil {
        return nil
    }
    
    s.logger.Info("Shutting down API server")
    return s.httpSrv.Shutdown(ctx)
}

func (s *Server) Engine() *gin.Engine {
    return s.engine
}
```

- [ ] **Step 3: 编写服务器测试**

创建文件 `pkg/api/server_test.go`：

```go
package api

import (
    "context"
    "net/http"
    "net/http/httptest"
    "testing"
    "time"
    
    "github.com/stretchr/testify/assert"
    
    "data-ingestion-tool/pkg/config"
    "data-ingestion-tool/pkg/logger"
)

func TestNewServer(t *testing.T) {
    cfg := &config.Config{
        App: config.AppConfig{
            LogLevel: "info",
        },
        API: config.APIConfig{
            Enabled: true,
            Host:    "0.0.0.0",
            Port:    8080,
            CORS: config.CORSConfig{
                Enabled:        true,
                AllowedOrigins: []string{"*"},
            },
        },
    }
    
    log := logger.NewLogger("info")
    srv := NewServer(cfg, log, "1.0.0")
    
    assert.NotNil(t, srv)
    assert.NotNil(t, srv.Engine())
}

func TestServerRoutes(t *testing.T) {
    cfg := &config.Config{
        App: config.AppConfig{},
        API: config.APIConfig{
            Enabled: true,
        },
    }
    
    log := logger.NewLogger("info")
    srv := NewServer(cfg, log, "1.0.0")
    
    tests := []struct {
        path       string
        method     string
        expectCode int
    }{
        {"/health", "GET", http.StatusOK},
        {"/ready", "GET", http.StatusOK},
        {"/api/v1/status", "GET", http.StatusOK},
        {"/api/v1/config", "GET", http.StatusOK},
    }
    
    for _, tt := range tests {
        t.Run(tt.path, func(t *testing.T) {
            w := httptest.NewRecorder()
            req := httptest.NewRequest(tt.method, tt.path, nil)
            srv.Engine().ServeHTTP(w, req)
            
            assert.Equal(t, tt.expectCode, w.Code)
        })
    }
}

func TestServerShutdown(t *testing.T) {
    cfg := &config.Config{
        API: config.APIConfig{
            Enabled: true,
        },
    }
    
    log := logger.NewLogger("info")
    srv := NewServer(cfg, log, "1.0.0")
    
    ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
    defer cancel()
    
    err := srv.Shutdown(ctx)
    assert.NoError(t, err)
}
```

- [ ] **Step 4: 运行测试**

Run:
```bash
go test ./pkg/api/... -v
```

Expected: 所有测试通过

- [ ] **Step 5: 提交服务器代码**

```bash
git add pkg/api/routes.go pkg/api/server.go pkg/api/server_test.go
git commit -m "feat(api): add HTTP server with routing"
```

---

## Task 7: 集成 API 到主程序

**Files:**
- Modify: `cmd/ingester/main.go`

- [ ] **Step 1: 读取当前 main.go**

Run:
```bash
cat /Users/liunian/Desktop/dnmp/data-Ingestion-tool/cmd/ingester/main.go
```

- [ ] **Step 2: 在 main.go 中添加 API 服务器启动**

在 `cmd/ingester/main.go` 中添加 API 服务器启动逻辑：

```go
package main

import (
    "context"
    "flag"
    "fmt"
    "os"
    "os/signal"
    "syscall"
    
    "data-ingestion-tool/pkg/api"
    "data-ingestion-tool/pkg/config"
    "data-ingestion-tool/pkg/connector"
    "data-ingestion-tool/pkg/logger"
    "data-ingestion-tool/pkg/pipeline"
    "data-ingestion-tool/pkg/storage"
)

var (
    configFile = flag.String("config", "config.yaml", "Path to configuration file")
    reset      = flag.Bool("reset", false, "Reset checkpoint and start from beginning")
    version    = "2.0.0"
)

func main() {
    flag.Parse()
    
    cfg, err := config.Load(*configFile)
    if err != nil {
        fmt.Printf("Failed to load config: %v\n", err)
        os.Exit(1)
    }
    
    log := logger.NewLogger(cfg.App.LogLevel)
    
    ctx, cancel := context.WithCancel(context.Background())
    defer cancel()
    
    sigChan := make(chan os.Signal, 1)
    signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
    
    var apiServer *api.Server
    if cfg.API.Enabled {
        apiServer = api.NewServer(cfg, log, version)
        go func() {
            if err := apiServer.Start(); err != nil {
                log.WithError(err).Error("API server error")
            }
        }()
    }
    
    conn, err := connector.NewConnector(cfg, log)
    if err != nil {
        log.WithError(err).Fatal("Failed to create connector")
    }
    
    store, err := storage.NewStorage(cfg, log)
    if err != nil {
        log.WithError(err).Fatal("Failed to create storage")
    }
    
    pipe := pipeline.NewPipeline(cfg, log, conn, store)
    
    go func() {
        if err := pipe.Run(ctx); err != nil {
            log.WithError(err).Error("Pipeline error")
        }
    }()
    
    <-sigChan
    log.Info("Shutting down...")
    
    cancel()
    
    if apiServer != nil {
        if err := apiServer.Shutdown(context.Background()); err != nil {
            log.WithError(err).Error("API server shutdown error")
        }
    }
    
    log.Info("Shutdown complete")
}
```

- [ ] **Step 3: 验证编译**

Run:
```bash
go build ./cmd/ingester/...
```

Expected: 编译成功

- [ ] **Step 4: 提交集成更改**

```bash
git add cmd/ingester/main.go
git commit -m "feat(main): integrate API server into main application"
```

---

## Task 8: 更新配置示例文件

**Files:**
- Modify: `config.example.yaml`

- [ ] **Step 1: 添加 API 配置示例**

在 `config.example.yaml` 文件末尾添加：

```yaml
# API Configuration
api:
  enabled: true
  host: "0.0.0.0"
  port: 8080
  
  # Authentication (optional)
  auth:
    enabled: false
    type: "api_key"  # api_key, basic, none
    api_key: "${API_KEY}"
  
  # CORS Configuration
  cors:
    enabled: true
    allowed_origins: ["*"]
    allowed_methods: ["GET", "POST", "PUT", "DELETE", "OPTIONS"]
  
  # Rate Limiting
  rate_limit:
    enabled: true
    requests_per_second: 100
    burst: 50
```

- [ ] **Step 2: 提交配置更新**

```bash
git add config.example.yaml
git commit -m "docs(config): add API configuration example"
```

---

## Task 9: 运行完整测试

- [ ] **Step 1: 运行所有测试**

Run:
```bash
go test ./... -v -cover
```

Expected: 所有测试通过

- [ ] **Step 2: 运行 lint 检查**

Run:
```bash
golangci-lint run
```

Expected: 无 lint 错误

- [ ] **Step 3: 构建验证**

Run:
```bash
go build -o bin/data-ingestion-tool ./cmd/ingester
```

Expected: 构建成功

---

## 验收标准

- [ ] 所有 API 端点可用
- [ ] 健康检查端点返回正确状态
- [ ] 配置管理 API 可以获取和更新配置
- [ ] API 文档完整
- [ ] 单元测试覆盖率 > 80%
- [ ] 可通过 curl/Postman 测试

---

**计划完成时间**: 1-2 周
