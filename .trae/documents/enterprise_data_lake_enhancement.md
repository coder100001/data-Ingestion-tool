# 企业级数据湖增强计划

## 目标
将当前数据摄取工具的数据湖存储层升级为企业级架构，支持列式存储、数据压缩和 Schema 演化。

## 背景
当前数据湖使用 JSON 格式存储，存在以下问题：
- 存储效率低（JSON 冗余字段名）
- 查询性能差（无法列裁剪）
- 无压缩支持（存储成本高）
- Schema 变更困难（无版本管理）

## 实施步骤

### Phase 1: Parquet 格式支持

#### 1.1 依赖引入
- 添加 `github.com/apache/arrow/go/v15` 到 go.mod
- 添加 `github.com/xitongsys/parquet-go` 作为备选方案
- 执行 `go mod tidy` 确保依赖可用

#### 1.2 核心类型定义
创建 `pkg/storage/parquet/` 目录：
- `writer.go` - Parquet 文件写入器
- `reader.go` - Parquet 文件读取器  
- `schema.go` - Parquet Schema 转换
- `types.go` - 类型映射（Go类型 ↔ Parquet类型）

#### 1.3 Schema 转换逻辑
实现自动将 Go struct / map 转换为 Parquet Schema：
```go
// 示例映射
string -> UTF8
int64  -> INT64
float64-> DOUBLE
bool   -> BOOLEAN
time.Time -> TIMESTAMP
```

#### 1.4 写入器实现
- 支持批量写入（batch write）
- 内存缓冲（buffer）
- 自动 flush（按大小/时间）

#### 1.5 与现有 Storage 接口集成
修改 `LocalStorage` 和 `LayeredStorage`：
- 新增 `format: "parquet"` 配置支持
- `Write()` 方法根据格式选择 JSON 或 Parquet 写入

### Phase 2: 数据压缩

#### 2.1 压缩算法选择
| 算法 | 压缩比 | 速度 | 使用场景 |
|------|--------|------|----------|
| Zstd | 高 | 快 | Silver/Gold 层默认 |
| Snappy | 中 | 极快 | Bronze 层可选 |
| Gzip | 最高 | 慢 | 归档场景 |

#### 2.2 实现压缩包装器
创建 `pkg/storage/compression/`：
- `zstd.go` - Zstd 压缩/解压
- `snappy.go` - Snappy 压缩/解压
- `compressor.go` - 通用接口

#### 2.3 Parquet 内置压缩
利用 Parquet 原生压缩支持：
```go
writer.CompressionType = parquet.CompressionCodec_ZSTD
```

#### 2.4 独立文件压缩
对非 Parquet 文件（如 JSON）提供透明压缩：
- 写入时自动压缩
- 读取时自动解压
- 文件扩展名标记 `.json.zst`

### Phase 3: Schema 演化

#### 3.1 Schema 注册中心
创建 `pkg/storage/schema/`：
- `registry.go` - Schema 注册与管理
- `version.go` - 版本控制
- `compatibility.go` - 兼容性检查

#### 3.2 支持格式
- **Avro Schema** - 用于 Kafka 生态兼容
- **Protobuf** - 用于高性能场景
- **JSON Schema** - 用于简单场景

#### 3.3 兼容性策略
| 策略 | 说明 | 使用场景 |
|------|------|----------|
| BACKWARD | 新代码可读旧数据 | 默认推荐 |
| FORWARD | 旧代码可读新数据 | 滚动升级 |
| FULL | 双向兼容 | 严格场景 |
| NONE | 无兼容保证 | 内部使用 |

#### 3.4 Schema 变更检测
- 自动检测上游 Schema 变更
- 触发兼容性检查
- 记录 Schema 演进历史到 Catalog

#### 3.5 数据迁移
- 不兼容变更时自动数据迁移
- 支持字段重命名映射
- 默认值填充

### Phase 4: 集成与测试

#### 4.1 配置更新
更新 `config.yaml` 支持新选项：
```yaml
storage:
  local:
    format: "parquet"          # json / csv / parquet
    compression: "zstd"        # none / snappy / zstd / gzip
    parquet:
      row_group_size: 10000
      page_size: 8192
      enable_dictionary: true
    schema:
      registry_path: "./metadata/schemas"
      compatibility: "backward"
      auto_register: true
```

#### 4.2 演示程序更新
更新 `demo/main_demo.go`：
- 展示 Parquet 写入
- 展示压缩效果对比
- 展示 Schema 演化

#### 4.3 测试覆盖
- 单元测试：Parquet 读写
- 单元测试：压缩/解压
- 单元测试：Schema 兼容性
- 集成测试：端到端数据流

## 文件变更清单

### 新增文件
```
pkg/storage/parquet/
  ├── writer.go
  ├── reader.go
  ├── schema.go
  └── types.go

pkg/storage/compression/
  ├── compressor.go
  ├── zstd.go
  └── snappy.go

pkg/storage/schema/
  ├── registry.go
  ├── version.go
  └── compatibility.go

test/
  ├── parquet_test.go
  ├── compression_test.go
  └── schema_test.go
```

### 修改文件
```
pkg/config/config.go          - 新增配置项
pkg/pipeline/pipeline.go      - 集成压缩
pkg/storage/layered_storage.go - 支持 Parquet
pkg/storage/local_storage.go   - 支持压缩
demo/main_demo.go             - 演示新功能
```

## 验收标准

1. **Parquet 支持**
   - [ ] 可配置使用 Parquet 格式
   - [ ] 支持所有基础数据类型
   - [ ] 文件可被 pandas/pyarrow 读取

2. **压缩支持**
   - [ ] Zstd 压缩集成
   - [ ] 压缩比达到 3:1 以上（相比 JSON）
   - [ ] 透明压缩/解压

3. **Schema 演化**
   - [ ] Schema 自动注册
   - [ ] 兼容性检查通过
   - [ ] 历史版本可追溯

## 风险评估

| 风险 | 概率 | 影响 | 缓解措施 |
|------|------|------|----------|
| Arrow 库体积大 | 高 | 中 | 使用 parquet-go 轻量替代 |
| CGO 依赖 | 中 | 高 | 优先纯 Go 实现 |
| 向后兼容 | 低 | 高 | 保留 JSON 格式作为 fallback |

## 时间线

| Phase | 工作量 | 依赖 |
|-------|--------|------|
| Phase 1: Parquet | 2-3 天 | 无 |
| Phase 2: 压缩 | 1-2 天 | Phase 1 |
| Phase 3: Schema | 2-3 天 | Phase 1 |
| Phase 4: 集成测试 | 1-2 天 | Phase 2,3 |
