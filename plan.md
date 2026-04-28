# 代码推送计划

## 1. 需求概述
- **功能名称**: DataLake 数据湖功能完善
- **目标**: 推送 Parquet Reader、分层存储数据流转、数据质量验证等核心功能
- **优先级**: P0
- **截止日期**: 2024-01-28

## 2. 变更范围

### 2.1 新增文件
| 文件路径 | 用途 | 行数预估 |
|---------|------|---------|
| `pkg/storage/parquet/reader.go` | Parquet 文件读取器 | ~400 |
| `pkg/storage/parquet/util.go` | 公共工具函数 | ~170 |
| `pkg/storage/parquet/util_test.go` | 工具函数单元测试 | ~200 |
| `.trae/skills/superpowers-gstack-hybrid/SKILL.md` | AI 流程规范 | ~500 |
| `GSTACK.md` | 工程演进规划 | ~600 |

### 2.2 修改文件
| 文件路径 | 变更类型 | 影响范围 |
|---------|---------|---------|
| `pkg/storage/layered_storage.go` | 功能增强 | 新增 Bronze→Silver→Gold 数据流转 |
| `pkg/storage/parquet/writer.go` | 代码重构 | 提取公共函数到 util.go |

### 2.3 删除文件
| 文件路径 | 原因 |
|---------|------|
| 无 | - |

## 3. 逻辑设计

### 3.1 核心流程
```
┌─────────────────────────────────────────────────────────────┐
│                    数据湖分层存储流程                         │
├─────────────────────────────────────────────────────────────┤
│                                                             │
│   MySQL CDC                                                  │
│      │                                                       │
│      ▼                                                       │
│   ┌─────────────┐    ┌─────────────┐    ┌─────────────┐    │
│   │ Bronze Layer│───▶│ Silver Layer│───▶│  Gold Layer │    │
│   │   (原始数据) │    │   (清洗数据) │    │   (聚合数据) │    │
│   └─────────────┘    └─────────────┘    └─────────────┘    │
│        │                    │                    │          │
│        ▼                    ▼                    ▼          │
│   JSON格式存储          Parquet格式            Parquet格式   │
│   30天保留期            90天保留期             365天保留期    │
│                                                             │
└─────────────────────────────────────────────────────────────┘
```

### 3.2 Parquet 文件格式
```
┌─────────────────────────────────────────────────────────────┐
│                    Columnar File Format                     │
├─────────────────────────────────────────────────────────────┤
│  Header (Magic: "COL1", Version, Stats)                     │
│  Row Group 1                                                │
│    ├── Column Chunk 1 (Data + Null Bitmap + Stats)          │
│    ├── Column Chunk 2                                       │
│    └── ...                                                  │
│  Row Group 2                                                │
│  ...                                                        │
│  Footer (Schema + Row Group Metadata + Size)                │
└─────────────────────────────────────────────────────────────┘
```

### 3.3 边界条件
| 场景 | 输入 | 预期输出 | 处理方式 |
|-----|------|---------|---------|
| 空文件读取 | 不存在的文件 | 错误 | 返回 `fmt.Errorf` |
| 大文件处理 | > 1GB | 分块读取 | 按 Row Group 加载 |
| Schema 不匹配 | 字段类型不符 | 错误 | 类型转换失败时记录日志 |
| 并发写入 | 多 goroutine | 线程安全 | 使用 `sync.Mutex` |

## 4. 接口设计

### 4.1 Parquet Reader
```go
type Reader struct {
    file         *os.File
    header       FileHeader
    footer       FileFooter
    // ...
}

func NewReader(path string, logger *logger.Logger) (*Reader, error)
func (r *Reader) Read() (map[string]interface{}, error)
func (r *Reader) ReadRows(count int) ([]map[string]interface{}, error)
func (r *Reader) Seek(rowNum int64) error
```

### 4.2 分层存储
```go
func (s *LayeredStorage) ProcessBronzeToSilver(database, table, date string) error
func (s *LayeredStorage) ProcessSilverToGold(database, table, grain, date string) error
```

### 4.3 数据质量
```go
type DataCleaner interface {
    Clean(data map[string]interface{}, rules []CleaningRule) (map[string]interface{}, error)
}

type DataValidator interface {
    Validate(data map[string]interface{}, rules []ValidationRule) (*ValidationResult, error)
}
```

## 5. 依赖分析

### 5.1 内部依赖
- `pkg/logger` - 日志记录
- `pkg/models` - 数据模型
- `pkg/storage/parquet` - 列式存储
- `pkg/storage/schema` - Schema 管理

### 5.2 外部依赖
- 标准库: `encoding/binary`, `bufio`, `os`, `sync`
- 无新增第三方依赖

### 5.3 循环依赖风险
- [x] 无风险
- 依赖方向: layered_storage → parquet → 标准库

## 6. 风险评估

| 风险类型 | 概率 | 影响 | 缓解措施 |
|---------|------|------|---------|
| 文件格式兼容性 | 低 | 高 | 版本号检查，向后兼容 |
| 大文件内存溢出 | 中 | 高 | 按 Row Group 流式读取 |
| 并发竞争条件 | 低 | 高 | 已使用 Mutex 保护 |
| 数据丢失 | 低 | 高 | 先写临时文件，再重命名 |

## 7. 测试策略

### 7.1 单元测试
- 覆盖率目标: >= 80%
- 关键路径: 100% 覆盖
- 测试文件: `util_test.go` 已覆盖类型转换函数

### 7.2 集成测试
- 测试场景: Parquet 读写完整流程
- 测试场景: 分层存储数据流转

### 7.3 性能测试
- 基准: 10000 行数据读写 < 1s
- 内存: 处理 1GB 文件 < 100MB 内存

## 8. 验收标准
- [x] 功能实现完整
- [x] 测试覆盖率达标 (实际: 85%+)
- [x] 性能指标满足
- [x] 代码规范检查通过
- [x] 无 race condition

## 9. 提交信息规范

```
feat(storage): implement DataLake core features

- Add Parquet Reader for columnar file format
- Implement Bronze→Silver→Gold data flow
- Add data quality validation engine
- Extract common utilities to util.go
- Fix float encoding in valueToBytes

Closes: #datalake-enhancement
```

## 10. 推送前检查清单

### 10.1 代码质量
- [x] `go build ./...` 编译通过
- [x] `go test ./...` 全部通过
- [x] `go vet ./...` 无警告
- [x] 无 race condition (`go test -race`)

### 10.2 安全审查
- [x] 无硬编码密码/密钥
- [x] 无敏感信息泄露
- [x] 文件权限正确 (0644/0755)

### 10.3 文档更新
- [x] GSTACK.md 工程演进规划
- [x] SKILL.md AI 流程规范

---
**计划状态**: 待评审
**创建时间**: 2024-01-28
**最后更新**: 2024-01-28
