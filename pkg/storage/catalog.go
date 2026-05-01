package storage

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"data-ingestion-tool/pkg/logger"
)

// DataCatalog manages metadata for the data lake
type DataCatalog struct {
	mu       sync.RWMutex
	basePath string
	logger   *logger.Logger
	tables   map[string]*TableMetadata
}

// TableMetadata represents metadata for a table
type TableMetadata struct {
	TableName     string                 `json:"table_name"`
	Database      string                 `json:"database"`
	Source        string                 `json:"source"`
	Schema        *SchemaVersion         `json:"schema"`
	SchemaHistory []SchemaVersion        `json:"schema_history"`
	Partitions    []PartitionInfo        `json:"partitions"`
	Statistics    *TableStatistics       `json:"statistics"`
	Lineage       []LineageInfo          `json:"lineage"`
	Quality       *DataQuality           `json:"quality"`
	CreatedAt     time.Time              `json:"created_at"`
	UpdatedAt     time.Time              `json:"updated_at"`
	Properties    map[string]interface{} `json:"properties"`
}

// SchemaVersion represents a schema version
type SchemaVersion struct {
	Version     int          `json:"version"`
	Columns     []ColumnInfo `json:"columns"`
	PrimaryKey  []string     `json:"primary_key"`
	PartitionBy []string     `json:"partition_by"`
	CreatedAt   time.Time    `json:"created_at"`
	Checksum    string       `json:"checksum"`
}

// CatalogColumnInfo represents column metadata in the data catalog.
// Note: This is distinct from models.ColumnInfo which is used for CDC event schema.
type ColumnInfo struct {
	Name        string `json:"name"`
	Type        string `json:"type"`
	Nullable    bool   `json:"nullable"`
	Default     string `json:"default,omitempty"`
	Description string `json:"description,omitempty"`
}

// PartitionInfo represents partition metadata
type PartitionInfo struct {
	Values      map[string]string `json:"values"`
	Path        string            `json:"path"`
	FileCount   int               `json:"file_count"`
	RecordCount int64             `json:"record_count"`
	SizeBytes   int64             `json:"size_bytes"`
	MinTime     time.Time         `json:"min_time"`
	MaxTime     time.Time         `json:"max_time"`
	CreatedAt   time.Time         `json:"created_at"`
}

// TableStatistics represents table statistics
type TableStatistics struct {
	TotalFiles     int64                  `json:"total_files"`
	TotalRecords   int64                  `json:"total_records"`
	TotalSizeBytes int64                  `json:"total_size_bytes"`
	ColumnStats    map[string]ColumnStats `json:"column_stats"`
	LastAnalyzedAt time.Time              `json:"last_analyzed_at"`
}

// ColumnStats represents column-level statistics
type ColumnStats struct {
	NullCount     int64       `json:"null_count"`
	DistinctCount int64       `json:"distinct_count"`
	MinValue      interface{} `json:"min_value,omitempty"`
	MaxValue      interface{} `json:"max_value,omitempty"`
	AvgValue      interface{} `json:"avg_value,omitempty"`
}

// LineageInfo represents data lineage
type LineageInfo struct {
	Source         string                 `json:"source"`
	Transformation string                 `json:"transformation"`
	Timestamp      time.Time              `json:"timestamp"`
	Properties     map[string]interface{} `json:"properties"`
}

// DataQuality represents data quality metrics
type DataQuality struct {
	Completeness  float64        `json:"completeness"`
	Uniqueness    float64        `json:"uniqueness"`
	Validity      float64        `json:"validity"`
	Timeliness    float64        `json:"timeliness"`
	Consistency   float64        `json:"consistency"`
	Checks        []QualityCheck `json:"checks"`
	LastCheckedAt time.Time      `json:"last_checked_at"`
}

// QualityCheck represents a single quality check
type QualityCheck struct {
	Name        string  `json:"name"`
	Passed      bool    `json:"passed"`
	Score       float64 `json:"score"`
	Description string  `json:"description"`
}

// NewDataCatalog creates a new data catalog
func NewDataCatalog(basePath string, logger *logger.Logger) *DataCatalog {
	return &DataCatalog{
		basePath: basePath,
		logger:   logger,
		tables:   make(map[string]*TableMetadata),
	}
}

// Initialize initializes the catalog
func (c *DataCatalog) Initialize() error {
	catalogPath := filepath.Join(c.basePath, "_catalog")
	if err := os.MkdirAll(catalogPath, 0755); err != nil {
		return fmt.Errorf("failed to create catalog directory: %w", err)
	}

	// Load existing catalog
	if err := c.Load(); err != nil {
		c.logger.WithError(err).Warn("Failed to load existing catalog, starting fresh")
	}

	c.logger.Info("Data catalog initialized")
	return nil
}

// RegisterTable registers a new table in the catalog
func (c *DataCatalog) RegisterTable(database, table, source string, schema *SchemaVersion) (*TableMetadata, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	key := fmt.Sprintf("%s.%s", database, table)

	now := time.Now().UTC()
	metadata := &TableMetadata{
		TableName:     table,
		Database:      database,
		Source:        source,
		Schema:        schema,
		SchemaHistory: []SchemaVersion{*schema},
		Partitions:    make([]PartitionInfo, 0),
		Statistics:    &TableStatistics{ColumnStats: make(map[string]ColumnStats)},
		Lineage:       make([]LineageInfo, 0),
		Quality:       &DataQuality{Checks: make([]QualityCheck, 0)},
		CreatedAt:     now,
		UpdatedAt:     now,
		Properties:    make(map[string]interface{}),
	}

	c.tables[key] = metadata

	if err := c.saveTable(key, metadata); err != nil {
		return nil, fmt.Errorf("failed to save table metadata: %w", err)
	}

	c.logger.WithField("table", key).Info("Table registered in catalog")
	return metadata, nil
}

// UpdateSchema updates the schema for a table
func (c *DataCatalog) UpdateSchema(database, table string, newSchema *SchemaVersion) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	key := fmt.Sprintf("%s.%s", database, table)
	metadata, exists := c.tables[key]
	if !exists {
		return fmt.Errorf("table not found: %s", key)
	}

	// Add to history
	newSchema.Version = len(metadata.SchemaHistory) + 1
	newSchema.CreatedAt = time.Now().UTC()
	metadata.SchemaHistory = append(metadata.SchemaHistory, *newSchema)
	metadata.Schema = newSchema
	metadata.UpdatedAt = time.Now().UTC()

	// Add lineage
	metadata.Lineage = append(metadata.Lineage, LineageInfo{
		Source:         "schema_update",
		Transformation: fmt.Sprintf("schema_v%d", newSchema.Version),
		Timestamp:      time.Now().UTC(),
		Properties: map[string]interface{}{
			"previous_version": newSchema.Version - 1,
		},
	})

	if err := c.saveTable(key, metadata); err != nil {
		return fmt.Errorf("failed to save updated schema: %w", err)
	}

	c.logger.WithFields(map[string]interface{}{
		"table":   key,
		"version": newSchema.Version,
	}).Info("Schema updated")

	return nil
}

// AddPartition adds a partition to a table
func (c *DataCatalog) AddPartition(database, table string, partition PartitionInfo) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	key := fmt.Sprintf("%s.%s", database, table)
	metadata, exists := c.tables[key]
	if !exists {
		return fmt.Errorf("table not found: %s", key)
	}

	// Check if partition already exists
	for i, p := range metadata.Partitions {
		if p.Path == partition.Path {
			metadata.Partitions[i] = partition
			metadata.UpdatedAt = time.Now().UTC()
			return c.saveTable(key, metadata)
		}
	}

	metadata.Partitions = append(metadata.Partitions, partition)
	metadata.UpdatedAt = time.Now().UTC()

	// Update statistics
	metadata.Statistics.TotalFiles += int64(partition.FileCount)
	metadata.Statistics.TotalRecords += partition.RecordCount
	metadata.Statistics.TotalSizeBytes += partition.SizeBytes

	if err := c.saveTable(key, metadata); err != nil {
		return fmt.Errorf("failed to save partition: %w", err)
	}

	return nil
}

// UpdateStatistics updates table statistics
func (c *DataCatalog) UpdateStatistics(database, table string, stats *TableStatistics) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	key := fmt.Sprintf("%s.%s", database, table)
	metadata, exists := c.tables[key]
	if !exists {
		return fmt.Errorf("table not found: %s", key)
	}

	metadata.Statistics = stats
	metadata.UpdatedAt = time.Now().UTC()

	if err := c.saveTable(key, metadata); err != nil {
		return fmt.Errorf("failed to save statistics: %w", err)
	}

	return nil
}

// UpdateQuality updates data quality metrics
func (c *DataCatalog) UpdateQuality(database, table string, quality *DataQuality) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	key := fmt.Sprintf("%s.%s", database, table)
	metadata, exists := c.tables[key]
	if !exists {
		return fmt.Errorf("table not found: %s", key)
	}

	metadata.Quality = quality
	metadata.UpdatedAt = time.Now().UTC()

	if err := c.saveTable(key, metadata); err != nil {
		return fmt.Errorf("failed to save quality metrics: %w", err)
	}

	c.logger.WithFields(map[string]interface{}{
		"table":        key,
		"completeness": quality.Completeness,
		"validity":     quality.Validity,
	}).Info("Data quality updated")

	return nil
}

// GetTable returns table metadata
func (c *DataCatalog) GetTable(database, table string) (*TableMetadata, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	key := fmt.Sprintf("%s.%s", database, table)
	metadata, exists := c.tables[key]
	if !exists {
		return nil, fmt.Errorf("table not found: %s", key)
	}

	return metadata, nil
}

// ListTables returns all registered tables
func (c *DataCatalog) ListTables() []string {
	c.mu.RLock()
	defer c.mu.RUnlock()

	tables := make([]string, 0, len(c.tables))
	for key := range c.tables {
		tables = append(tables, key)
	}
	return tables
}

// saveTable saves table metadata to disk
func (c *DataCatalog) saveTable(key string, metadata *TableMetadata) error {
	catalogPath := filepath.Join(c.basePath, "_catalog")
	filePath := filepath.Join(catalogPath, fmt.Sprintf("%s.json", key))

	// Create directory if needed
	dir := filepath.Dir(filePath)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return err
	}

	data, err := json.MarshalIndent(metadata, "", "  ")
	if err != nil {
		return err
	}

	return os.WriteFile(filePath, data, 0644)
}

// Load loads the catalog from disk
func (c *DataCatalog) Load() error {
	catalogPath := filepath.Join(c.basePath, "_catalog")

	entries, err := os.ReadDir(catalogPath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}

	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}

		filePath := filepath.Join(catalogPath, entry.Name())
		data, err := os.ReadFile(filePath)
		if err != nil {
			c.logger.WithError(err).WithField("file", filePath).Warn("Failed to read catalog file")
			continue
		}

		var metadata TableMetadata
		if err := json.Unmarshal(data, &metadata); err != nil {
			c.logger.WithError(err).WithField("file", filePath).Warn("Failed to parse catalog file")
			continue
		}

		key := fmt.Sprintf("%s.%s", metadata.Database, metadata.TableName)
		c.tables[key] = &metadata
	}

	c.logger.WithField("count", len(c.tables)).Info("Catalog loaded")
	return nil
}

// GetPartitionStats returns statistics for a specific partition
func (c *DataCatalog) GetPartitionStats(database, table, partitionPath string) (*PartitionInfo, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	key := fmt.Sprintf("%s.%s", database, table)
	metadata, exists := c.tables[key]
	if !exists {
		return nil, fmt.Errorf("table not found: %s", key)
	}

	for _, p := range metadata.Partitions {
		if p.Path == partitionPath {
			return &p, nil
		}
	}

	return nil, fmt.Errorf("partition not found: %s", partitionPath)
}
