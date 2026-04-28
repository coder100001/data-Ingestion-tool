package storage

import (
	"fmt"
	"path/filepath"
	"sync"
	"time"

	"data-ingestion-tool/pkg/config"
	"data-ingestion-tool/pkg/logger"
	"data-ingestion-tool/pkg/models"
	"data-ingestion-tool/pkg/storage/compression"
	"data-ingestion-tool/pkg/storage/parquet"
	"data-ingestion-tool/pkg/storage/schema"
)

// Manager manages all storage operations
type Manager struct {
	config     *config.LocalConfig
	logger     *logger.Logger
	basePath   string
	compressor compression.Compressor
	catalog    *DataCatalog
	registry   *schema.Registry

	// Writers per table
	writers   map[string]*parquet.Writer
	writersMu sync.Mutex

	// JSON fallback
	jsonStorage *LocalStorage
}

// NewManager creates a new storage manager
func NewManager(cfg *config.LocalConfig, logger *logger.Logger) (*Manager, error) {
	// Create compressor
	var codec compression.Codec
	switch cfg.Compression {
	case "snappy":
		codec = compression.Snappy
	case "gzip":
		codec = compression.Gzip
	case "zstd":
		codec = compression.Zstd
	default:
		codec = compression.None
	}

	compressor, err := compression.NewCompressor(codec)
	if err != nil {
		return nil, fmt.Errorf("failed to create compressor: %w", err)
	}

	// Create catalog
	catalog := NewDataCatalog(cfg.BasePath, logger)
	if err := catalog.Initialize(); err != nil {
		return nil, fmt.Errorf("failed to initialize catalog: %w", err)
	}

	// Create schema registry
	registry := schema.NewRegistry(cfg.Schema.RegistryPath, logger)
	if err := registry.Initialize(); err != nil {
		return nil, fmt.Errorf("failed to initialize schema registry: %w", err)
	}

	// Create JSON fallback storage
	jsonStorage, err := NewLocalStorage(cfg, logger)
	if err != nil {
		return nil, fmt.Errorf("failed to create JSON storage: %w", err)
	}

	return &Manager{
		config:      cfg,
		logger:      logger,
		basePath:    cfg.BasePath,
		compressor:  compressor,
		catalog:     catalog,
		registry:    registry,
		writers:     make(map[string]*parquet.Writer),
		jsonStorage: jsonStorage,
	}, nil
}

// Write writes a data change to storage
func (m *Manager) Write(change *models.DataChange) error {
	switch m.config.FileFormat {
	case "parquet":
		return m.writeParquet(change)
	case "json", "csv":
		return m.writeJSON(change)
	default:
		return fmt.Errorf("unsupported format: %s", m.config.FileFormat)
	}
}

// writeParquet writes data in Parquet format
func (m *Manager) writeParquet(change *models.DataChange) error {
	tableKey := fmt.Sprintf("%s.%s", change.Database, change.Table)

	m.writersMu.Lock()
	defer m.writersMu.Unlock()

	// Get or create writer
	writer, exists := m.writers[tableKey]
	if !exists {
		// Infer schema from data
		data := change.After
		if change.Type == models.Delete {
			data = change.Before
		}

		if data == nil {
			return fmt.Errorf("no data to infer schema")
		}

		pschema := parquet.NewSchemaFromMap(data)

		// Register schema
		if m.config.Schema.AutoRegister {
			compatibility := schema.Backward
			switch m.config.Schema.Compatibility {
			case "forward":
				compatibility = schema.Forward
			case "full":
				compatibility = schema.Full
			case "none":
				compatibility = schema.None
			}

			if _, regErr := m.registry.Register(tableKey, pschema, compatibility); regErr != nil {
				m.logger.WithError(regErr).Warn("Failed to register schema")
			}
		}

		// Create writer
		partition := time.Now().UTC().Format("2006-01-02")
		path := filepath.Join(m.basePath, partition, change.Database, tableKey, fmt.Sprintf("data_%d.parquet", time.Now().Unix()))

		newWriter, err := parquet.NewWriter(path, pschema, m.logger)
		if err != nil {
			return fmt.Errorf("failed to create parquet writer: %w", err)
		}
		writer = newWriter

		m.writers[tableKey] = writer

		// Register table in catalog
		schemaVersion := &SchemaVersion{
			Version: 1,
			Columns: make([]ColumnInfo, 0, len(pschema.Columns)),
		}
		for _, col := range pschema.Columns {
			schemaVersion.Columns = append(schemaVersion.Columns, ColumnInfo{
				Name: col.Name,
				Type: col.Type.String(),
			})
		}
		if _, catErr := m.catalog.RegisterTable(change.Database, change.Table, change.Source, schemaVersion); catErr != nil {
			m.logger.WithError(catErr).Warn("Failed to register table in catalog")
		}
	}

	// Prepare row data
	row := make(map[string]interface{})
	if change.After != nil {
		for k, v := range change.After {
			row[k] = v
		}
	}

	// Add metadata columns
	row["_id"] = change.ID
	row["_timestamp"] = change.Timestamp
	row["_type"] = string(change.Type)
	row["_source"] = change.Source
	row["_database"] = change.Database
	row["_table"] = change.Table

	// Write row
	if err := writer.WriteRow(row); err != nil {
		return fmt.Errorf("failed to write row: %w", err)
	}

	return nil
}

// writeJSON writes data in JSON format with optional compression
func (m *Manager) writeJSON(change *models.DataChange) error {
	if m.config.Compression == "none" {
		return m.jsonStorage.Write(change)
	}

	// Write with compression
	return m.writeCompressedJSON(change)
}

// writeCompressedJSON writes compressed JSON data
// TODO: Implement actual compression for JSON format
func (m *Manager) writeCompressedJSON(change *models.DataChange) error {
	m.logger.Warn("Compressed JSON storage is not yet implemented, writing uncompressed data")
	return m.jsonStorage.Write(change)
}

// Flush flushes all pending data
func (m *Manager) Flush() error {
	m.writersMu.Lock()
	defer m.writersMu.Unlock()

	// Flush all parquet writers
	for key, writer := range m.writers {
		if err := writer.Flush(); err != nil {
			m.logger.WithError(err).WithField("table", key).Warn("Failed to flush writer")
		}
	}

	// Flush JSON storage
	if err := m.jsonStorage.Flush(); err != nil {
		return err
	}

	return nil
}

// Close closes all resources
func (m *Manager) Close() error {
	m.writersMu.Lock()
	defer m.writersMu.Unlock()

	// Close all parquet writers
	for key, writer := range m.writers {
		if err := writer.Close(); err != nil {
			m.logger.WithError(err).WithField("table", key).Warn("Failed to close writer")
		}
	}

	// Close JSON storage
	if err := m.jsonStorage.Close(); err != nil {
		return err
	}

	return nil
}

// GetCatalog returns the data catalog
func (m *Manager) GetCatalog() *DataCatalog {
	return m.catalog
}

// GetRegistry returns the schema registry
func (m *Manager) GetRegistry() *schema.Registry {
	return m.registry
}

// GetStats returns storage statistics
func (m *Manager) GetStats() *StorageStats {
	stats := &StorageStats{
		Tables: make(map[string]*TableStats),
	}

	m.writersMu.Lock()
	defer m.writersMu.Unlock()

	for key, writer := range m.writers {
		stats.Tables[key] = &TableStats{
			NumRows: writer.NumRows(),
			Path:    writer.Path(),
		}
	}

	return stats
}

// StorageStats represents storage statistics
type StorageStats struct {
	Tables map[string]*TableStats `json:"tables"`
}

// TableStats represents table statistics
type TableStats struct {
	NumRows int64  `json:"num_rows"`
	Path    string `json:"path"`
}

// RotateWriters rotates all active writers (for partitioning)
func (m *Manager) RotateWriters() error {
	m.writersMu.Lock()
	defer m.writersMu.Unlock()

	for key, writer := range m.writers {
		if err := writer.Close(); err != nil {
			m.logger.WithError(err).WithField("table", key).Warn("Failed to close writer during rotation")
		}
		delete(m.writers, key)
	}

	m.logger.Info("All writers rotated")
	return nil
}

// Ensure Manager implements the common Storage interface pattern
var _ interface {
	Write(change *models.DataChange) error
	Flush() error
	Close() error
} = (*Manager)(nil)
