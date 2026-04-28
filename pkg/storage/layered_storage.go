package storage

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"data-ingestion-tool/pkg/logger"
	"data-ingestion-tool/pkg/models"
)

// LayerType represents the storage layer type
type LayerType string

const (
	// BronzeLayer raw data layer
	BronzeLayer LayerType = "bronze"
	// SilverLayer cleaned data layer
	SilverLayer LayerType = "silver"
	// GoldLayer aggregated data layer
	GoldLayer LayerType = "gold"
)

// LayeredStorage implements a multi-layer data lake architecture
type LayeredStorage struct {
	basePath string
	catalog  *DataCatalog
	logger   *logger.Logger
	layers   map[LayerType]*LayerConfig
}

// LayerConfig represents configuration for a storage layer
type LayerConfig struct {
	Type           LayerType
	Format         string
	Compression    string
	PartitionBy    []string
	RetentionDays  int
	EnableIndexing bool
}

// BronzeRecord represents raw data in bronze layer
type BronzeRecord struct {
	ID            string                 `json:"_id"`
	IngestedAt    time.Time              `json:"_ingested_at"`
	Source        string                 `json:"_source"`
	RawData       map[string]interface{} `json:"_raw_data"`
	BinlogInfo    *BinlogInfo            `json:"_binlog_info,omitempty"`
	SchemaVersion int                    `json:"_schema_version"`
}

// BinlogInfo represents MySQL binlog information
type BinlogInfo struct {
	File string `json:"file"`
	Pos  uint32 `json:"pos"`
}

// SilverRecord represents cleaned data in silver layer
type SilverRecord struct {
	ID           string                 `json:"_id"`
	IngestedAt   time.Time              `json:"_ingested_at"`
	ProcessedAt  time.Time              `json:"_processed_at"`
	Source       string                 `json:"_source"`
	CleanedData  map[string]interface{} `json:"_cleaned_data"`
	QualityScore float64                `json:"_quality_score"`
	Validation   *ValidationResult      `json:"_validation,omitempty"`
}

// ValidationResult represents data validation result
type ValidationResult struct {
	Valid    bool     `json:"valid"`
	Errors   []string `json:"errors,omitempty"`
	Warnings []string `json:"warnings,omitempty"`
}

// GoldRecord represents aggregated data in gold layer
type GoldRecord struct {
	ID           string                 `json:"_id"`
	AggregatedAt time.Time              `json:"_aggregated_at"`
	Grain        string                 `json:"_grain"`
	Metrics      map[string]interface{} `json:"_metrics"`
	Dimensions   map[string]interface{} `json:"_dimensions"`
	SourceTables []string               `json:"_source_tables"`
}

// NewLayeredStorage creates a new layered storage instance
func NewLayeredStorage(basePath string, catalog *DataCatalog, logger *logger.Logger) *LayeredStorage {
	return &LayeredStorage{
		basePath: basePath,
		catalog:  catalog,
		logger:   logger,
		layers: map[LayerType]*LayerConfig{
			BronzeLayer: {
				Type:           BronzeLayer,
				Format:         "json",
				Compression:    "none",
				PartitionBy:    []string{"date"},
				RetentionDays:  30,
				EnableIndexing: false,
			},
			SilverLayer: {
				Type:           SilverLayer,
				Format:         "parquet",
				Compression:    "zstd",
				PartitionBy:    []string{"date", "database"},
				RetentionDays:  90,
				EnableIndexing: true,
			},
			GoldLayer: {
				Type:           GoldLayer,
				Format:         "parquet",
				Compression:    "zstd",
				PartitionBy:    []string{"date", "grain"},
				RetentionDays:  365,
				EnableIndexing: true,
			},
		},
	}
}

// Initialize initializes the layered storage
func (s *LayeredStorage) Initialize() error {
	// Create layer directories
	for layerType := range s.layers {
		layerPath := filepath.Join(s.basePath, string(layerType))
		if err := os.MkdirAll(layerPath, 0755); err != nil {
			return fmt.Errorf("failed to create %s layer directory: %w", layerType, err)
		}
	}

	s.logger.Info("Layered storage initialized")
	return nil
}

// WriteToBronze writes raw data to bronze layer
func (s *LayeredStorage) WriteToBronze(change *models.DataChange) error {
	_ = s.layers[BronzeLayer] // layer config available for future use

	record := BronzeRecord{
		ID:         change.ID,
		IngestedAt: change.Timestamp,
		Source:     change.Source,
		RawData: map[string]interface{}{
			"type":     change.Type,
			"database": change.Database,
			"table":    change.Table,
			"before":   change.Before,
			"after":    change.After,
			"schema":   change.Schema,
		},
		SchemaVersion: 1,
	}

	if change.BinlogFile != "" {
		record.BinlogInfo = &BinlogInfo{
			File: change.BinlogFile,
			Pos:  change.BinlogPos,
		}
	}

	// Build path: bronze/date/database/table/
	partition := time.Now().UTC().Format("2006-01-02")
	path := filepath.Join(s.basePath, string(BronzeLayer), partition, change.Database, change.Table)

	if err := os.MkdirAll(path, 0755); err != nil {
		return fmt.Errorf("failed to create directory: %w", err)
	}

	// Write to file
	filename := fmt.Sprintf("bronze_%s_%d.json", change.ID, time.Now().UnixNano())
	filePath := filepath.Join(path, filename)

	data, err := json.Marshal(record)
	if err != nil {
		return fmt.Errorf("failed to marshal record: %w", err)
	}

	if err := os.WriteFile(filePath, data, 0644); err != nil {
		return fmt.Errorf("failed to write file: %w", err)
	}

	// Update catalog
	partitionInfo := PartitionInfo{
		Values: map[string]string{
			"date": partition,
		},
		Path:      path,
		FileCount: 1,
		SizeBytes: int64(len(data)),
		CreatedAt: time.Now().UTC(),
	}

	if err := s.catalog.AddPartition(change.Database, change.Table, partitionInfo); err != nil {
		s.logger.WithError(err).Warn("Failed to update catalog partition")
	}

	s.logger.WithFields(map[string]interface{}{
		"layer":    BronzeLayer,
		"database": change.Database,
		"table":    change.Table,
		"path":     filePath,
	}).Debug("Written to bronze layer")

	return nil
}

// WriteToSilver writes cleaned data to silver layer
func (s *LayeredStorage) WriteToSilver(database, table string, data map[string]interface{}, quality float64) error {
	layer := s.layers[SilverLayer]

	record := SilverRecord{
		ID:           fmt.Sprintf("silver_%d", time.Now().UnixNano()),
		IngestedAt:   time.Now().UTC(),
		ProcessedAt:  time.Now().UTC(),
		Source:       fmt.Sprintf("bronze.%s.%s", database, table),
		CleanedData:  data,
		QualityScore: quality,
		Validation: &ValidationResult{
			Valid: quality > 0.8,
		},
	}

	// Build path: silver/date/database/table/
	partition := time.Now().UTC().Format("2006-01-02")
	path := filepath.Join(s.basePath, string(SilverLayer), partition, database, table)

	if err := os.MkdirAll(path, 0755); err != nil {
		return fmt.Errorf("failed to create directory: %w", err)
	}

	// Write to file (JSON for now, can be Parquet)
	filename := fmt.Sprintf("silver_%s.json", record.ID)
	filePath := filepath.Join(path, filename)

	recordData, err := json.Marshal(record)
	if err != nil {
		return fmt.Errorf("failed to marshal record: %w", err)
	}

	if err := os.WriteFile(filePath, recordData, 0644); err != nil {
		return fmt.Errorf("failed to write file: %w", err)
	}

	s.logger.WithFields(map[string]interface{}{
		"layer":       SilverLayer,
		"database":    database,
		"table":       table,
		"quality":     quality,
		"format":      layer.Format,
		"compression": layer.Compression,
	}).Debug("Written to silver layer")

	return nil
}

// WriteToGold writes aggregated data to gold layer
func (s *LayeredStorage) WriteToGold(grain string, dimensions, metrics map[string]interface{}, sources []string) error {
	layer := s.layers[GoldLayer]

	record := GoldRecord{
		ID:           fmt.Sprintf("gold_%d", time.Now().UnixNano()),
		AggregatedAt: time.Now().UTC(),
		Grain:        grain,
		Metrics:      metrics,
		Dimensions:   dimensions,
		SourceTables: sources,
	}

	// Build path: gold/date/grain/
	partition := time.Now().UTC().Format("2006-01-02")
	path := filepath.Join(s.basePath, string(GoldLayer), partition, grain)

	if err := os.MkdirAll(path, 0755); err != nil {
		return fmt.Errorf("failed to create directory: %w", err)
	}

	// Write to file
	filename := fmt.Sprintf("gold_%s.json", record.ID)
	filePath := filepath.Join(path, filename)

	data, err := json.Marshal(record)
	if err != nil {
		return fmt.Errorf("failed to marshal record: %w", err)
	}

	if err := os.WriteFile(filePath, data, 0644); err != nil {
		return fmt.Errorf("failed to write file: %w", err)
	}

	s.logger.WithFields(map[string]interface{}{
		"layer":       GoldLayer,
		"grain":       grain,
		"format":      layer.Format,
		"compression": layer.Compression,
	}).Debug("Written to gold layer")

	return nil
}

// GetLayerPath returns the base path for a layer
func (s *LayeredStorage) GetLayerPath(layer LayerType) string {
	return filepath.Join(s.basePath, string(layer))
}

// GetLayerConfig returns configuration for a layer
func (s *LayeredStorage) GetLayerConfig(layer LayerType) *LayerConfig {
	return s.layers[layer]
}

// CleanExpiredData removes expired data based on retention policy
func (s *LayeredStorage) CleanExpiredData() error {
	for layerType, config := range s.layers {
		if config.RetentionDays <= 0 {
			continue
		}

		layerPath := filepath.Join(s.basePath, string(layerType))
		cutoffDate := time.Now().UTC().AddDate(0, 0, -config.RetentionDays)

		entries, err := os.ReadDir(layerPath)
		if err != nil {
			if os.IsNotExist(err) {
				continue
			}
			return fmt.Errorf("failed to read %s layer: %w", layerType, err)
		}

		for _, entry := range entries {
			if !entry.IsDir() {
				continue
			}

			// Parse date from directory name (YYYY-MM-DD)
			date, err := time.Parse("2006-01-02", entry.Name())
			if err != nil {
				continue
			}

			if date.Before(cutoffDate) {
				oldPath := filepath.Join(layerPath, entry.Name())
				if err := os.RemoveAll(oldPath); err != nil {
					s.logger.WithError(err).WithField("path", oldPath).Warn("Failed to remove expired data")
				} else {
					s.logger.WithField("path", oldPath).Info("Removed expired data")
				}
			}
		}
	}

	return nil
}

// GetStorageStats returns statistics for all layers
func (s *LayeredStorage) GetStorageStats() map[LayerType]*LayerStats {
	stats := make(map[LayerType]*LayerStats)

	for layerType := range s.layers {
		layerPath := filepath.Join(s.basePath, string(layerType))
		layerStats := &LayerStats{}

		err := filepath.Walk(layerPath, func(path string, info os.FileInfo, err error) error {
			if err != nil {
				return err
			}
			if !info.IsDir() {
				layerStats.TotalFiles++
				layerStats.TotalSizeBytes += info.Size()
			}
			return nil
		})

		if err != nil {
			s.logger.WithError(err).WithField("layer", layerType).Warn("Failed to calculate layer stats")
		}

		stats[layerType] = layerStats
	}

	return stats
}

// LayerStats represents statistics for a storage layer
type LayerStats struct {
	TotalFiles     int64 `json:"total_files"`
	TotalSizeBytes int64 `json:"total_size_bytes"`
}

// Ensure LayeredStorage implements the common Storage interface pattern
var _ interface {
	Write(change *models.DataChange) error
	Flush() error
	Close() error
} = (*LayeredStorage)(nil)

// Write implements Storage interface (delegates to bronze layer)
func (s *LayeredStorage) Write(change *models.DataChange) error {
	return s.WriteToBronze(change)
}

// Flush implements Storage interface
func (s *LayeredStorage) Flush() error {
	// No-op for now, can be extended for batch flushing
	return nil
}

// Close implements Storage interface
func (s *LayeredStorage) Close() error {
	// Clean up resources
	return nil
}
