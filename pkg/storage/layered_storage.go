package storage

import (
	"fmt"
	"os"
	"path/filepath"
	"time"

	"data-ingestion-tool/pkg/logger"
	"data-ingestion-tool/pkg/models"
)

// LayeredStorage manages the three data layers (bronze, silver, gold)
type LayeredStorage struct {
	basePath      string
	logger        *logger.Logger
	layers        map[LayerType]*LayerConfig
	cleaner       DataCleaner
	validator     DataValidator
	aggregator    DataAggregator
	catalog       *DataCatalog
	processTicker *time.Ticker
	processDone   chan struct{}
}

// NewLayeredStorage creates a new LayeredStorage instance
func NewLayeredStorage(basePath string, log *logger.Logger, catalog *DataCatalog, layers []LayerConfig) *LayeredStorage {
	ls := &LayeredStorage{
		basePath:   basePath,
		logger:     log,
		layers:     make(map[LayerType]*LayerConfig),
		cleaner:    &DefaultDataCleaner{},
		validator:  &DefaultDataValidator{},
		aggregator: &DefaultDataAggregator{},
		catalog:    catalog,
	}

	for i := range layers {
		ls.layers[layers[i].Type] = &layers[i]
	}

	return ls
}

// SetCleaner sets the data cleaner implementation
func (s *LayeredStorage) SetCleaner(cleaner DataCleaner) {
	s.cleaner = cleaner
}

// SetValidator sets the data validator implementation
func (s *LayeredStorage) SetValidator(validator DataValidator) {
	s.validator = validator
}

// SetAggregator sets the data aggregator implementation
func (s *LayeredStorage) SetAggregator(aggregator DataAggregator) {
	s.aggregator = aggregator
}

// Initialize creates the layer directories
func (s *LayeredStorage) Initialize() error {
	for layerType, config := range s.layers {
		path := filepath.Join(s.basePath, string(layerType))
		if err := os.MkdirAll(path, 0755); err != nil {
			return fmt.Errorf("failed to create %s layer directory: %w", layerType, err)
		}
		_ = config
		s.logger.WithFields(map[string]interface{}{
			"layer": layerType,
			"path":  path,
		}).Debug("Initialized layer directory")
	}
	return nil
}

func (s *LayeredStorage) Write(change *models.DataChange) error {
	if change == nil {
		return fmt.Errorf("nil data change")
	}

	// Write to bronze layer only — Silver/Gold processing is handled by background timer
	if err := s.WriteToBronze(change); err != nil {
		return fmt.Errorf("bronze layer write failed: %w", err)
	}

	return nil
}

// StartProcessing starts the background goroutine that periodically processes
// Bronze→Silver and Silver→Gold layers
func (s *LayeredStorage) StartProcessing(interval time.Duration) {
	if s.processTicker != nil {
		return // already started
	}
	s.processTicker = time.NewTicker(interval)
	s.processDone = make(chan struct{})

	go func() {
		defer close(s.processDone)
		for {
			select {
			case <-s.processTicker.C:
				s.processAllLayers()
			case <-s.processDone:
				return
			}
		}
	}()

	s.logger.WithField("interval", interval).Info("Layered storage background processing started")
}

// StopProcessing stops the background processing goroutine
func (s *LayeredStorage) StopProcessing() {
	if s.processTicker == nil {
		return
	}
	s.processTicker.Stop()
	close(s.processDone)
	s.processTicker = nil
	s.logger.Info("Layered storage background processing stopped")
}

// processAllLayers processes Bronze→Silver→Gold for all known partitions
func (s *LayeredStorage) processAllLayers() {
	// Scan all date partitions in the bronze layer
	bronzePath := filepath.Join(s.basePath, string(BronzeLayer))
	dateEntries, err := os.ReadDir(bronzePath)
	if err != nil {
		if !os.IsNotExist(err) {
			s.logger.WithError(err).Warn("Failed to scan bronze layer")
		}
		return
	}

	for _, dateEntry := range dateEntries {
		if !dateEntry.IsDir() {
			continue
		}
		date := dateEntry.Name()

		datePath := filepath.Join(bronzePath, date)
		entries, err := os.ReadDir(datePath)
		if err != nil {
			continue
		}

		for _, dbEntry := range entries {
			if !dbEntry.IsDir() {
				continue
			}
			dbPath := filepath.Join(datePath, dbEntry.Name())
			tableEntries, err := os.ReadDir(dbPath)
			if err != nil {
				continue
			}
			for _, tableEntry := range tableEntries {
				if !tableEntry.IsDir() {
					continue
				}
				database := dbEntry.Name()
				table := tableEntry.Name()

				if err := s.ProcessBronzeToSilver(database, table, date); err != nil {
					s.logger.WithError(err).WithFields(map[string]interface{}{
						"database": database,
						"table":    table,
						"date":     date,
					}).Warn("Silver layer processing failed")
				}

				if err := s.ProcessSilverToGold(database, table, "daily", date); err != nil {
					s.logger.WithError(err).WithFields(map[string]interface{}{
						"database": database,
						"table":    table,
						"date":     date,
					}).Warn("Gold layer processing failed")
				}
			}
		}
	}
}

func (s *LayeredStorage) getCleaningRules(database, table string) []CleaningRule {
	return []CleaningRule{
		{
			Field:     "*",
			Operation: "trim",
		},
	}
}

func (s *LayeredStorage) getValidationRules(database, table string) []ValidationRule {
	// No default validation rules — rules should come from config or schema registry
	return nil
}

func (s *LayeredStorage) calculateQualityScore(vr *ValidationResult) float64 {
	if vr == nil {
		return 0.0
	}

	totalChecks := len(vr.Errors) + len(vr.Warnings)
	if totalChecks == 0 {
		return 1.0
	}

	// Weighted scoring: errors count 3x more than warnings
	errorWeight := 3.0
	warnWeight := 1.0
	deduction := (errorWeight*float64(len(vr.Errors)) + warnWeight*float64(len(vr.Warnings))) / (errorWeight + warnWeight)
	score := 1.0 - deduction/float64(totalChecks+1)
	if score < 0.0 {
		score = 0.0
	}
	return score
}

// GetLayerPath returns the path for a given layer, database and table
func (s *LayeredStorage) GetLayerPath(layerType LayerType, database, table string) string {
	return filepath.Join(s.basePath, string(layerType), database, table)
}

// GetLayerConfig returns the configuration for a given layer type
func (s *LayeredStorage) GetLayerConfig(layerType LayerType) *LayerConfig {
	return s.layers[layerType]
}

// CleanExpiredData removes data that exceeds the retention period
func (s *LayeredStorage) CleanExpiredData() error {
	for layerType, config := range s.layers {
		if config.RetentionDays <= 0 {
			continue
		}

		layerPath := filepath.Join(s.basePath, string(layerType))
		cutoff := time.Now().AddDate(0, 0, -config.RetentionDays)

		partitions, err := os.ReadDir(layerPath)
		if err != nil {
			if os.IsNotExist(err) {
				continue
			}
			return fmt.Errorf("failed to read %s layer: %w", layerType, err)
		}

		for _, partition := range partitions {
			if !partition.IsDir() {
				continue
			}

			partitionDate, err := time.Parse("2006-01-02", partition.Name())
			if err != nil {
				continue
			}

			if partitionDate.Before(cutoff) {
				partitionPath := filepath.Join(layerPath, partition.Name())
				if err := os.RemoveAll(partitionPath); err != nil {
					s.logger.WithError(err).WithField("path", partitionPath).Warn("Failed to remove expired partition")
					continue
				}
				s.logger.WithFields(map[string]interface{}{
					"layer":     layerType,
					"partition": partition.Name(),
				}).Debug("Removed expired partition")
			}
		}
	}

	return nil
}

// GetStorageStats returns storage statistics for each layer
func (s *LayeredStorage) GetStorageStats() (map[LayerType]*LayerStats, error) {
	stats := make(map[LayerType]*LayerStats)

	for layerType := range s.layers {
		layerPath := filepath.Join(s.basePath, string(layerType))
		layerStats := &LayerStats{}

		if err := filepath.Walk(layerPath, func(path string, info os.FileInfo, err error) error {
			if err != nil {
				return nil
			}
			if !info.IsDir() {
				layerStats.TotalFiles++
				layerStats.TotalSizeBytes += info.Size()
			}
			return nil
		}); err != nil {
			if !os.IsNotExist(err) {
				s.logger.WithError(err).WithField("layer", layerType).Warn("Failed to walk layer")
			}
		}

		stats[layerType] = layerStats
	}

	return stats, nil
}

var (
	_ DataCleaner    = (*DefaultDataCleaner)(nil)
	_ DataValidator  = (*DefaultDataValidator)(nil)
	_ DataAggregator = (*DefaultDataAggregator)(nil)
)
