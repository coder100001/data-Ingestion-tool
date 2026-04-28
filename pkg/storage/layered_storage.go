package storage

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"data-ingestion-tool/pkg/logger"
	"data-ingestion-tool/pkg/models"
	"data-ingestion-tool/pkg/storage/parquet"
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

	// Processing components
	cleaner    DataCleaner
	validator  DataValidator
	aggregator DataAggregator
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

// DataCleaner defines the interface for data cleaning
type DataCleaner interface {
	Clean(data map[string]interface{}, rules []CleaningRule) (map[string]interface{}, error)
}

// DataValidator defines the interface for data validation
type DataValidator interface {
	Validate(data map[string]interface{}, rules []ValidationRule) (*ValidationResult, error)
}

// DataAggregator defines the interface for data aggregation
type DataAggregator interface {
	Aggregate(records []SilverRecord, grain string) (map[string]interface{}, map[string]interface{}, error)
}

// CleaningRule defines a data cleaning rule
type CleaningRule struct {
	Field     string                 `json:"field"`
	Operation string                 `json:"operation"` // trim, lowercase, uppercase, replace
	Params    map[string]interface{} `json:"params,omitempty"`
}

// ValidationRule defines a data validation rule
type ValidationRule struct {
	Field    string                 `json:"field"`
	RuleType string                 `json:"rule_type"` // required, type, range, regex, enum
	Params   map[string]interface{} `json:"params,omitempty"`
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
		cleaner:    &DefaultDataCleaner{},
		validator:  &DefaultDataValidator{},
		aggregator: &DefaultDataAggregator{},
	}
}

// SetCleaner sets the data cleaner
func (s *LayeredStorage) SetCleaner(cleaner DataCleaner) {
	s.cleaner = cleaner
}

// SetValidator sets the data validator
func (s *LayeredStorage) SetValidator(validator DataValidator) {
	s.validator = validator
}

// SetAggregator sets the data aggregator
func (s *LayeredStorage) SetAggregator(aggregator DataAggregator) {
	s.aggregator = aggregator
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

// ProcessBronzeToSilver processes data from bronze to silver layer
func (s *LayeredStorage) ProcessBronzeToSilver(database, table string, date string) error {
	layer := s.layers[SilverLayer]

	// Read bronze data
	bronzePath := filepath.Join(s.basePath, string(BronzeLayer), date, database, table)
	entries, err := os.ReadDir(bronzePath)
	if err != nil {
		if os.IsNotExist(err) {
			s.logger.WithField("path", bronzePath).Debug("No bronze data to process")
			return nil
		}
		return fmt.Errorf("failed to read bronze data: %w", err)
	}

	var processedCount int
	var totalQuality float64

	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}

		// Read bronze record
		filePath := filepath.Join(bronzePath, entry.Name())
		data, err := os.ReadFile(filePath)
		if err != nil {
			s.logger.WithError(err).WithField("file", filePath).Warn("Failed to read bronze record")
			continue
		}

		var bronzeRecord BronzeRecord
		if err := json.Unmarshal(data, &bronzeRecord); err != nil {
			s.logger.WithError(err).WithField("file", filePath).Warn("Failed to unmarshal bronze record")
			continue
		}

		// Extract raw data for processing
		rawData, ok := bronzeRecord.RawData["after"].(map[string]interface{})
		if !ok {
			rawData, ok = bronzeRecord.RawData["before"].(map[string]interface{})
			if !ok {
				s.logger.WithField("file", filePath).Warn("No data to process in bronze record")
				continue
			}
		}

		// Apply cleaning rules
		cleaningRules := s.getCleaningRules(database, table)
		cleanedData, err := s.cleaner.Clean(rawData, cleaningRules)
		if err != nil {
			s.logger.WithError(err).WithField("file", filePath).Warn("Failed to clean data")
			continue
		}

		// Apply validation rules
		validationRules := s.getValidationRules(database, table)
		validationResult, err := s.validator.Validate(cleanedData, validationRules)
		if err != nil {
			s.logger.WithError(err).WithField("file", filePath).Warn("Failed to validate data")
			continue
		}

		// Calculate quality score
		qualityScore := s.calculateQualityScore(validationResult)
		totalQuality += qualityScore

		// Create silver record
		record := SilverRecord{
			ID:           fmt.Sprintf("silver_%d", time.Now().UnixNano()),
			IngestedAt:   bronzeRecord.IngestedAt,
			ProcessedAt:  time.Now().UTC(),
			Source:       fmt.Sprintf("bronze.%s.%s", database, table),
			CleanedData:  cleanedData,
			QualityScore: qualityScore,
			Validation:   validationResult,
		}

		// Write to silver layer
		if err := s.writeSilverRecord(record, database, table, date, layer); err != nil {
			s.logger.WithError(err).WithField("file", filePath).Warn("Failed to write silver record")
			continue
		}

		processedCount++
	}

	if processedCount > 0 {
		avgQuality := totalQuality / float64(processedCount)
		s.logger.WithFields(map[string]interface{}{
			"database":    database,
			"table":       table,
			"date":        date,
			"processed":   processedCount,
			"avg_quality": avgQuality,
		}).Info("Processed bronze to silver")
	}

	return nil
}

// writeSilverRecord writes a silver record to storage
func (s *LayeredStorage) writeSilverRecord(record SilverRecord, database, table, date string, layer *LayerConfig) error {
	// Build path: silver/date/database/table/
	path := filepath.Join(s.basePath, string(SilverLayer), date, database, table)
	if err := os.MkdirAll(path, 0755); err != nil {
		return fmt.Errorf("failed to create directory: %w", err)
	}

	switch layer.Format {
	case "parquet":
		return s.writeSilverParquet(record, path, database, table)
	case "json":
		return s.writeSilverJSON(record, path)
	default:
		return fmt.Errorf("unsupported format: %s", layer.Format)
	}
}

// writeSilverParquet writes silver record in Parquet format
func (s *LayeredStorage) writeSilverParquet(record SilverRecord, path, database, table string) error {
	filename := fmt.Sprintf("silver_%s.parquet", record.ID)
	filePath := filepath.Join(path, filename)

	// Create schema from cleaned data
	pschema := parquet.NewSchemaFromMap(record.CleanedData)

	// Create writer
	writer, err := parquet.NewWriter(filePath, pschema, s.logger)
	if err != nil {
		return fmt.Errorf("failed to create parquet writer: %w", err)
	}
	defer writer.Close()

	// Write data
	if err := writer.WriteRow(record.CleanedData); err != nil {
		return fmt.Errorf("failed to write row: %w", err)
	}

	return nil
}

// writeSilverJSON writes silver record in JSON format
func (s *LayeredStorage) writeSilverJSON(record SilverRecord, path string) error {
	filename := fmt.Sprintf("silver_%s.json", record.ID)
	filePath := filepath.Join(path, filename)

	data, err := json.Marshal(record)
	if err != nil {
		return fmt.Errorf("failed to marshal record: %w", err)
	}

	if err := os.WriteFile(filePath, data, 0644); err != nil {
		return fmt.Errorf("failed to write file: %w", err)
	}

	return nil
}

// ProcessSilverToGold processes data from silver to gold layer
func (s *LayeredStorage) ProcessSilverToGold(database, table, grain string, date string) error {
	// Read silver data
	silverPath := filepath.Join(s.basePath, string(SilverLayer), date, database, table)
	entries, err := os.ReadDir(silverPath)
	if err != nil {
		if os.IsNotExist(err) {
			s.logger.WithField("path", silverPath).Debug("No silver data to process")
			return nil
		}
		return fmt.Errorf("failed to read silver data: %w", err)
	}

	var records []SilverRecord
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}

		filePath := filepath.Join(silverPath, entry.Name())
		data, err := os.ReadFile(filePath)
		if err != nil {
			s.logger.WithError(err).WithField("file", filePath).Warn("Failed to read silver record")
			continue
		}

		var record SilverRecord
		if err := json.Unmarshal(data, &record); err != nil {
			s.logger.WithError(err).WithField("file", filePath).Warn("Failed to unmarshal silver record")
			continue
		}

		records = append(records, record)
	}

	if len(records) == 0 {
		s.logger.WithField("path", silverPath).Debug("No valid silver records to aggregate")
		return nil
	}

	// Aggregate data
	metrics, dimensions, err := s.aggregator.Aggregate(records, grain)
	if err != nil {
		return fmt.Errorf("failed to aggregate data: %w", err)
	}

	// Create gold record
	record := GoldRecord{
		ID:           fmt.Sprintf("gold_%d", time.Now().UnixNano()),
		AggregatedAt: time.Now().UTC(),
		Grain:        grain,
		Metrics:      metrics,
		Dimensions:   dimensions,
		SourceTables: []string{fmt.Sprintf("%s.%s", database, table)},
	}

	// Write to gold layer
	if err := s.writeGoldRecord(record, grain, date); err != nil {
		return fmt.Errorf("failed to write gold record: %w", err)
	}

	s.logger.WithFields(map[string]interface{}{
		"database": database,
		"table":    table,
		"grain":    grain,
		"date":     date,
		"records":  len(records),
	}).Info("Processed silver to gold")

	return nil
}

// writeGoldRecord writes a gold record to storage
func (s *LayeredStorage) writeGoldRecord(record GoldRecord, grain, date string) error {
	layer := s.layers[GoldLayer]

	// Build path: gold/date/grain/
	path := filepath.Join(s.basePath, string(GoldLayer), date, grain)
	if err := os.MkdirAll(path, 0755); err != nil {
		return fmt.Errorf("failed to create directory: %w", err)
	}

	switch layer.Format {
	case "parquet":
		return s.writeGoldParquet(record, path)
	case "json":
		return s.writeGoldJSON(record, path)
	default:
		return fmt.Errorf("unsupported format: %s", layer.Format)
	}
}

// writeGoldParquet writes gold record in Parquet format
func (s *LayeredStorage) writeGoldParquet(record GoldRecord, path string) error {
	filename := fmt.Sprintf("gold_%s.parquet", record.ID)
	filePath := filepath.Join(path, filename)

	// Combine metrics and dimensions for schema
	data := make(map[string]interface{})
	for k, v := range record.Dimensions {
		data[k] = v
	}
	for k, v := range record.Metrics {
		data[k] = v
	}

	pschema := parquet.NewSchemaFromMap(data)

	writer, err := parquet.NewWriter(filePath, pschema, s.logger)
	if err != nil {
		return fmt.Errorf("failed to create parquet writer: %w", err)
	}
	defer writer.Close()

	if err := writer.WriteRow(data); err != nil {
		return fmt.Errorf("failed to write row: %w", err)
	}

	return nil
}

// writeGoldJSON writes gold record in JSON format
func (s *LayeredStorage) writeGoldJSON(record GoldRecord, path string) error {
	filename := fmt.Sprintf("gold_%s.json", record.ID)
	filePath := filepath.Join(path, filename)

	data, err := json.Marshal(record)
	if err != nil {
		return fmt.Errorf("failed to marshal record: %w", err)
	}

	if err := os.WriteFile(filePath, data, 0644); err != nil {
		return fmt.Errorf("failed to write file: %w", err)
	}

	return nil
}

// getCleaningRules returns cleaning rules for a table
func (s *LayeredStorage) getCleaningRules(database, table string) []CleaningRule {
	// TODO: Load from configuration
	return []CleaningRule{
		{Field: "*", Operation: "trim"},
		{Field: "email", Operation: "lowercase"},
	}
}

// getValidationRules returns validation rules for a table
func (s *LayeredStorage) getValidationRules(database, table string) []ValidationRule {
	// TODO: Load from configuration
	return []ValidationRule{
		{Field: "id", RuleType: "required"},
		{Field: "created_at", RuleType: "type", Params: map[string]interface{}{"type": "datetime"}},
	}
}

// calculateQualityScore calculates quality score from validation result
func (s *LayeredStorage) calculateQualityScore(result *ValidationResult) float64 {
	if result == nil || result.Valid {
		return 1.0
	}

	// Simple scoring: each error reduces score by 0.1, minimum 0
	score := 1.0 - float64(len(result.Errors))*0.1
	if score < 0 {
		score = 0
	}
	return score
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
} = (*LayeredStorage)(nil)

// Write implements Storage interface (delegates to bronze layer)
func (s *LayeredStorage) Write(change *models.DataChange) error {
	return s.WriteToBronze(change)
}

// DefaultDataCleaner implements basic data cleaning
type DefaultDataCleaner struct{}

// Clean applies cleaning rules to data
func (c *DefaultDataCleaner) Clean(data map[string]interface{}, rules []CleaningRule) (map[string]interface{}, error) {
	cleaned := make(map[string]interface{})

	// Copy data
	for k, v := range data {
		cleaned[k] = v
	}

	// Apply rules
	for _, rule := range rules {
		if rule.Field == "*" {
			// Apply to all string fields
			for k, v := range cleaned {
				if str, ok := v.(string); ok {
					switch rule.Operation {
					case "trim":
						cleaned[k] = trimString(str)
					case "lowercase":
						cleaned[k] = toLowerCase(str)
					case "uppercase":
						cleaned[k] = toUpperCase(str)
					}
				}
			}
		} else {
			// Apply to specific field
			if v, exists := cleaned[rule.Field]; exists {
				if str, ok := v.(string); ok {
					switch rule.Operation {
					case "trim":
						cleaned[rule.Field] = trimString(str)
					case "lowercase":
						cleaned[rule.Field] = toLowerCase(str)
					case "uppercase":
						cleaned[rule.Field] = toUpperCase(str)
					case "replace":
						if oldVal, ok := rule.Params["old"]; ok {
							if newVal, ok := rule.Params["new"]; ok {
								cleaned[rule.Field] = replaceString(str, oldVal.(string), newVal.(string))
							}
						}
					}
				}
			}
		}
	}

	return cleaned, nil
}

// DefaultDataValidator implements basic data validation
type DefaultDataValidator struct{}

// Validate applies validation rules to data
func (v *DefaultDataValidator) Validate(data map[string]interface{}, rules []ValidationRule) (*ValidationResult, error) {
	result := &ValidationResult{
		Valid:    true,
		Errors:   []string{},
		Warnings: []string{},
	}

	for _, rule := range rules {
		value, exists := data[rule.Field]

		switch rule.RuleType {
		case "required":
			if !exists || value == nil || value == "" {
				result.Valid = false
				result.Errors = append(result.Errors, fmt.Sprintf("Field '%s' is required", rule.Field))
			}

		case "type":
			if exists && value != nil {
				expectedType := rule.Params["type"].(string)
				if !checkType(value, expectedType) {
					result.Valid = false
					result.Errors = append(result.Errors, fmt.Sprintf("Field '%s' should be of type %s", rule.Field, expectedType))
				}
			}

		case "range":
			if exists && value != nil {
				if min, ok := rule.Params["min"]; ok {
					if compareValues(value, min) < 0 {
						result.Valid = false
						result.Errors = append(result.Errors, fmt.Sprintf("Field '%s' is below minimum", rule.Field))
					}
				}
				if max, ok := rule.Params["max"]; ok {
					if compareValues(value, max) > 0 {
						result.Valid = false
						result.Errors = append(result.Errors, fmt.Sprintf("Field '%s' is above maximum", rule.Field))
					}
				}
			}
		}
	}

	return result, nil
}

// DefaultDataAggregator implements basic data aggregation
type DefaultDataAggregator struct{}

// Aggregate aggregates silver records
func (a *DefaultDataAggregator) Aggregate(records []SilverRecord, grain string) (map[string]interface{}, map[string]interface{}, error) {
	metrics := make(map[string]interface{})
	dimensions := make(map[string]interface{})

	// Count records
	metrics["record_count"] = len(records)

	// Calculate average quality
	var totalQuality float64
	for _, r := range records {
		totalQuality += r.QualityScore
	}
	if len(records) > 0 {
		metrics["avg_quality"] = totalQuality / float64(len(records))
	}

	// Set dimensions based on grain
	switch grain {
	case "hourly":
		dimensions["hour"] = records[0].ProcessedAt.Hour()
		fallthrough
	case "daily":
		dimensions["date"] = records[0].ProcessedAt.Format("2006-01-02")
	case "weekly":
		_, week := records[0].ProcessedAt.ISOWeek()
		dimensions["week"] = week
		dimensions["year"] = records[0].ProcessedAt.Year()
	case "monthly":
		dimensions["month"] = records[0].ProcessedAt.Month()
		dimensions["year"] = records[0].ProcessedAt.Year()
	}

	return metrics, dimensions, nil
}

// Helper functions

func trimString(s string) string {
	// Simple trim implementation
	start := 0
	end := len(s)

	for start < end && (s[start] == ' ' || s[start] == '\t' || s[start] == '\n' || s[start] == '\r') {
		start++
	}

	for end > start && (s[end-1] == ' ' || s[end-1] == '\t' || s[end-1] == '\n' || s[end-1] == '\r') {
		end--
	}

	return s[start:end]
}

func toLowerCase(s string) string {
	result := make([]byte, len(s))
	for i := 0; i < len(s); i++ {
		c := s[i]
		if c >= 'A' && c <= 'Z' {
			c = c + ('a' - 'A')
		}
		result[i] = c
	}
	return string(result)
}

func toUpperCase(s string) string {
	result := make([]byte, len(s))
	for i := 0; i < len(s); i++ {
		c := s[i]
		if c >= 'a' && c <= 'z' {
			c = c - ('a' - 'A')
		}
		result[i] = c
	}
	return string(result)
}

func replaceString(s, old, new string) string {
	// Simple replace implementation
	if old == "" {
		return s
	}

	result := ""
	i := 0
	for i < len(s) {
		if i+len(old) <= len(s) && s[i:i+len(old)] == old {
			result += new
			i += len(old)
		} else {
			result += string(s[i])
			i++
		}
	}
	return result
}

func checkType(value interface{}, expectedType string) bool {
	switch expectedType {
	case "string":
		_, ok := value.(string)
		return ok
	case "int":
		switch value.(type) {
		case int, int32, int64:
			return true
		}
		return false
	case "float":
		switch value.(type) {
		case float32, float64:
			return true
		}
		return false
	case "bool":
		_, ok := value.(bool)
		return ok
	case "datetime":
		_, ok := value.(time.Time)
		return ok
	default:
		return true
	}
}

// compareValues compares two values
func compareValues(a, b interface{}) int {
	if a == nil && b == nil {
		return 0
	}
	if a == nil {
		return -1
	}
	if b == nil {
		return 1
	}

	switch av := a.(type) {
	case int:
		bv, ok := b.(int)
		if !ok {
			return 0
		}
		if av < bv {
			return -1
		}
		if av > bv {
			return 1
		}
		return 0
	case int32:
		bv, ok := b.(int32)
		if !ok {
			return 0
		}
		if av < bv {
			return -1
		}
		if av > bv {
			return 1
		}
		return 0
	case int64:
		bv, ok := b.(int64)
		if !ok {
			return 0
		}
		if av < bv {
			return -1
		}
		if av > bv {
			return 1
		}
		return 0
	case float32:
		bv, ok := b.(float32)
		if !ok {
			return 0
		}
		if av < bv {
			return -1
		}
		if av > bv {
			return 1
		}
		return 0
	case float64:
		bv, ok := b.(float64)
		if !ok {
			return 0
		}
		if av < bv {
			return -1
		}
		if av > bv {
			return 1
		}
		return 0
	case string:
		bv, ok := b.(string)
		if !ok {
			return 0
		}
		if av < bv {
			return -1
		}
		if av > bv {
			return 1
		}
		return 0
	default:
		return 0
	}
}
