package storage

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"data-ingestion-tool/pkg/storage/parquet"
	"data-ingestion-tool/pkg/util"

	"github.com/google/uuid"
)

// ProcessBronzeToSilver processes data from bronze to silver layer
func (s *LayeredStorage) ProcessBronzeToSilver(database, table string, date string) error {
	layer := s.layers[SilverLayer]

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

		rawData, ok := bronzeRecord.RawData["after"].(map[string]interface{})
		if !ok {
			rawData, ok = bronzeRecord.RawData["before"].(map[string]interface{})
			if !ok {
				s.logger.WithField("file", filePath).Warn("No data to process in bronze record")
				continue
			}
		}

		cleaningRules := s.getCleaningRules(database, table)
		cleanedData, err := s.cleaner.Clean(rawData, cleaningRules)
		if err != nil {
			s.logger.WithError(err).WithField("file", filePath).Warn("Failed to clean data")
			continue
		}

		validationRules := s.getValidationRules(database, table)
		validationResult, err := s.validator.Validate(cleanedData, validationRules)
		if err != nil {
			s.logger.WithError(err).WithField("file", filePath).Warn("Failed to validate data")
			continue
		}

		qualityScore := s.calculateQualityScore(validationResult)
		totalQuality += qualityScore

		record := SilverRecord{
			ID:           uuid.New().String(),
			IngestedAt:   bronzeRecord.IngestedAt,
			ProcessedAt:  time.Now().UTC(),
			Source:       fmt.Sprintf("bronze.%s.%s", database, table),
			CleanedData:  cleanedData,
			QualityScore: qualityScore,
			Validation:   validationResult,
		}

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

	pschema := parquet.NewSchemaFromMap(record.CleanedData)

	writer, err := parquet.NewWriter(filePath, pschema, s.logger)
	if err != nil {
		return fmt.Errorf("failed to create parquet writer: %w", err)
	}
	defer writer.Close()

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

// DefaultDataCleaner implements basic data cleaning
type DefaultDataCleaner struct{}

// Clean applies cleaning rules to data
func (c *DefaultDataCleaner) Clean(data map[string]interface{}, rules []CleaningRule) (map[string]interface{}, error) {
	cleaned := make(map[string]interface{})

	for k, v := range data {
		cleaned[k] = v
	}

	for _, rule := range rules {
		if rule.Field == "*" {
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
					if util.CompareValues(value, min) < 0 {
						result.Valid = false
						result.Errors = append(result.Errors, fmt.Sprintf("Field '%s' is below minimum", rule.Field))
					}
				}
				if max, ok := rule.Params["max"]; ok {
					if util.CompareValues(value, max) > 0 {
						result.Valid = false
						result.Errors = append(result.Errors, fmt.Sprintf("Field '%s' is above maximum", rule.Field))
					}
				}
			}
		}
	}

	return result, nil
}
