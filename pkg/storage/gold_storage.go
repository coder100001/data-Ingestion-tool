package storage

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"data-ingestion-tool/pkg/storage/parquet"

	"github.com/google/uuid"
)

// ProcessSilverToGold processes data from silver to gold layer
func (s *LayeredStorage) ProcessSilverToGold(database, table, grain string, date string) error {
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

	metrics, dimensions, err := s.aggregator.Aggregate(records, grain)
	if err != nil {
		return fmt.Errorf("failed to aggregate data: %w", err)
	}

	record := GoldRecord{
		ID:           uuid.New().String(),
		AggregatedAt: time.Now().UTC(),
		Grain:        grain,
		Metrics:      metrics,
		Dimensions:   dimensions,
		SourceTables: []string{fmt.Sprintf("%s.%s", database, table)},
	}

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

// DefaultDataAggregator implements basic data aggregation
type DefaultDataAggregator struct{}

// Aggregate aggregates silver records
func (a *DefaultDataAggregator) Aggregate(records []SilverRecord, grain string) (map[string]interface{}, map[string]interface{}, error) {
	metrics := make(map[string]interface{})
	dimensions := make(map[string]interface{})

	metrics["record_count"] = len(records)

	var totalQuality float64
	for _, r := range records {
		totalQuality += r.QualityScore
	}
	if len(records) > 0 {
		metrics["avg_quality"] = totalQuality / float64(len(records))
	}

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
