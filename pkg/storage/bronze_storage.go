package storage

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"data-ingestion-tool/pkg/models"
)

// WriteToBronze writes raw data to bronze layer
func (s *LayeredStorage) WriteToBronze(change *models.DataChange) error {
	if _, exists := s.layers[BronzeLayer]; !exists {
		return fmt.Errorf("bronze layer not configured")
	}

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

	partition := time.Now().UTC().Format("2006-01-02")
	path := filepath.Join(s.basePath, string(BronzeLayer), partition, change.Database, change.Table)

	if err := os.MkdirAll(path, 0755); err != nil {
		return fmt.Errorf("failed to create directory: %w", err)
	}

	filename := fmt.Sprintf("bronze_%s_%d.json", change.ID, time.Now().UnixNano())
	filePath := filepath.Join(path, filename)

	data, err := json.Marshal(record)
	if err != nil {
		return fmt.Errorf("failed to marshal record: %w", err)
	}

	if err := os.WriteFile(filePath, data, 0644); err != nil {
		return fmt.Errorf("failed to write file: %w", err)
	}

	partitionInfo := PartitionInfo{
		Values: map[string]string{
			"date": partition,
		},
		Path:        path,
		FileCount:   1,
		RecordCount: 1,
		SizeBytes:   int64(len(data)),
		MinTime:     record.IngestedAt,
		MaxTime:     record.IngestedAt,
		CreatedAt:   time.Now().UTC(),
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
