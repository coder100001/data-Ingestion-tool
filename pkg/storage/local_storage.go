package storage

import (
	"encoding/csv"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"data-ingestion-tool/pkg/config"
	"data-ingestion-tool/pkg/logger"
	"data-ingestion-tool/pkg/models"
)

// LocalStorage implements local file system storage
type LocalStorage struct {
	cfg           *config.LocalConfig
	logger        *logger.Logger
	basePath      string
	currentFile   *os.File
	currentWriter interface{}
	recordCount   int
	fileSize      int64
	currentPath   string
	mu            sync.Mutex
}

// NewLocalStorage creates a new local storage instance
func NewLocalStorage(cfg *config.LocalConfig, logger *logger.Logger) (*LocalStorage, error) {
	// Create base directory
	if err := os.MkdirAll(cfg.BasePath, 0755); err != nil {
		return nil, fmt.Errorf("failed to create base directory: %w", err)
	}

	return &LocalStorage{
		cfg:      cfg,
		logger:   logger,
		basePath: cfg.BasePath,
	}, nil
}

// Write writes a data change to storage
func (s *LocalStorage) Write(change *models.DataChange) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Check if we need to rotate file
	if s.shouldRotate() {
		if err := s.rotateFile(); err != nil {
			return fmt.Errorf("failed to rotate file: %w", err)
		}
	}

	// Write based on format
	switch s.cfg.FileFormat {
	case "json":
		return s.writeJSON(change)
	case "csv":
		return s.writeCSV(change)
	default:
		return fmt.Errorf("unsupported file format: %s", s.cfg.FileFormat)
	}
}

// shouldRotate checks if the current file should be rotated
func (s *LocalStorage) shouldRotate() bool {
	if s.currentFile == nil {
		return true
	}

	if s.recordCount >= s.cfg.MaxRecordsPerFile {
		return true
	}

	if s.fileSize >= int64(s.cfg.MaxFileSizeMB)*1024*1024 {
		return true
	}

	return false
}

// rotateFile rotates to a new file
func (s *LocalStorage) rotateFile() error {
	// Close current file
	if s.currentFile != nil {
		if err := s.closeCurrentFile(); err != nil {
			return err
		}
	}

	// Generate new file path
	partition := s.getPartitionPath()
	timestamp := time.Now().UTC().Format("20060102_150405")
	filename := fmt.Sprintf("data_%s.%s", timestamp, s.cfg.FileFormat)
	s.currentPath = filepath.Join(s.basePath, partition, filename)

	// Create directory
	dir := filepath.Dir(s.currentPath)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return fmt.Errorf("failed to create directory: %w", err)
	}

	// Create file
	file, err := os.OpenFile(s.currentPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return fmt.Errorf("failed to create file: %w", err)
	}

	s.currentFile = file
	s.recordCount = 0
	s.fileSize = 0

	// Initialize writer based on format
	switch s.cfg.FileFormat {
	case "csv":
		s.currentWriter = csv.NewWriter(file)
	}

	s.logger.WithField("path", s.currentPath).Info("Created new storage file")
	return nil
}

// closeCurrentFile closes the current file
func (s *LocalStorage) closeCurrentFile() error {
	if s.currentFile == nil {
		return nil
	}

	// Flush writer if needed
	if writer, ok := s.currentWriter.(*csv.Writer); ok {
		writer.Flush()
	}

	if err := s.currentFile.Close(); err != nil {
		return fmt.Errorf("failed to close file: %w", err)
	}

	s.logger.WithFields(map[string]interface{}{
		"path":         s.currentPath,
		"record_count": s.recordCount,
		"size":         s.fileSize,
	}).Info("Closed storage file")

	s.currentFile = nil
	s.currentWriter = nil
	return nil
}

// getPartitionPath returns the partition path based on strategy
func (s *LocalStorage) getPartitionPath() string {
	now := time.Now().UTC()

	switch s.cfg.PartitionStrategy {
	case "date":
		return now.Format("2006-01-02")
	case "hour":
		return now.Format("2006-01-02/15")
	case "none":
		return ""
	default:
		return now.Format("2006-01-02")
	}
}

// writeJSON writes a change as JSON
func (s *LocalStorage) writeJSON(change *models.DataChange) error {
	data, err := json.Marshal(change)
	if err != nil {
		return fmt.Errorf("failed to marshal change: %w", err)
	}

	// Add newline
	data = append(data, '\n')

	n, err := s.currentFile.Write(data)
	if err != nil {
		return fmt.Errorf("failed to write to file: %w", err)
	}

	s.fileSize += int64(n)
	s.recordCount++

	return nil
}

// writeCSV writes a change as CSV
func (s *LocalStorage) writeCSV(change *models.DataChange) error {
	writer := s.currentWriter.(*csv.Writer)

	// Convert change to CSV record
	record := []string{
		change.ID,
		change.Timestamp.Format(time.RFC3339),
		string(change.Type),
		change.Database,
		change.Table,
	}

	// Add After data as JSON
	if change.After != nil {
		data, _ := json.Marshal(change.After)
		record = append(record, string(data))
	} else {
		record = append(record, "")
	}

	// Add Before data as JSON
	if change.Before != nil {
		data, _ := json.Marshal(change.Before)
		record = append(record, string(data))
	} else {
		record = append(record, "")
	}

	if err := writer.Write(record); err != nil {
		return fmt.Errorf("failed to write CSV record: %w", err)
	}

	estimatedSize := 0
	for _, field := range record {
		estimatedSize += len(field) + 1
	}
	s.fileSize += int64(estimatedSize)
	s.recordCount++

	return nil
}

// Flush flushes any buffered data
func (s *LocalStorage) Flush() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if writer, ok := s.currentWriter.(*csv.Writer); ok {
		writer.Flush()
	}

	return nil
}

// Close closes the storage
func (s *LocalStorage) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.closeCurrentFile()
}
