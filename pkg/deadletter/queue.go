package deadletter

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"data-ingestion-tool/pkg/models"
)

// DeadLetterRecord represents a single dead letter record in JSON Lines format
type DeadLetterRecord struct {
	// OriginalChange is the raw data change that failed processing
	OriginalChange *models.DataChange `json:"original_change"`
	// FailureReason describes why the processing failed
	FailureReason string `json:"failure_reason"`
	// Timestamp is when the record was written to the dead letter queue
	Timestamp time.Time `json:"timestamp"`
	// RetryCount indicates how many retries were attempted
	RetryCount int `json:"retry_count"`
}

// Queue handles dead letter queue operations
type Queue struct {
	path        string
	maxSizeMB   int
	mu          sync.Mutex
	file        *os.File
	writer      *bufio.Writer
	size        int64
	recordCount int64
}

// NewQueue creates a new dead letter queue
func NewQueue(path string, maxSizeMB int) (*Queue, error) {
	if path == "" {
		path = "./metadata/deadletter.jsonl"
	}
	if maxSizeMB == 0 {
		maxSizeMB = 100
	}

	// Ensure directory exists
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create dead letter queue directory: %w", err)
	}

	// Open file in append mode
	file, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return nil, fmt.Errorf("failed to open dead letter queue file: %w", err)
	}

	// Get current file size
	info, err := file.Stat()
	if err != nil {
		file.Close()
		return nil, fmt.Errorf("failed to stat dead letter queue file: %w", err)
	}

	q := &Queue{
		path:      path,
		maxSizeMB: maxSizeMB,
		file:      file,
		writer:    bufio.NewWriter(file),
		size:      info.Size(),
	}

	return q, nil
}

// Write writes a failed record to the dead letter queue
func (q *Queue) Write(change *models.DataChange, failureReason string, retryCount int) error {
	q.mu.Lock()
	defer q.mu.Unlock()

	record := DeadLetterRecord{
		OriginalChange: change,
		FailureReason:  failureReason,
		Timestamp:      time.Now().UTC(),
		RetryCount:     retryCount,
	}

	data, err := json.Marshal(record)
	if err != nil {
		return fmt.Errorf("failed to marshal dead letter record: %w", err)
	}

	// Check size limit
	if q.size+int64(len(data))+1 > int64(q.maxSizeMB)*1024*1024 {
		return fmt.Errorf("dead letter queue exceeded maximum size of %d MB", q.maxSizeMB)
	}

	// Write JSON line
	if _, err := q.writer.Write(data); err != nil {
		return fmt.Errorf("failed to write dead letter record: %w", err)
	}
	if _, err := q.writer.WriteString("\n"); err != nil {
		return fmt.Errorf("failed to write newline: %w", err)
	}

	if err := q.writer.Flush(); err != nil {
		return fmt.Errorf("failed to flush dead letter record: %w", err)
	}

	q.size += int64(len(data)) + 1
	q.recordCount++

	return nil
}

// ReadAll reads all records from the dead letter queue
func (q *Queue) ReadAll() ([]*DeadLetterRecord, error) {
	q.mu.Lock()
	defer q.mu.Unlock()

	// Flush any pending writes
	if err := q.writer.Flush(); err != nil {
		return nil, fmt.Errorf("failed to flush before reading: %w", err)
	}

	// Reopen file for reading
	file, err := os.Open(q.path)
	if err != nil {
		return nil, fmt.Errorf("failed to open dead letter queue for reading: %w", err)
	}
	defer file.Close()

	var records []*DeadLetterRecord
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := scanner.Text()
		if line == "" {
			continue
		}

		var record DeadLetterRecord
		if err := json.Unmarshal([]byte(line), &record); err != nil {
			return nil, fmt.Errorf("failed to unmarshal dead letter record: %w", err)
		}
		records = append(records, &record)
	}

	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("failed to scan dead letter queue: %w", err)
	}

	return records, nil
}

// Replay reads all records and returns them for reprocessing
func (q *Queue) Replay() ([]*models.DataChange, error) {
	records, err := q.ReadAll()
	if err != nil {
		return nil, err
	}

	changes := make([]*models.DataChange, 0, len(records))
	for _, record := range records {
		if record.OriginalChange != nil {
			changes = append(changes, record.OriginalChange)
		}
	}

	return changes, nil
}

// Clear removes all records from the dead letter queue
func (q *Queue) Clear() error {
	q.mu.Lock()
	defer q.mu.Unlock()

	if err := q.writer.Flush(); err != nil {
		return fmt.Errorf("failed to flush before clearing: %w", err)
	}

	// Truncate file
	if err := q.file.Truncate(0); err != nil {
		return fmt.Errorf("failed to truncate dead letter queue: %w", err)
	}

	if _, err := q.file.Seek(0, 0); err != nil {
		return fmt.Errorf("failed to seek dead letter queue: %w", err)
	}

	q.size = 0
	q.recordCount = 0

	return nil
}

// Close closes the dead letter queue file
func (q *Queue) Close() error {
	q.mu.Lock()
	defer q.mu.Unlock()

	if err := q.writer.Flush(); err != nil {
		return fmt.Errorf("failed to flush on close: %w", err)
	}

	if err := q.file.Close(); err != nil {
		return fmt.Errorf("failed to close dead letter queue file: %w", err)
	}

	return nil
}

// GetPath returns the file path of the dead letter queue
func (q *Queue) GetPath() string {
	return q.path
}

// GetRecordCount returns the number of records in the queue
func (q *Queue) GetRecordCount() int64 {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.recordCount
}

// GetSize returns the current size of the queue in bytes
func (q *Queue) GetSize() int64 {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.size
}
