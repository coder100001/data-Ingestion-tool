package deadletter

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"data-ingestion-tool/pkg/models"
)

func setupTestQueue(t *testing.T) (*Queue, string) {
	t.Helper()
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "deadletter.jsonl")
	q, err := NewQueue(path, 10)
	if err != nil {
		t.Fatalf("failed to create queue: %v", err)
	}
	return q, path
}

func createTestChange() *models.DataChange {
	change := models.NewDataChange(models.Insert, "testdb", "testtable")
	change.After["id"] = 1
	change.After["name"] = "test"
	return change
}

func TestQueue_WriteAndRead(t *testing.T) {
	q, _ := setupTestQueue(t)
	defer q.Close()

	change := createTestChange()
	if err := q.Write(change, "test failure", 2); err != nil {
		t.Fatalf("Write failed: %v", err)
	}

	records, err := q.ReadAll()
	if err != nil {
		t.Fatalf("ReadAll failed: %v", err)
	}
	if len(records) != 1 {
		t.Fatalf("expected 1 record, got %d", len(records))
	}

	r := records[0]
	if r.FailureReason != "test failure" {
		t.Errorf("expected failure reason 'test failure', got %s", r.FailureReason)
	}
	if r.RetryCount != 2 {
		t.Errorf("expected retry count 2, got %d", r.RetryCount)
	}
	if r.OriginalChange == nil {
		t.Fatal("expected OriginalChange to be non-nil")
	}
	if r.OriginalChange.Database != "testdb" {
		t.Errorf("expected database 'testdb', got %s", r.OriginalChange.Database)
	}
}

func TestQueue_JSONLinesFormat(t *testing.T) {
	q, path := setupTestQueue(t)
	defer q.Close()

	change1 := createTestChange()
	change2 := createTestChange()
	if err := q.Write(change1, "failure 1", 1); err != nil {
		t.Fatalf("Write failed: %v", err)
	}
	if err := q.Write(change2, "failure 2", 2); err != nil {
		t.Fatalf("Write failed: %v", err)
	}

	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("failed to read file: %v", err)
	}

	lines := strings.Split(strings.TrimSpace(string(data)), "\n")
	if len(lines) != 2 {
		t.Fatalf("expected 2 lines, got %d", len(lines))
	}
	for i, line := range lines {
		if !strings.HasPrefix(line, "{") || !strings.HasSuffix(line, "}") {
			t.Errorf("line %d is not valid JSON object: %s", i, line)
		}
	}
}

func TestQueue_Replay(t *testing.T) {
	q, _ := setupTestQueue(t)
	defer q.Close()

	change1 := createTestChange()
	change2 := createTestChange()
	if err := q.Write(change1, "failure 1", 1); err != nil {
		t.Fatalf("Write failed: %v", err)
	}
	if err := q.Write(change2, "failure 2", 2); err != nil {
		t.Fatalf("Write failed: %v", err)
	}

	changes, err := q.Replay()
	if err != nil {
		t.Fatalf("Replay failed: %v", err)
	}
	if len(changes) != 2 {
		t.Fatalf("expected 2 changes, got %d", len(changes))
	}
	if changes[0].Database != "testdb" || changes[1].Database != "testdb" {
		t.Error("expected changes to have correct database")
	}
}

func TestQueue_Clear(t *testing.T) {
	q, _ := setupTestQueue(t)
	defer q.Close()

	change := createTestChange()
	if err := q.Write(change, "failure", 1); err != nil {
		t.Fatalf("Write failed: %v", err)
	}

	if q.GetRecordCount() != 1 {
		t.Fatalf("expected 1 record before clear, got %d", q.GetRecordCount())
	}

	if err := q.Clear(); err != nil {
		t.Fatalf("Clear failed: %v", err)
	}

	if q.GetRecordCount() != 0 {
		t.Fatalf("expected 0 records after clear, got %d", q.GetRecordCount())
	}

	records, err := q.ReadAll()
	if err != nil {
		t.Fatalf("ReadAll after clear failed: %v", err)
	}
	if len(records) != 0 {
		t.Fatalf("expected 0 records after clear, got %d", len(records))
	}
}

func TestQueue_ConcurrentSafety(t *testing.T) {
	q, _ := setupTestQueue(t)
	defer q.Close()

	var wg sync.WaitGroup
	numGoroutines := 10
	numWrites := 50

	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < numWrites; j++ {
				change := createTestChange()
				if err := q.Write(change, "concurrent failure", 1); err != nil {
					t.Errorf("Write failed: %v", err)
				}
			}
		}(i)
	}

	wg.Wait()

	expected := int64(numGoroutines * numWrites)
	if q.GetRecordCount() != expected {
		t.Fatalf("expected %d records, got %d", expected, q.GetRecordCount())
	}

	records, err := q.ReadAll()
	if err != nil {
		t.Fatalf("ReadAll failed: %v", err)
	}
	if int64(len(records)) != expected {
		t.Fatalf("expected %d records from ReadAll, got %d", expected, len(records))
	}
}

func TestQueue_ReadAllAfterClose(t *testing.T) {
	q, _ := setupTestQueue(t)

	change := createTestChange()
	if err := q.Write(change, "failure", 1); err != nil {
		t.Fatalf("Write failed: %v", err)
	}
	if err := q.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	q2, err := NewQueue(q.GetPath(), 10)
	if err != nil {
		t.Fatalf("failed to reopen queue: %v", err)
	}
	defer q2.Close()

	records, err := q2.ReadAll()
	if err != nil {
		t.Fatalf("ReadAll failed: %v", err)
	}
	if len(records) != 1 {
		t.Fatalf("expected 1 record, got %d", len(records))
	}
}

func TestQueue_SizeLimit(t *testing.T) {
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "deadletter.jsonl")
	q, err := NewQueue(path, 1)
	if err != nil {
		t.Fatalf("failed to create queue: %v", err)
	}
	defer q.Close()

	bigChange := createTestChange()
	bigChange.After = make(map[string]interface{})
	for i := 0; i < 10000; i++ {
		bigChange.After[string(rune(i))] = strings.Repeat("x", 100)
	}

	for i := 0; i < 100; i++ {
		err := q.Write(bigChange, "failure", 1)
		if err != nil {
			if !strings.Contains(err.Error(), "exceeded maximum size") {
				t.Fatalf("unexpected error: %v", err)
			}
			return
		}
	}
	t.Fatal("expected size limit error, but never exceeded")
}

func TestTimestampIsUTC(t *testing.T) {
	tmpDir := t.TempDir()
	queuePath := filepath.Join(tmpDir, "deadletter.jsonl")

	q, err := NewQueue(queuePath, 10)
	if err != nil {
		t.Fatalf("Failed to create queue: %v", err)
	}
	defer q.Close()

	change := models.NewDataChange(models.Insert, "testdb", "testtable")
	change.After["id"] = 1

	beforeWrite := time.Now().UTC()
	if err := q.Write(change, "test failure", 0); err != nil {
		t.Fatalf("Failed to write: %v", err)
	}

	records, err := q.ReadAll()
	if err != nil {
		t.Fatalf("Failed to read: %v", err)
	}

	if len(records) != 1 {
		t.Fatalf("Expected 1 record, got %d", len(records))
	}

	// Verify timestamp is UTC
	if records[0].Timestamp.Location() != time.UTC {
		t.Errorf("Expected UTC timestamp, got %v", records[0].Timestamp.Location())
	}

	// Verify timestamp is recent
	afterWrite := time.Now().UTC()
	if records[0].Timestamp.Before(beforeWrite) || records[0].Timestamp.After(afterWrite) {
		t.Errorf("Timestamp %v is not between %v and %v", records[0].Timestamp, beforeWrite, afterWrite)
	}
}

func TestGetPath(t *testing.T) {
	tmpDir := t.TempDir()
	queuePath := filepath.Join(tmpDir, "deadletter.jsonl")

	q, err := NewQueue(queuePath, 10)
	if err != nil {
		t.Fatalf("Failed to create queue: %v", err)
	}
	defer q.Close()

	if q.GetPath() != queuePath {
		t.Errorf("Expected path %s, got %s", queuePath, q.GetPath())
	}
}

func TestGetRecordCount(t *testing.T) {
	tmpDir := t.TempDir()
	queuePath := filepath.Join(tmpDir, "deadletter.jsonl")

	q, err := NewQueue(queuePath, 10)
	if err != nil {
		t.Fatalf("Failed to create queue: %v", err)
	}
	defer q.Close()

	if q.GetRecordCount() != 0 {
		t.Errorf("Expected 0 records, got %d", q.GetRecordCount())
	}

	change := models.NewDataChange(models.Insert, "testdb", "testtable")
	for i := 0; i < 5; i++ {
		if err := q.Write(change, "test failure", i); err != nil {
			t.Fatalf("Failed to write: %v", err)
		}
	}

	if q.GetRecordCount() != 5 {
		t.Errorf("Expected 5 records, got %d", q.GetRecordCount())
	}
}

func TestGetSize(t *testing.T) {
	tmpDir := t.TempDir()
	queuePath := filepath.Join(tmpDir, "deadletter.jsonl")

	q, err := NewQueue(queuePath, 10)
	if err != nil {
		t.Fatalf("Failed to create queue: %v", err)
	}
	defer q.Close()

	initialSize := q.GetSize()
	if initialSize != 0 {
		t.Errorf("Expected initial size 0, got %d", initialSize)
	}

	change := models.NewDataChange(models.Insert, "testdb", "testtable")
	change.After["id"] = 1
	change.After["name"] = "test"

	if err := q.Write(change, "test failure", 0); err != nil {
		t.Fatalf("Failed to write: %v", err)
	}

	afterSize := q.GetSize()
	if afterSize <= initialSize {
		t.Errorf("Expected size to increase, got %d -> %d", initialSize, afterSize)
	}
}

func TestWriteAfterClear(t *testing.T) {
	tmpDir := t.TempDir()
	queuePath := filepath.Join(tmpDir, "deadletter.jsonl")

	q, err := NewQueue(queuePath, 10)
	if err != nil {
		t.Fatalf("Failed to create queue: %v", err)
	}
	defer q.Close()

	change := models.NewDataChange(models.Insert, "testdb", "testtable")
	if err := q.Write(change, "test failure", 0); err != nil {
		t.Fatalf("Failed to write: %v", err)
	}

	if err := q.Clear(); err != nil {
		t.Fatalf("Failed to clear: %v", err)
	}

	// Write after clear
	change2 := models.NewDataChange(models.Update, "testdb", "testtable")
	if err := q.Write(change2, "another failure", 1); err != nil {
		t.Fatalf("Failed to write after clear: %v", err)
	}

	records, err := q.ReadAll()
	if err != nil {
		t.Fatalf("Failed to read: %v", err)
	}

	if len(records) != 1 {
		t.Errorf("Expected 1 record after clear, got %d", len(records))
	}

	if records[0].OriginalChange.Type != models.Update {
		t.Errorf("Expected UPDATE type, got %s", records[0].OriginalChange.Type)
	}
}

func TestReplayWithNilChange(t *testing.T) {
	tmpDir := t.TempDir()
	queuePath := filepath.Join(tmpDir, "deadletter.jsonl")

	q, err := NewQueue(queuePath, 10)
	if err != nil {
		t.Fatalf("Failed to create queue: %v", err)
	}
	defer q.Close()

	// Write record with nil change
	record := &DeadLetterRecord{
		OriginalChange: nil,
		FailureReason:  "test failure",
		Timestamp:      time.Now().UTC(),
		RetryCount:     0,
	}

	data, err := json.Marshal(record)
	if err != nil {
		t.Fatalf("Failed to marshal: %v", err)
	}

	q.mu.Lock()
	q.writer.Write(data)
	q.writer.WriteString("\n")
	q.writer.Flush()
	q.mu.Unlock()

	// Replay should skip nil changes
	changes, err := q.Replay()
	if err != nil {
		t.Fatalf("Failed to replay: %v", err)
	}

	if len(changes) != 0 {
		t.Errorf("Expected 0 changes, got %d", len(changes))
	}
}

func TestWriteMultipleRecords(t *testing.T) {
	tmpDir := t.TempDir()
	queuePath := filepath.Join(tmpDir, "deadletter.jsonl")

	q, err := NewQueue(queuePath, 10)
	if err != nil {
		t.Fatalf("Failed to create queue: %v", err)
	}
	defer q.Close()

	for i := 0; i < 10; i++ {
		change := models.NewDataChange(models.Insert, "testdb", "testtable")
		change.After["id"] = i
		if err := q.Write(change, fmt.Sprintf("failure %d", i), i); err != nil {
			t.Fatalf("Failed to write record %d: %v", i, err)
		}
	}

	records, err := q.ReadAll()
	if err != nil {
		t.Fatalf("Failed to read: %v", err)
	}

	if len(records) != 10 {
		t.Errorf("Expected 10 records, got %d", len(records))
	}

	for i, record := range records {
		if record.RetryCount != i {
			t.Errorf("Record %d: expected retry count %d, got %d", i, i, record.RetryCount)
		}
	}
}

func TestDeadLetterRecordJSONRoundTrip(t *testing.T) {
	change := models.NewDataChange(models.Insert, "testdb", "testtable")
	change.After["id"] = 123
	change.After["name"] = "test"

	record := &DeadLetterRecord{
		OriginalChange: change,
		FailureReason:  "connection timeout",
		Timestamp:      time.Now().UTC(),
		RetryCount:     3,
	}

	data, err := json.Marshal(record)
	if err != nil {
		t.Fatalf("Failed to marshal: %v", err)
	}

	var unmarshaled DeadLetterRecord
	if err := json.Unmarshal(data, &unmarshaled); err != nil {
		t.Fatalf("Failed to unmarshal: %v", err)
	}

	if unmarshaled.FailureReason != "connection timeout" {
		t.Errorf("Expected failure reason 'connection timeout', got '%s'", unmarshaled.FailureReason)
	}

	if unmarshaled.RetryCount != 3 {
		t.Errorf("Expected retry count 3, got %d", unmarshaled.RetryCount)
	}

	if unmarshaled.OriginalChange.Database != "testdb" {
		t.Errorf("Expected database 'testdb', got '%s'", unmarshaled.OriginalChange.Database)
	}
}

func TestNewQueueDefaultPath(t *testing.T) {
	q, err := NewQueue("", 0)
	if err != nil {
		t.Fatalf("Failed to create queue with default path: %v", err)
	}
	defer q.Close()

	expectedPath := "./metadata/deadletter.jsonl"
	if q.GetPath() != expectedPath {
		t.Errorf("Expected default path %s, got %s", expectedPath, q.GetPath())
	}

	// Clean up
	os.RemoveAll("./metadata")
}

func TestNewQueueDefaultMaxSize(t *testing.T) {
	tmpDir := t.TempDir()
	queuePath := filepath.Join(tmpDir, "deadletter.jsonl")

	q, err := NewQueue(queuePath, 0)
	if err != nil {
		t.Fatalf("Failed to create queue: %v", err)
	}
	defer q.Close()

	// Default max size should be 100 MB
	if q.maxSizeMB != 100 {
		t.Errorf("Expected default max size 100 MB, got %d MB", q.maxSizeMB)
	}
}

func TestWriteAfterClose(t *testing.T) {
	tmpDir := t.TempDir()
	queuePath := filepath.Join(tmpDir, "deadletter.jsonl")

	q, err := NewQueue(queuePath, 10)
	if err != nil {
		t.Fatalf("Failed to create queue: %v", err)
	}

	if err := q.Close(); err != nil {
		t.Fatalf("Failed to close: %v", err)
	}

	change := models.NewDataChange(models.Insert, "testdb", "testtable")
	err = q.Write(change, "test failure", 0)
	if err == nil {
		t.Error("Expected error when writing to closed queue")
	}
}

func TestReadAllEmptyFile(t *testing.T) {
	tmpDir := t.TempDir()
	queuePath := filepath.Join(tmpDir, "deadletter.jsonl")

	q, err := NewQueue(queuePath, 10)
	if err != nil {
		t.Fatalf("Failed to create queue: %v", err)
	}
	defer q.Close()

	records, err := q.ReadAll()
	if err != nil {
		t.Fatalf("Failed to read empty file: %v", err)
	}

	if len(records) != 0 {
		t.Errorf("Expected 0 records from empty file, got %d", len(records))
	}
}

func TestClearEmptyQueue(t *testing.T) {
	tmpDir := t.TempDir()
	queuePath := filepath.Join(tmpDir, "deadletter.jsonl")

	q, err := NewQueue(queuePath, 10)
	if err != nil {
		t.Fatalf("Failed to create queue: %v", err)
	}
	defer q.Close()

	// Clear should work on empty queue
	if err := q.Clear(); err != nil {
		t.Fatalf("Failed to clear empty queue: %v", err)
	}

	if q.GetRecordCount() != 0 {
		t.Errorf("Expected 0 records after clear, got %d", q.GetRecordCount())
	}
}

func TestCloseMultipleTimes(t *testing.T) {
	tmpDir := t.TempDir()
	queuePath := filepath.Join(tmpDir, "deadletter.jsonl")

	q, err := NewQueue(queuePath, 10)
	if err != nil {
		t.Fatalf("Failed to create queue: %v", err)
	}

	// First close
	if err := q.Close(); err != nil {
		t.Fatalf("Failed to close first time: %v", err)
	}

	// Second close should fail
	err = q.Close()
	if err == nil {
		t.Error("Expected error when closing already closed queue")
	}
}
