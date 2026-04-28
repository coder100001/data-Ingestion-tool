package deadletter

import (
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

func TestQueue_TimestampIsUTC(t *testing.T) {
	q, _ := setupTestQueue(t)
	defer q.Close()

	beforeWrite := time.Now().UTC()
	change := createTestChange()
	if err := q.Write(change, "failure", 1); err != nil {
		t.Fatalf("Write failed: %v", err)
	}
	afterWrite := time.Now().UTC()

	records, err := q.ReadAll()
	if err != nil {
		t.Fatalf("ReadAll failed: %v", err)
	}
	if len(records) != 1 {
		t.Fatalf("expected 1 record, got %d", len(records))
	}

	ts := records[0].Timestamp
	if ts.Before(beforeWrite) || ts.After(afterWrite) {
		t.Fatalf("timestamp %v not in expected range [%v, %v]", ts, beforeWrite, afterWrite)
	}
}
