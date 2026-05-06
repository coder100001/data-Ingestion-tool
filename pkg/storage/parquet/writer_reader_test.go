package parquet

import (
	"os"
	"path/filepath"
	"testing"

	"data-ingestion-tool/pkg/logger"
)

func newTestLogger(t *testing.T) *logger.Logger {
	t.Helper()
	l, err := logger.New("error", "")
	if err != nil {
		t.Fatalf("Failed to create logger: %v", err)
	}
	return l
}

func TestWriter_WriteAndClose(t *testing.T) {
	tmpDir := t.TempDir()
	filePath := filepath.Join(tmpDir, "test.parquet")
	log := newTestLogger(t)

	schema := NewSchema([]ColumnDefinition{
		{Name: "id", Type: TypeInt32},
		{Name: "name", Type: TypeByteArray},
		{Name: "score", Type: TypeDouble},
	})

	writer, err := NewWriter(filePath, schema, log)
	if err != nil {
		t.Fatalf("NewWriter() error = %v", err)
	}

	rows := []map[string]interface{}{
		{"id": int32(1), "name": "Alice", "score": 95.5},
		{"id": int32(2), "name": "Bob", "score": 87.0},
		{"id": int32(3), "name": "Charlie", "score": 92.3},
	}

	for i, row := range rows {
		if err := writer.WriteRow(row); err != nil {
			t.Fatalf("WriteRow %d error = %v", i, err)
		}
	}

	if err := writer.Close(); err != nil {
		t.Fatalf("Writer.Close() error = %v", err)
	}

	info, err := os.Stat(filePath)
	if err != nil {
		t.Fatalf("os.Stat() error = %v", err)
	}
	if info.Size() == 0 {
		t.Error("written file is empty")
	}
}

func TestWriterReader_Roundtrip(t *testing.T) {
	tmpDir := t.TempDir()
	filePath := filepath.Join(tmpDir, "roundtrip.parquet")
	log := newTestLogger(t)

	schema := NewSchema([]ColumnDefinition{
		{Name: "id", Type: TypeInt32},
		{Name: "name", Type: TypeByteArray},
		{Name: "active", Type: TypeBoolean},
		{Name: "score", Type: TypeDouble},
	})

	writer, err := NewWriter(filePath, schema, log)
	if err != nil {
		t.Fatalf("NewWriter() error = %v", err)
	}

	inputRows := []map[string]interface{}{
		{"id": int32(1), "name": "Alice", "active": true, "score": 95.5},
		{"id": int32(2), "name": "Bob", "active": false, "score": 87.0},
		{"id": int32(3), "name": "Charlie", "active": true, "score": 92.3},
	}

	for i, row := range inputRows {
		if err := writer.WriteRow(row); err != nil {
			t.Fatalf("WriteRow %d error = %v", i, err)
		}
	}
	writer.Close()

	reader, err := NewReader(filePath, log)
	if err != nil {
		t.Fatalf("NewReader() error = %v", err)
	}
	defer reader.Close()

	readRows, err := reader.ReadAll()
	if err != nil {
		t.Fatalf("ReadAll() error = %v", err)
	}

	if len(readRows) != len(inputRows) {
		t.Fatalf("ReadAll returned %d rows, want %d", len(readRows), len(inputRows))
	}

	for i, expected := range inputRows {
		got := readRows[i]
		for key, expectedVal := range expected {
			gotVal, ok := got[key]
			if !ok {
				t.Errorf("row %d: missing key %s", i, key)
				continue
			}
			if gotVal != expectedVal {
				t.Errorf("row %d: key %s = %v (type: %T), want %v (type: %T)",
					i, key, gotVal, gotVal, expectedVal, expectedVal)
			}
		}
	}
}

func TestWriterReader_EmptyFile(t *testing.T) {
	tmpDir := t.TempDir()
	filePath := filepath.Join(tmpDir, "empty.parquet")
	log := newTestLogger(t)

	writer, err := NewWriter(filePath, NewSchema([]ColumnDefinition{
		{Name: "id", Type: TypeInt32},
	}), log)
	if err != nil {
		t.Fatalf("NewWriter() error = %v", err)
	}
	writer.Close()

	reader, err := NewReader(filePath, log)
	if err != nil {
		t.Fatalf("NewReader() error = %v", err)
	}
	defer reader.Close()

	rows, err := reader.ReadAll()
	if err != nil {
		t.Fatalf("ReadAll() error = %v", err)
	}
	if len(rows) != 0 {
		t.Errorf("ReadAll() returned %d rows, want 0", len(rows))
	}
}

func TestWriter_MultipleRowGroups(t *testing.T) {
	tmpDir := t.TempDir()
	filePath := filepath.Join(tmpDir, "multi_group.parquet")
	log := newTestLogger(t)

	writer, err := NewWriter(filePath, NewSchema([]ColumnDefinition{
		{Name: "id", Type: TypeInt32},
		{Name: "value", Type: TypeByteArray},
	}), log)
	if err != nil {
		t.Fatalf("NewWriter() error = %v", err)
	}

	for i := 0; i < 100; i++ {
		row := map[string]interface{}{
			"id":    int32(i),
			"value": "data",
		}
		if err := writer.WriteRow(row); err != nil {
			t.Fatalf("WriteRow %d error = %v", i, err)
		}
	}
	writer.Close()

	reader, err := NewReader(filePath, log)
	if err != nil {
		t.Fatalf("NewReader() error = %v", err)
	}
	defer reader.Close()

	rows, err := reader.ReadAll()
	if err != nil {
		t.Fatalf("ReadAll() error = %v", err)
	}
	if len(rows) != 100 {
		t.Errorf("ReadAll() returned %d rows, want 100", len(rows))
	}
}

func TestReader_InvalidPath(t *testing.T) {
	log := newTestLogger(t)

	_, err := NewReader("/nonexistent/path/file.parquet", log)
	if err == nil {
		t.Error("NewReader() should error for invalid path")
	}
}

func TestNewWriter_InvalidPath(t *testing.T) {
	log := newTestLogger(t)

	_, err := NewWriter("/nonexistent/directory/file.parquet",
		NewSchema([]ColumnDefinition{{Name: "id", Type: TypeInt32}}), log)
	if err == nil {
		t.Error("NewWriter() should error for invalid directory")
	}
}
