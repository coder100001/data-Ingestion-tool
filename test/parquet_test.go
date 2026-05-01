package test

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"data-ingestion-tool/pkg/logger"
	"data-ingestion-tool/pkg/storage/parquet"
)

func TestParquetWriter(t *testing.T) {
	tmpDir := t.TempDir()
	log, _ := logger.New("debug", "")

	// Create schema
	schema := parquet.NewSchema([]parquet.ColumnDefinition{
		{Name: "id", Type: parquet.TypeInt64, Nullable: false},
		{Name: "name", Type: parquet.TypeByteArray, Nullable: false},
		{Name: "age", Type: parquet.TypeInt32, Nullable: true},
		{Name: "score", Type: parquet.TypeDouble, Nullable: true},
	})

	// Create writer
	path := filepath.Join(tmpDir, "test.parquet")
	writer, err := parquet.NewWriter(path, schema, log)
	if err != nil {
		t.Fatalf("Failed to create writer: %v", err)
	}

	// Write test data
	rows := []map[string]interface{}{
		{"id": int64(1), "name": "Alice", "age": int32(30), "score": 95.5},
		{"id": int64(2), "name": "Bob", "age": int32(25), "score": 88.0},
		{"id": int64(3), "name": "Charlie", "age": nil, "score": 92.3},
	}

	for _, row := range rows {
		if err := writer.WriteRow(row); err != nil {
			t.Fatalf("Failed to write row: %v", err)
		}
	}

	// Close writer
	if err := writer.Close(); err != nil {
		t.Fatalf("Failed to close writer: %v", err)
	}

	// Verify file exists
	if _, err := os.Stat(path); os.IsNotExist(err) {
		t.Fatal("Parquet file was not created")
	}

	// Verify row count
	if writer.NumRows() != int64(len(rows)) {
		t.Fatalf("Expected %d rows, got %d", len(rows), writer.NumRows())
	}

	t.Logf("Successfully wrote %d rows to Parquet file", writer.NumRows())
}

func TestParquetSchemaInference(t *testing.T) {
	data := map[string]interface{}{
		"id":         int64(1),
		"name":       "Alice",
		"active":     true,
		"score":      95.5,
		"age":        int32(30),
		"created_at": time.Now(),
	}

	schema := parquet.NewSchemaFromMap(data)

	if len(schema.Columns) != len(data) {
		t.Fatalf("Expected %d columns, got %d", len(data), len(schema.Columns))
	}

	// Verify type inference
	col := schema.GetColumn("id")
	if col == nil {
		t.Fatal("Column 'id' not found")
	}
	if col.Type != parquet.TypeInt64 {
		t.Fatalf("Expected INT64 for id, got %s", col.Type)
	}

	col = schema.GetColumn("name")
	if col == nil {
		t.Fatal("Column 'name' not found")
	}
	if col.Type != parquet.TypeByteArray {
		t.Fatalf("Expected BYTE_ARRAY for name, got %s", col.Type)
	}

	t.Log("Schema inference test passed")
}

func TestParquetTypeConversion(t *testing.T) {
	tests := []struct {
		typ      parquet.Type
		input    interface{}
		expected interface{}
	}{
		{parquet.TypeBoolean, true, true},
		{parquet.TypeBoolean, 1, true},
		{parquet.TypeInt32, int(42), int32(42)},
		{parquet.TypeInt64, int(42), int64(42)},
		{parquet.TypeFloat, float64(3.14), float32(3.14)},
		{parquet.TypeDouble, float64(3.14), float64(3.14)},
		{parquet.TypeByteArray, "hello", "hello"},
	}

	for _, test := range tests {
		result, err := test.typ.ConvertValue(test.input)
		if err != nil {
			t.Errorf("Failed to convert %v to %s: %v", test.input, test.typ, err)
			continue
		}
		if result != test.expected {
			t.Errorf("Expected %v, got %v for type %s", test.expected, result, test.typ)
		}
	}
}
