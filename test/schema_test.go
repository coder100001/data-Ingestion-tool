package test

import (
	"os"
	"path/filepath"
	"testing"

	"data-ingestion-tool/pkg/logger"
	"data-ingestion-tool/pkg/storage/parquet"
	"data-ingestion-tool/pkg/storage/schema"
)

func TestSchemaRegistry(t *testing.T) {
	tmpDir := t.TempDir()
	log, _ := logger.New("debug", "")

	registry := schema.NewRegistry(tmpDir, log)
	if err := registry.Initialize(); err != nil {
		t.Fatalf("Failed to initialize registry: %v", err)
	}

	// Create initial schema
	v1Schema := parquet.NewSchema([]parquet.ColumnDefinition{
		{Name: "id", Type: parquet.TypeInt64, Nullable: false},
		{Name: "name", Type: parquet.TypeByteArray, Nullable: false},
		{Name: "age", Type: parquet.TypeInt32, Nullable: true},
	})

	// Register schema
	version, err := registry.Register("users", v1Schema, schema.Backward)
	if err != nil {
		t.Fatalf("Failed to register schema: %v", err)
	}

	if version.Version != 1 {
		t.Fatalf("Expected version 1, got %d", version.Version)
	}

	// Retrieve schema
	retrieved, err := registry.GetLatestSchema("users")
	if err != nil {
		t.Fatalf("Failed to get schema: %v", err)
	}

	if retrieved.Version != 1 {
		t.Fatalf("Expected version 1, got %d", retrieved.Version)
	}

	t.Logf("Schema registered: version=%d, columns=%d", retrieved.Version, len(retrieved.Schema.Columns))
}

func TestSchemaCompatibility(t *testing.T) {
	tmpDir := t.TempDir()
	log, _ := logger.New("debug", "")

	registry := schema.NewRegistry(tmpDir, log)
	if err := registry.Initialize(); err != nil {
		t.Fatalf("Failed to initialize registry: %v", err)
	}

	// Register initial schema
	v1Schema := parquet.NewSchema([]parquet.ColumnDefinition{
		{Name: "id", Type: parquet.TypeInt64, Nullable: false},
		{Name: "name", Type: parquet.TypeByteArray, Nullable: false},
	})

	_, err := registry.Register("test", v1Schema, schema.Backward)
	if err != nil {
		t.Fatalf("Failed to register v1: %v", err)
	}

	// Test backward compatible change: add optional field
	v2Schema := parquet.NewSchema([]parquet.ColumnDefinition{
		{Name: "id", Type: parquet.TypeInt64, Nullable: false},
		{Name: "name", Type: parquet.TypeByteArray, Nullable: false},
		{Name: "email", Type: parquet.TypeByteArray, Nullable: true}, // New optional field
	})

	_, err = registry.Register("test", v2Schema, schema.Backward)
	if err != nil {
		t.Fatalf("Backward compatible change should succeed: %v", err)
	}

	// Test backward incompatible change: add required field
	v3Schema := parquet.NewSchema([]parquet.ColumnDefinition{
		{Name: "id", Type: parquet.TypeInt64, Nullable: false},
		{Name: "name", Type: parquet.TypeByteArray, Nullable: false},
		{Name: "email", Type: parquet.TypeByteArray, Nullable: false}, // New required field
	})

	_, err = registry.Register("test", v3Schema, schema.Backward)
	if err == nil {
		t.Fatal("Backward incompatible change should fail")
	}

	t.Logf("Compatibility check passed: %v", err)
}

func TestSchemaDiff(t *testing.T) {
	tmpDir := t.TempDir()
	log, _ := logger.New("debug", "")

	registry := schema.NewRegistry(tmpDir, log)
	if err := registry.Initialize(); err != nil {
		t.Fatalf("Failed to initialize registry: %v", err)
	}

	// Register v1
	v1Schema := parquet.NewSchema([]parquet.ColumnDefinition{
		{Name: "id", Type: parquet.TypeInt64, Nullable: false},
		{Name: "name", Type: parquet.TypeByteArray, Nullable: false},
		{Name: "age", Type: parquet.TypeInt32, Nullable: true},
	})

	_, err := registry.Register("diff_test", v1Schema, schema.None)
	if err != nil {
		t.Fatalf("Failed to register v1: %v", err)
	}

	// Register v2 with changes
	v2Schema := parquet.NewSchema([]parquet.ColumnDefinition{
		{Name: "id", Type: parquet.TypeInt64, Nullable: false},
		{Name: "name", Type: parquet.TypeByteArray, Nullable: false},
		{Name: "email", Type: parquet.TypeByteArray, Nullable: true}, // Added
		// "age" removed
	})

	_, err = registry.Register("diff_test", v2Schema, schema.None)
	if err != nil {
		t.Fatalf("Failed to register v2: %v", err)
	}

	// Get diff
	diff, err := registry.GetSchemaDiff("diff_test", 1, 2)
	if err != nil {
		t.Fatalf("Failed to get diff: %v", err)
	}

	if len(diff.Added) != 1 || diff.Added[0].Name != "email" {
		t.Fatalf("Expected 'email' to be added, got: %v", diff.Added)
	}

	if len(diff.Removed) != 1 || diff.Removed[0].Name != "age" {
		t.Fatalf("Expected 'age' to be removed, got: %v", diff.Removed)
	}

	t.Logf("Schema diff: added=%d, removed=%d, modified=%d", len(diff.Added), len(diff.Removed), len(diff.Modified))
}

func TestSchemaPersistence(t *testing.T) {
	tmpDir := t.TempDir()
	log, _ := logger.New("debug", "")

	// Create and populate registry
	registry1 := schema.NewRegistry(tmpDir, log)
	if err := registry1.Initialize(); err != nil {
		t.Fatalf("Failed to initialize registry: %v", err)
	}

	schema1 := parquet.NewSchema([]parquet.ColumnDefinition{
		{Name: "id", Type: parquet.TypeInt64, Nullable: false},
		{Name: "value", Type: parquet.TypeDouble, Nullable: true},
	})

	_, err := registry1.Register("persist", schema1, schema.Full)
	if err != nil {
		t.Fatalf("Failed to register: %v", err)
	}

	// Create new registry instance (simulating restart)
	registry2 := schema.NewRegistry(tmpDir, log)
	if err := registry2.Initialize(); err != nil {
		t.Fatalf("Failed to re-initialize registry: %v", err)
	}

	// Verify schema was loaded
	versions, err := registry2.ListVersions("persist")
	if err != nil {
		t.Fatalf("Failed to list versions: %v", err)
	}

	if len(versions) != 1 || versions[0] != 1 {
		t.Fatalf("Expected version [1], got: %v", versions)
	}

	// Verify schema file exists
	schemaFile := filepath.Join(tmpDir, "persist.json")
	if _, err := os.Stat(schemaFile); os.IsNotExist(err) {
		t.Fatal("Schema file was not persisted")
	}

	t.Log("Schema persistence test passed")
}
