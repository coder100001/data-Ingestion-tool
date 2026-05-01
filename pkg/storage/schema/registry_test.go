package schema

import (
	"os"
	"path/filepath"
	"testing"

	"data-ingestion-tool/pkg/logger"
	"data-ingestion-tool/pkg/storage/parquet"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func setupRegistry(t *testing.T) *Registry {
	tmpDir := t.TempDir()
	log, err := logger.New("debug", "")
	require.NoError(t, err)

	registry := NewRegistry(tmpDir, log)
	err = registry.Initialize()
	require.NoError(t, err)

	return registry
}

func createTestSchema(columns []parquet.ColumnDefinition) *parquet.Schema {
	return parquet.NewSchema(columns)
}

func TestNewRegistry(t *testing.T) {
	tmpDir := t.TempDir()
	log, err := logger.New("debug", "")
	require.NoError(t, err)

	registry := NewRegistry(tmpDir, log)
	assert.NotNil(t, registry)
	assert.Equal(t, tmpDir, registry.basePath)
	assert.NotNil(t, registry.schemas)
}

func TestRegistry_Initialize(t *testing.T) {
	tmpDir := t.TempDir()
	log, err := logger.New("debug", "")
	require.NoError(t, err)

	registry := NewRegistry(tmpDir, log)
	err = registry.Initialize()
	assert.NoError(t, err)

	_, err = os.Stat(tmpDir)
	assert.NoError(t, err)
}

func TestRegistry_Register_FirstVersion(t *testing.T) {
	registry := setupRegistry(t)

	schema := createTestSchema([]parquet.ColumnDefinition{
		{Name: "id", Type: parquet.TypeInt64, Nullable: false},
		{Name: "name", Type: parquet.TypeByteArray, Nullable: true},
	})

	version, err := registry.Register("test-subject", schema, Backward)
	require.NoError(t, err)
	assert.NotNil(t, version)
	assert.Equal(t, 1, version.Version)
	assert.Equal(t, "test-subject-v1", version.ID)
	assert.NotEmpty(t, version.Timestamp)
}

func TestRegistry_Register_MultipleVersions(t *testing.T) {
	registry := setupRegistry(t)

	v1 := createTestSchema([]parquet.ColumnDefinition{
		{Name: "id", Type: parquet.TypeInt64, Nullable: false},
		{Name: "name", Type: parquet.TypeByteArray, Nullable: true},
	})

	version1, err := registry.Register("multi-test", v1, Backward)
	require.NoError(t, err)
	assert.Equal(t, 1, version1.Version)

	v2 := createTestSchema([]parquet.ColumnDefinition{
		{Name: "id", Type: parquet.TypeInt64, Nullable: false},
		{Name: "name", Type: parquet.TypeByteArray, Nullable: true},
		{Name: "email", Type: parquet.TypeByteArray, Nullable: true},
	})

	version2, err := registry.Register("multi-test", v2, Backward)
	require.NoError(t, err)
	assert.Equal(t, 2, version2.Version)
	assert.Equal(t, "multi-test-v2", version2.ID)
}

func TestRegistry_GetSchema(t *testing.T) {
	registry := setupRegistry(t)

	schema := createTestSchema([]parquet.ColumnDefinition{
		{Name: "id", Type: parquet.TypeInt64, Nullable: false},
	})
	_, err := registry.Register("get-test", schema, Backward)
	require.NoError(t, err)

	tests := []struct {
		name        string
		subject     string
		version     int
		expectError bool
		errorMsg    string
	}{
		{"valid version", "get-test", 1, false, ""},
		{"latest version", "get-test", 0, false, ""},
		{"invalid subject", "nonexistent", 1, true, "subject not found"},
		{"invalid version", "get-test", 999, true, "version 999 not found"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			version, err := registry.GetSchema(tt.subject, tt.version)
			if tt.expectError {
				assert.Error(t, err)
				assert.Contains(t, err.Error(), tt.errorMsg)
				assert.Nil(t, version)
			} else {
				assert.NoError(t, err)
				assert.NotNil(t, version)
			}
		})
	}
}

func TestRegistry_GetLatestSchema(t *testing.T) {
	registry := setupRegistry(t)

	v1 := createTestSchema([]parquet.ColumnDefinition{
		{Name: "id", Type: parquet.TypeInt64, Nullable: false},
	})
	_, err := registry.Register("latest-test", v1, Backward)
	require.NoError(t, err)

	v2 := createTestSchema([]parquet.ColumnDefinition{
		{Name: "id", Type: parquet.TypeInt64, Nullable: false},
		{Name: "name", Type: parquet.TypeByteArray, Nullable: true},
	})
	_, err = registry.Register("latest-test", v2, Backward)
	require.NoError(t, err)

	latest, err := registry.GetLatestSchema("latest-test")
	require.NoError(t, err)
	assert.Equal(t, 2, latest.Version)

	_, err = registry.GetLatestSchema("nonexistent")
	assert.Error(t, err)
}

func TestRegistry_ListSubjects(t *testing.T) {
	registry := setupRegistry(t)

	schema := createTestSchema([]parquet.ColumnDefinition{
		{Name: "id", Type: parquet.TypeInt64, Nullable: false},
	})

	_, err := registry.Register("subject1", schema, Backward)
	require.NoError(t, err)

	_, err = registry.Register("subject2", schema, Forward)
	require.NoError(t, err)

	subjects := registry.ListSubjects()
	assert.Len(t, subjects, 2)
	assert.Contains(t, subjects, "subject1")
	assert.Contains(t, subjects, "subject2")
}

func TestRegistry_ListVersions(t *testing.T) {
	registry := setupRegistry(t)

	schema := createTestSchema([]parquet.ColumnDefinition{
		{Name: "id", Type: parquet.TypeInt64, Nullable: false},
	})

	_, err := registry.Register("versions-test", schema, Backward)
	require.NoError(t, err)

	v2 := createTestSchema([]parquet.ColumnDefinition{
		{Name: "id", Type: parquet.TypeInt64, Nullable: false},
		{Name: "name", Type: parquet.TypeByteArray, Nullable: true},
	})
	_, err = registry.Register("versions-test", v2, Backward)
	require.NoError(t, err)

	versions, err := registry.ListVersions("versions-test")
	require.NoError(t, err)
	assert.Equal(t, []int{1, 2}, versions)

	_, err = registry.ListVersions("nonexistent")
	assert.Error(t, err)
}

func TestRegistry_DeleteSubject(t *testing.T) {
	registry := setupRegistry(t)

	schema := createTestSchema([]parquet.ColumnDefinition{
		{Name: "id", Type: parquet.TypeInt64, Nullable: false},
	})

	_, err := registry.Register("delete-test", schema, Backward)
	require.NoError(t, err)

	err = registry.DeleteSubject("delete-test")
	assert.NoError(t, err)

	subjects := registry.ListSubjects()
	assert.NotContains(t, subjects, "delete-test")

	_, err = registry.GetSchema("delete-test", 1)
	assert.Error(t, err)
}

func TestRegistry_SetCompatibility(t *testing.T) {
	registry := setupRegistry(t)

	schema := createTestSchema([]parquet.ColumnDefinition{
		{Name: "id", Type: parquet.TypeInt64, Nullable: false},
	})

	_, err := registry.Register("compat-test", schema, Backward)
	require.NoError(t, err)

	err = registry.SetCompatibility("compat-test", Forward)
	require.NoError(t, err)

	_, err = registry.GetSchema("compat-test", 1)
	require.NoError(t, err)

	err = registry.SetCompatibility("nonexistent", Forward)
	assert.Error(t, err)
}

func TestBackwardCompatibility_AddOptionalField(t *testing.T) {
	registry := setupRegistry(t)

	v1 := createTestSchema([]parquet.ColumnDefinition{
		{Name: "id", Type: parquet.TypeInt64, Nullable: false},
		{Name: "name", Type: parquet.TypeByteArray, Nullable: false},
	})

	_, err := registry.Register("backward-test", v1, Backward)
	require.NoError(t, err)

	v2 := createTestSchema([]parquet.ColumnDefinition{
		{Name: "id", Type: parquet.TypeInt64, Nullable: false},
		{Name: "name", Type: parquet.TypeByteArray, Nullable: false},
		{Name: "email", Type: parquet.TypeByteArray, Nullable: true},
	})

	version, err := registry.Register("backward-test", v2, Backward)
	require.NoError(t, err)
	assert.Equal(t, 2, version.Version)
}

func TestBackwardCompatibility_AddRequiredField_ShouldFail(t *testing.T) {
	registry := setupRegistry(t)

	v1 := createTestSchema([]parquet.ColumnDefinition{
		{Name: "id", Type: parquet.TypeInt64, Nullable: false},
	})

	_, err := registry.Register("backward-fail", v1, Backward)
	require.NoError(t, err)

	v2 := createTestSchema([]parquet.ColumnDefinition{
		{Name: "id", Type: parquet.TypeInt64, Nullable: false},
		{Name: "required_field", Type: parquet.TypeByteArray, Nullable: false},
	})

	_, err = registry.Register("backward-fail", v2, Backward)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "must be optional for backward compatibility")
}

func TestBackwardCompatibility_ChangeFieldType_ShouldFail(t *testing.T) {
	registry := setupRegistry(t)

	v1 := createTestSchema([]parquet.ColumnDefinition{
		{Name: "id", Type: parquet.TypeInt64, Nullable: false},
	})

	_, err := registry.Register("type-change", v1, Backward)
	require.NoError(t, err)

	v2 := createTestSchema([]parquet.ColumnDefinition{
		{Name: "id", Type: parquet.TypeInt32, Nullable: false},
	})

	_, err = registry.Register("type-change", v2, Backward)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "type changed")
}

func TestBackwardCompatibility_TightenNullable_ShouldFail(t *testing.T) {
	registry := setupRegistry(t)

	v1 := createTestSchema([]parquet.ColumnDefinition{
		{Name: "id", Type: parquet.TypeInt64, Nullable: true},
	})

	_, err := registry.Register("nullable-tighten", v1, Backward)
	require.NoError(t, err)

	v2 := createTestSchema([]parquet.ColumnDefinition{
		{Name: "id", Type: parquet.TypeInt64, Nullable: false},
	})

	_, err = registry.Register("nullable-tighten", v2, Backward)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "nullable constraint tightened")
}

func TestForwardCompatibility_RemoveField(t *testing.T) {
	registry := setupRegistry(t)

	v1 := createTestSchema([]parquet.ColumnDefinition{
		{Name: "id", Type: parquet.TypeInt64, Nullable: false},
		{Name: "name", Type: parquet.TypeByteArray, Nullable: false},
		{Name: "age", Type: parquet.TypeInt32, Nullable: true},
	})

	_, err := registry.Register("forward-test", v1, Forward)
	require.NoError(t, err)

	v2 := createTestSchema([]parquet.ColumnDefinition{
		{Name: "id", Type: parquet.TypeInt64, Nullable: false},
		{Name: "name", Type: parquet.TypeByteArray, Nullable: false},
	})

	version, err := registry.Register("forward-test", v2, Forward)
	require.NoError(t, err)
	assert.Equal(t, 2, version.Version)
}

func TestForwardCompatibility_ChangeFieldType_ShouldFail(t *testing.T) {
	registry := setupRegistry(t)

	v1 := createTestSchema([]parquet.ColumnDefinition{
		{Name: "id", Type: parquet.TypeInt64, Nullable: false},
	})

	_, err := registry.Register("forward-type", v1, Forward)
	require.NoError(t, err)

	v2 := createTestSchema([]parquet.ColumnDefinition{
		{Name: "id", Type: parquet.TypeByteArray, Nullable: false},
	})

	_, err = registry.Register("forward-type", v2, Forward)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "type changed")
}

func TestFullCompatibility_ValidChange(t *testing.T) {
	registry := setupRegistry(t)

	v1 := createTestSchema([]parquet.ColumnDefinition{
		{Name: "id", Type: parquet.TypeInt64, Nullable: false},
		{Name: "name", Type: parquet.TypeByteArray, Nullable: true},
	})

	_, err := registry.Register("full-test", v1, Full)
	require.NoError(t, err)

	v2 := createTestSchema([]parquet.ColumnDefinition{
		{Name: "id", Type: parquet.TypeInt64, Nullable: false},
		{Name: "name", Type: parquet.TypeByteArray, Nullable: true},
		{Name: "email", Type: parquet.TypeByteArray, Nullable: true},
	})

	version, err := registry.Register("full-test", v2, Full)
	require.NoError(t, err)
	assert.Equal(t, 2, version.Version)
}

func TestFullCompatibility_InvalidChange_ShouldFail(t *testing.T) {
	registry := setupRegistry(t)

	v1 := createTestSchema([]parquet.ColumnDefinition{
		{Name: "id", Type: parquet.TypeInt64, Nullable: false},
	})

	_, err := registry.Register("full-fail", v1, Full)
	require.NoError(t, err)

	v2 := createTestSchema([]parquet.ColumnDefinition{
		{Name: "id", Type: parquet.TypeInt32, Nullable: false},
	})

	_, err = registry.Register("full-fail", v2, Full)
	assert.Error(t, err)
}

func TestNoneCompatibility_AnyChange(t *testing.T) {
	registry := setupRegistry(t)

	v1 := createTestSchema([]parquet.ColumnDefinition{
		{Name: "id", Type: parquet.TypeInt64, Nullable: false},
	})

	_, err := registry.Register("none-test", v1, None)
	require.NoError(t, err)

	v2 := createTestSchema([]parquet.ColumnDefinition{
		{Name: "id", Type: parquet.TypeByteArray, Nullable: true},
		{Name: "new_field", Type: parquet.TypeDouble, Nullable: false},
	})

	version, err := registry.Register("none-test", v2, None)
	require.NoError(t, err)
	assert.Equal(t, 2, version.Version)
}

func TestUnknownCompatibilityType(t *testing.T) {
	registry := setupRegistry(t)

	v1 := createTestSchema([]parquet.ColumnDefinition{
		{Name: "id", Type: parquet.TypeInt64, Nullable: false},
	})

	_, err := registry.Register("unknown-compat", v1, Backward)
	require.NoError(t, err)

	v2 := createTestSchema([]parquet.ColumnDefinition{
		{Name: "id", Type: parquet.TypeInt32, Nullable: false},
	})

	registry.schemas["unknown-compat"].Compatibility = CompatibilityType("UNKNOWN")

	_, err = registry.Register("unknown-compat", v2, CompatibilityType("UNKNOWN"))
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "unknown compatibility type")
}

func TestGetSchemaDiff_AddedFields(t *testing.T) {
	registry := setupRegistry(t)

	v1 := createTestSchema([]parquet.ColumnDefinition{
		{Name: "id", Type: parquet.TypeInt64, Nullable: false},
	})

	_, err := registry.Register("diff-add", v1, None)
	require.NoError(t, err)

	v2 := createTestSchema([]parquet.ColumnDefinition{
		{Name: "id", Type: parquet.TypeInt64, Nullable: false},
		{Name: "name", Type: parquet.TypeByteArray, Nullable: true},
		{Name: "email", Type: parquet.TypeByteArray, Nullable: true},
	})

	_, err = registry.Register("diff-add", v2, None)
	require.NoError(t, err)

	diff, err := registry.GetSchemaDiff("diff-add", 1, 2)
	require.NoError(t, err)
	assert.Len(t, diff.Added, 2)
	assert.Len(t, diff.Removed, 0)
	assert.Len(t, diff.Modified, 0)

	addedNames := make([]string, len(diff.Added))
	for i, col := range diff.Added {
		addedNames[i] = col.Name
	}
	assert.Contains(t, addedNames, "name")
	assert.Contains(t, addedNames, "email")
}

func TestGetSchemaDiff_RemovedFields(t *testing.T) {
	registry := setupRegistry(t)

	v1 := createTestSchema([]parquet.ColumnDefinition{
		{Name: "id", Type: parquet.TypeInt64, Nullable: false},
		{Name: "name", Type: parquet.TypeByteArray, Nullable: true},
		{Name: "age", Type: parquet.TypeInt32, Nullable: true},
	})

	_, err := registry.Register("diff-remove", v1, None)
	require.NoError(t, err)

	v2 := createTestSchema([]parquet.ColumnDefinition{
		{Name: "id", Type: parquet.TypeInt64, Nullable: false},
	})

	_, err = registry.Register("diff-remove", v2, None)
	require.NoError(t, err)

	diff, err := registry.GetSchemaDiff("diff-remove", 1, 2)
	require.NoError(t, err)
	assert.Len(t, diff.Added, 0)
	assert.Len(t, diff.Removed, 2)
	assert.Len(t, diff.Modified, 0)

	removedNames := make([]string, len(diff.Removed))
	for i, col := range diff.Removed {
		removedNames[i] = col.Name
	}
	assert.Contains(t, removedNames, "name")
	assert.Contains(t, removedNames, "age")
}

func TestGetSchemaDiff_ModifiedFields(t *testing.T) {
	registry := setupRegistry(t)

	v1 := createTestSchema([]parquet.ColumnDefinition{
		{Name: "id", Type: parquet.TypeInt64, Nullable: false},
		{Name: "value", Type: parquet.TypeInt32, Nullable: false},
	})

	_, err := registry.Register("diff-modify", v1, None)
	require.NoError(t, err)

	v2 := createTestSchema([]parquet.ColumnDefinition{
		{Name: "id", Type: parquet.TypeInt64, Nullable: false},
		{Name: "value", Type: parquet.TypeInt64, Nullable: true},
	})

	_, err = registry.Register("diff-modify", v2, None)
	require.NoError(t, err)

	diff, err := registry.GetSchemaDiff("diff-modify", 1, 2)
	require.NoError(t, err)
	assert.Len(t, diff.Added, 0)
	assert.Len(t, diff.Removed, 0)
	assert.Len(t, diff.Modified, 1)

	assert.Equal(t, "value", diff.Modified[0].Name)
	assert.Equal(t, parquet.TypeInt32, diff.Modified[0].FromType)
	assert.Equal(t, parquet.TypeInt64, diff.Modified[0].ToType)
	assert.False(t, diff.Modified[0].FromNull)
	assert.True(t, diff.Modified[0].ToNull)
}

func TestGetSchemaDiff_InvalidVersions(t *testing.T) {
	registry := setupRegistry(t)

	schema := createTestSchema([]parquet.ColumnDefinition{
		{Name: "id", Type: parquet.TypeInt64, Nullable: false},
	})

	_, err := registry.Register("diff-invalid", schema, None)
	require.NoError(t, err)

	_, err = registry.GetSchemaDiff("diff-invalid", 1, 999)
	assert.Error(t, err)

	_, err = registry.GetSchemaDiff("diff-invalid", 999, 1)
	assert.Error(t, err)

	_, err = registry.GetSchemaDiff("nonexistent", 1, 2)
	assert.Error(t, err)
}

func TestRegistry_Persistence(t *testing.T) {
	tmpDir := t.TempDir()
	log, err := logger.New("debug", "")
	require.NoError(t, err)

	registry1 := NewRegistry(tmpDir, log)
	err = registry1.Initialize()
	require.NoError(t, err)

	schema := createTestSchema([]parquet.ColumnDefinition{
		{Name: "id", Type: parquet.TypeInt64, Nullable: false},
		{Name: "name", Type: parquet.TypeByteArray, Nullable: true},
	})

	_, err = registry1.Register("persist-test", schema, Full)
	require.NoError(t, err)

	schemaFile := filepath.Join(tmpDir, "persist-test.json")
	_, err = os.Stat(schemaFile)
	require.NoError(t, err)

	registry2 := NewRegistry(tmpDir, log)
	err = registry2.Initialize()
	require.NoError(t, err)

	versions, err := registry2.ListVersions("persist-test")
	require.NoError(t, err)
	assert.Equal(t, []int{1}, versions)

	loadedSchema, err := registry2.GetLatestSchema("persist-test")
	require.NoError(t, err)
	assert.Len(t, loadedSchema.Schema.Columns, 2)
}

func TestRegistry_ConcurrentAccess(t *testing.T) {
	registry := setupRegistry(t)

	done := make(chan bool)

	for i := 0; i < 10; i++ {
		go func(id int) {
			schema := createTestSchema([]parquet.ColumnDefinition{
				{Name: "id", Type: parquet.TypeInt64, Nullable: false},
			})
			subject := "concurrent-test"
			registry.Register(subject, schema, None)
			registry.GetLatestSchema(subject)
			registry.ListSubjects()
			done <- true
		}(i)
	}

	for i := 0; i < 10; i++ {
		<-done
	}

	versions, err := registry.ListVersions("concurrent-test")
	require.NoError(t, err)
	assert.GreaterOrEqual(t, len(versions), 1)
}

func TestRegistry_Load_EmptyDirectory(t *testing.T) {
	tmpDir := t.TempDir()
	log, err := logger.New("debug", "")
	require.NoError(t, err)

	registry := NewRegistry(tmpDir, log)
	err = registry.Load()
	assert.NoError(t, err)
	assert.Empty(t, registry.schemas)
}

func TestRegistry_Load_InvalidJSON(t *testing.T) {
	tmpDir := t.TempDir()
	log, err := logger.New("debug", "")
	require.NoError(t, err)

	invalidFile := filepath.Join(tmpDir, "invalid.json")
	err = os.WriteFile(invalidFile, []byte("invalid json"), 0644)
	require.NoError(t, err)

	registry := NewRegistry(tmpDir, log)
	err = registry.Load()
	assert.NoError(t, err)
}

func TestCheckBackwardCompatibility_AllowRelaxNullable(t *testing.T) {
	oldSchema := createTestSchema([]parquet.ColumnDefinition{
		{Name: "id", Type: parquet.TypeInt64, Nullable: false},
	})

	newSchema := createTestSchema([]parquet.ColumnDefinition{
		{Name: "id", Type: parquet.TypeInt64, Nullable: true},
	})

	err := checkBackwardCompatibility(oldSchema, newSchema)
	assert.NoError(t, err)
}

func TestCheckForwardCompatibility_AllowAddRequiredField(t *testing.T) {
	oldSchema := createTestSchema([]parquet.ColumnDefinition{
		{Name: "id", Type: parquet.TypeInt64, Nullable: false},
	})

	newSchema := createTestSchema([]parquet.ColumnDefinition{
		{Name: "id", Type: parquet.TypeInt64, Nullable: false},
		{Name: "new_required", Type: parquet.TypeByteArray, Nullable: false},
	})

	err := checkForwardCompatibility(oldSchema, newSchema)
	assert.NoError(t, err)
}
