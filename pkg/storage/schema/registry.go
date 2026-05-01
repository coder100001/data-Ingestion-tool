package schema

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"data-ingestion-tool/pkg/logger"
	"data-ingestion-tool/pkg/storage/parquet"
)

// CompatibilityType represents schema compatibility strategy
type CompatibilityType string

const (
	// Backward new readers can read old data
	Backward CompatibilityType = "BACKWARD"
	// Forward old readers can read new data
	Forward CompatibilityType = "FORWARD"
	// Full both backward and forward
	Full CompatibilityType = "FULL"
	// None no compatibility checks
	None CompatibilityType = "NONE"
)

// Registry manages schema versions
type Registry struct {
	mu       sync.RWMutex
	basePath string
	logger   *logger.Logger
	schemas  map[string]*SubjectSchemas // key: subject name
}

// SubjectSchemas holds all versions of a subject's schema
type SubjectSchemas struct {
	Subject       string            `json:"subject"`
	Versions      []SchemaVersion   `json:"versions"`
	Compatibility CompatibilityType `json:"compatibility"`
	LatestVersion int               `json:"latest_version"`
}

// SchemaVersion represents a single schema version
type SchemaVersion struct {
	Version     int             `json:"version"`
	Schema      *parquet.Schema `json:"schema"`
	Timestamp   time.Time       `json:"timestamp"`
	ID          string          `json:"id"`
	Description string          `json:"description,omitempty"`
}

// NewRegistry creates a new schema registry
func NewRegistry(basePath string, logger *logger.Logger) *Registry {
	return &Registry{
		basePath: basePath,
		logger:   logger,
		schemas:  make(map[string]*SubjectSchemas),
	}
}

// Initialize initializes the registry
func (r *Registry) Initialize() error {
	if err := os.MkdirAll(r.basePath, 0755); err != nil {
		return fmt.Errorf("failed to create registry directory: %w", err)
	}

	// Load existing schemas
	if err := r.Load(); err != nil {
		r.logger.WithError(err).Warn("Failed to load existing schemas")
	}

	r.logger.Info("Schema registry initialized")
	return nil
}

// Register registers a new schema version
func (r *Registry) Register(subject string, schema *parquet.Schema, compatibility CompatibilityType) (*SchemaVersion, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	subjectSchemas, exists := r.schemas[subject]
	if !exists {
		subjectSchemas = &SubjectSchemas{
			Subject:       subject,
			Versions:      make([]SchemaVersion, 0),
			Compatibility: compatibility,
		}
		r.schemas[subject] = subjectSchemas
	}

	// Check compatibility if there are existing versions
	if len(subjectSchemas.Versions) > 0 {
		latest := subjectSchemas.Versions[len(subjectSchemas.Versions)-1]
		if err := r.checkCompatibility(latest.Schema, schema, subjectSchemas.Compatibility); err != nil {
			return nil, fmt.Errorf("schema compatibility check failed: %w", err)
		}
	}

	// Create new version
	version := SchemaVersion{
		Version:     len(subjectSchemas.Versions) + 1,
		Schema:      schema,
		Timestamp:   time.Now().UTC(),
		ID:          fmt.Sprintf("%s-v%d", subject, len(subjectSchemas.Versions)+1),
		Description: fmt.Sprintf("Auto-registered schema for %s", subject),
	}

	subjectSchemas.Versions = append(subjectSchemas.Versions, version)
	subjectSchemas.LatestVersion = version.Version

	// Save to disk
	if err := r.saveSubject(subject); err != nil {
		return nil, fmt.Errorf("failed to save schema: %w", err)
	}

	r.logger.WithFields(map[string]interface{}{
		"subject": subject,
		"version": version.Version,
	}).Info("Schema registered")

	return &version, nil
}

// GetSchema returns a specific schema version
func (r *Registry) GetSchema(subject string, version int) (*SchemaVersion, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	subjectSchemas, exists := r.schemas[subject]
	if !exists {
		return nil, fmt.Errorf("subject not found: %s", subject)
	}

	if version <= 0 {
		version = subjectSchemas.LatestVersion
	}

	for _, v := range subjectSchemas.Versions {
		if v.Version == version {
			return &v, nil
		}
	}

	return nil, fmt.Errorf("version %d not found for subject %s", version, subject)
}

// GetLatestSchema returns the latest schema version
func (r *Registry) GetLatestSchema(subject string) (*SchemaVersion, error) {
	return r.GetSchema(subject, 0)
}

// ListSubjects returns all registered subjects
func (r *Registry) ListSubjects() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()

	subjects := make([]string, 0, len(r.schemas))
	for subject := range r.schemas {
		subjects = append(subjects, subject)
	}
	return subjects
}

// ListVersions returns all versions for a subject
func (r *Registry) ListVersions(subject string) ([]int, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	subjectSchemas, exists := r.schemas[subject]
	if !exists {
		return nil, fmt.Errorf("subject not found: %s", subject)
	}

	versions := make([]int, len(subjectSchemas.Versions))
	for i, v := range subjectSchemas.Versions {
		versions[i] = v.Version
	}
	return versions, nil
}

// SetCompatibility sets compatibility level for a subject
func (r *Registry) SetCompatibility(subject string, compatibility CompatibilityType) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	subjectSchemas, exists := r.schemas[subject]
	if !exists {
		return fmt.Errorf("subject not found: %s", subject)
	}

	subjectSchemas.Compatibility = compatibility

	if err := r.saveSubject(subject); err != nil {
		return fmt.Errorf("failed to save compatibility: %w", err)
	}

	return nil
}

// DeleteSubject deletes a subject and all its versions
func (r *Registry) DeleteSubject(subject string) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	delete(r.schemas, subject)

	filePath := filepath.Join(r.basePath, fmt.Sprintf("%s.json", subject))
	if err := os.Remove(filePath); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("failed to delete schema file: %w", err)
	}

	return nil
}

// checkCompatibility checks schema compatibility
func (r *Registry) checkCompatibility(oldSchema, newSchema *parquet.Schema, compatibility CompatibilityType) error {
	switch compatibility {
	case None:
		return nil
	case Backward:
		return checkBackwardCompatibility(oldSchema, newSchema)
	case Forward:
		return checkForwardCompatibility(oldSchema, newSchema)
	case Full:
		if err := checkBackwardCompatibility(oldSchema, newSchema); err != nil {
			return err
		}
		return checkForwardCompatibility(oldSchema, newSchema)
	default:
		return fmt.Errorf("unknown compatibility type: %s", compatibility)
	}
}

// checkBackwardCompatibility checks if new schema is backward compatible
func checkBackwardCompatibility(oldSchema, newSchema *parquet.Schema) error {
	// New schema can read old data
	// Rules:
	// 1. Can add optional fields
	// 2. Can remove fields with defaults
	// 3. Cannot change field types

	oldColumns := make(map[string]parquet.ColumnDefinition)
	for _, col := range oldSchema.Columns {
		oldColumns[col.Name] = col
	}

	for _, newCol := range newSchema.Columns {
		oldCol, exists := oldColumns[newCol.Name]
		if !exists {
			// New field added - must be optional
			if !newCol.Nullable {
				return fmt.Errorf("added field %s must be optional for backward compatibility", newCol.Name)
			}
			continue
		}

		// Type cannot change
		if oldCol.Type != newCol.Type {
			return fmt.Errorf("field %s type changed from %s to %s", newCol.Name, oldCol.Type, newCol.Type)
		}

		// Nullable can only become more permissive
		if oldCol.Nullable && !newCol.Nullable {
			return fmt.Errorf("field %s nullable constraint tightened", newCol.Name)
		}
	}

	return nil
}

// checkForwardCompatibility checks if new schema is forward compatible
func checkForwardCompatibility(oldSchema, newSchema *parquet.Schema) error {
	// Old schema can read new data
	// Rules:
	// 1. Can remove fields
	// 2. Can add required fields with defaults
	// 3. Cannot change field types

	newColumns := make(map[string]parquet.ColumnDefinition)
	for _, col := range newSchema.Columns {
		newColumns[col.Name] = col
	}

	for _, oldCol := range oldSchema.Columns {
		newCol, exists := newColumns[oldCol.Name]
		if !exists {
			// Field removed - old schema won't see it
			continue
		}

		// Type cannot change
		if oldCol.Type != newCol.Type {
			return fmt.Errorf("field %s type changed from %s to %s", oldCol.Name, oldCol.Type, newCol.Type)
		}
	}

	return nil
}

// saveSubject saves a subject's schemas to disk
func (r *Registry) saveSubject(subject string) error {
	subjectSchemas := r.schemas[subject]
	filePath := filepath.Join(r.basePath, fmt.Sprintf("%s.json", subject))

	data, err := json.MarshalIndent(subjectSchemas, "", "  ")
	if err != nil {
		return err
	}

	return os.WriteFile(filePath, data, 0644)
}

// Load loads all schemas from disk
func (r *Registry) Load() error {
	entries, err := os.ReadDir(r.basePath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}

	for _, entry := range entries {
		if entry.IsDir() || filepath.Ext(entry.Name()) != ".json" {
			continue
		}

		filePath := filepath.Join(r.basePath, entry.Name())
		data, err := os.ReadFile(filePath)
		if err != nil {
			r.logger.WithError(err).WithField("file", filePath).Warn("Failed to read schema file")
			continue
		}

		var subjectSchemas SubjectSchemas
		if err := json.Unmarshal(data, &subjectSchemas); err != nil {
			r.logger.WithError(err).WithField("file", filePath).Warn("Failed to parse schema file")
			continue
		}

		r.schemas[subjectSchemas.Subject] = &subjectSchemas
	}

	r.logger.WithField("count", len(r.schemas)).Info("Schemas loaded")
	return nil
}

// GetSchemaDiff returns the difference between two schema versions
func (r *Registry) GetSchemaDiff(subject string, fromVersion, toVersion int) (*SchemaDiff, error) {
	fromSchema, err := r.GetSchema(subject, fromVersion)
	if err != nil {
		return nil, err
	}

	toSchema, err := r.GetSchema(subject, toVersion)
	if err != nil {
		return nil, err
	}

	diff := &SchemaDiff{
		FromVersion: fromVersion,
		ToVersion:   toVersion,
		Added:       make([]parquet.ColumnDefinition, 0),
		Removed:     make([]parquet.ColumnDefinition, 0),
		Modified:    make([]ColumnChange, 0),
	}

	fromColumns := make(map[string]parquet.ColumnDefinition)
	for _, col := range fromSchema.Schema.Columns {
		fromColumns[col.Name] = col
	}

	toColumns := make(map[string]parquet.ColumnDefinition)
	for _, col := range toSchema.Schema.Columns {
		toColumns[col.Name] = col
	}

	// Find added and modified columns
	for _, col := range toSchema.Schema.Columns {
		fromCol, exists := fromColumns[col.Name]
		if !exists {
			diff.Added = append(diff.Added, col)
		} else if fromCol.Type != col.Type || fromCol.Nullable != col.Nullable {
			diff.Modified = append(diff.Modified, ColumnChange{
				Name:     col.Name,
				FromType: fromCol.Type,
				ToType:   col.Type,
				FromNull: fromCol.Nullable,
				ToNull:   col.Nullable,
			})
		}
	}

	// Find removed columns
	for _, col := range fromSchema.Schema.Columns {
		if _, exists := toColumns[col.Name]; !exists {
			diff.Removed = append(diff.Removed, col)
		}
	}

	return diff, nil
}

// SchemaDiff represents the difference between two schemas
type SchemaDiff struct {
	FromVersion int                        `json:"from_version"`
	ToVersion   int                        `json:"to_version"`
	Added       []parquet.ColumnDefinition `json:"added"`
	Removed     []parquet.ColumnDefinition `json:"removed"`
	Modified    []ColumnChange             `json:"modified"`
}

// ColumnChange represents a column modification
type ColumnChange struct {
	Name     string       `json:"name"`
	FromType parquet.Type `json:"from_type"`
	ToType   parquet.Type `json:"to_type"`
	FromNull bool         `json:"from_nullable"`
	ToNull   bool         `json:"to_nullable"`
}
