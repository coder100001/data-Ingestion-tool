package models

import (
	"encoding/json"
	"time"

	"github.com/google/uuid"
)

// ChangeType represents the type of data change
type ChangeType string

const (
	// Insert represents a new row insertion
	Insert ChangeType = "INSERT"
	// Update represents a row update
	Update ChangeType = "UPDATE"
	// Delete represents a row deletion
	Delete ChangeType = "DELETE"
)

// DataChange represents a single data change event
type DataChange struct {
	// Metadata
	ID        string    `json:"id"`
	Timestamp time.Time `json:"timestamp"`
	Source    string    `json:"source"`

	// Change information
	Type     ChangeType             `json:"type"`
	Database string                 `json:"database"`
	Table    string                 `json:"table"`
	Schema   map[string]interface{} `json:"schema,omitempty"`

	// Data
	Before map[string]interface{} `json:"before,omitempty"`
	After  map[string]interface{} `json:"after,omitempty"`

	// Binlog position for checkpointing
	BinlogFile string `json:"binlog_file"`
	BinlogPos  uint32 `json:"binlog_pos"`
}

// NewDataChange creates a new DataChange instance
func NewDataChange(changeType ChangeType, database, table string) *DataChange {
	return &DataChange{
		ID:        generateID(),
		Timestamp: time.Now().UTC(),
		Type:      changeType,
		Database:  database,
		Table:     table,
		Before:    make(map[string]interface{}),
		After:     make(map[string]interface{}),
		Schema:    make(map[string]interface{}),
	}
}

// generateID generates a unique ID for the change event
func generateID() string {
	return uuid.New().String()
}

// ToJSON converts the DataChange to JSON bytes
func (dc *DataChange) ToJSON() ([]byte, error) {
	return json.Marshal(dc)
}

// ToJSONString converts the DataChange to a JSON string
func (dc *DataChange) ToJSONString() (string, error) {
	bytes, err := dc.ToJSON()
	if err != nil {
		return "", err
	}
	return string(bytes), nil
}

// Checkpoint represents the current ingestion position
type Checkpoint struct {
	SourceType string    `json:"source_type"`
	Position   Position  `json:"position"`
	UpdatedAt  time.Time `json:"updated_at"`
}

// Position represents the source-specific position information
type Position struct {
	// MySQL binlog position
	BinlogFile string `json:"binlog_file,omitempty"`
	BinlogPos  uint32 `json:"binlog_pos,omitempty"`

	// Kafka offset
	Topic     string `json:"topic,omitempty"`
	Partition int32  `json:"partition,omitempty"`
	Offset    int64  `json:"offset,omitempty"`

	// PostgreSQL LSN
	LSN string `json:"lsn,omitempty"`

	// Generic timestamp for other sources
	Timestamp time.Time `json:"timestamp,omitempty"`
}

// NewCheckpoint creates a new Checkpoint instance
func NewCheckpoint(sourceType string) *Checkpoint {
	return &Checkpoint{
		SourceType: sourceType,
		UpdatedAt:  time.Now().UTC(),
	}
}

// UpdatePosition updates the checkpoint position
func (cp *Checkpoint) UpdatePosition(pos Position) {
	cp.Position = pos
	cp.UpdatedAt = time.Now().UTC()
}

// TableInfo represents metadata about a table
type TableInfo struct {
	Database   string       `json:"database"`
	Table      string       `json:"table"`
	Columns    []ColumnInfo `json:"columns"`
	PrimaryKey []string     `json:"primary_key"`
}

// ColumnInfo represents metadata about a column
type ColumnInfo struct {
	Name     string `json:"name"`
	Type     string `json:"type"`
	Nullable bool   `json:"nullable"`
	Default  string `json:"default,omitempty"`
}

// FilterRule represents a data filtering rule
type FilterRule struct {
	Column   string      `json:"column"`
	Operator string      `json:"operator"`
	Value    interface{} `json:"value"`
}

// TransformRule represents a data transformation rule
type TransformRule struct {
	Type       string                 `json:"type"`
	Column     string                 `json:"column,omitempty"`
	Expression string                 `json:"expression,omitempty"`
	Config     map[string]interface{} `json:"config,omitempty"`
}

// StorageFile represents a file in the data lake
type StorageFile struct {
	Path        string    `json:"path"`
	Size        int64     `json:"size"`
	RecordCount int       `json:"record_count"`
	CreatedAt   time.Time `json:"created_at"`
	ClosedAt    time.Time `json:"closed_at,omitempty"`
	Partition   string    `json:"partition"`
	Format      string    `json:"format"`
}
