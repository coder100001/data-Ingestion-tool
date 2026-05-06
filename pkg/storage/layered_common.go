package storage

import (
	"strings"
	"time"
)

// LayerType represents the storage layer type
type LayerType string

const (
	// BronzeLayer raw data layer
	BronzeLayer LayerType = "bronze"
	// SilverLayer cleaned data layer
	SilverLayer LayerType = "silver"
	// GoldLayer aggregated data layer
	GoldLayer LayerType = "gold"
)

// LayerConfig represents configuration for a storage layer
type LayerConfig struct {
	Type           LayerType
	Format         string
	Compression    string
	PartitionBy    []string
	RetentionDays  int
	EnableIndexing bool
}

// BronzeRecord represents raw data in bronze layer
type BronzeRecord struct {
	ID            string                 `json:"_id"`
	IngestedAt    time.Time              `json:"_ingested_at"`
	Source        string                 `json:"_source"`
	RawData       map[string]interface{} `json:"_raw_data"`
	BinlogInfo    *BinlogInfo            `json:"_binlog_info,omitempty"`
	SchemaVersion int                    `json:"_schema_version"`
}

// BinlogInfo represents MySQL binlog information
type BinlogInfo struct {
	File string `json:"file"`
	Pos  uint32 `json:"pos"`
}

// SilverRecord represents cleaned data in silver layer
type SilverRecord struct {
	ID           string                 `json:"_id"`
	IngestedAt   time.Time              `json:"_ingested_at"`
	ProcessedAt  time.Time              `json:"_processed_at"`
	Source       string                 `json:"_source"`
	CleanedData  map[string]interface{} `json:"_cleaned_data"`
	QualityScore float64                `json:"_quality_score"`
	Validation   *ValidationResult      `json:"_validation,omitempty"`
}

// ValidationResult represents data validation result
type ValidationResult struct {
	Valid    bool     `json:"valid"`
	Errors   []string `json:"errors,omitempty"`
	Warnings []string `json:"warnings,omitempty"`
}

// GoldRecord represents aggregated data in gold layer
type GoldRecord struct {
	ID           string                 `json:"_id"`
	AggregatedAt time.Time              `json:"_aggregated_at"`
	Grain        string                 `json:"_grain"`
	Metrics      map[string]interface{} `json:"_metrics"`
	Dimensions   map[string]interface{} `json:"_dimensions"`
	SourceTables []string               `json:"_source_tables"`
}

// DataCleaner defines the interface for data cleaning
type DataCleaner interface {
	Clean(data map[string]interface{}, rules []CleaningRule) (map[string]interface{}, error)
}

// DataValidator defines the interface for data validation
type DataValidator interface {
	Validate(data map[string]interface{}, rules []ValidationRule) (*ValidationResult, error)
}

// DataAggregator defines the interface for data aggregation
type DataAggregator interface {
	Aggregate(records []SilverRecord, grain string) (map[string]interface{}, map[string]interface{}, error)
}

// CleaningRule defines a data cleaning rule
type CleaningRule struct {
	Field     string                 `json:"field"`
	Operation string                 `json:"operation"` // trim, lowercase, uppercase, replace
	Params    map[string]interface{} `json:"params,omitempty"`
}

// ValidationRule defines a data validation rule
type ValidationRule struct {
	Field    string                 `json:"field"`
	RuleType string                 `json:"rule_type"` // required, type, range, regex, enum
	Params   map[string]interface{} `json:"params,omitempty"`
}

// LayerStats represents statistics for a storage layer
type LayerStats struct {
	TotalFiles     int64 `json:"total_files"`
	TotalSizeBytes int64 `json:"total_size_bytes"`
}

func trimString(s string) string {
	return strings.TrimSpace(s)
}

func toLowerCase(s string) string {
	return strings.ToLower(s)
}

func toUpperCase(s string) string {
	return strings.ToUpper(s)
}

func replaceString(s, old, new string) string {
	if old == "" {
		return s
	}
	return strings.ReplaceAll(s, old, new)
}

func checkType(value interface{}, expectedType string) bool {
	switch expectedType {
	case "string":
		_, ok := value.(string)
		return ok
	case "int":
		switch value.(type) {
		case int, int32, int64:
			return true
		}
		return false
	case "float":
		switch value.(type) {
		case float32, float64:
			return true
		}
		return false
	case "bool":
		_, ok := value.(bool)
		return ok
	case "datetime":
		_, ok := value.(time.Time)
		return ok
	default:
		return true
	}
}
