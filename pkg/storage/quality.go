package storage

import (
	"fmt"
	"reflect"
	"time"

	"data-ingestion-tool/pkg/models"
)

// QualityChecker performs data quality checks
type QualityChecker struct {
	checks []QualityCheckRule
}

// QualityCheckRule defines a quality check rule
type QualityCheckRule struct {
	Name        string
	Description string
	CheckFunc   func(data map[string]interface{}) (bool, float64, string)
}

// NewQualityChecker creates a new quality checker
func NewQualityChecker() *QualityChecker {
	return &QualityChecker{
		checks: []QualityCheckRule{
			{
				Name:        "completeness",
				Description: "Check if required fields are present",
				CheckFunc:   checkCompleteness,
			},
			{
				Name:        "validity",
				Description: "Check if data types are valid",
				CheckFunc:   checkValidity,
			},
			{
				Name:        "timeliness",
				Description: "Check if timestamps are reasonable",
				CheckFunc:   checkTimeliness,
			},
		},
	}
}

// Check performs quality checks on data change
func (qc *QualityChecker) Check(change *models.DataChange) *DataQuality {
	data := change.After
	if change.Type == models.Delete {
		data = change.Before
	}

	if data == nil {
		return &DataQuality{
			Completeness: 0,
			Validity:     0,
			Timeliness:   0,
			Consistency:  0,
			Uniqueness:   0,
			Checks: []QualityCheck{
				{
					Name:        "data_presence",
					Passed:      false,
					Score:       0,
					Description: "No data to check",
				},
			},
			LastCheckedAt: time.Now().UTC(),
		}
	}

	checks := make([]QualityCheck, 0, len(qc.checks))
	var completeness, validity, timeliness float64

	for _, rule := range qc.checks {
		passed, score, desc := rule.CheckFunc(data)
		checks = append(checks, QualityCheck{
			Name:        rule.Name,
			Passed:      passed,
			Score:       score,
			Description: desc,
		})

		switch rule.Name {
		case "completeness":
			completeness = score
		case "validity":
			validity = score
		case "timeliness":
			timeliness = score
		}
	}

	// Calculate overall consistency (simplified)
	consistency := (completeness + validity) / 2

	// Uniqueness check would require historical data
	uniqueness := 1.0 // Assume unique for now

	return &DataQuality{
		Completeness:  completeness,
		Validity:      validity,
		Timeliness:    timeliness,
		Consistency:   consistency,
		Uniqueness:    uniqueness,
		Checks:        checks,
		LastCheckedAt: time.Now().UTC(),
	}
}

// checkCompleteness checks if required fields are present
func checkCompleteness(data map[string]interface{}) (bool, float64, string) {
	if len(data) == 0 {
		return false, 0, "No data fields found"
	}

	// Count non-nil fields
	nonNilCount := 0
	for _, v := range data {
		if v != nil {
			nonNilCount++
		}
	}

	score := float64(nonNilCount) / float64(len(data))
	passed := score >= 0.8

	return passed, score, fmt.Sprintf("Completeness: %.2f%% (%d/%d fields)", score*100, nonNilCount, len(data))
}

// checkValidity checks if data types are valid
func checkValidity(data map[string]interface{}) (bool, float64, string) {
	if len(data) == 0 {
		return false, 0, "No data to validate"
	}

	validCount := 0
	errors := make([]string, 0)

	for key, value := range data {
		if value == nil {
			validCount++ // nil is valid (nullable field)
			continue
		}

		// Check type validity
		switch v := value.(type) {
		case string:
			if len(v) < 10000 { // Reasonable string length
				validCount++
			} else {
				errors = append(errors, fmt.Sprintf("Field %s: string too long", key))
			}
		case int, int32, int64, float32, float64:
			validCount++
		case bool:
			validCount++
		case time.Time:
			validCount++
		default:
			// Check if it's a valid complex type
			if reflect.TypeOf(v).Kind() == reflect.Map || reflect.TypeOf(v).Kind() == reflect.Slice {
				validCount++
			} else {
				errors = append(errors, fmt.Sprintf("Field %s: unknown type %T", key, v))
			}
		}
	}

	score := float64(validCount) / float64(len(data))
	passed := score >= 0.9

	desc := fmt.Sprintf("Validity: %.2f%% (%d/%d fields valid)", score*100, validCount, len(data))
	if len(errors) > 0 && len(errors) <= 3 {
		desc += ", errors: " + fmt.Sprintf("%v", errors)
	}

	return passed, score, desc
}

// checkTimeliness checks if timestamps are reasonable
func checkTimeliness(data map[string]interface{}) (bool, float64, string) {
	now := time.Now().UTC()
	minTime := now.AddDate(-1, 0, 0)  // 1 year ago
	maxTime := now.AddDate(0, 0, 1)   // 1 day in future

	timeFields := []string{"created_at", "updated_at", "timestamp", "time", "date"}
	checkedCount := 0
	validCount := 0

	for _, field := range timeFields {
		if value, exists := data[field]; exists {
			checkedCount++

			var t time.Time
			switch v := value.(type) {
			case time.Time:
				t = v
			case string:
				// Try to parse common time formats
				for _, layout := range []string{
					time.RFC3339,
					time.RFC3339Nano,
					"2006-01-02T15:04:05",
					"2006-01-02 15:04:05",
					"2006-01-02",
				} {
					if parsed, err := time.Parse(layout, v); err == nil {
						t = parsed
						break
					}
				}
			}

			if !t.IsZero() {
				if t.After(minTime) && t.Before(maxTime) {
					validCount++
				}
			}
		}
	}

	if checkedCount == 0 {
		return true, 1.0, "No timestamp fields to check"
	}

	score := float64(validCount) / float64(checkedCount)
	passed := score >= 0.8

	return passed, score, fmt.Sprintf("Timeliness: %.2f%% (%d/%d timestamps valid)", score*100, validCount, checkedCount)
}

// AddCustomCheck adds a custom quality check
func (qc *QualityChecker) AddCustomCheck(rule QualityCheckRule) {
	qc.checks = append(qc.checks, rule)
}

// SchemaValidator validates data against schema
type SchemaValidator struct {
	schema *SchemaVersion
}

// NewSchemaValidator creates a new schema validator
func NewSchemaValidator(schema *SchemaVersion) *SchemaValidator {
	return &SchemaValidator{schema: schema}
}

// Validate validates data against the schema
func (sv *SchemaValidator) Validate(data map[string]interface{}) *ValidationResult {
	result := &ValidationResult{
		Valid:    true,
		Errors:   make([]string, 0),
		Warnings: make([]string, 0),
	}

	if sv.schema == nil {
		result.Warnings = append(result.Warnings, "No schema defined for validation")
		return result
	}

	// Check required fields
	for _, col := range sv.schema.Columns {
		if !col.Nullable {
			if value, exists := data[col.Name]; !exists || value == nil {
				result.Valid = false
				result.Errors = append(result.Errors, fmt.Sprintf("Required field missing: %s", col.Name))
			}
		}
	}

	// Check data types
	for key, value := range data {
		if value == nil {
			continue
		}

		// Find column definition
		var colDef *ColumnInfo
		for _, col := range sv.schema.Columns {
			if col.Name == key {
				colDef = &col
				break
			}
		}

		if colDef == nil {
			result.Warnings = append(result.Warnings, fmt.Sprintf("Unknown field: %s", key))
			continue
		}

		// Type checking (simplified)
		if !isValidType(value, colDef.Type) {
			result.Warnings = append(result.Warnings,
				fmt.Sprintf("Type mismatch for %s: expected %s, got %T", key, colDef.Type, value))
		}
	}

	return result
}

// isValidType checks if value matches expected type
func isValidType(value interface{}, expectedType string) bool {
	switch expectedType {
	case "string", "varchar", "text":
		_, ok := value.(string)
		return ok
	case "int", "integer", "bigint":
		switch value.(type) {
		case int, int32, int64:
			return true
		}
		return false
	case "float", "double", "decimal":
		switch value.(type) {
		case float32, float64:
			return true
		}
		return false
	case "bool", "boolean":
		_, ok := value.(bool)
		return ok
	case "timestamp", "datetime", "date":
		switch value.(type) {
		case time.Time:
			return true
		case string:
			// Try to parse as time
			_, err := time.Parse(time.RFC3339, value.(string))
			return err == nil
		}
		return false
	default:
		return true // Unknown types pass
	}
}
