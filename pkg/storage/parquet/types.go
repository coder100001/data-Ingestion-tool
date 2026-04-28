package parquet

import (
	"fmt"
	"time"
)

// Type represents a column data type
type Type int32

const (
	TypeBoolean Type = iota
	TypeInt32
	TypeInt64
	TypeFloat
	TypeDouble
	TypeByteArray
	TypeFixedLenByteArray
)

// String returns the string representation of the type
func (t Type) String() string {
	switch t {
	case TypeBoolean:
		return "BOOLEAN"
	case TypeInt32:
		return "INT32"
	case TypeInt64:
		return "INT64"
	case TypeFloat:
		return "FLOAT"
	case TypeDouble:
		return "DOUBLE"
	case TypeByteArray:
		return "BYTE_ARRAY"
	case TypeFixedLenByteArray:
		return "FIXED_LEN_BYTE_ARRAY"
	default:
		return "UNKNOWN"
	}
}

// GoType returns the Go type string
func (t Type) GoType() string {
	switch t {
	case TypeBoolean:
		return "bool"
	case TypeInt32:
		return "int32"
	case TypeInt64:
		return "int64"
	case TypeFloat:
		return "float32"
	case TypeDouble:
		return "float64"
	case TypeByteArray, TypeFixedLenByteArray:
		return "string"
	default:
		return "interface{}"
	}
}

// ConvertValue converts an interface{} value to the appropriate type
func (t Type) ConvertValue(v interface{}) (interface{}, error) {
	if v == nil {
		return nil, nil
	}

	switch t {
	case TypeBoolean:
		switch val := v.(type) {
		case bool:
			return val, nil
		case int:
			return val != 0, nil
		case string:
			return val == "true" || val == "1", nil
		default:
			return nil, fmt.Errorf("cannot convert %T to boolean", v)
		}
	case TypeInt32:
		switch val := v.(type) {
		case int32:
			return val, nil
		case int:
			return int32(val), nil
		case int64:
			return int32(val), nil
		case float64:
			return int32(val), nil
		case string:
			var i int32
			_, err := fmt.Sscanf(val, "%d", &i)
			return i, err
		default:
			return nil, fmt.Errorf("cannot convert %T to int32", v)
		}
	case TypeInt64:
		switch val := v.(type) {
		case int64:
			return val, nil
		case int:
			return int64(val), nil
		case int32:
			return int64(val), nil
		case float64:
			return int64(val), nil
		case string:
			var i int64
			_, err := fmt.Sscanf(val, "%d", &i)
			return i, err
		default:
			return nil, fmt.Errorf("cannot convert %T to int64", v)
		}
	case TypeFloat:
		switch val := v.(type) {
		case float32:
			return val, nil
		case float64:
			return float32(val), nil
		case int:
			return float32(val), nil
		case string:
			var f float32
			_, err := fmt.Sscanf(val, "%f", &f)
			return f, err
		default:
			return nil, fmt.Errorf("cannot convert %T to float32", v)
		}
	case TypeDouble:
		switch val := v.(type) {
		case float64:
			return val, nil
		case float32:
			return float64(val), nil
		case int:
			return float64(val), nil
		case string:
			var f float64
			_, err := fmt.Sscanf(val, "%f", &f)
			return f, err
		default:
			return nil, fmt.Errorf("cannot convert %T to float64", v)
		}
	case TypeByteArray, TypeFixedLenByteArray:
		switch val := v.(type) {
		case string:
			return val, nil
		case []byte:
			return string(val), nil
		case time.Time:
			return val.Format(time.RFC3339), nil
		default:
			return fmt.Sprintf("%v", v), nil
		}
	default:
		return v, nil
	}
}

// InferType infers the Parquet type from a Go value
func InferType(v interface{}) Type {
	if v == nil {
		return TypeByteArray
	}

	switch v.(type) {
	case bool:
		return TypeBoolean
	case int32:
		return TypeInt32
	case int, int64:
		return TypeInt64
	case float32:
		return TypeFloat
	case float64:
		return TypeDouble
	case string, []byte:
		return TypeByteArray
	case time.Time:
		return TypeByteArray
	default:
		return TypeByteArray
	}
}

// ColumnDefinition defines a column in the schema
type ColumnDefinition struct {
	Name        string `json:"name"`
	Type        Type   `json:"type"`
	Nullable    bool   `json:"nullable"`
	Length      int    `json:"length,omitempty"`
	Description string `json:"description,omitempty"`
}

// Schema defines the structure of a Parquet file
type Schema struct {
	Columns []ColumnDefinition `json:"columns"`
}

// NewSchema creates a new schema from column definitions
func NewSchema(columns []ColumnDefinition) *Schema {
	return &Schema{Columns: columns}
}

// NewSchemaFromMap creates a schema from a sample data map
func NewSchemaFromMap(data map[string]interface{}) *Schema {
	columns := make([]ColumnDefinition, 0, len(data))
	for key, value := range data {
		columns = append(columns, ColumnDefinition{
			Name:     key,
			Type:     InferType(value),
			Nullable: true,
		})
	}
	return NewSchema(columns)
}

// GetColumn returns a column definition by name
func (s *Schema) GetColumn(name string) *ColumnDefinition {
	for i := range s.Columns {
		if s.Columns[i].Name == name {
			return &s.Columns[i]
		}
	}
	return nil
}

// ColumnNames returns all column names
func (s *Schema) ColumnNames() []string {
	names := make([]string, len(s.Columns))
	for i, col := range s.Columns {
		names[i] = col.Name
	}
	return names
}
