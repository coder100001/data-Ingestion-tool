package parquet

import (
	"testing"
	"time"
)

func TestType_String(t *testing.T) {
	tests := []struct {
		name string
		typ  Type
		want string
	}{
		{"BOOLEAN", TypeBoolean, "BOOLEAN"},
		{"INT32", TypeInt32, "INT32"},
		{"INT64", TypeInt64, "INT64"},
		{"FLOAT", TypeFloat, "FLOAT"},
		{"DOUBLE", TypeDouble, "DOUBLE"},
		{"BYTE_ARRAY", TypeByteArray, "BYTE_ARRAY"},
		{"FIXED_LEN_BYTE_ARRAY", TypeFixedLenByteArray, "FIXED_LEN_BYTE_ARRAY"},
		{"UNKNOWN", Type(-1), "UNKNOWN"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.typ.String(); got != tt.want {
				t.Errorf("Type.String() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestType_GoType(t *testing.T) {
	tests := []struct {
		name string
		typ  Type
		want string
	}{
		{"BOOLEAN", TypeBoolean, "bool"},
		{"INT32", TypeInt32, "int32"},
		{"INT64", TypeInt64, "int64"},
		{"FLOAT", TypeFloat, "float32"},
		{"DOUBLE", TypeDouble, "float64"},
		{"BYTE_ARRAY", TypeByteArray, "string"},
		{"FIXED_LEN_BYTE_ARRAY", TypeFixedLenByteArray, "string"},
		{"UNKNOWN", Type(-1), "interface{}"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.typ.GoType(); got != tt.want {
				t.Errorf("Type.GoType() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestType_ConvertValue(t *testing.T) {
	now := time.Now()
	t.Run("TypeBoolean", func(t *testing.T) {
		tests := []struct {
			name    string
			input   interface{}
			want    interface{}
			wantErr bool
		}{
			{"nil", nil, nil, false},
			{"bool true", true, true, false},
			{"bool false", false, false, false},
			{"int 0", 0, false, false},
			{"int 1", 1, true, false},
			{"string true", "true", true, false},
			{"string 1", "1", true, false},
			{"string false", "false", false, false},
			{"invalid type", 3.14, nil, true},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				got, err := TypeBoolean.ConvertValue(tt.input)
				if (err != nil) != tt.wantErr {
					t.Errorf("TypeBoolean.ConvertValue() error = %v, wantErr %v", err, tt.wantErr)
					return
				}
				if got != tt.want {
					t.Errorf("TypeBoolean.ConvertValue() = %v, want %v", got, tt.want)
				}
			})
		}
	})

	t.Run("TypeInt32", func(t *testing.T) {
		tests := []struct {
			name    string
			input   interface{}
			want    interface{}
			wantErr bool
		}{
			{"nil", nil, nil, false},
			{"int32", int32(42), int32(42), false},
			{"int", 42, int32(42), false},
			{"int64", int64(42), int32(42), false},
			{"float64", float64(3.99), int32(3), false},
			{"string", "123", int32(123), false},
			{"invalid string", "abc", int32(0), true},
			{"invalid type", true, nil, true},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				got, err := TypeInt32.ConvertValue(tt.input)
				if (err != nil) != tt.wantErr {
					t.Errorf("TypeInt32.ConvertValue() error = %v, wantErr %v", err, tt.wantErr)
					return
				}
				if got != tt.want {
					t.Errorf("TypeInt32.ConvertValue() = %v, want %v", got, tt.want)
				}
			})
		}
	})

	t.Run("TypeInt64", func(t *testing.T) {
		tests := []struct {
			name    string
			input   interface{}
			want    interface{}
			wantErr bool
		}{
			{"nil", nil, nil, false},
			{"int64", int64(42), int64(42), false},
			{"int", 42, int64(42), false},
			{"int32", int32(42), int64(42), false},
			{"float64", float64(3.99), int64(3), false},
			{"string", "123", int64(123), false},
			{"invalid type", true, nil, true},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				got, err := TypeInt64.ConvertValue(tt.input)
				if (err != nil) != tt.wantErr {
					t.Errorf("TypeInt64.ConvertValue() error = %v, wantErr %v", err, tt.wantErr)
					return
				}
				if got != tt.want {
					t.Errorf("TypeInt64.ConvertValue() = %v, want %v", got, tt.want)
				}
			})
		}
	})

	t.Run("TypeFloat", func(t *testing.T) {
		tests := []struct {
			name    string
			input   interface{}
			want    interface{}
			wantErr bool
		}{
			{"nil", nil, nil, false},
			{"float32", float32(3.14), float32(3.14), false},
			{"float64", float64(3.14), float32(3.14), false},
			{"int", 42, float32(42), false},
			{"string", "3.14", float32(3.14), false},
			{"invalid type", true, nil, true},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				got, err := TypeFloat.ConvertValue(tt.input)
				if (err != nil) != tt.wantErr {
					t.Errorf("TypeFloat.ConvertValue() error = %v, wantErr %v", err, tt.wantErr)
					return
				}
				if got != tt.want {
					t.Errorf("TypeFloat.ConvertValue() = %v, want %v", got, tt.want)
				}
			})
		}
	})

	t.Run("TypeDouble", func(t *testing.T) {
		tests := []struct {
			name    string
			input   interface{}
			want    interface{}
			wantErr bool
		}{
			{"nil", nil, nil, false},
			{"float64", float64(3.14159), float64(3.14159), false},
			{"float32", float64(float32(3.14)), float64(3.140000104904175), false},
			{"int", 42, float64(42), false},
			{"string", "3.14", float64(3.14), false},
			{"invalid type", true, nil, true},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				got, err := TypeDouble.ConvertValue(tt.input)
				if (err != nil) != tt.wantErr {
					t.Errorf("TypeDouble.ConvertValue() error = %v, wantErr %v", err, tt.wantErr)
					return
				}
				if got != tt.want {
					t.Errorf("TypeDouble.ConvertValue() = %v, want %v", got, tt.want)
				}
			})
		}
	})

	t.Run("TypeByteArray", func(t *testing.T) {
		tests := []struct {
			name  string
			input interface{}
			want  interface{}
		}{
			{"nil", nil, nil},
			{"string", "hello", "hello"},
			{"bytes", []byte("hello"), "hello"},
			{"time.Time", now, now.Format(time.RFC3339)},
			{"int fallback", 42, "42"},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				got, err := TypeByteArray.ConvertValue(tt.input)
				if err != nil {
					t.Errorf("TypeByteArray.ConvertValue() error = %v", err)
					return
				}
				if got != tt.want {
					t.Errorf("TypeByteArray.ConvertValue() = %v, want %v", got, tt.want)
				}
			})
		}
	})

	t.Run("Unknown type", func(t *testing.T) {
		got, err := Type(-1).ConvertValue("anything")
		if err != nil {
			t.Errorf("Unknown type ConvertValue() error = %v", err)
		}
		if got != "anything" {
			t.Errorf("Unknown type ConvertValue() = %v, want %v", got, "anything")
		}
	})
}

func TestInferType(t *testing.T) {
	tests := []struct {
		name string
		val  interface{}
		want Type
	}{
		{"nil", nil, TypeByteArray},
		{"bool", true, TypeBoolean},
		{"int32", int32(1), TypeInt32},
		{"int", 1, TypeInt64},
		{"int64", int64(1), TypeInt64},
		{"float32", float32(1.0), TypeFloat},
		{"float64", float64(1.0), TypeDouble},
		{"string", "hello", TypeByteArray},
		{"bytes", []byte("hello"), TypeByteArray},
		{"time.Time", time.Now(), TypeByteArray},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := InferType(tt.val); got != tt.want {
				t.Errorf("InferType(%v) = %v, want %v", tt.val, got, tt.want)
			}
		})
	}
}

func TestNewSchema(t *testing.T) {
	columns := []ColumnDefinition{
		{Name: "id", Type: TypeInt32, Nullable: false},
		{Name: "name", Type: TypeByteArray, Nullable: true},
	}

	schema := NewSchema(columns)
	if schema == nil {
		t.Fatal("NewSchema() returned nil")
	}
	if len(schema.Columns) != 2 {
		t.Errorf("expected 2 columns, got %d", len(schema.Columns))
	}
}

func TestNewSchemaFromMap(t *testing.T) {
	data := map[string]interface{}{
		"id":   1,
		"name": "test",
		"age":  int32(30),
	}

	schema := NewSchemaFromMap(data)
	if schema == nil {
		t.Fatal("NewSchemaFromMap() returned nil")
	}
	if len(schema.Columns) != 3 {
		t.Errorf("expected 3 columns, got %d", len(schema.Columns))
	}
}

func TestSchema_GetColumn(t *testing.T) {
	schema := NewSchema([]ColumnDefinition{
		{Name: "id", Type: TypeInt32},
		{Name: "name", Type: TypeByteArray},
	})

	t.Run("existing column", func(t *testing.T) {
		col := schema.GetColumn("id")
		if col == nil {
			t.Fatal("GetColumn('id') returned nil")
		}
		if col.Name != "id" {
			t.Errorf("column name = %v, want id", col.Name)
		}
		if col.Type != TypeInt32 {
			t.Errorf("column type = %v, want TypeInt32", col.Type)
		}
	})

	t.Run("non-existing column", func(t *testing.T) {
		col := schema.GetColumn("nonexistent")
		if col != nil {
			t.Errorf("GetColumn('nonexistent') = %v, want nil", col)
		}
	})
}

func TestSchema_ColumnNames(t *testing.T) {
	schema := NewSchema([]ColumnDefinition{
		{Name: "id"},
		{Name: "name"},
		{Name: "email"},
	})

	names := schema.ColumnNames()
	expected := []string{"id", "name", "email"}
	if len(names) != len(expected) {
		t.Errorf("ColumnNames() length = %d, want %d", len(names), len(expected))
	}
	for i, name := range expected {
		if names[i] != name {
			t.Errorf("ColumnNames()[%d] = %s, want %s", i, names[i], name)
		}
	}
}

func TestValueToBytes_Roundtrip(t *testing.T) {
	tests := []struct {
		name  string
		value interface{}
		typ   Type
	}{
		{"bool true", true, TypeBoolean},
		{"bool false", false, TypeBoolean},
		{"int32 42", int32(42), TypeInt32},
		{"int64 42", int64(42), TypeInt64},
		{"int 42", 42, TypeInt64},
		{"float32", float32(3.14), TypeFloat},
		{"float64", float64(3.14159), TypeDouble},
		{"string", "hello", TypeByteArray},
		{"nil", nil, TypeByteArray},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			bytes := valueToBytes(tt.value)
			restored := bytesToValue(bytes, tt.typ)

			if tt.value == nil {
				if restored != nil {
					t.Errorf("bytesToValue(nil) = %v, want nil", restored)
				}
				return
			}

			switch tt.typ {
			case TypeBoolean:
				if restored != tt.value {
					t.Errorf("roundtrip = %v, want %v", restored, tt.value)
				}
			case TypeInt32:
				if restored != tt.value {
					t.Errorf("roundtrip = %v, want %v", restored, tt.value)
				}
			case TypeInt64:
				restoredInt, ok := restored.(int64)
				if !ok {
					t.Errorf("bytesToValue returned %T, want int64", restored)
				}
				expected := int64(0)
				switch v := tt.value.(type) {
				case int:
					expected = int64(v)
				case int64:
					expected = v
				}
				if restoredInt != expected {
					t.Errorf("roundtrip = %v, want %v", restoredInt, expected)
				}
			case TypeFloat:
				if restored != tt.value {
					t.Errorf("roundtrip = %v, want %v", restored, tt.value)
				}
			case TypeDouble:
				if restored != tt.value {
					t.Errorf("roundtrip = %v, want %v", restored, tt.value)
				}
			case TypeByteArray, TypeFixedLenByteArray:
				restoredStr, ok := restored.(string)
				if !ok {
					t.Errorf("bytesToValue returned %T, want string", restored)
				}
				if restoredStr != tt.value {
					t.Errorf("roundtrip = %v, want %v", restoredStr, tt.value)
				}
			}
		})
	}
}

func TestBytesToValue_Empty(t *testing.T) {
	result := bytesToValue(nil, TypeInt32)
	if result != nil {
		t.Errorf("bytesToValue(nil) = %v, want nil", result)
	}

	result = bytesToValue([]byte{}, TypeInt32)
	if result != nil {
		t.Errorf("bytesToValue([]) = %v, want nil", result)
	}
}

func TestBytesToValue_UnknownType(t *testing.T) {
	result := bytesToValue([]byte{1, 2, 3}, Type(-1))
	if result == nil {
		t.Error("bytesToValue() should return data for unknown type")
	}
}
