package parquet

import (
	"bytes"
	"encoding/binary"
	"math"
	"testing"
)

func TestValueToBytes_Nil(t *testing.T) {
	result := valueToBytes(nil)
	if result != nil {
		t.Errorf("valueToBytes(nil) = %v, want nil", result)
	}
}

func TestValueToBytes_Bool(t *testing.T) {
	tests := []struct {
		name     string
		input    bool
		expected []byte
	}{
		{"true", true, []byte{1}},
		{"false", false, []byte{0}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := valueToBytes(tt.input)
			if !bytes.Equal(result, tt.expected) {
				t.Errorf("valueToBytes(%v) = %v, want %v", tt.input, result, tt.expected)
			}
		})
	}
}

func TestValueToBytes_Int32(t *testing.T) {
	tests := []struct {
		name  string
		input int32
	}{
		{"zero", 0},
		{"positive", 42},
		{"negative", -42},
		{"max", math.MaxInt32},
		{"min", math.MinInt32},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := valueToBytes(tt.input)
			if len(result) != 4 {
				t.Errorf("valueToBytes(%d) returned %d bytes, want 4", tt.input, len(result))
			}
			got := int32(binary.LittleEndian.Uint32(result))
			if got != tt.input {
				t.Errorf("valueToBytes(%d) = %d after roundtrip, want %d", tt.input, got, tt.input)
			}
		})
	}
}

func TestValueToBytes_Int64(t *testing.T) {
	tests := []struct {
		name  string
		input int64
	}{
		{"zero", 0},
		{"positive", 42},
		{"negative", -42},
		{"large positive", 1 << 62},
		{"large negative", -1 << 62},
		{"max", math.MaxInt64},
		{"min", math.MinInt64},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := valueToBytes(tt.input)
			if len(result) != 8 {
				t.Errorf("valueToBytes(%d) returned %d bytes, want 8", tt.input, len(result))
			}
			got := int64(binary.LittleEndian.Uint64(result))
			if got != tt.input {
				t.Errorf("valueToBytes(%d) = %d after roundtrip, want %d", tt.input, got, tt.input)
			}
		})
	}
}

func TestValueToBytes_Int(t *testing.T) {
	tests := []struct {
		name  string
		input int
	}{
		{"zero", 0},
		{"positive", 123},
		{"negative", -123},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := valueToBytes(tt.input)
			if len(result) != 8 {
				t.Errorf("valueToBytes(%d) returned %d bytes, want 8", tt.input, len(result))
			}
			got := int64(binary.LittleEndian.Uint64(result))
			if got != int64(tt.input) {
				t.Errorf("valueToBytes(%d) = %d after roundtrip, want %d", tt.input, got, tt.input)
			}
		})
	}
}

func TestValueToBytes_Float32(t *testing.T) {
	tests := []struct {
		name  string
		input float32
	}{
		{"zero", 0.0},
		{"positive", 3.14},
		{"negative", -3.14},
		{"max", math.MaxFloat32},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := valueToBytes(tt.input)
			if len(result) != 4 {
				t.Errorf("valueToBytes(%f) returned %d bytes, want 4", tt.input, len(result))
			}
			got := math.Float32frombits(binary.LittleEndian.Uint32(result))
			if got != tt.input {
				t.Errorf("valueToBytes(%f) = %f after roundtrip, want %f", tt.input, got, tt.input)
			}
		})
	}
}

func TestValueToBytes_Float64(t *testing.T) {
	tests := []struct {
		name  string
		input float64
	}{
		{"zero", 0.0},
		{"positive", 3.14159265359},
		{"negative", -3.14159265359},
		{"max", math.MaxFloat64},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := valueToBytes(tt.input)
			if len(result) != 8 {
				t.Errorf("valueToBytes(%f) returned %d bytes, want 8", tt.input, len(result))
			}
			got := math.Float64frombits(binary.LittleEndian.Uint64(result))
			if got != tt.input {
				t.Errorf("valueToBytes(%f) = %f after roundtrip, want %f", tt.input, got, tt.input)
			}
		})
	}
}

func TestValueToBytes_String(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected []byte
	}{
		{"empty", "", []byte{}},
		{"simple", "hello", []byte("hello")},
		{"with spaces", "hello world", []byte("hello world")},
		{"unicode", "你好", []byte("你好")},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := valueToBytes(tt.input)
			if !bytes.Equal(result, tt.expected) {
				t.Errorf("valueToBytes(%q) = %v, want %v", tt.input, result, tt.expected)
			}
		})
	}
}

func TestValueToBytes_Default(t *testing.T) {
	type customStruct struct {
		field string
	}

	tests := []struct {
		name     string
		input    interface{}
		expected string
	}{
		{"custom struct", customStruct{field: "test"}, "{test}"},
		{"uint", uint(42), "42"},
		{"uint32", uint32(42), "42"},
		{"uint64", uint64(42), "42"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := valueToBytes(tt.input)
			if !bytes.Equal(result, []byte(tt.expected)) {
				t.Errorf("valueToBytes(%v) = %v, want %v", tt.input, result, []byte(tt.expected))
			}
		})
	}
}
