package util

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestCompareValues(t *testing.T) {
	tests := []struct {
		name     string
		a        interface{}
		b        interface{}
		expected int
	}{
		{"both nil", nil, nil, 0},
		{"a nil", nil, 1, -1},
		{"b nil", 1, nil, 1},
		{"int equal", 5, 5, 0},
		{"int less", 3, 5, -1},
		{"int greater", 5, 3, 1},
		{"int64 equal", int64(5), int64(5), 0},
		{"int64 less", int64(3), int64(5), -1},
		{"int32 equal", int32(5), int32(5), 0},
		{"float64 equal", 3.14, 3.14, 0},
		{"float64 less", 3.14, 6.28, -1},
		{"float32 equal", float32(3.14), float32(3.14), 0},
		{"string equal", "abc", "abc", 0},
		{"string less", "abc", "def", -1},
		{"string greater", "def", "abc", 1},
		{"bool equal true", true, true, 0},
		{"bool equal false", false, false, 0},
		{"bool false less than true", false, true, -1},
		{"bool true greater than false", true, false, 1},
		{"mixed int and float64", 5, 5.0, 0},
		{"string number comparison", "3.14", 3.14, 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := CompareValues(tt.a, tt.b)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestToFloat64(t *testing.T) {
	tests := []struct {
		name     string
		input    interface{}
		expected float64
		ok       bool
	}{
		{"float64", 3.14, 3.14, true},
		{"float32", float32(3.14), float64(float32(3.14)), true},
		{"int", 42, 42.0, true},
		{"int32", int32(42), 42.0, true},
		{"int64", int64(42), 42.0, true},
		{"string number", "3.14", 3.14, true},
		{"string invalid", "abc", 0, false},
		{"bool", true, 0, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, ok := toFloat64(tt.input)
			assert.Equal(t, tt.ok, ok)
			if ok {
				assert.InDelta(t, tt.expected, result, 0.0001)
			}
		})
	}
}
