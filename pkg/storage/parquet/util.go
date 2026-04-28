package parquet

import (
	"encoding/binary"
	"fmt"
	"math"
)

// valueToBytes converts a value to bytes for min/max storage
func valueToBytes(v interface{}) []byte {
	if v == nil {
		return nil
	}

	switch val := v.(type) {
	case bool:
		if val {
			return []byte{1}
		}
		return []byte{0}
	case int32:
		b := make([]byte, 4)
		binary.LittleEndian.PutUint32(b, uint32(val))
		return b
	case int:
		b := make([]byte, 8)
		binary.LittleEndian.PutUint64(b, uint64(val))
		return b
	case int64:
		b := make([]byte, 8)
		binary.LittleEndian.PutUint64(b, uint64(val))
		return b
	case float32:
		b := make([]byte, 4)
		binary.LittleEndian.PutUint32(b, math.Float32bits(val))
		return b
	case float64:
		b := make([]byte, 8)
		binary.LittleEndian.PutUint64(b, math.Float64bits(val))
		return b
	case string:
		return []byte(val)
	default:
		return []byte(fmt.Sprintf("%v", v))
	}
}

// bytesToValue converts bytes back to a value
func bytesToValue(data []byte, dataType Type) interface{} {
	if len(data) == 0 {
		return nil
	}

	switch dataType {
	case TypeBoolean:
		return data[0] != 0
	case TypeInt32:
		return int32(binary.LittleEndian.Uint32(data))
	case TypeInt64:
		return int64(binary.LittleEndian.Uint64(data))
	case TypeFloat:
		return math.Float32frombits(binary.LittleEndian.Uint32(data))
	case TypeDouble:
		return math.Float64frombits(binary.LittleEndian.Uint64(data))
	case TypeByteArray, TypeFixedLenByteArray:
		return string(data)
	default:
		return data
	}
}

// compareValues compares two values
func compareValues(a, b interface{}) int {
	if a == nil && b == nil {
		return 0
	}
	if a == nil {
		return -1
	}
	if b == nil {
		return 1
	}

	switch av := a.(type) {
	case bool:
		bv, ok := b.(bool)
		if !ok {
			return 0
		}
		if av == bv {
			return 0
		}
		if !av && bv {
			return -1
		}
		return 1
	case int:
		bv, ok := b.(int)
		if !ok {
			return 0
		}
		if av < bv {
			return -1
		}
		if av > bv {
			return 1
		}
		return 0
	case int32:
		bv, ok := b.(int32)
		if !ok {
			return 0
		}
		if av < bv {
			return -1
		}
		if av > bv {
			return 1
		}
		return 0
	case int64:
		bv, ok := b.(int64)
		if !ok {
			return 0
		}
		if av < bv {
			return -1
		}
		if av > bv {
			return 1
		}
		return 0
	case float32:
		bv, ok := b.(float32)
		if !ok {
			return 0
		}
		if av < bv {
			return -1
		}
		if av > bv {
			return 1
		}
		return 0
	case float64:
		bv, ok := b.(float64)
		if !ok {
			return 0
		}
		if av < bv {
			return -1
		}
		if av > bv {
			return 1
		}
		return 0
	case string:
		bv, ok := b.(string)
		if !ok {
			return 0
		}
		if av < bv {
			return -1
		}
		if av > bv {
			return 1
		}
		return 0
	default:
		return 0
	}
}
