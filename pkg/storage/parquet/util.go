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
