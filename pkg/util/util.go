package util

import (
	"fmt"
	"strconv"
	"strings"
)

func CompareValues(a, b interface{}) int {
	if a == nil && b == nil {
		return 0
	}
	if a == nil {
		return -1
	}
	if b == nil {
		return 1
	}

	aFloat, aOk := toFloat64(a)
	bFloat, bOk := toFloat64(b)

	if aOk && bOk {
		if aFloat < bFloat {
			return -1
		} else if aFloat > bFloat {
			return 1
		}
		return 0
	}

	switch av := a.(type) {
	case bool:
		bv, ok := b.(bool)
		if !ok {
			return strings.Compare(fmt.Sprintf("%v", a), fmt.Sprintf("%v", b))
		}
		if av == bv {
			return 0
		}
		if !av && bv {
			return -1
		}
		return 1
	case string:
		bv, ok := b.(string)
		if !ok {
			return strings.Compare(av, fmt.Sprintf("%v", b))
		}
		return strings.Compare(av, bv)
	default:
		aStr := fmt.Sprintf("%v", a)
		bStr := fmt.Sprintf("%v", b)
		return strings.Compare(aStr, bStr)
	}
}

func toFloat64(v interface{}) (float64, bool) {
	switch val := v.(type) {
	case float64:
		return val, true
	case float32:
		return float64(val), true
	case int:
		return float64(val), true
	case int32:
		return float64(val), true
	case int64:
		return float64(val), true
	case string:
		f, err := strconv.ParseFloat(val, 64)
		return f, err == nil
	default:
		return 0, false
	}
}
