package events

import (
	"strconv"
	"strings"

	"go.uber.org/zap"
)

// ParseFloat64 converts various types to float64
func ParseFloat64(value interface{}) (float64, bool) {
	switch v := value.(type) {
	case float64:
		return v, true
	case float32:
		return float64(v), true
	case int:
		return float64(v), true
	case int64:
		return float64(v), true
	case int32:
		return float64(v), true
	case string:
		f, err := strconv.ParseFloat(v, 64)
		if err != nil {
			return 0, false
		}
		return f, true
	default:
		return 0, false
	}
}

// CompareValues performs comparison based on operator
func CompareValues(value, threshold float64, operator string, logger *zap.Logger) bool {
	switch operator {
	case "gt":
		return value > threshold
	case "lt":
		return value < threshold
	case "gte":
		return value >= threshold
	case "lte":
		return value <= threshold
	case "eq":
		return value == threshold
	case "ne":
		return value != threshold
	default:
		if logger != nil {
			logger.Warn("Unknown operator", zap.String("operator", operator))
		}
		return false
	}
}

// Contains checks if the target string contains the substring (case-insensitive)
func Contains(str, substr string) bool {
	return strings.Contains(strings.ToLower(str), strings.ToLower(substr))
}
