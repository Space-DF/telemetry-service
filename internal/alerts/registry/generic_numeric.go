package registry

import (
	"fmt"
	"strconv"
)

// GenericNumericProcessor is a config-driven processor for numeric sensors.
type GenericNumericProcessor struct {
	CategoryValue  string
	ValueKeyValue  string
	UnitValue      string
	StatePred      string
	DefaultCaution float64
	DefaultWarn    float64
	DefaultSafe    float64
	Messages       map[string]string
}

func (p *GenericNumericProcessor) Category() string { return p.CategoryValue }

func (p *GenericNumericProcessor) DefaultCautionThreshold() float64 { return p.DefaultCaution }
func (p *GenericNumericProcessor) DefaultWarningThreshold() float64 { return p.DefaultWarn }
func (p *GenericNumericProcessor) DefaultSafeThreshold() float64 {
	return p.DefaultSafe
}
func (p *GenericNumericProcessor) Unit() string     { return p.UnitValue }
func (p *GenericNumericProcessor) ValueKey() string { return p.ValueKeyValue }

func (p *GenericNumericProcessor) ParseValue(raw string) (float64, error) {
	return strconv.ParseFloat(raw, 64)
}

func (p *GenericNumericProcessor) DetermineLevel(value, safeThreshold, cautionThreshold, warningThreshold float64) string {
	switch {
	case value > warningThreshold:
		return "critical"
	case value >= cautionThreshold:
		return "warning"
	case value > safeThreshold:
		return "caution"
	default:
		return "safe"
	}
}

func (p *GenericNumericProcessor) DetermineType(value, safeThreshold, cautionThreshold, warningThreshold float64) string {
	return p.DetermineLevel(value, safeThreshold, cautionThreshold, warningThreshold)
}

func (p *GenericNumericProcessor) GenerateMessage(level string, value float64) string {
	if p.Messages != nil {
		if msg, ok := p.Messages[level]; ok {
			return msg
		}
	}
	// No fallback on purpose to surface missing templates during debugging
	return fmt.Sprintf("missing message template for level %s (value %.2f %s)", level, value, p.UnitValue)
}
