package registry

import (
	"fmt"
	"strconv"
)

// GenericNumericProcessor is a config-driven processor for numeric sensors.
type GenericNumericProcessor struct {
	CategoryValue   string
	ValueKeyValue   string
	UnitValue       string
	StatePred       string
	DefaultCaution  float64
	DefaultWarn     float64
	DefaultCritical float64
	Messages        map[string]string
}

func (p *GenericNumericProcessor) Category() string { return p.CategoryValue }

func (p *GenericNumericProcessor) DefaultCautionThreshold() float64 { return p.DefaultCaution }
func (p *GenericNumericProcessor) DefaultWarningThreshold() float64 { return p.DefaultWarn }
func (p *GenericNumericProcessor) DefaultCriticalThreshold() float64 {
	return p.DefaultCritical
}
func (p *GenericNumericProcessor) Unit() string     { return p.UnitValue }
func (p *GenericNumericProcessor) ValueKey() string { return p.ValueKeyValue }
func (p *GenericNumericProcessor) StatePredicate() string {
	if p.StatePred == "" {
		return "TRUE"
	}
	return p.StatePred
}

func (p *GenericNumericProcessor) ParseValue(raw string) (float64, error) {
	return strconv.ParseFloat(raw, 64)
}

func (p *GenericNumericProcessor) DetermineLevel(value, cautionThreshold, warningThreshold, criticalThreshold float64) string {
  switch {
  case value > criticalThreshold:
    return "critical"
  case value >= warningThreshold:
    return "warning"
  case value > cautionThreshold:
    return "caution"
  default:
    return "safe"
  }
}

func (p *GenericNumericProcessor) DetermineType(value, cautionThreshold, warningThreshold, criticalThreshold float64) string {
	return p.DetermineLevel(value, cautionThreshold, warningThreshold, criticalThreshold)
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
