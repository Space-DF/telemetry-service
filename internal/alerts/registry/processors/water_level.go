package processors

import (
	"fmt"

	"github.com/Space-DF/telemetry-service/internal/alerts/registry"
)

type waterLevelProcessor struct{}

func (waterLevelProcessor) Category() string { return "water_level" }

func (waterLevelProcessor) DefaultWarningThreshold() float64  { return 50.0 }
func (waterLevelProcessor) DefaultCriticalThreshold() float64 { return 200.0 }
func (waterLevelProcessor) Unit() string                      { return "cm" }
func (waterLevelProcessor) ValueKey() string                  { return "water_level" }
func (waterLevelProcessor) StatePredicate() string            { return "s.state ~ '^-?[0-9]+(\\.[0-9]+)?$'" }

func (waterLevelProcessor) DetermineType(value, warningThreshold, criticalThreshold float64) string {
	if value >= criticalThreshold {
		return "flood_risk"
	} else if value >= warningThreshold {
		return "water_rising"
	}
	return "normal"
}

func (waterLevelProcessor) DetermineLevel(value, warningThreshold, criticalThreshold float64) string {
	if value >= criticalThreshold {
		return "critical"
	} else if value >= warningThreshold {
		return "warning"
	}
	return "normal"
}

func (waterLevelProcessor) GenerateMessage(level string, value float64) string {
	switch level {
	case "critical":
		return fmt.Sprintf("Critical flood risk detected. Water level at %.0f cm", value)
	case "warning":
		return fmt.Sprintf("Water is rising quickly. Current level: %.0f cm", value)
	default:
		return fmt.Sprintf("Water level monitoring: %.0f cm", value)
	}
}

func (waterLevelProcessor) ParseValue(state string) (float64, error) {
	var value float64
	_, err := fmt.Sscanf(state, "%f", &value)
	return value, err
}

func init() {
	_ = registry.Register(waterLevelProcessor{})
}
