package evaluator

import (
	"fmt"
	"strings"
	"time"

	"github.com/Space-DF/telemetry-service/internal/events"
	"github.com/Space-DF/telemetry-service/internal/events/loader"
	"github.com/Space-DF/telemetry-service/internal/models"
	"go.uber.org/zap"
)

// Evaluator handles rule evaluation logic
type Evaluator struct {
	logger *zap.Logger
}

// NewEvaluator creates a new rule evaluator
func NewEvaluator(logger *zap.Logger) *Evaluator {
	return &Evaluator{
		logger: logger,
	}
}

// EvaluateRule evaluates a single YAML rule against an entity
func (e *Evaluator) EvaluateRule(rule loader.YAMLRule, deviceID string, entity models.TelemetryEntity) *models.MatchedEvent {
	// Skip inactive rules
	if !rule.IsActive {
		return nil
	}

	// Check if rule allows creating new events
	if !rule.AllowNewEvent {
		return nil
	}

	// Get the value from entity attributes based on rule_key
	value, exists := e.getEntityValue(entity, rule.RuleKey)
	if !exists {
		return nil
	}

	// Parse the operand as float64
	operand, ok := events.ParseFloat64(rule.Operand)
	if !ok {
		e.logger.Warn("Failed to parse operand as float64",
			zap.String("rule_key", rule.RuleKey),
			zap.String("operand", rule.Operand))
		return nil
	}

	// Evaluate the condition
	matched := events.CompareValues(value, operand, rule.Operator, e.logger)
	if !matched {
		return nil
	}

	matchedEvent := &models.MatchedEvent{
		EntityID:    deviceID,
		EntityType:  entity.EntityType,
		RuleKey:     rule.RuleKey,
		EventType:   rule.EventType,
		EventLevel:  rule.EventLevel,
		Description: rule.Description,
		Value:       value,
		Threshold:   operand,
		Operator:    rule.Operator,
		RuleSource:  "default",
		Timestamp:   time.Now().UnixMilli(),
	}

	return matchedEvent
}

// EvaluateRuleDB evaluates a database rule against an entity
func (e *Evaluator) EvaluateRuleDB(rule models.EventRule, deviceID string, entity models.TelemetryEntity) *models.MatchedEvent {
	// Skip inactive rules
	if rule.IsActive != nil && !*rule.IsActive {
		return nil
	}

	// Check if rule allows creating new events
	if rule.AllowNewEvent != nil && !*rule.AllowNewEvent {
		return nil
	}

	// Get rule key from database rule
	ruleKey := ""
	if rule.RuleKey != nil {
		ruleKey = *rule.RuleKey
	}
	if ruleKey == "" {
		return nil
	}

	// Get the value from entity attributes based on rule_key
	value, exists := e.getEntityValue(entity, ruleKey)
	if !exists {
		return nil
	}

	// Parse the operand as float64
	operand, ok := events.ParseFloat64(rule.Operand)
	if !ok {
		e.logger.Warn("Failed to parse operand as float64",
			zap.String("rule_key", ruleKey),
			zap.String("operand", rule.Operand))
		return nil
	}

	// Get operator
	operator := ""
	if rule.Operator != nil {
		operator = *rule.Operator
	}
	if operator == "" {
		operator = "eq" // default
	}

	// Evaluate the condition
	matched := events.CompareValues(value, operand, operator, e.logger)
	if !matched {
		return nil
	}

	// Build description
	description := fmt.Sprintf("Rule %s matched: %.2f %s %.2f", ruleKey, value, operator, operand)

	matchedEvent := &models.MatchedEvent{
		EntityID:    deviceID,
		EntityType:  entity.EntityType,
		RuleKey:     ruleKey,
		EventType:   "device_event",
		EventLevel:  "automation",
		Description: description,
		Value:       value,
		Threshold:   operand,
		Operator:    operator,
		RuleSource:  "automation",
		Timestamp:   time.Now().UnixMilli(),
	}

	return matchedEvent
}

// getEntityValue extracts a numeric value from an entity based on the rule key
func (e *Evaluator) getEntityValue(entity models.TelemetryEntity, ruleKey string) (float64, bool) {
	// Try to get value from Attributes map first
	if entity.Attributes != nil {
		if val, ok := entity.Attributes[ruleKey]; ok {
			return events.ParseFloat64(val)
		}
	}

	// Try to get value from State
	if entity.State != nil {
		switch s := entity.State.(type) {
		case map[string]interface{}:
			if val, ok := s[ruleKey]; ok {
				return events.ParseFloat64(val)
			}
		default:
			// For other types, try to parse as float64
			if val, ok := events.ParseFloat64(s); ok {
				if e.isRuleKeyRelevant(ruleKey, entity.EntityType, entity.Name) {
					return val, true
				}
			}
		}
	}

	return 0, false
}

// isRuleKeyRelevant checks if a rule key is relevant to an entity
func (e *Evaluator) isRuleKeyRelevant(ruleKey, entityType, entityName string) bool {
	if ruleKey == entityType {
		return true
	}

	if strings.HasPrefix(ruleKey, entityType) {
		return true
	}

	if strings.HasPrefix(entityType, ruleKey) {
		return true
	}

	// Check entity name for common sensor patterns
	entityNameLower := strings.ToLower(entityName)
	ruleKeyLower := strings.ToLower(ruleKey)

	// Direct match: "temperature" rule_key with "Temperature" entity name
	if ruleKeyLower == entityNameLower {
		return true
	}

	// Contains match: "battery_v" rule_key with "Battery Level" entity
	if strings.Contains(entityNameLower, ruleKeyLower) {
		return true
	}

	// Prefix match: "humidity" rule_key with "Humidity Sensor" entity
	if strings.HasPrefix(entityNameLower, ruleKeyLower) {
		return true
	}

	return false
}
