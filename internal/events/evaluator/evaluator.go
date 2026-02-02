package evaluator

import (
	"fmt"
	"strconv"
	"strings"

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
func (e *Evaluator) EvaluateRule(rule loader.YAMLRule, entity models.TelemetryEntity) *models.MatchedEvent {
	// Skip inactive rules
	if !rule.IsActive || rule.Status != "active" {
		return nil
	}

	// Check if rule applies to this entity (if entity_id_pattern is specified)
	if rule.EntityIDPattern != "" {
		if !contains(entity.EntityID, rule.EntityIDPattern) && entity.EntityID != rule.EntityIDPattern {
			return nil
		}
	}

	// Get the value from entity attributes based on rule_key
	value, exists := e.getEntityValue(entity, rule.RuleKey)
	if !exists {
		return nil
	}

	// Parse the operand as float64
	operand, err := strconv.ParseFloat(rule.Operand, 64)
	if err != nil {
		e.logger.Warn("Failed to parse operand as float64",
			zap.String("rule_key", rule.RuleKey),
			zap.String("operand", rule.Operand),
			zap.Error(err))
		return nil
	}

	// Evaluate the condition
	matched := e.compareValues(value, operand, rule.Operator)
	if !matched {
		return nil
	}

	return &models.MatchedEvent{
		EntityID:    entity.EntityID,
		EntityType:  entity.EntityType,
		RuleKey:     rule.RuleKey,
		EventType:   rule.EventType,
		EventLevel:  rule.EventLevel,
		Description: rule.Description,
		Value:       value,
		Threshold:   operand,
		Operator:    rule.Operator,
		RuleSource:  "default",
	}
}

// EvaluateRuleDB evaluates a database rule against an entity
func (e *Evaluator) EvaluateRuleDB(rule models.EventRule, entity models.TelemetryEntity) *models.MatchedEvent {
	// Skip inactive rules
	if rule.IsActive != nil && !*rule.IsActive {
		return nil
	}
	if rule.Status != nil && *rule.Status != "active" {
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
	operand, err := strconv.ParseFloat(rule.Operand, 64)
	if err != nil {
		e.logger.Warn("Failed to parse operand as float64",
			zap.String("rule_key", ruleKey),
			zap.String("operand", rule.Operand),
			zap.Error(err))
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
	matched := e.compareValues(value, operand, operator)
	if !matched {
		return nil
	}

	// Build description
	description := fmt.Sprintf("Rule %s matched: %.2f %s %.2f", ruleKey, value, operator, operand)

	return &models.MatchedEvent{
		EntityID:    entity.EntityID,
		EntityType:  entity.EntityType,
		RuleKey:     ruleKey,
		EventType:   "device_event",
		EventLevel:  "automation",
		Description: description,
		Value:       value,
		Threshold:   operand,
		Operator:    operator,
		RuleSource:  "automation",
	}
}

// getEntityValue extracts a numeric value from an entity based on the rule key
func (e *Evaluator) getEntityValue(entity models.TelemetryEntity, ruleKey string) (float64, bool) {
	// Try to get value from Attributes map first
	if entity.Attributes != nil {
		if val, ok := entity.Attributes[ruleKey]; ok {
			return e.parseFloat64(val)
		}
	}

	// Try to get value from State (which could be a map or direct value)
	if entity.State != nil {
		switch s := entity.State.(type) {
		case map[string]interface{}:
			if val, ok := s[ruleKey]; ok {
				return e.parseFloat64(val)
			}
		default:
			// Check if rule_key matches or is a prefix of entity_type
			// e.g., rule_key="battery_v" matches entity_type="battery"
			//       rule_key="temperature" matches entity_type="temperature"
			if e.isRuleKeyRelevant(ruleKey, entity.EntityType, entity.Name) {
				if val, ok := e.parseFloat64(s); ok {
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

	return false
}

// parseFloat64 converts various types to float64
func (e *Evaluator) parseFloat64(value interface{}) (float64, bool) {
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

// compareValues performs comparison based on operator
func (e *Evaluator) compareValues(value, threshold float64, operator string) bool {
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
		e.logger.Warn("Unknown operator", zap.String("operator", operator))
		return false
	}
}

// contains checks if the target string contains the substring (case-insensitive)
func contains(str, substr string) bool {
	return strings.Contains(strings.ToLower(str), strings.ToLower(substr))
}
