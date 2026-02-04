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
		Timestamp:   time.Now().UnixMilli(),
		StateID:     entity.StateID,
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

	// Get the value from entity based on rule_key
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
		Timestamp:   time.Now().UnixMilli(),
		EventRuleID: &rule.EventRuleID,
		StateID:     entity.StateID,
	}

	return matchedEvent
}

// getEntityValue extracts a numeric value from an entity state based on the rule key
func (e *Evaluator) getEntityValue(entity models.TelemetryEntity, ruleKey string) (float64, bool) {
	if entity.State == nil {
		return 0, false
	}
	
	switch s := entity.State.(type) {
	case map[string]interface{}:
		if val, ok := s[ruleKey]; ok {
			return events.ParseFloat64(val)
		}
	default:
		if val, ok := events.ParseFloat64(s); ok {
			if e.isRuleKeyMatched(ruleKey, entity.EntityType) {
				return val, true
			}
		}
	}

	return 0, false
}

// isRuleKeyMatched checks if a rule key matches an entity type
func (e *Evaluator) isRuleKeyMatched(ruleKey, entityType string) bool {
	ruleKeyLower := strings.ToLower(ruleKey)

	return ruleKeyLower == strings.ToLower(entityType)
}
