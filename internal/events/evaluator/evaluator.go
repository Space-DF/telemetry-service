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
	logger             *zap.Logger
	conditionEvaluator *ConditionEvaluator
}

// NewEvaluator creates a new rule evaluator
func NewEvaluator(logger *zap.Logger) *Evaluator {
	return &Evaluator{
		logger:             logger,
		conditionEvaluator: NewConditionEvaluator(logger),
	}
}

// EvaluateRule evaluates a single YAML rule against an entity
func (e *Evaluator) EvaluateRule(rule loader.YAMLRule, deviceID string, entity models.TelemetryEntity) *models.MatchedEvent {
	// Skip inactive rules
	if !rule.IsActive {
		return nil
	}

	// Check if rule allows repeating events
	if !rule.RepeatAble {
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
	if rule.RepeatAble != nil && !*rule.RepeatAble {
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

	// Parse and evaluate definition conditions
	if rule.Definition == nil || *rule.Definition == "" {
		e.logger.Warn("Rule definition is null or empty",
			zap.String("rule_key", ruleKey))
		return nil
	}

	// Evaluate definition using condition evaluator
	matched, matchDetails, err := e.conditionEvaluator.EvaluateDefinition(*rule.Definition, entity, deviceID)
	if err != nil {
		e.logger.Warn("Failed to evaluate rule definition",
			zap.String("rule_key", ruleKey),
			zap.String("definition", *rule.Definition),
			zap.Error(err))
		return nil
	}
	if !matched {
		return nil
	}

	// Build description from match details
	description := fmt.Sprintf("Rule %s matched: %s", ruleKey, matchDetails)

	matchedEvent := &models.MatchedEvent{
		EntityID:    deviceID,
		EntityType:  entity.EntityType,
		RuleKey:     ruleKey,
		EventType:   "device_event",
		EventLevel:  "automation",
		Description: description,
		Value:       0,
		Threshold:   0,
		Operator:    "complex",
		Timestamp:   time.Now().UnixMilli(),
		EventRuleID: &rule.EventRuleID,
		StateID:     entity.StateID,
	}

	return matchedEvent
}

// EvaluateRuleDBWithEntities evaluates a database rule against ALL entities combined into a single context.
func (e *Evaluator) EvaluateRuleDBWithEntities(rule models.EventRule, deviceID string, entities []models.TelemetryEntity, deviceInfo map[string]interface{}, extraContext map[string]interface{}) *models.MatchedEvent {
	// Skip inactive rules
	if rule.IsActive != nil && !*rule.IsActive {
		return nil
	}

	// Check if rule allows creating new events
	if rule.RepeatAble != nil && !*rule.RepeatAble {
		return nil
	}

	// Get rule key from database rule
	ruleKey := ""
	if rule.RuleKey != nil {
		ruleKey = *rule.RuleKey
	}

	// Parse and evaluate definition conditions
	if rule.Definition == nil || *rule.Definition == "" {
		return nil
	}

	// Build unified context from ALL entities
	context := e.conditionEvaluator.BuildUnifiedContext(entities, deviceID, deviceInfo)

	// Merge extra context (e.g. distance_from_geofence_km)
	for k, v := range extraContext {
		context[k] = v
	}

	// Evaluate definition using condition evaluator with unified context
	matched, matchDetails, err := e.conditionEvaluator.EvaluateDefinitionWithContext(*rule.Definition, context)
	if err != nil {
		return nil
	}
	if !matched {
		return nil
	}

	// Build description from match details or use rule's description
	description := fmt.Sprintf("Rule %s matched: %s", ruleKey, matchDetails)
	if rule.Description != nil && *rule.Description != "" {
		description = *rule.Description
	}

	matchedEvent := &models.MatchedEvent{
		EntityID:    deviceID,
		EntityType:  "automation",
		RuleKey:     ruleKey,
		EventType:   "device_event",
		EventLevel:  "automation",
		Description: description,
		Value:       0,
		Threshold:   0,
		Operator:    "complex",
		Timestamp:   time.Now().UnixMilli(),
		EventRuleID: &rule.EventRuleID,
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
