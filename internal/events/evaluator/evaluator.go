package evaluator

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/Space-DF/telemetry-service/internal/events"
	"github.com/Space-DF/telemetry-service/internal/events/loader"
	"github.com/Space-DF/telemetry-service/internal/models"
	"github.com/google/uuid"
	"go.uber.org/zap"
)

// EventRuleForEvaluation represents an event rule from automation or geofence
// This is used for evaluation purposes, combining automation and geofence event rules
type EventRuleForEvaluation struct {
	EventRuleID    string  // UUID of the event_rule row in event_rules table
	AutomationID   string  // UUID of the automation (empty for geofence rules)
	AutomationName string  // Name of the automation (used as event title)
	Title          *string // Optional title for the rule, used as event title if automation name is not set
	RuleKey        *string
	Definition     json.RawMessage
	IsActive       *bool
	RepeatAble     *bool
	Description    *string
	GeofenceID     *string // Set if this is a geofence rule
	GeofenceName   string  // Name of the geofence (used for event title)
	IsAutomation   bool    // true if from automation, false if from geofence
}

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
		DeviceID:    deviceID,
		EntityType:  entity.EntityType,
		RuleKey:     rule.RuleKey,
		EventType:   rule.EventType,
		EventLevel:  rule.EventLevel,
		Title:       rule.Description,
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
func (e *Evaluator) EvaluateRuleDB(rule EventRuleForEvaluation, deviceID string, entity models.TelemetryEntity) *models.MatchedEvent {
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
	if len(rule.Definition) == 0 {
		e.logger.Warn("Rule definition is null or empty",
			zap.String("rule_key", ruleKey))
		return nil
	}

	// Evaluate definition using condition evaluator
	matched, matchDetails, err := e.conditionEvaluator.EvaluateDefinition(string(rule.Definition), entity, deviceID)
	if err != nil {
		e.logger.Warn("Failed to evaluate rule definition",
			zap.String("rule_key", ruleKey),
			zap.String("definition", string(rule.Definition)),
			zap.Error(err))
		return nil
	}
	if !matched {
		return nil
	}

	// Build description from match details
	title := ruleKey
	if rule.Title != nil && *rule.Title != "" {
		title = *rule.Title
	}
	description := fmt.Sprintf("Rule %s matched: %s", ruleKey, matchDetails)
	if rule.Description != nil && *rule.Description != "" {
		description = *rule.Description
	}

	var automationID *string
	var automationName *string
	if rule.IsAutomation && rule.AutomationID != "" {
		aid := rule.AutomationID
		automationID = &aid
		if rule.AutomationName != "" {
			aname := rule.AutomationName
			automationName = &aname
		}
	}
	var geofenceID *string
	if !rule.IsAutomation && rule.GeofenceID != nil {
		geofenceID = rule.GeofenceID
	}
	var eventRuleID *string
	if rule.EventRuleID != "" {
		erid := rule.EventRuleID
		eventRuleID = &erid
	}

	matchedEvent := &models.MatchedEvent{
		DeviceID:       deviceID,
		EntityType:     entity.EntityType,
		RuleKey:        ruleKey,
		EventType:      "device_event",
		EventLevel:     "automation",
		Title:          title,
		Description:    description,
		Value:          0,
		Threshold:      0,
		Operator:       "complex",
		Timestamp:      time.Now().UnixMilli(),
		EventRuleID:    eventRuleID,
		AutomationID:   automationID,
		AutomationName: automationName,
		GeofenceID:     geofenceID,
		StateID:        entity.StateID,
	}

	return matchedEvent
}

// EvaluateRuleDBWithEntities evaluates a database rule against ALL entities combined into a single context.
func (e *Evaluator) EvaluateRuleDBWithEntities(rule EventRuleForEvaluation, deviceID string, entities []models.TelemetryEntity, deviceInfo map[string]interface{}, extraContext map[string]interface{}) *models.MatchedEvent {
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
	if len(rule.Definition) == 0 {
		return nil
	}

	// Build unified context from ALL entities
	context := e.conditionEvaluator.BuildUnifiedContext(entities, deviceID, deviceInfo)

	// Merge extra context (e.g. distance_from_geofence_km)
	for k, v := range extraContext {
		context[k] = v
	}

	e.logger.Info("Context data: ", zap.Any("context", context))

	// Evaluate definition using condition evaluator with unified context
	matched, matchDetails, err := e.conditionEvaluator.EvaluateDefinitionWithContext(string(rule.Definition), context)
	if err != nil {
		e.logger.Info("Error evaluating rule definition",
			zap.String("device_id", deviceID),
			zap.String("rule_key", ruleKey),
			zap.String("definition", string(rule.Definition)),
			zap.Error(err),
		)
		return nil
	}
	if !matched {
		e.logger.Info("Rule definition did not match",
			zap.String("device_id", deviceID),
			zap.String("rule_key", ruleKey),
			zap.String("definition", string(rule.Definition)),
		)
		return nil
	}

	// Build description from match details or use rule's description
	// Title: automation name, fallback to description, fallback to rule_key
	title := ruleKey
	if rule.Title != nil && *rule.Title != "" {
		title = *rule.Title
	}
	description := fmt.Sprintf("Rule %s matched: %s", ruleKey, matchDetails)
	if rule.Description != nil && *rule.Description != "" {
		description = *rule.Description
	}

	// Set AutomationID only when this is an automation rule
	var automationID *string
	var automationName *string
	if rule.IsAutomation && rule.AutomationID != "" {
		aid := rule.AutomationID
		automationID = &aid
		if rule.AutomationName != "" {
			aname := rule.AutomationName
			automationName = &aname
		}
	}

	// Set EventRuleID
	var eventRuleID *string
	if rule.EventRuleID != "" {
		erid := rule.EventRuleID
		eventRuleID = &erid
	}

	// Find the first available StateID from entities
	var stateID uuid.UUID
	for _, ent := range entities {
		if ent.StateID != (uuid.UUID{}) { // Check if not empty UUID
			stateID = ent.StateID
			break
		}
	}

	matchedEvent := &models.MatchedEvent{
		DeviceID:       deviceID,
		EntityType:     "automation",
		RuleKey:        ruleKey,
		EventType:      "device_event",
		EventLevel:     "automation",
		Title:          title,
		Description:    description,
		Value:          0,
		Threshold:      0,
		Operator:       "complex",
		Timestamp:      time.Now().UnixMilli(),
		EventRuleID:    eventRuleID,
		AutomationID:   automationID,
		AutomationName: automationName,
		GeofenceID:     nil,
		GeofenceName:   nil,
		StateID:        stateID,
	}

	e.logger.Debug("Rule matched and event created",
		zap.String("device_id", deviceID),
		zap.String("rule_key", ruleKey),
		zap.Any("matchedEvent", matchedEvent),
	)

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
